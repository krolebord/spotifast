//! Local playback through Spotify Soloist, Spotify's own headless Spotify
//! Connect receiver, as an alternative to librespot's player.
//!
//! Soloist plays with Spotify's official playback engine, so it is not
//! subject to the audio key refusals librespot meets on some accounts. It
//! runs as a child process with its local WebSocket API bound to loopback.
//! This module starts it, pairs it with the playback account over ZeroConf
//! when it has no stored session, and folds its events into the same
//! [`LocalState`] librespot's player reports, so the interface cannot tell
//! the two apart.
//!
//! Soloist is not redistributable: Spotifast never bundles it. When the
//! user asks, it downloads Spotify's own archive from Spotify's servers into
//! its data folder, and replaces it the same way when a build nears expiry.
//! Its API key is the user's own, read from an owner-only file and handed to
//! the process at startup; it is never logged.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use librespot_core::session::Session;
use serde_json::{Value, json};
use tokio::io::AsyncBufReadExt;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

use crate::api::models::ArtistRef;
use crate::player::{EngineEvent, LoadSpec, LocalState, LocalTrack, Notify, Playback, RepeatMode};

pub mod queue;

/// The executable's file name.
pub const BINARY: &str = if cfg!(windows) {
    "soloist.exe"
} else {
    "soloist"
};

/// Soloist exits with this code once its build is older than 90 days.
const EXIT_EXPIRED: i32 = 10;
/// How long Soloist may take to open its WebSocket port.
const START_TIMEOUT: Duration = Duration::from_secs(20);
/// How long a restored session may take to report itself logged in.
const RESTORE_WAIT: Duration = Duration::from_secs(4);
/// How often an unpaired Soloist is asked whether it has signed in.
const PAIRING_POLL: Duration = Duration::from_secs(2);
/// Where Soloist keeps its Connect device id, in its data directory.
const DEVICE_ID_FILE: &str = ".device_id";
/// Volume drags are forwarded at most this often.
const PREVIEW_INTERVAL: Duration = Duration::from_millis(80);

/// Where Soloist lives and keeps its state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SoloistConfig {
    /// Where downloaded builds are kept (`find_binary`). The build itself
    /// is not part of the configuration: a newer one, downloaded while
    /// Soloist plays, takes over at its next start.
    pub programs: PathBuf,
    /// Owner-only file holding the user's Soloist API key.
    pub key_file: PathBuf,
    /// Soloist's own data directory: device identity and stored session.
    pub data_dir: PathBuf,
    pub cache_dir: Option<PathBuf>,
    pub cache_mb: Option<u64>,
}

/// The Soloist that outlives librespot's reconnects: Spotify drops that
/// session every so often, and Soloist, with a connection of its own, plays
/// on. The backend owns the slot; an engine adopts what it holds when the
/// configuration still matches, and starts a new Soloist when not.
pub struct Running {
    pub config: SoloistConfig,
    pub device_name: String,
    pub soloist: Arc<Soloist>,
}

pub type Slot = Arc<tokio::sync::Mutex<Option<Running>>>;

/// Stops the slot's Soloist: playback is no longer Soloist's to do.
pub fn stop_slot(slot: &Slot) {
    match slot.try_lock() {
        Ok(mut running) => {
            if let Some(running) = running.take() {
                running.soloist.shutdown();
            }
        }
        // An engine is starting one right now; stop it once it is in.
        Err(_) => {
            let slot = Arc::clone(slot);
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    if let Some(running) = slot.lock().await.take() {
                        running.soloist.shutdown();
                    }
                });
            }
        }
    }
}

/// Whether the slot holds a Soloist that is still running.
pub fn slot_alive(slot: &Slot) -> bool {
    slot.try_lock()
        .map(|running| {
            running
                .as_ref()
                .is_some_and(|running| !running.soloist.has_ended())
        })
        .unwrap_or(true)
}

/// Names a Soloist executable to use instead of the downloaded one, for
/// trying a particular build.
pub const BINARY_ENV: &str = "SPOTIFAST_SOLOIST";

/// The Soloist to run: `SPOTIFAST_SOLOIST` when set, else the newest build
/// downloaded into `programs`. Only reads file metadata.
pub fn find_binary(programs: &Path) -> Option<PathBuf> {
    match binary_override() {
        Some(path) => path.is_file().then_some(path),
        None => installed(programs).map(|(_, path)| path),
    }
}

fn binary_override() -> Option<PathBuf> {
    std::env::var_os(BINARY_ENV)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
}

/// Each downloaded build sits in a folder named for its build time, so a
/// newer one can arrive while an older one runs (Windows cannot replace a
/// running executable), and the highest is the one to start.
fn installed(programs: &Path) -> Option<(i64, PathBuf)> {
    std::fs::read_dir(programs)
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let built = entry.file_name().to_str()?.parse::<i64>().ok()?;
            let binary = entry.path().join(BINARY);
            binary.is_file().then_some((built, binary))
        })
        .max_by_key(|(built, _)| *built)
}

/// Where Spotify publishes new Soloist builds, with their terms and licenses.
pub const DOWNLOADS_URL: &str =
    "https://developer.spotify.com/documentation/soloist/reference/downloads-and-updates";
/// Spotify's server for the archives `DOWNLOADS_URL` links to.
const ARCHIVES_URL: &str = "https://soloist-builds.spotifycdn.com/";
/// A download may be redirected only within Spotify's own servers.
const ARCHIVE_HOSTS: [&str; 2] = ["spotifycdn.com", "spotify.com"];
/// Archives are about 10 MB and the program about 25 MB; a larger answer
/// is not a Soloist build and must not fill the disk.
const MAX_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_BINARY_BYTES: u64 = 128 * 1024 * 1024;

/// Spotify's archive for this computer, or `None` where it publishes none
/// (Intel Macs, among others).
pub const ARCHIVE: Option<&str> = if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
    Some("soloist_release_osx_arm64.tar.gz")
} else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
    Some("soloist_release_windows_x86_64.zip")
} else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
    Some("soloist_release_x86_64.tar.gz")
} else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
    Some("soloist_release_arm64.tar.gz")
} else if cfg!(all(target_os = "linux", target_arch = "arm")) {
    Some("soloist_release_arm32.tar.gz")
} else {
    None
};

/// Whether a download may follow a redirect to `host`.
fn archive_host(host: &str) -> bool {
    ARCHIVE_HOSTS
        .iter()
        .any(|allowed| host == *allowed || host.ends_with(&format!(".{allowed}")))
}

/// The client for `download`: the user's proxy, redirects only within
/// Spotify's servers, and time enough for a slow connection.
pub fn download_client(proxy: &crate::settings::ProxyConfig) -> Result<reqwest::blocking::Client> {
    let redirects = reqwest::redirect::Policy::custom(|attempt| {
        if attempt.previous().len() >= 5 {
            attempt.error("too many redirects")
        } else if attempt.url().scheme() == "https"
            && attempt.url().host_str().is_some_and(archive_host)
        {
            attempt.follow()
        } else {
            attempt.error("redirected away from Spotify's servers")
        }
    });
    crate::http::blocking_builder(proxy)
        .map_err(|error| anyhow!(error))?
        .redirect(redirects)
        .timeout(Duration::from_secs(300))
        .build()
        .map_err(|error| anyhow!(error.without_url().to_string()))
}

/// What `download` brought in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Downloaded {
    pub build: Build,
    /// `false` when Spotify's newest build is the one already here.
    pub new: bool,
}

/// Blocking: downloads Spotify's newest Soloist for this computer into
/// `programs`, checks that it runs and has not expired, and removes the
/// builds it replaces. A failure leaves the installed build as it was.
pub fn download(client: &reqwest::blocking::Client, programs: &Path) -> Result<Downloaded> {
    let archive = ARCHIVE.context("Spotify publishes no Soloist build for this computer")?;
    std::fs::create_dir_all(programs).context("unable to create Soloist's program folder")?;
    // Not a build time, so `installed` never picks it.
    let staging = programs.join(format!(".download-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&staging);
    std::fs::create_dir_all(&staging).context("unable to create a download folder")?;
    let result = download_into(client, archive, &staging, programs);
    let _ = std::fs::remove_dir_all(&staging);
    let downloaded = result?;
    // Older builds; one still running on Windows stays until next time.
    if let Ok(entries) = std::fs::read_dir(programs) {
        for entry in entries.filter_map(Result::ok) {
            let older = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<i64>().ok())
                .is_some_and(|built| built < downloaded.build.built.as_second());
            if older {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }
    Ok(downloaded)
}

fn download_into(
    client: &reqwest::blocking::Client,
    archive: &str,
    staging: &Path,
    programs: &Path,
) -> Result<Downloaded> {
    use std::io::Read;
    let response = client
        .get(format!("{ARCHIVES_URL}{archive}"))
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|error| anyhow!("unable to download Soloist: {}", error.without_url()))?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_ARCHIVE_BYTES)
    {
        bail!("Spotify's answer is too large to be a Soloist build");
    }
    let archive_path = staging.join(archive);
    let mut file =
        std::fs::File::create(&archive_path).context("unable to save the Soloist download")?;
    let copied = std::io::copy(&mut response.take(MAX_ARCHIVE_BYTES + 1), &mut file)
        .context("the Soloist download was interrupted")?;
    if copied > MAX_ARCHIVE_BYTES {
        bail!("Spotify's answer is too large to be a Soloist build");
    }
    file.sync_all()
        .context("unable to save the Soloist download")?;
    drop(file);
    install_archive(&archive_path, staging, programs)
}

/// Unpacks the program from Spotify's archive and installs it under its
/// build time once it runs.
fn install_archive(archive_path: &Path, staging: &Path, programs: &Path) -> Result<Downloaded> {
    // The system's tar reads both archive kinds (Windows 10 ships bsdtar),
    // and naming the one member keeps anything else in it out.
    let unpacked = staging.join("build");
    std::fs::create_dir_all(&unpacked).context("unable to create a download folder")?;
    let status = std::process::Command::new("tar")
        .arg("-xf")
        .arg(archive_path)
        .arg("-C")
        .arg(&unpacked)
        .arg(BINARY)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .context("unable to run tar to unpack Soloist")?;
    let binary = unpacked.join(BINARY);
    let size = std::fs::metadata(&binary).map(|meta| meta.len()).ok();
    if !status.success() || size.is_none_or(|size| size == 0 || size > MAX_BINARY_BYTES) {
        bail!("Spotify's archive does not hold a Soloist program");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755))
            .context("unable to make Soloist runnable")?;
    }
    let build = read_build(&binary).context("the downloaded program is not a Soloist build")?;
    if days_left(&build, jiff::Timestamp::now()) < 0 {
        bail!("Spotify's newest Soloist build has already expired");
    }
    let target = programs.join(build.built.as_second().to_string());
    if target.join(BINARY).is_file() {
        return Ok(Downloaded { build, new: false });
    }
    let _ = std::fs::remove_dir_all(&target);
    std::fs::rename(&unpacked, &target).context("unable to install the Soloist download")?;
    Ok(Downloaded { build, new: true })
}
/// Soloist builds stop working this many days after they were built.
pub const LIFETIME_DAYS: i64 = 90;

/// An installed Soloist build, as `soloist --version` describes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Build {
    pub version: String,
    pub built: jiff::Timestamp,
}

impl Build {
    /// When this build stops working.
    pub fn expires(&self) -> jiff::Timestamp {
        self.built
            .checked_add(jiff::SignedDuration::from_hours(LIFETIME_DAYS * 24))
            .unwrap_or(self.built)
    }
}

/// "soloist 1.3.10.12 build 1792756912 (20261023) (g19412df6ea) (…)": the
/// version, and the build's Unix time.
pub fn parse_version(text: &str) -> Option<Build> {
    let mut words = text.split_whitespace();
    words.find(|word| *word == "soloist")?;
    let version = words.next()?.to_string();
    if words.next()? != "build" {
        return None;
    }
    let built = jiff::Timestamp::from_second(words.next()?.parse().ok()?).ok()?;
    Some(Build { version, built })
}

/// Blocking: runs `binary --version`.
pub fn read_build(binary: &Path) -> Option<Build> {
    let output = std::process::Command::new(binary)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .output()
        .ok()?;
    parse_version(&String::from_utf8_lossy(&output.stdout))
}

/// Blocking: the accounts Soloist has signed in to with `data_dir`. It
/// keeps a `settings/Users/<username>-user` folder for each.
pub fn accounts(data_dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(data_dir.join("settings").join("Users")) else {
        return Vec::new();
    };
    let mut accounts: Vec<String> = entries
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()?
                .strip_suffix("-user")
                .map(str::to_string)
        })
        .collect();
    accounts.sort();
    accounts
}

/// What the Settings page shows about the Soloist on this computer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Install {
    pub binary: Option<PathBuf>,
    /// `binary` is the one `SPOTIFAST_SOLOIST` names, not a download.
    pub overridden: bool,
    pub build: Option<Build>,
    pub accounts: Vec<String>,
    /// The API key file exists.
    pub key_saved: bool,
}

/// Blocking: finds Soloist and reads its build, accounts and key file.
pub fn inspect(programs: &Path, data_dir: &Path, key_file: &Path) -> Install {
    let binary = find_binary(programs);
    let build = binary.as_deref().and_then(read_build);
    Install {
        binary,
        overridden: binary_override().is_some(),
        build,
        accounts: accounts(data_dir),
        key_saved: key_file.is_file(),
    }
}

/// Days left before the build expires is "soon" from this many.
pub const EXPIRY_WARNING_DAYS: i64 = 14;

/// Whom Soloist's stored session belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Paired {
    /// The account Spotifast is signed in to.
    ThisAccount,
    /// Only other accounts.
    OtherAccount,
    /// No account yet.
    No,
}

/// Matches Soloist's accounts against Spotifast's, while that is known.
pub fn paired(install: &Install, account: Option<&str>) -> Paired {
    if install.accounts.is_empty() {
        Paired::No
    } else if account.is_none_or(|account| install.accounts.iter().any(|held| held == account)) {
        Paired::ThisAccount
    } else {
        Paired::OtherAccount
    }
}

/// The next thing Soloist needs, for the section's status line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Readiness {
    Checking,
    NotFound,
    Expired,
    NeedsKey,
    NeedsPairing,
    OtherAccount,
    ExpiringSoon { days: i64 },
    Ready,
}

/// What Settings knows about Soloist, gathered for `readiness`.
pub struct Facts<'a> {
    pub install: Option<&'a Install>,
    pub key_saved: bool,
    pub account: Option<&'a str>,
    /// The running Soloist reported it has no session.
    pub signed_out: bool,
    pub now: jiff::Timestamp,
}

/// Days from `now` until `build` expires; negative once it has.
pub fn days_left(build: &Build, now: jiff::Timestamp) -> i64 {
    (build.expires().as_second() - now.as_second()).div_euclid(86_400)
}

pub fn readiness(facts: &Facts<'_>) -> Readiness {
    let Some(install) = facts.install else {
        return Readiness::Checking;
    };
    if install.binary.is_none() {
        return Readiness::NotFound;
    }
    let days = install
        .build
        .as_ref()
        .map(|build| days_left(build, facts.now));
    if days.is_some_and(|days| days < 0) {
        return Readiness::Expired;
    }
    if !facts.key_saved {
        return Readiness::NeedsKey;
    }
    match paired(install, facts.account) {
        Paired::No => return Readiness::NeedsPairing,
        Paired::OtherAccount => return Readiness::OtherAccount,
        Paired::ThisAccount if facts.signed_out => return Readiness::NeedsPairing,
        Paired::ThisAccount => {}
    }
    match days {
        Some(days) if days < EXPIRY_WARNING_DAYS => Readiness::ExpiringSoon { days },
        _ => Readiness::Ready,
    }
}

/// Whether an engine error is Soloist waiting to be paired.
pub fn is_pairing_needed(error: &str) -> bool {
    error.starts_with(PAIRING_NEEDED)
}

/// A running Soloist and the connection to its local API.
pub struct Soloist {
    commands: mpsc::UnboundedSender<Value>,
    /// What Soloist is playing, as the interface reads it. It outlives the
    /// engines that come and go around it, so it is Soloist's own.
    state: Arc<Mutex<LocalState>>,
    /// Where state changes go: the current engine's channel to the app.
    notify: Arc<Mutex<Notify>>,
    /// The process, for a stop that cannot wait on the runtime.
    pid: Option<u32>,
    stopping: std::sync::atomic::AtomicBool,
    device_id: String,
    stop: watch::Sender<bool>,
    ended: watch::Receiver<bool>,
    last_preview: Mutex<Option<Instant>>,
}

impl Soloist {
    /// Starts Soloist, connects to its API and waits for its stored
    /// session. Its events update `state` from then on.
    pub async fn start(
        config: &SoloistConfig,
        device_name: &str,
        initial_volume: u16,
        state: Arc<Mutex<LocalState>>,
        notify: Notify,
    ) -> Result<Self> {
        let binary = find_binary(&config.programs).ok_or_else(|| {
            anyhow!(
                "Spotify Soloist is not downloaded yet. Download it in Settings, under Spotify Soloist, or turn Use Soloist off."
            )
        })?;
        let key = read_key(&config.key_file).await?;
        tokio::fs::create_dir_all(&config.data_dir)
            .await
            .context("unable to create Soloist's data folder")?;
        if let Some(cache) = &config.cache_dir {
            tokio::fs::create_dir_all(cache)
                .await
                .context("unable to create Soloist's cache folder")?;
        }
        let (addr_file, port_file) = runtime_files(&config.data_dir);
        // Left behind by a Soloist that did not shut down cleanly.
        let _ = tokio::fs::remove_file(&addr_file).await;
        let _ = tokio::fs::remove_file(&port_file).await;
        // A Soloist a previous run left behind, after a crash or a kill,
        // holds the data directory's lock and its Connect device.
        stop_stale(&config.data_dir).await;

        let mut command = tokio::process::Command::new(&binary);
        command
            .arg("--device-name")
            .arg(device_name)
            .arg("--api-key")
            .arg(&key)
            .arg("--data-dir")
            .arg(&config.data_dir)
            .arg("--ws")
            .arg("127.0.0.1:0")
            .arg("--initial-volume")
            .arg(to_percent(initial_volume).to_string())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // On Unix it is stopped with a signal it can shut down cleanly
            // on (`Soloist::shutdown`), and a watchdog stops it if this
            // process dies first; a kill on drop would cut that short.
            .kill_on_drop(cfg!(not(unix)));
        if verbose() {
            command.arg("--verbose");
        }
        if let Some(cache) = &config.cache_dir {
            command.arg("--cache-dir").arg(cache);
            if let Some(mb) = config.cache_mb {
                command.arg("--cache-size").arg(mb.max(100).to_string());
            }
        }
        log::info!("starting Soloist from {}", binary.display());
        let mut child = command.spawn().context("unable to start Spotify Soloist")?;
        let pid = child.id();
        #[cfg(unix)]
        if let Some(pid) = pid {
            watch_parent(pid);
        }
        if let Some(out) = child.stdout.take() {
            tokio::spawn(forward_log(out, key.clone()));
        }
        if let Some(err) = child.stderr.take() {
            tokio::spawn(forward_log(err, key.clone()));
        }
        drop(key);

        let port = match wait_for_port(&mut child, &port_file).await {
            Ok(port) => port,
            Err(error) => {
                let _ = child.start_kill();
                return Err(error);
            }
        };
        let url = format!("ws://127.0.0.1:{port}");
        let (socket, _) = match tokio_tungstenite::connect_async(url.as_str()).await {
            Ok(connected) => connected,
            Err(error) => {
                let _ = child.start_kill();
                return Err(anyhow!("unable to reach Soloist's local API: {error}"));
            }
        };
        let (mut sink, mut stream) = socket.split();

        let (commands, mut outgoing) = mpsc::unbounded_channel::<Value>();
        let (stop, mut stop_rx) = watch::channel(false);
        let (ended_tx, ended) = watch::channel(false);
        let (auth_tx, mut auth) = watch::channel(None::<bool>);

        // Writer: commands from the engine, in order.
        tokio::spawn(async move {
            while let Some(message) = outgoing.recv().await {
                if sink
                    .send(Message::Text(message.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            let _ = sink.close().await;
        });

        // Reader: events into the shared state.
        let shared_notify = Arc::new(Mutex::new(Arc::clone(&notify)));
        let reader_state = Arc::clone(&state);
        let reader_notify = Arc::clone(&shared_notify);
        let reader_ended = ended_tx.clone();
        let reader_commands = commands.clone();
        tokio::spawn(async move {
            while let Some(message) = stream.next().await {
                let text = match message {
                    Ok(Message::Text(text)) => text,
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => continue,
                };
                let Ok(event) = serde_json::from_str::<Value>(text.as_str()) else {
                    continue;
                };
                if event["type"] == "auth_state" {
                    let logged_in = event["logged_in"].as_bool().unwrap_or(false);
                    let was = auth_tx.send_replace(Some(logged_in));
                    if logged_in && was == Some(false) {
                        // Paired after the engine started: catch up.
                        let _ = reader_commands
                            .send(json!({ "type": "command", "command": "get_state" }));
                        let notify = reader_notify
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .clone();
                        notify(EngineEvent::SoloistSignedIn);
                    }
                }
                if event["type"] == "error" {
                    log::warn!(
                        "Soloist: {}",
                        event["message"].as_str().unwrap_or("unknown error")
                    );
                }
                let snapshot = {
                    let mut current = reader_state.lock().unwrap_or_else(|p| p.into_inner());
                    apply_event(&mut current, &event, now_ms()).then(|| current.clone())
                };
                if let Some(snapshot) = snapshot {
                    let notify = reader_notify
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .clone();
                    notify(EngineEvent::State(snapshot));
                }
            }
            let _ = reader_ended.send(true);
        });

        // Supervisor: the process, until it exits or the engine stops it.
        let supervisor_state = Arc::clone(&state);
        tokio::spawn(async move {
            tokio::select! {
                status = child.wait() => {
                    let message = match status.ok().and_then(|status| status.code()) {
                        Some(EXIT_EXPIRED) => Some(
                            "This Spotify Soloist build has expired. Update it in Settings, under Spotify Soloist."
                                .to_string(),
                        ),
                        Some(0) => None,
                        code => Some(format!(
                            "Spotify Soloist stopped unexpectedly (exit code {}). See the log for details.",
                            code.map_or_else(|| "none".into(), |code| code.to_string())
                        )),
                    };
                    if let Some(message) = message {
                        log::error!("{message}");
                        supervisor_state
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .error = Some(message);
                    }
                }
                () = async {
                    let _ = stop_rx.wait_for(|stop| *stop).await;
                } => {
                    terminate(&mut child).await;
                }
            }
            let _ = ended_tx.send(true);
        });

        let mut soloist = Self {
            commands,
            state: Arc::clone(&state),
            notify: shared_notify,
            pid,
            stopping: std::sync::atomic::AtomicBool::new(false),
            device_id: String::new(),
            stop,
            ended,
            last_preview: Mutex::new(None),
        };
        match soloist.sign_in(config, &mut auth).await {
            Ok((device_id, paired)) => {
                soloist.device_id = device_id;
                if !paired {
                    // Soloist keeps waiting for a pick in Spotify's app; its
                    // sign-in clears this. It does not always announce one,
                    // so it is asked until it has signed in or stopped.
                    let snapshot = {
                        let mut current = state.lock().unwrap_or_else(|p| p.into_inner());
                        current.error = Some(pairing_needed(device_name));
                        current.clone()
                    };
                    notify(EngineEvent::State(snapshot));
                    let commands = soloist.commands.clone();
                    let mut auth = auth.clone();
                    tokio::spawn(async move {
                        while auth.borrow_and_update().is_none_or(|logged_in| !logged_in) {
                            if commands
                                .send(json!({ "type": "command", "command": "get_auth_state" }))
                                .is_err()
                            {
                                return;
                            }
                            let _ = tokio::time::timeout(PAIRING_POLL, auth.changed()).await;
                        }
                    });
                }
                Ok(soloist)
            }
            Err(error) => {
                soloist.shutdown();
                Err(error)
            }
        }
    }

    /// Waits for a stored session and returns Soloist's Connect device id,
    /// and whether it signed in. Spotifast does not hand Soloist the account
    /// (over ZeroConf that did not work): an unpaired Soloist waits for a
    /// pick in Spotify's app, which macOS lets the phone see only while
    /// Spotifast runs as an app allowed on the local network.
    async fn sign_in(
        &self,
        config: &SoloistConfig,
        auth: &mut watch::Receiver<Option<bool>>,
    ) -> Result<(String, bool)> {
        let restored = tokio::time::timeout(
            RESTORE_WAIT,
            auth.wait_for(|logged_in| *logged_in == Some(true)),
        )
        .await
        .is_ok_and(|seen| seen.is_ok());
        // Soloist writes its Connect identity next to its stored session.
        let device_id = read_device_id(&config.data_dir).await;
        if !restored {
            log::warn!("Soloist has no stored session; waiting for it to be paired");
        }
        if device_id.is_empty() {
            // Playback still works through Soloist's own API; only loads
            // that start partway into a list need the id, and they fall
            // back to the list's start.
            log::warn!("Soloist's Connect device id is unknown");
        }
        Ok((device_id, restored))
    }

    /// Soloist's Spotify Connect device id.
    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// Resolves once Soloist exited or its API connection closed.
    pub async fn ended(&self) {
        let mut ended = self.ended.clone();
        let _ = ended.wait_for(|ended| *ended).await;
    }

    /// Whether Soloist has exited or its API connection closed.
    pub fn has_ended(&self) -> bool {
        *self.ended.borrow()
    }

    /// What Soloist is playing.
    pub fn state(&self) -> Arc<Mutex<LocalState>> {
        Arc::clone(&self.state)
    }

    /// Sends state changes to `notify` from now on: the engine that
    /// adopted this Soloist after the previous one's session dropped.
    pub fn set_notify(&self, notify: Notify) {
        *self.notify.lock().unwrap_or_else(|p| p.into_inner()) = notify;
    }

    /// The current snapshot, sent to the current engine's channel.
    pub fn publish(&self) {
        let snapshot = self.state.lock().unwrap_or_else(|p| p.into_inner()).clone();
        let notify = self
            .notify
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        notify(EngineEvent::State(snapshot));
    }

    /// Stops the Soloist process.
    /// Signals at once rather than through a task: the app may be
    /// quitting, and its runtime would drop that task before it ran.
    pub fn shutdown(&self) {
        if !self
            .stopping
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            #[cfg(unix)]
            if let Some(pid) = self.pid {
                signal(pid, libc::SIGTERM);
            }
        }
        let _ = self.stop.send(true);
    }

    /// Sends one command, with its fields, to Soloist's API.
    pub fn send(&self, command: &str, fields: Value) -> Result<()> {
        let mut message = json!({ "type": "command", "command": command });
        if let (Some(message), Value::Object(fields)) = (message.as_object_mut(), fields) {
            message.extend(fields);
        }
        self.commands
            .send(message)
            .map_err(|_| anyhow!("Soloist is not running"))
    }

    /// Sets the volume, 0..=65535 as librespot counts it. Drags are
    /// forwarded at most every [`PREVIEW_INTERVAL`].
    pub fn set_volume(&self, volume: u16, preview: bool) -> Result<()> {
        if preview {
            let mut last = self.last_preview.lock().unwrap_or_else(|p| p.into_inner());
            if last.is_some_and(|at| at.elapsed() < PREVIEW_INTERVAL) {
                return Ok(());
            }
            *last = Some(Instant::now());
        }
        self.send("set_volume", json!({ "volume": to_percent(volume) }))
    }

    pub fn set_repeat(&self, mode: RepeatMode) -> Result<()> {
        match mode {
            RepeatMode::Off => {
                self.send("set_repeat_track", json!({ "enabled": false }))?;
                self.send("set_repeat_context", json!({ "enabled": false }))
            }
            RepeatMode::Context => {
                self.send("set_repeat_track", json!({ "enabled": false }))?;
                self.send("set_repeat_context", json!({ "enabled": true }))
            }
            RepeatMode::Track => {
                self.send("set_repeat_context", json!({ "enabled": false }))?;
                self.send("set_repeat_track", json!({ "enabled": true }))
            }
        }
    }

    /// Starts `spec` on Soloist. Soloist's own `play` takes one URI and no
    /// starting point, so the load goes over the session's Connect state
    /// service, as Spotify's apps steer any device. If Spotify refuses that,
    /// Soloist plays the chosen song or context from its start.
    pub async fn load(self: Arc<Self>, session: Session, spec: LoadSpec) {
        let body = load_body(&spec);
        let mut result = if self.device_id.is_empty() {
            Err(anyhow!("Soloist's Connect device id is unknown"))
        } else {
            connect_command(&session, session.device_id(), &self.device_id, &body).await
        };
        if result.is_err() && !self.device_id.is_empty() {
            // `to` as the source too means "from whichever device is active".
            result = connect_command(&session, &self.device_id, &self.device_id, &body).await;
        }
        let Err(error) = result else {
            return;
        };
        log::warn!("Connect load for Soloist failed, playing over its API instead: {error:#}");
        if let Some(shuffle) = spec.shuffle {
            let _ = self.send("set_shuffle", json!({ "enabled": shuffle }));
        }
        if let Some(repeat) = spec.repeat {
            let _ = self.set_repeat(repeat);
        }
        let uri = spec
            .offset_uri
            .clone()
            .or_else(|| spec.context_uri.clone())
            .or_else(|| spec.uris.first().cloned());
        let Some(uri) = uri else {
            return;
        };
        let _ = self.send("play", json!({ "uri": uri }));
        if spec.position_ms > 0 {
            let _ = self.send("seek", json!({ "position_ms": spec.position_ms }));
        }
        if !spec.play {
            let _ = self.send("pause", json!({}));
        }
    }
}

impl Drop for Soloist {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// The Connect state service's `play` command for `spec`.
fn load_body(spec: &LoadSpec) -> Value {
    let request = crate::api::client::PlayRequest {
        context_uri: spec.context_uri.clone(),
        uris: spec.uris.clone(),
        offset_uri: spec.offset_uri.clone(),
        offset_position: spec.offset_index,
        position_ms: spec.position_ms,
    };
    let mut command = crate::sonos::play_command(&request);
    let mut overrides = serde_json::Map::new();
    if let Some(shuffle) = spec.shuffle {
        overrides.insert("shuffling_context".into(), json!(shuffle));
    }
    if let Some(repeat) = spec.repeat {
        overrides.insert("repeating_context".into(), json!(repeat != RepeatMode::Off));
        overrides.insert("repeating_track".into(), json!(repeat == RepeatMode::Track));
    }
    command["options"]["player_options_override"] = Value::Object(overrides);
    if !spec.play {
        command["options"]["initially_paused"] = json!(true);
    }
    json!({ "command": command })
}

/// Sends a player command from `from` to `to` over the session.
async fn connect_command(session: &Session, from: &str, to: &str, body: &Value) -> Result<()> {
    let path = format!("/connect-state/v1/player/command/from/{from}/to/{to}");
    session
        .spclient()
        .request(
            &reqwest::Method::POST,
            &path,
            None,
            Some(body.to_string().as_bytes()),
        )
        .await
        .map(drop)
        .map_err(|error| anyhow!("{error}"))
}

/// The start of the message shown while Soloist waits to be paired.
const PAIRING_NEEDED: &str = "Soloist is waiting to be paired.";

fn pairing_needed(device_name: &str) -> String {
    format!(
        "{PAIRING_NEEDED} In the Spotify app on your phone, on the same Wi-Fi, choose \"{device_name}\" once."
    )
}

/// Reads Soloist's Connect device id, waiting briefly for a first start
/// to write it. Empty when it never appears.
async fn read_device_id(data_dir: &Path) -> String {
    let path = data_dir.join(DEVICE_ID_FILE);
    for _ in 0..20 {
        if let Ok(id) = tokio::fs::read_to_string(&path).await {
            let id = id.trim();
            if !id.is_empty() {
                return id.to_string();
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    String::new()
}

async fn read_key(path: &Path) -> Result<String> {
    let key = tokio::fs::read_to_string(path).await.map_err(|_| {
        anyhow!("Add your Spotify Soloist API key in Settings to play through Soloist.")
    })?;
    let key = key.trim().to_string();
    if key.is_empty() {
        bail!("Add your Spotify Soloist API key in Settings to play through Soloist.");
    }
    Ok(key)
}

fn runtime_files(data_dir: &Path) -> (PathBuf, PathBuf) {
    (data_dir.join("ws.addr"), data_dir.join("ws.port"))
}

/// Waits for Soloist to write the port its API listens on.
async fn wait_for_port(child: &mut tokio::process::Child, port_file: &Path) -> Result<u16> {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(match status.code() {
                Some(EXIT_EXPIRED) => anyhow!(
                    "This Spotify Soloist build has expired. Update it in Settings, under Spotify Soloist."
                ),
                code => anyhow!(
                    "Spotify Soloist exited while starting (exit code {}). Check its API key; the log has details.",
                    code.map_or_else(|| "none".into(), |code| code.to_string())
                ),
            });
        }
        if let Ok(text) = tokio::fs::read_to_string(port_file).await
            && let Ok(port) = text.trim().parse::<u16>()
            && port != 0
        {
            return Ok(port);
        }
        if Instant::now() >= deadline {
            bail!("Spotify Soloist did not open its local API in time");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Asks Soloist to shut down cleanly, so it leaves Spotify Connect and
/// removes its runtime files, and kills it if it does not.
async fn terminate(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        signal(pid, libc::SIGTERM);
        if tokio::time::timeout(Duration::from_secs(3), child.wait())
            .await
            .is_ok()
        {
            return;
        }
    }
    let _ = child.kill().await;
}

#[cfg(unix)]
fn signal(pid: u32, signal: libc::c_int) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    // SAFETY: kill(2) takes plain integers and touches no memory of ours.
    unsafe { libc::kill(pid, signal) == 0 }
}

/// Stops Soloist if this process ends without doing so itself: crashed,
/// killed, or quit by a signal that skips shutdown. A shell polls both
/// processes every two seconds and leaves with Soloist.
#[cfg(unix)]
fn watch_parent(soloist: u32) {
    let parent = std::process::id();
    let script = format!(
        "while kill -0 {parent} 2>/dev/null && kill -0 {soloist} 2>/dev/null; do sleep 2; done; \
         kill -0 {parent} 2>/dev/null || kill -TERM {soloist} 2>/dev/null"
    );
    match std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(script)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        // Reaped on a thread of its own, so it leaves no zombie behind.
        Ok(mut watchdog) => {
            let _ = std::thread::Builder::new()
                .name("soloist-watchdog".into())
                .spawn(move || watchdog.wait());
        }
        Err(error) => log::warn!("no watchdog for Soloist: {error}"),
    }
}

/// Stops a Soloist still running on `data_dir` from an earlier run. It is
/// recognised by the pid file Soloist keeps there and its command line,
/// so an unrelated process that reused the pid is left alone.
async fn stop_stale(data_dir: &Path) {
    #[cfg(unix)]
    {
        let Ok(text) = tokio::fs::read_to_string(data_dir.join("soloist.pid")).await else {
            return;
        };
        let Ok(pid) = text.trim().parse::<u32>() else {
            return;
        };
        if pid == 0 || !signal(pid, 0) {
            return;
        }
        let Ok(output) = tokio::process::Command::new("ps")
            .args(["-o", "command=", "-p", &pid.to_string()])
            .output()
            .await
        else {
            return;
        };
        let command = String::from_utf8_lossy(&output.stdout);
        if !command.contains(BINARY) || !command.contains(&*data_dir.to_string_lossy()) {
            return;
        }
        log::warn!("stopping a Soloist left running by an earlier run (pid {pid})");
        signal(pid, libc::SIGTERM);
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if !signal(pid, 0) {
                return;
            }
        }
        signal(pid, libc::SIGKILL);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    #[cfg(not(unix))]
    let _ = data_dir;
}

/// SPOTIFAST_SOLOIST_VERBOSE=1 runs Soloist verbosely and logs all of it,
/// for diagnosing.
fn verbose() -> bool {
    std::env::var_os("SPOTIFAST_SOLOIST_VERBOSE").is_some()
}

/// Soloist's output goes to the log, with the API key masked and detail
/// below warning level left out.
async fn forward_log(output: impl tokio::io::AsyncRead + Unpin, key: String) {
    let verbose = verbose();
    let mut lines = tokio::io::BufReader::new(output).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let line = strip_ansi(&line.replace(&key, "[api key]"));
        if verbose || !is_verbose_detail(&line) {
            log::info!("soloist: {line}");
        }
    }
}

/// The line without terminal colour codes.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // ESC [ parameters final-letter
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// A verbose line, "13:46:04.449 I [...] text", below warning level.
fn is_verbose_detail(line: &str) -> bool {
    let bytes = line.as_bytes();
    bytes.len() > 14
        && bytes[2] == b':'
        && bytes[5] == b':'
        && bytes[8] == b'.'
        && bytes[12] == b' '
        && matches!(bytes[13], b'D' | b'I' | b'V' | b'T')
        && bytes[14] == b' '
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as i64)
}

/// librespot's 0..=65535 as Soloist's percentage.
pub fn to_percent(volume: u16) -> u8 {
    ((u32::from(volume) * 100 + 32_767) / 65_535) as u8
}

/// Soloist's percentage as librespot's 0..=65535.
pub fn from_percent(percent: u64) -> u16 {
    (percent.min(100) * 65_535 / 100) as u16
}

fn set<T: PartialEq>(target: &mut T, value: T) -> bool {
    if *target == value {
        false
    } else {
        *target = value;
        true
    }
}

/// Folds one Soloist event into the state. True when something changed.
/// `now_ms` is the wall clock, to age Soloist's position anchors.
pub(crate) fn apply_event(state: &mut LocalState, event: &Value, now_ms: i64) -> bool {
    match event["type"].as_str().unwrap_or_default() {
        "auth_state" => {
            let mut changed = false;
            if event["logged_in"] == true
                && state
                    .error
                    .as_deref()
                    .is_some_and(|error| error.starts_with(PAIRING_NEEDED))
            {
                state.error = None;
                changed = true;
            }
            if event["is_active"] == false {
                changed |= deactivate(state);
            }
            changed
        }
        "device_changed" => {
            if event["is_active"] == false {
                deactivate(state)
            } else {
                false
            }
        }
        "playback_state" => {
            let mut changed = false;
            if let Some(item) = event.get("item") {
                changed |= apply_item(state, item);
            }
            if let Some(volume) = event["volume"].as_u64() {
                changed |= set(&mut state.volume, from_percent(volume));
            }
            if let Some(options) = event.get("options") {
                changed |= apply_options(state, options);
            }
            if let Some(status) = event["status"].as_str() {
                changed |= apply_status(state, status);
            }
            if let Some(position) = event.get("position") {
                changed |= apply_position(state, position, now_ms);
            }
            if event["is_active"] == false {
                changed |= deactivate(state);
            }
            changed
        }
        "track_changed" => apply_item(state, &event["item"]),
        "playback_changed" => apply_status(state, event["status"].as_str().unwrap_or_default()),
        "volume_changed" => event["volume"]
            .as_u64()
            .is_some_and(|volume| set(&mut state.volume, from_percent(volume))),
        "options_changed" => apply_options(state, &event["options"]),
        "position_sync" => apply_position(state, &event["position"], now_ms),
        "error" => set(
            &mut state.error,
            Some(
                event["message"]
                    .as_str()
                    .unwrap_or("Spotify Soloist reported an error")
                    .to_string(),
            ),
        ),
        _ => false,
    }
}

/// Another device took over: this computer plays nothing now.
fn deactivate(state: &mut LocalState) -> bool {
    let mut changed = set(&mut state.playback, Playback::Stopped);
    changed |= set(&mut state.loading, false);
    if changed {
        state.position_ms = state.position_now();
        state.position_at = None;
    }
    changed
}

fn apply_status(state: &mut LocalState, status: &str) -> bool {
    match status {
        "playing" => {
            let mut changed = set(&mut state.playback, Playback::Playing);
            changed |= set(&mut state.loading, false);
            if state.position_at.is_none() {
                state.position_at = Some(Instant::now());
                changed = true;
            }
            changed
        }
        "paused" => {
            let position = state.position_now();
            let mut changed = set(&mut state.playback, Playback::Paused);
            changed |= set(&mut state.loading, false);
            changed |= set(&mut state.position_ms, position);
            changed |= set(&mut state.position_at, None);
            changed
        }
        "buffering" => {
            let mut changed = if state.playback == Playback::Stopped {
                set(&mut state.playback, Playback::Loading)
            } else {
                false
            };
            changed |= set(&mut state.loading, true);
            changed
        }
        "idle" => {
            let mut changed = set(&mut state.playback, Playback::Stopped);
            changed |= set(&mut state.loading, false);
            changed |= set(&mut state.position_at, None);
            changed
        }
        _ => false,
    }
}

fn apply_options(state: &mut LocalState, options: &Value) -> bool {
    let mut changed = false;
    if let Some(shuffle) = options["shuffle"].as_bool() {
        changed |= set(&mut state.shuffle, shuffle);
    }
    if let Some(repeat) = options["repeat"].as_str() {
        changed |= set(&mut state.repeat, RepeatMode::from_api(repeat));
    }
    changed
}

/// A position anchor: where playback was at a wall-clock moment, and how
/// fast it moves. A jump the running position does not explain is a seek.
fn apply_position(state: &mut LocalState, position: &Value, now_ms: i64) -> bool {
    let Some(at) = position["position_ms"].as_u64() else {
        return false;
    };
    let speed = position["speed"].as_f64().unwrap_or(0.0);
    let age = position["timestamp_ms"]
        .as_i64()
        .map_or(0, |stamp| (now_ms - stamp).clamp(0, 5_000));
    let now = (at as f64 + age as f64 * speed).max(0.0) as u32;
    let expected = state.position_now();
    if state.track.is_some() && now.abs_diff(expected) > 2_000 {
        state.seek_sequence = state.seek_sequence.wrapping_add(1);
    }
    state.position_ms = now;
    state.position_at = (speed > 0.0).then(Instant::now);
    if speed > 0.0 && state.playback != Playback::Playing {
        state.playback = Playback::Playing;
        state.loading = false;
    }
    true
}

fn apply_item(state: &mut LocalState, item: &Value) -> bool {
    let Some(track) = local_track(item) else {
        return false;
    };
    if state.track.as_ref() == Some(&track) {
        return false;
    }
    let new_song = state
        .track
        .as_ref()
        .is_none_or(|previous| previous.uri != track.uri);
    state.track = Some(track);
    if new_song {
        state.error = None;
        state.track_sequence = state.track_sequence.wrapping_add(1);
        state.position_ms = 0;
        if state.playback == Playback::Playing {
            state.position_at = Some(Instant::now());
        }
    }
    true
}

/// Soloist's entity envelope as the player bar's track.
fn local_track(item: &Value) -> Option<LocalTrack> {
    let uri = item["uri"].as_str().filter(|uri| !uri.is_empty())?;
    let decorations = &item["decorations"];
    let name = |entity: &Value| {
        entity["decorations"]["identity"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    };
    let is_episode = item["entity_type"] == "episode";
    let parent = &decorations["parent"]["entity"];
    let artists: Vec<ArtistRef> = decorations["creators"]
        .as_array()
        .map(|creators| {
            creators
                .iter()
                .map(|creator| {
                    let entity = &creator["entity"];
                    let uri = entity["uri"]
                        .as_str()
                        .filter(|uri| !uri.is_empty())
                        .map(str::to_string);
                    ArtistRef {
                        id: uri
                            .as_deref()
                            .and_then(crate::util::uri_id)
                            .map(str::to_string),
                        name: name(entity),
                        uri,
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let artists = if artists.is_empty() && is_episode {
        vec![ArtistRef {
            name: name(parent),
            ..ArtistRef::default()
        }]
    } else {
        artists
    };
    let covers = decorations["visual_identity"]["cover"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let cover = |sizes: &[&str]| {
        sizes.iter().find_map(|size| {
            covers
                .iter()
                .find(|cover| cover["size"] == *size)
                .and_then(|cover| cover["url"].as_str())
                .map(str::to_string)
        })
    };
    Some(LocalTrack {
        uri: uri.to_string(),
        title: decorations["identity"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        artists,
        album: name(parent),
        art_url: cover(&["xlarge", "large", "default", "small"]),
        art_small_url: cover(&["small", "default", "large", "xlarge"]),
        duration_ms: decorations["playback"]["duration_ms"]
            .as_u64()
            .map_or(0, |ms| ms.min(u64::from(u32::MAX)) as u32),
        is_episode,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_names_the_next_missing_step() {
        let built = jiff::Timestamp::from_second(1_792_756_912).unwrap();
        let day = jiff::SignedDuration::from_hours(24);
        let install = Install {
            binary: Some("/bin/soloist".into()),
            overridden: false,
            build: Some(Build {
                version: "1".into(),
                built,
            }),
            accounts: vec!["alice".into()],
            key_saved: true,
        };
        fn facts(install: Option<&Install>, now: jiff::Timestamp) -> Facts<'_> {
            Facts {
                install,
                key_saved: true,
                account: Some("alice"),
                signed_out: false,
                now,
            }
        }
        let now = built.checked_add(day * 10).unwrap();
        assert_eq!(readiness(&facts(None, now)), Readiness::Checking);
        assert_eq!(readiness(&facts(Some(&install), now)), Readiness::Ready);
        let missing = Install {
            binary: None,
            ..install.clone()
        };
        assert_eq!(readiness(&facts(Some(&missing), now)), Readiness::NotFound);
        let soon = built.checked_add(day * 80).unwrap();
        assert_eq!(
            readiness(&facts(Some(&install), soon)),
            Readiness::ExpiringSoon { days: 10 }
        );
        let late = built.checked_add(day * 91).unwrap();
        assert_eq!(readiness(&facts(Some(&install), late)), Readiness::Expired);
        let mut keyless = facts(Some(&install), now);
        keyless.key_saved = false;
        assert_eq!(readiness(&keyless), Readiness::NeedsKey);
        let mut signed_out = facts(Some(&install), now);
        signed_out.signed_out = true;
        assert_eq!(readiness(&signed_out), Readiness::NeedsPairing);
        let mut other = facts(Some(&install), now);
        other.account = Some("bob");
        assert_eq!(readiness(&other), Readiness::OtherAccount);
        let unpaired = Install {
            accounts: Vec::new(),
            ..install.clone()
        };
        assert_eq!(
            readiness(&facts(Some(&unpaired), now)),
            Readiness::NeedsPairing
        );
    }

    #[test]
    fn the_build_and_its_expiry_are_read_from_the_version() {
        let build = parse_version(
            "soloist 1.3.10.12 build 1792756912 (20261023) (g19412df6ea) (macos/aarch64)\n",
        )
        .unwrap();
        assert_eq!(build.version, "1.3.10.12");
        assert_eq!(build.built.as_second(), 1_792_756_912);
        assert_eq!(build.expires().to_string(), "2027-01-21T12:01:52Z");
        assert!(parse_version("something else").is_none());
    }

    #[test]
    fn accounts_are_read_from_soloists_user_folders() {
        let dir =
            std::env::temp_dir().join(format!("spotifast-soloist-accounts-{}", std::process::id()));
        let users = dir.join("settings").join("Users");
        std::fs::create_dir_all(users.join("alice-user")).unwrap();
        std::fs::create_dir_all(users.join("not-an-account")).unwrap();
        std::fs::write(users.join("bob-user"), "").unwrap();
        assert_eq!(accounts(&dir), ["alice"]);
        assert!(accounts(&dir.join("missing")).is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_newest_downloaded_build_is_the_one_to_run() {
        let dir =
            std::env::temp_dir().join(format!("spotifast-soloist-builds-{}", std::process::id()));
        for build in ["1792756912", "1792778483", ".download-1"] {
            std::fs::create_dir_all(dir.join(build)).unwrap();
            std::fs::write(dir.join(build).join(BINARY), "").unwrap();
        }
        // A folder without the program, as an interrupted removal leaves.
        std::fs::create_dir_all(dir.join("1799999999")).unwrap();
        assert_eq!(
            installed(&dir),
            Some((1_792_778_483, dir.join("1792778483").join(BINARY)))
        );
        assert_eq!(installed(&dir.join("missing")), None);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn downloads_follow_redirects_only_within_spotify() {
        assert!(archive_host("soloist-builds.spotifycdn.com"));
        assert!(archive_host("spotify.com"));
        assert!(!archive_host("notspotify.com"));
        assert!(!archive_host("spotifycdn.com.example.net"));
    }

    /// Spotify's archive holds the program beside its changelog and
    /// licenses; only a program that answers `--version` is installed.
    #[cfg(unix)]
    #[test]
    fn an_archive_installs_under_its_build_time() {
        let dir =
            std::env::temp_dir().join(format!("spotifast-soloist-archive-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let source = dir.join("source");
        std::fs::create_dir_all(&source).unwrap();
        let built = jiff::Timestamp::now().as_second() - 86_400;
        std::fs::write(
            source.join(BINARY),
            format!("#!/bin/sh\necho 'soloist 1.3.10.13 build {built} (x)'\n"),
        )
        .unwrap();
        std::fs::write(source.join("CHANGELOG.md"), "").unwrap();
        let archive = dir.join("soloist.tar.gz");
        let packed = std::process::Command::new("tar")
            .arg("-czf")
            .arg(&archive)
            .arg("-C")
            .arg(&source)
            .args([BINARY, "CHANGELOG.md"])
            .status()
            .unwrap();
        assert!(packed.success());
        let programs = dir.join("programs");
        let staging = programs.join(".download-test");
        std::fs::create_dir_all(&staging).unwrap();
        let downloaded = install_archive(&archive, &staging, &programs).unwrap();
        assert!(downloaded.new);
        assert_eq!(downloaded.build.version, "1.3.10.13");
        let program = programs.join(built.to_string()).join(BINARY);
        assert_eq!(find_binary_in(&programs), Some(program.clone()));
        assert!(
            !programs
                .join(built.to_string())
                .join("CHANGELOG.md")
                .exists()
        );

        // The same build again is not new, and the installed one stays.
        std::fs::create_dir_all(&staging).unwrap();
        let again = install_archive(&archive, &staging, &programs).unwrap();
        assert!(!again.new);
        assert!(program.is_file());

        // An archive without the program installs nothing.
        let empty = dir.join("empty.tar.gz");
        std::process::Command::new("tar")
            .arg("-czf")
            .arg(&empty)
            .arg("-C")
            .arg(&source)
            .arg("CHANGELOG.md")
            .status()
            .unwrap();
        let other = dir.join("other");
        std::fs::create_dir_all(other.join("staging")).unwrap();
        assert!(install_archive(&empty, &other.join("staging"), &other).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Run by hand: `cargo test --lib soloist::tests::downloads -- --ignored`.
    #[test]
    #[ignore = "downloads Soloist from Spotify"]
    fn downloads_spotifys_newest_build() {
        // About 25 MB: kept out of the small temporary filesystem.
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/soloist-download-test");
        let _ = std::fs::remove_dir_all(&dir);
        let client = download_client(&crate::settings::ProxyConfig::System).unwrap();
        let downloaded = download(&client, &dir).unwrap();
        assert!(downloaded.new);
        assert_eq!(
            read_build(&find_binary_in(&dir).unwrap()),
            Some(downloaded.build)
        );
        assert!(!download(&client, &dir).unwrap().new);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn find_binary_in(programs: &Path) -> Option<PathBuf> {
        installed(programs).map(|(_, path)| path)
    }

    fn track_event() -> Value {
        json!({
            "type": "track_changed",
            "item": {
                "uri": "spotify:track:2JRo0gjbX4GrCqBYdRohoo",
                "entity_type": "track",
                "decorations": {
                    "identity": { "name": "My Song" },
                    "visual_identity": { "cover": [
                        { "url": "https://i.scdn.co/image/small", "size": "small" },
                        { "url": "https://i.scdn.co/image/large", "size": "large" }
                    ] },
                    "parent": { "entity": {
                        "uri": "spotify:album:4aawyAB9vmqN3uQ7FjRGTy",
                        "entity_type": "album",
                        "decorations": { "identity": { "name": "Album Name" } }
                    } },
                    "creators": [ { "entity": {
                        "uri": "spotify:artist:0OdUWJ0sBjDrqHygGUXeCF",
                        "entity_type": "artist",
                        "decorations": { "identity": { "name": "Artist Name" } }
                    } } ],
                    "playback": { "duration_ms": 210000, "content_ratings": [] }
                }
            }
        })
    }

    #[test]
    fn a_track_event_becomes_the_player_bar_track() {
        let mut state = LocalState::default();
        assert!(apply_event(&mut state, &track_event(), 0));
        let track = state.track.as_ref().unwrap();
        assert_eq!(track.title, "My Song");
        assert_eq!(track.album, "Album Name");
        assert_eq!(track.artist_names(), "Artist Name");
        assert_eq!(
            track.artists[0].id.as_deref(),
            Some("0OdUWJ0sBjDrqHygGUXeCF")
        );
        assert_eq!(
            track.art_url.as_deref(),
            Some("https://i.scdn.co/image/large")
        );
        assert_eq!(
            track.art_small_url.as_deref(),
            Some("https://i.scdn.co/image/small")
        );
        assert_eq!(track.duration_ms, 210_000);
        assert_eq!(state.track_sequence, 1);
        // The same song again changes nothing.
        assert!(!apply_event(&mut state, &track_event(), 0));
        assert_eq!(state.track_sequence, 1);
    }

    #[test]
    fn status_and_position_drive_playback() {
        let mut state = LocalState::default();
        apply_event(&mut state, &track_event(), 0);
        apply_event(
            &mut state,
            &json!({ "type": "playback_changed", "status": "buffering" }),
            0,
        );
        assert_eq!(state.playback, Playback::Loading);
        assert!(state.loading);
        let sync = json!({ "type": "position_sync", "position": {
            "position_ms": 45_000, "timestamp_ms": 1_000, "speed": 1.0 } });
        apply_event(&mut state, &sync, 1_500);
        assert_eq!(state.playback, Playback::Playing);
        assert!(!state.loading);
        assert!(state.position_now() >= 45_500);
        assert_eq!(state.seek_sequence, 1);
        apply_event(
            &mut state,
            &json!({ "type": "playback_changed", "status": "paused" }),
            0,
        );
        assert_eq!(state.playback, Playback::Paused);
        assert_eq!(state.position_at, None);
    }

    #[test]
    fn another_device_taking_over_stops_this_one() {
        let mut state = LocalState::default();
        apply_event(&mut state, &track_event(), 0);
        apply_event(
            &mut state,
            &json!({ "type": "playback_changed", "status": "playing" }),
            0,
        );
        assert!(apply_event(
            &mut state,
            &json!({ "type": "device_changed", "is_active": false, "device_name": "Spotifast" }),
            0
        ));
        assert_eq!(state.playback, Playback::Stopped);
        assert!(!state.is_active());
    }

    #[test]
    fn snapshots_carry_volume_and_options() {
        let mut state = LocalState::default();
        let snapshot = json!({
            "type": "playback_state",
            "status": "paused",
            "item": track_event()["item"],
            "position": { "position_ms": 1_000, "timestamp_ms": 0, "speed": 0.0 },
            "volume": 50,
            "is_active": true,
            "options": { "shuffle": true, "repeat": "track" }
        });
        assert!(apply_event(&mut state, &snapshot, 0));
        assert_eq!(state.volume, 32_767);
        assert!(state.shuffle);
        assert_eq!(state.repeat, RepeatMode::Track);
        assert_eq!(state.playback, Playback::Paused);
        assert_eq!(state.position_ms, 1_000);
    }

    #[test]
    fn verbose_detail_is_told_from_warnings() {
        let line = "13:46:04.484 I [f:52                            ] Started ZeroConf service on port 61155 path /zc";
        assert!(is_verbose_detail(line));
        assert!(!is_verbose_detail("13:46:04.484 W [f:52 ] something odd"));
        assert!(!is_verbose_detail("2026-10-23 15:36:14.723: running"));
        assert_eq!(strip_ansi("\u{1b}[32mready\u{1b}[0m"), "ready");
    }

    #[test]
    fn volume_round_trips_through_percent() {
        assert_eq!(to_percent(0), 0);
        assert_eq!(to_percent(65_535), 100);
        assert_eq!(to_percent(from_percent(42)), 42);
    }

    #[test]
    fn a_load_keeps_its_starting_song_and_options() {
        let body = load_body(&LoadSpec {
            context_uri: Some("spotify:playlist:37i9dQZF1DXcBWIGoYBM5M".into()),
            offset_uri: Some("spotify:track:2JRo0gjbX4GrCqBYdRohoo".into()),
            position_ms: 30_000,
            play: false,
            shuffle: Some(true),
            ..LoadSpec::default()
        });
        let command = &body["command"];
        assert_eq!(command["endpoint"], "play");
        assert_eq!(
            command["context"]["uri"],
            "spotify:playlist:37i9dQZF1DXcBWIGoYBM5M"
        );
        assert_eq!(
            command["options"]["skip_to"]["track_uri"],
            "spotify:track:2JRo0gjbX4GrCqBYdRohoo"
        );
        assert_eq!(command["options"]["seek_to"], 30_000);
        assert_eq!(command["options"]["initially_paused"], true);
        assert_eq!(
            command["options"]["player_options_override"]["shuffling_context"],
            true
        );
    }
}
