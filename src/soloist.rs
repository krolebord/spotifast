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
use tokio::sync::{broadcast, mpsc, watch};
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
/// How long Soloist may take to report itself logged in without a
/// stored session, which a pick in Spotify's app may still bring.
const RESTORE_WAIT: Duration = Duration::from_secs(4);
/// How long a stored session may take to restore before Soloist is taken
/// to need pairing again.
const STORED_RESTORE_WAIT: Duration = Duration::from_secs(15);
/// How often an unpaired Soloist is asked whether it has signed in.
const PAIRING_POLL: Duration = Duration::from_secs(2);
/// Where Soloist keeps its Connect device id, in its data directory.
const DEVICE_ID_FILE: &str = ".device_id";
/// Volume drags are forwarded at most this often.
const PREVIEW_INTERVAL: Duration = Duration::from_millis(80);
/// How long Soloist may take to acknowledge a command.
const ACK_TIMEOUT: Duration = Duration::from_secs(5);
/// A seek Soloist has not carried out yet is sent again this often.
const SEEK_RETRY: Duration = Duration::from_millis(500);
/// How long a start partway into a song keeps seeking.
const SEEK_PATIENCE: Duration = Duration::from_secs(15);
/// How far from its target a reported position still counts as the seek.
const SEEK_GRACE_MS: u32 = 3_000;

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
    /// Audio quality and Normalize volume, which Soloist reads from its
    /// prefs files only as it starts: a change starts a new Soloist.
    pub bitrate_kbps: u16,
    pub normalisation: bool,
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

/// The slot's Soloist, while it runs. None also while an engine is
/// starting one, which holds the slot.
pub fn running(slot: &Slot) -> Option<Arc<Soloist>> {
    slot.try_lock().ok().and_then(|running| {
        running
            .as_ref()
            .filter(|running| !running.soloist.has_ended())
            .map(|running| Arc::clone(&running.soloist))
    })
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
/// Where the user makes the API key Soloist starts with.
pub const DASHBOARD_URL: &str = "https://developer.spotify.com/dashboard";
/// Spotify's server for the archives `DOWNLOADS_URL` links to.
const ARCHIVES_URL: &str = "https://soloist-builds.spotifycdn.com/";
/// A download may be redirected only within Spotify's own servers.
const ARCHIVE_HOSTS: [&str; 2] = ["spotifycdn.com", "spotify.com"];
/// Archives are about 10 MB and the program about 25 MB; a larger answer
/// is not a Soloist build and must not fill the disk.
const MAX_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_BINARY_BYTES: u64 = 128 * 1024 * 1024;
/// Beside each build: the `ETag` Spotify's server sent with its archive.
const ETAG_FILE: &str = "etag";

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
    // The installed build and the fingerprint Spotify's server gave it, so
    // an unchanged archive is not downloaded again.
    let known = installed(programs).and_then(|(_, binary)| {
        let etag = std::fs::read_to_string(binary.parent()?.join(ETAG_FILE)).ok()?;
        Some((etag.trim().to_string(), read_build(&binary)?))
    });
    // Not a build time, so `installed` never picks it.
    let staging = programs.join(format!(".download-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&staging);
    std::fs::create_dir_all(&staging).context("unable to create a download folder")?;
    let result = download_into(client, archive, &staging, programs, known);
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
    known: Option<(String, Build)>,
) -> Result<Downloaded> {
    use std::io::Read;
    let url = format!("{ARCHIVES_URL}{archive}");
    // Spotify's server ignores If-None-Match, so the fingerprint is
    // compared here, from the headers alone.
    if let Some((etag, build)) = known {
        let current = client
            .head(&url)
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| anyhow!("unable to reach Spotify: {}", error.without_url()))?
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|etag| etag.to_str().ok())
            .map(str::to_string);
        if current.as_deref() == Some(etag.as_str()) {
            return Ok(Downloaded { build, new: false });
        }
    }
    let response = client
        .get(&url)
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|error| anyhow!("unable to download Soloist: {}", error.without_url()))?;
    let etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|etag| etag.to_str().ok())
        .map(str::to_string);
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
    let downloaded = install_archive(&archive_path, staging, programs)?;
    if let Some(etag) = etag {
        let folder = programs.join(downloaded.build.built.as_second().to_string());
        let _ = std::fs::write(folder.join(ETAG_FILE), etag);
    }
    Ok(downloaded)
}

/// Unpacks the program from Spotify's archive and installs it under its
/// build time once it runs.
fn install_archive(archive_path: &Path, staging: &Path, programs: &Path) -> Result<Downloaded> {
    // The system's tar reads both archive kinds (Windows 10 ships bsdtar),
    // and naming the one member keeps anything else in it out.
    let unpacked = staging.join("build");
    std::fs::create_dir_all(&unpacked).context("unable to create a download folder")?;
    let mut tar = std::process::Command::new("tar");
    no_console(&mut tar);
    let status = tar
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

/// Windows opens a console window for each console program a windowed app
/// starts, unless asked not to.
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// Starts `command` without a console window of its own on Windows.
fn no_console(command: &mut std::process::Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    #[cfg(not(windows))]
    let _ = command;
}

/// Blocking: runs `binary --version`.
pub fn read_build(binary: &Path) -> Option<Build> {
    let mut command = std::process::Command::new(binary);
    no_console(&mut command);
    let output = command
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

/// Whether an engine error is Soloist saying Spotify refused its key.
pub fn is_key_suspect(error: &str) -> bool {
    error.starts_with(KEY_REJECTED)
}

/// What the Soloist that playback goes through is doing, as far as the
/// interface knows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    /// Playback does not go through Soloist.
    Off,
    /// Local playback is not set up, so nothing starts.
    NoPlayback,
    Starting,
    /// Running, without a session: it waits for a pick in Spotify's app.
    WaitingForPairing,
    Playing,
    /// It did not start, or stopped; the engine's words.
    Failed(String),
}

/// `on`: playback goes through Soloist. `starting`: a restart was asked
/// for and the engine has not answered yet. `error`: local playback's
/// error, Soloist's own while it plays.
pub fn status(
    on: bool,
    starting: bool,
    playback: &crate::backend::LocalPlayback,
    error: Option<&str>,
) -> Status {
    use crate::backend::LocalPlayback;
    if !on {
        return Status::Off;
    }
    if starting {
        return Status::Starting;
    }
    match playback {
        LocalPlayback::Unavailable => Status::NoPlayback,
        LocalPlayback::Authorizing
        | LocalPlayback::Connecting
        | LocalPlayback::WaitingForNetwork => Status::Starting,
        LocalPlayback::Failed(message) => Status::Failed(message.clone()),
        LocalPlayback::Ready { .. } => match error {
            Some(error) if is_pairing_needed(error) => Status::WaitingForPairing,
            Some(error) => Status::Failed(error.to_string()),
            None => Status::Playing,
        },
    }
}

/// A Soloist API key on its way to its file, kept out of logs.
#[derive(Clone, PartialEq, Eq)]
pub struct ApiKey(pub String);

impl std::fmt::Debug for ApiKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ApiKey(..)")
    }
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
    /// Spotify reports another device as the active one. Soloist's own
    /// events may still describe its last song as paused; while this holds
    /// they cannot make this computer the player again.
    elsewhere: Arc<std::sync::atomic::AtomicBool>,
    /// Until when a play asked of this computer outweighs Spotify still
    /// naming another device: its report lags behind the request.
    claimed_until: Mutex<Option<Instant>>,
    device_id: String,
    stop: watch::Sender<bool>,
    ended: watch::Receiver<bool>,
    /// Set once the process has exited, which `ended` may come before:
    /// Soloist closes its API first and lets go of its data directory last.
    exited: watch::Receiver<bool>,
    last_preview: Mutex<Option<Instant>>,
    /// The commands Soloist acknowledges, as it does.
    acks: broadcast::Sender<String>,
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
        // The Soloist this run started last, stopped just now to make way
        // for this one, may still be shutting down.
        wait_for_last_started().await;
        // A Soloist a previous run left behind, after a crash or a kill,
        // holds the data directory's lock and its Connect device. On
        // Windows its job stops it with this process instead.
        #[cfg(unix)]
        stop_stale(&config.data_dir).await;
        let (data_dir, bitrate_kbps, normalisation) = (
            config.data_dir.clone(),
            config.bitrate_kbps,
            config.normalisation,
        );
        match tokio::task::spawn_blocking(move || {
            write_prefs(&data_dir, bitrate_kbps, normalisation)
        })
        .await
        {
            Ok(Ok(())) => {}
            // Soloist still plays, with the quality and loudness it had.
            Ok(Err(error)) => log::warn!("unable to write Soloist's audio settings: {error}"),
            Err(error) => log::warn!("unable to write Soloist's audio settings: {error}"),
        }

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
        // Its output is read from the pipes above, never shown.
        #[cfg(windows)]
        command.creation_flags(CREATE_NO_WINDOW);
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
        // Closed with the supervisor below, or by Windows if this process
        // dies first, which stops Soloist with it.
        #[cfg(windows)]
        let job = job::contain(&child);
        // Sent once the process has exited. A start that fails before the
        // supervisor takes the process on kills it and waits for it, then
        // drops this unsent: nothing is left to wait for.
        let (exited_tx, exited) = watch::channel(false);
        *LAST_STARTED.lock().unwrap_or_else(|p| p.into_inner()) = Some(exited.clone());
        #[cfg(unix)]
        if let Some(pid) = pid {
            watch_parent(pid);
        }
        let said = Said::new();
        if let Some(out) = child.stdout.take() {
            tokio::spawn(forward_log(out, key.clone(), said.clone()));
        }
        if let Some(err) = child.stderr.take() {
            tokio::spawn(forward_log(err, key.clone(), said.clone()));
        }
        drop(key);

        let port = match wait_for_port(&mut child, &port_file, &said).await {
            Ok(port) => port,
            Err(error) => {
                let _ = child.kill().await;
                return Err(error);
            }
        };
        let url = format!("ws://127.0.0.1:{port}");
        let (socket, _) = match tokio_tungstenite::connect_async(url.as_str()).await {
            Ok(connected) => connected,
            Err(error) => {
                let _ = child.kill().await;
                return Err(anyhow!("unable to reach Soloist's local API: {error}"));
            }
        };
        let (mut sink, mut stream) = socket.split();

        let (commands, mut outgoing) = mpsc::unbounded_channel::<Value>();
        let (stop, mut stop_rx) = watch::channel(false);
        let (ended_tx, ended) = watch::channel(false);
        let (auth_tx, mut auth) = watch::channel(None::<bool>);
        let (acks, _) = broadcast::channel(16);

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
        let reader_acks = acks.clone();
        let reader_said = said.clone();
        let elsewhere = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader_elsewhere = Arc::clone(&elsewhere);
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
                if event["type"] == "command_result"
                    && let Some(command) = event["command"].as_str()
                {
                    let _ = reader_acks.send(command.to_string());
                }
                if event["type"] == "error" {
                    log::warn!(
                        "Soloist: {}",
                        event["message"].as_str().unwrap_or("unknown error")
                    );
                }
                // Sent under the lock, so snapshots reach the app in the
                // order they were taken.
                let mut current = reader_state.lock().unwrap_or_else(|p| p.into_inner());
                // Under the lock with the error it clears, so a refusal
                // printed meanwhile is shown again after both.
                if event["type"] == "auth_state" && event["logged_in"] == true {
                    reader_said.signed_in();
                }
                if apply_event_from(&mut current, &event, now_ms(), &reader_elsewhere) {
                    let notify = reader_notify
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .clone();
                    notify(EngineEvent::State(current.clone()));
                }
            }
            let _ = reader_ended.send(true);
        });

        // Supervisor: the process, until it exits or the engine stops it.
        let supervisor_state = Arc::clone(&state);
        let supervisor_said = said.clone();
        tokio::spawn(async move {
            #[cfg(windows)]
            let _job = job;
            tokio::select! {
                status = child.wait() => {
                    let code = status.ok().and_then(|status| status.code());
                    if let Some(message) = exit_message(code, supervisor_said.refusal().await, false) {
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
            let _ = exited_tx.send(true);
            let _ = ended_tx.send(true);
        });

        let mut soloist = Self {
            commands,
            state: Arc::clone(&state),
            notify: shared_notify,
            pid,
            stopping: std::sync::atomic::AtomicBool::new(false),
            elsewhere,
            claimed_until: Mutex::new(None),
            device_id: String::new(),
            stop,
            ended,
            exited,
            last_preview: Mutex::new(None),
            acks,
        };
        match soloist.sign_in(config, &mut auth, &said).await {
            Ok((device_id, paired)) => {
                soloist.device_id = device_id;
                if !paired {
                    // Soloist keeps waiting for a pick in Spotify's app, or
                    // for Spotify to take its key; its sign-in clears this.
                    // It does not always announce one, so it is asked until
                    // it has signed in or stopped.
                    {
                        let mut current = state.lock().unwrap_or_else(|p| p.into_inner());
                        // Checked, and sent, under the lock: a sign-in since
                        // is told before the reader takes it to clear this.
                        if *auth.borrow() != Some(true) {
                            current.error = Some(if said.key_rejected() {
                                key_rejected()
                            } else {
                                pairing_needed(device_name)
                            });
                            notify(EngineEvent::State(current.clone()));
                        }
                    }
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
                // Spotify may refuse the key later, which Soloist only
                // prints: it keeps running and reports nothing else.
                let mut refusal = said.refusal.subscribe();
                let refused_state = Arc::clone(&state);
                let refused_notify = Arc::clone(&soloist.notify);
                tokio::spawn(async move {
                    loop {
                        let rejected =
                            |refusal: &Option<Refusal>| *refusal == Some(Refusal::KeyRejected);
                        if refusal.wait_for(rejected).await.is_err() {
                            return;
                        }
                        {
                            let mut current =
                                refused_state.lock().unwrap_or_else(|p| p.into_inner());
                            // Checked again under the lock: a sign-in clears
                            // the refusal before it takes the lock to clear
                            // the error, so neither can undo the other.
                            if rejected(&refusal.borrow())
                                && set(&mut current.error, Some(key_rejected()))
                            {
                                log::warn!("Spotify refused Soloist's API key; it keeps running");
                                let notify = refused_notify
                                    .lock()
                                    .unwrap_or_else(|p| p.into_inner())
                                    .clone();
                                notify(EngineEvent::State(current.clone()));
                            }
                        }
                        // Any change, not only a cleared one: a sign-in and
                        // a new refusal may both land before this wakes.
                        if refusal.changed().await.is_err() {
                            return;
                        }
                    }
                });
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
        said: &Said,
    ) -> Result<(String, bool)> {
        let data_dir = config.data_dir.clone();
        let stored = tokio::task::spawn_blocking(move || !accounts(&data_dir).is_empty())
            .await
            .unwrap_or(false);
        // A stored session can take a while to restore, and asking for a
        // pairing meanwhile would be wrong.
        let wait = if stored {
            STORED_RESTORE_WAIT
        } else {
            RESTORE_WAIT
        };
        let mut refusal = said.refusal.subscribe();
        let restored = tokio::time::timeout(wait, async {
            tokio::select! {
                seen = auth.wait_for(|logged_in| *logged_in == Some(true)) => seen.is_ok(),
                // Spotify refused the key and Soloist keeps running: no
                // session is coming.
                _ = refusal.wait_for(|refusal| *refusal == Some(Refusal::KeyRejected)) => false,
            }
        })
        .await
        .unwrap_or(false);
        if !restored && self.has_ended() {
            // It exited instead of waiting to be paired: Spotify refusing
            // the key for its stored session, most often.
            let _ = tokio::time::timeout(EXIT_WAIT, self.exited()).await;
            let error = self
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .error
                .clone();
            bail!(error.unwrap_or_else(|| format!("{EXITED_STARTING}. See the log for details.")));
        }
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

    /// Resolves once the process has exited and let go of its data
    /// directory.
    pub async fn exited(&self) {
        let mut exited = self.exited.clone();
        let _ = exited.wait_for(|exited| *exited).await;
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

    /// Follows Spotify's active device, as `cluster` from the librespot
    /// device beside Soloist reports it, until that device goes. Another
    /// device taking over stops this computer's player, whatever Soloist's
    /// own events last said, so the app sends its commands there instead.
    pub fn follow_active_device(
        self: &Arc<Self>,
        mut cluster: watch::Receiver<librespot_connect::ClusterState>,
    ) {
        let soloist = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut seen = None;
            loop {
                let active = cluster.borrow_and_update().active_device_id.clone();
                let Some(current) = soloist.upgrade() else {
                    return;
                };
                if seen.as_ref() != Some(&active) {
                    current.active_device_changed(active.as_deref());
                    seen = Some(active);
                }
                let claimed_until = current.claimed_until();
                drop(current);
                match claimed_until {
                    Some(until) => {
                        tokio::select! {
                            changed = cluster.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                            }
                            () = tokio::time::sleep_until(until.into()) => {
                                // The claim lapsed: whoever Spotify names now
                                // is the player, even if unchanged.
                                seen = None;
                            }
                        }
                    }
                    None => {
                        if cluster.changed().await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
    }

    fn active_device_changed(&self, active: Option<&str>) {
        use std::sync::atomic::Ordering;
        let claimed = self.claimed_until().is_some();
        match plays_elsewhere(&self.device_id, active, claimed) {
            Some(false) => {
                *self.claimed_until.lock().unwrap_or_else(|p| p.into_inner()) = None;
                if self.elsewhere.swap(false, Ordering::SeqCst) {
                    // Picked again from elsewhere: catch up on what it plays.
                    let _ = self.send("get_state", json!({}));
                }
            }
            Some(true) => {
                self.elsewhere.store(true, Ordering::SeqCst);
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if deactivate(&mut state) {
                    self.send_state(&state);
                }
            }
            None => {}
        }
    }

    /// The listener asked this computer to play: Soloist's next events
    /// count again, before Spotify reports it as the active device.
    pub fn claim(&self) {
        *self.claimed_until.lock().unwrap_or_else(|p| p.into_inner()) =
            Some(Instant::now() + CLAIM);
        self.elsewhere
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// When the current claim lapses, if it has not yet.
    fn claimed_until(&self) -> Option<Instant> {
        let until = *self.claimed_until.lock().unwrap_or_else(|p| p.into_inner());
        until.filter(|until| *until > Instant::now())
    }

    /// The current snapshot, sent to the current engine's channel.
    pub fn publish(&self) {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        self.send_state(&state);
    }

    /// Sends `state`, which the caller holds locked so that a newer
    /// snapshot cannot be overtaken by this one.
    fn send_state(&self, state: &LocalState) {
        let notify = self
            .notify
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        notify(EngineEvent::State(state.clone()));
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
        // Volume drags send many; everything else is told at once.
        if command == "set_volume" {
            log::debug!("Soloist command: {command} {fields}");
        } else {
            log::info!("Soloist command: {command} {fields}");
        }
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
        let summary = load_summary(&spec);
        let mut result = if self.device_id.is_empty() {
            Err(anyhow!("Soloist's Connect device id is unknown"))
        } else {
            logged_load(
                &session,
                session.device_id(),
                &self.device_id,
                &body,
                "from this computer",
                &summary,
            )
            .await
        };
        if result.is_err() && !self.device_id.is_empty() {
            // `to` as the source too means "from whichever device is active".
            result = logged_load(
                &session,
                &self.device_id,
                &self.device_id,
                &body,
                "from Soloist itself",
                &summary,
            )
            .await;
        }
        let Err(error) = result else {
            return;
        };
        log::warn!("Connect load for Soloist failed, playing over its API instead: {error:#}");
        let uri = spec
            .offset_uri
            .clone()
            .or_else(|| spec.context_uri.clone())
            .or_else(|| spec.uris.first().cloned());
        let Some(uri) = uri else {
            return;
        };
        // A bare play starts Soloist without making it Spotify's active
        // device, and Spotify's apps would not know it plays.
        self.activate().await;
        if let Some(shuffle) = spec.shuffle {
            let _ = self.send("set_shuffle", json!({ "enabled": shuffle }));
        }
        if let Some(repeat) = spec.repeat {
            let _ = self.set_repeat(repeat);
        }
        let _ = self.send("play", json!({ "uri": uri }));
        if spec.position_ms > 0 {
            // A context's first song is not known here: any song counts.
            let song = matches!(crate::util::uri_kind(&uri), Some("track" | "episode"))
                .then_some(uri.as_str());
            self.seek_until_landed(spec.position_ms, song).await;
        }
        if !spec.play {
            let _ = self.send("pause", json!({}));
        }
    }

    /// Makes Soloist Spotify's active device, and waits for Soloist to
    /// acknowledge it before anything is played.
    async fn activate(&self) {
        let mut acks = self.acks.subscribe();
        if self.send("activate", json!({})).is_err() {
            return;
        }
        let acknowledged = tokio::time::timeout(ACK_TIMEOUT, async {
            loop {
                match acks.recv().await {
                    Ok(command) if command == "activate" => return true,
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return false,
                }
            }
        })
        .await
        .unwrap_or(false);
        if !acknowledged {
            log::warn!("Soloist did not acknowledge becoming the active device; playing anyway");
        }
    }

    /// Seeks to `position_ms` until Soloist reports playing there: it
    /// drops a seek that arrives while the song is still loading. `song`
    /// is the song the seek is for, when known.
    async fn seek_until_landed(&self, position_ms: u32, song: Option<&str>) {
        let started = Instant::now();
        loop {
            if self
                .send("seek", json!({ "position_ms": position_ms }))
                .is_err()
            {
                return;
            }
            tokio::time::sleep(SEEK_RETRY).await;
            let waited = started.elapsed();
            let landed = {
                let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                seek_landed(&state, position_ms, song, waited)
            };
            if landed || self.has_ended() {
                return;
            }
            if waited >= SEEK_PATIENCE {
                log::warn!("Soloist did not seek to {position_ms} ms");
                return;
            }
        }
    }
}

#[cfg(test)]
impl Soloist {
    /// A Soloist with no process behind it: what it is sent arrives on the
    /// returned channel.
    pub(crate) fn detached(state: LocalState) -> (Arc<Self>, mpsc::UnboundedReceiver<Value>) {
        let (commands, sent) = mpsc::unbounded_channel();
        let soloist = Self {
            commands,
            state: Arc::new(Mutex::new(state)),
            notify: Arc::new(Mutex::new(Arc::new(|_| {}))),
            pid: None,
            stopping: std::sync::atomic::AtomicBool::new(false),
            elsewhere: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            claimed_until: Mutex::new(None),
            device_id: String::new(),
            stop: watch::channel(false).0,
            ended: watch::channel(false).1,
            exited: watch::channel(false).1,
            last_preview: Mutex::new(None),
            acks: broadcast::channel(16).0,
        };
        (Arc::new(soloist), sent)
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

/// One load attempt over Connect, told in the log with how long it took:
/// librespot sends a request again after a timeout or a server error, so a
/// slow answer may stand for more than one play reaching Soloist.
async fn logged_load(
    session: &Session,
    from: &str,
    to: &str,
    body: &Value,
    route: &str,
    summary: &str,
) -> Result<()> {
    log::info!("Soloist load over Connect {route}: {summary}");
    let started = Instant::now();
    let result = connect_command(session, from, to, body).await;
    let elapsed = started.elapsed().as_millis();
    match &result {
        Ok(()) => log::info!("Soloist load over Connect {route} accepted after {elapsed} ms"),
        Err(error) => {
            log::warn!("Soloist load over Connect {route} failed after {elapsed} ms: {error:#}")
        }
    }
    result
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

/// A load, as the log tells it: what it plays, from where, and how.
fn load_summary(spec: &LoadSpec) -> String {
    let what = match (&spec.context_uri, spec.uris.len()) {
        (Some(context), _) => context.clone(),
        (None, 1) => spec.uris[0].clone(),
        (None, count) => format!("{count} songs"),
    };
    let start = match (&spec.offset_uri, spec.offset_index) {
        (Some(uri), _) => format!(" at {uri}"),
        (None, Some(index)) => format!(" at #{index}"),
        (None, None) => String::new(),
    };
    format!(
        "{what}{start}, position {} ms, shuffle {:?}, repeat {:?}, {}",
        spec.position_ms,
        spec.shuffle,
        spec.repeat,
        if spec.play { "playing" } else { "paused" }
    )
}

/// How long a play asked of this computer outweighs Spotify naming another
/// device. Past it, a play that never started leaves that device the player.
const CLAIM: Duration = Duration::from_secs(10);
/// The start of the message shown while Soloist waits to be paired.
const PAIRING_NEEDED: &str = "Soloist is waiting to be paired.";
/// The start of the message shown when Soloist exits before it is ready.
const EXITED_STARTING: &str = "Spotify Soloist exited while starting";
/// The start of the message shown when Soloist says Spotify refused its key.
const KEY_REJECTED: &str = "Spotify rejected the Soloist API key.";
/// How long the end of Soloist's output may take to arrive after it exits.
const OUTPUT_DRAIN: Duration = Duration::from_secs(1);
/// How long a stopped Soloist may take to exit: `terminate` waits three
/// seconds before it kills.
const EXIT_WAIT: Duration = Duration::from_secs(5);

/// A reason Soloist gives on its way out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refusal {
    /// Spotify did not accept the API key, or there was none.
    KeyRejected,
    /// Another Soloist still holds the data directory's lock.
    DataDirInUse,
}

/// The reason in one line of Soloist's output, if it gives one.
fn refusal(line: &str) -> Option<Refusal> {
    if line.contains("another session is running for data directory") {
        Some(Refusal::DataDirInUse)
    } else if line.contains("make sure --api-key is valid")
        || line.contains("--api-key is required")
    {
        Some(Refusal::KeyRejected)
    } else {
        None
    }
}

/// What Soloist said about why it stopped, read from its output.
#[derive(Clone)]
struct Said {
    /// The last reason it gave. Soloist does not always exit after giving
    /// one: it may refuse the key and keep running.
    refusal: Arc<watch::Sender<Option<Refusal>>>,
    /// How many of its two output streams have closed.
    closed: Arc<watch::Sender<u8>>,
}

impl Said {
    fn new() -> Self {
        Self {
            refusal: Arc::new(watch::channel(None).0),
            closed: Arc::new(watch::channel(0).0),
        }
    }

    /// The reason it gave, once its output has ended. The process has
    /// exited by now, but its last lines may still be on their way.
    async fn refusal(&self) -> Option<Refusal> {
        let mut closed = self.closed.subscribe();
        let _ = tokio::time::timeout(OUTPUT_DRAIN, closed.wait_for(|closed| *closed >= 2)).await;
        *self.refusal.borrow()
    }

    /// Whether the last reason it gave was a refused key.
    fn key_rejected(&self) -> bool {
        *self.refusal.borrow() == Some(Refusal::KeyRejected)
    }

    /// It signed in after all: an earlier refused key no longer explains
    /// anything.
    fn signed_in(&self) {
        self.refusal.send_if_modified(|refusal| {
            refusal
                .take_if(|refusal| *refusal == Refusal::KeyRejected)
                .is_some()
        });
    }
}

/// What to tell the listener about Soloist exiting with `code`, having
/// given `refusal`; `starting` until it has signed in. None for a clean
/// exit after it ran.
fn exit_message(code: Option<i32>, refusal: Option<Refusal>, starting: bool) -> Option<String> {
    let code_text = code.map_or_else(|| "none".into(), |code| code.to_string());
    Some(match (code, refusal) {
        (Some(EXIT_EXPIRED), _) => {
            "This Spotify Soloist build has expired. Update it in Settings, under Spotify Soloist."
                .to_string()
        }
        (_, Some(Refusal::KeyRejected)) => key_rejected(),
        (_, Some(Refusal::DataDirInUse)) => format!(
            "{EXITED_STARTING} (exit code {code_text}): another Soloist was still using its data folder."
        ),
        (Some(0), None) if !starting => return None,
        _ if starting => {
            format!("{EXITED_STARTING} (exit code {code_text}). See the log for details.")
        }
        _ => format!(
            "Spotify Soloist stopped unexpectedly (exit code {code_text}). See the log for details."
        ),
    })
}

/// The Soloist started last, until its process has exited. A new one
/// waits for it: Soloist locks its data directory, and one started while
/// the last still holds the lock exits at once.
static LAST_STARTED: Mutex<Option<watch::Receiver<bool>>> = Mutex::new(None);

/// Waits, for at most `EXIT_WAIT`, until the Soloist started last exits.
async fn wait_for_last_started() {
    let last = LAST_STARTED
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take();
    if let Some(mut last) = last {
        let _ = tokio::time::timeout(EXIT_WAIT, last.wait_for(|exited| *exited)).await;
    }
}

/// Whether `error` says what to fix before Soloist can sign in, which
/// only its sign-in clears.
fn is_setup_error(error: &str) -> bool {
    error.starts_with(PAIRING_NEEDED) || error.starts_with(KEY_REJECTED)
}

fn key_rejected() -> String {
    format!("{KEY_REJECTED} Check it in Settings, under Spotify Soloist.")
}

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

/// Soloist has no option for its audio quality or loudness normalization:
/// it reads them as it starts from the desktop client's prefs files in its
/// data folder, one shared and one per account, which wins key by key.
const PREF_NORMALIZE: &str = "audio.normalize_v2";
const PREF_BITRATE: &str = "audio.play_bitrate_enumeration";
const PREF_BITRATE_NON_METERED: &str = "audio.play_bitrate_non_metered_enumeration";
/// Without it Soloist derives the non-metered quality from the metered one.
const PREF_BITRATE_MIGRATED: &str = "audio.play_bitrate_non_metered_migrated";

/// Soloist's quality level for a bitrate: 2 is 96 kbps, 3 is 160 and 4 is
/// 320. Soloist reads anything outside 1 to 5 as 160, and 5 is lossless,
/// which Spotifast does not offer.
fn bitrate_level(kbps: u16) -> u8 {
    match kbps {
        96 => 2,
        160 => 3,
        _ => 4,
    }
}

/// `prefs` with Spotifast's audio settings in place of any it held, and
/// Soloist's own keys kept.
fn prefs_with(prefs: &str, bitrate_kbps: u16, normalisation: bool) -> String {
    let level = bitrate_level(bitrate_kbps);
    let managed = [
        (PREF_NORMALIZE, normalisation.to_string()),
        (PREF_BITRATE, level.to_string()),
        (PREF_BITRATE_NON_METERED, level.to_string()),
        (PREF_BITRATE_MIGRATED, "true".to_string()),
    ];
    let mut out = String::with_capacity(prefs.len() + 160);
    for line in prefs.lines() {
        let key = line.split_once('=').map_or(line, |(key, _)| key);
        if !line.is_empty() && !managed.iter().any(|(managed, _)| *managed == key) {
            out.push_str(line);
            out.push('\n');
        }
    }
    for (key, value) in managed {
        out.push_str(&format!("{key}={value}\n"));
    }
    out
}

/// Blocking: writes the audio settings into every prefs file Soloist
/// reads. Soloist may drop keys it does not own when it rewrites them, so
/// this runs before each start.
fn write_prefs(data_dir: &Path, bitrate_kbps: u16, normalisation: bool) -> std::io::Result<()> {
    let settings = data_dir.join("settings");
    let mut files = vec![settings.join("prefs")];
    match std::fs::read_dir(settings.join("Users")) {
        Ok(entries) => files.extend(
            entries
                .filter_map(Result::ok)
                .filter(|entry| entry.path().is_dir())
                .map(|entry| entry.path().join("prefs")),
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    for file in files {
        let prefs = match std::fs::read_to_string(&file) {
            Ok(prefs) => prefs,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error),
        };
        fastframe_file::write(&file, prefs_with(&prefs, bitrate_kbps, normalisation))?;
    }
    Ok(())
}

fn runtime_files(data_dir: &Path) -> (PathBuf, PathBuf) {
    (data_dir.join("ws.addr"), data_dir.join("ws.port"))
}

/// Waits for Soloist to write the port its API listens on.
async fn wait_for_port(
    child: &mut tokio::process::Child,
    port_file: &Path,
    said: &Said,
) -> Result<u16> {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait()? {
            let message = exit_message(status.code(), said.refusal().await, true)
                .unwrap_or_else(|| format!("{EXITED_STARTING}. See the log for details."));
            return Err(anyhow!(message));
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

/// Stops a Soloist still running on `data_dir` from an earlier run,
/// recognised by the pid file Soloist keeps there and its command line,
/// so an unrelated process that reused the pid is left alone.
#[cfg(unix)]
async fn stop_stale(data_dir: &Path) {
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

/// A Windows job that stops Soloist once its handle closes: when the
/// supervisor lets go of it, or Windows does as this process dies.
#[cfg(windows)]
mod job {
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject,
    };

    pub struct Job(HANDLE);

    // SAFETY: a job handle is a kernel object handle, valid on any thread.
    unsafe impl Send for Job {}

    /// Puts `child` in a new job; None, with a warning, if Windows refuses.
    pub fn contain(child: &tokio::process::Child) -> Option<Job> {
        let process = child.raw_handle()?;
        // SAFETY: plain Win32 calls on a new job handle, which `Job` owns
        // and closes, and on our own child's live process handle.
        let job = unsafe {
            let handle = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if handle.is_null() {
                log::warn!("no job for Soloist: {}", std::io::Error::last_os_error());
                return None;
            }
            let job = Job(handle);
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            let contained = SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                std::ptr::from_ref(&info).cast(),
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            ) != 0
                && AssignProcessToJobObject(handle, process) != 0;
            if !contained {
                log::warn!("no job for Soloist: {}", std::io::Error::last_os_error());
                return None;
            }
            job
        };
        Some(job)
    }

    impl Drop for Job {
        fn drop(&mut self) {
            // SAFETY: the handle came from CreateJobObjectW and is closed
            // only here.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

/// SPOTIFAST_SOLOIST_VERBOSE=1, or a diagnostic build, runs Soloist
/// verbosely and logs all of it, for diagnosing.
fn verbose() -> bool {
    crate::DIAGNOSTIC_BUILD || std::env::var_os("SPOTIFAST_SOLOIST_VERBOSE").is_some()
}

/// Soloist's output goes to the log, with the API key masked and detail
/// below warning level left out. A reason it gives for stopping goes to
/// `said`.
async fn forward_log(output: impl tokio::io::AsyncRead + Unpin, key: String, said: Said) {
    let verbose = verbose();
    let mut lines = tokio::io::BufReader::new(output).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let line = strip_ansi(&line.replace(&key, "[api key]"));
        if let Some(refusal) = refusal(&line) {
            said.refusal.send_replace(Some(refusal));
        }
        if verbose || !is_verbose_detail(&line) {
            log::info!("soloist: {line}");
        }
    }
    said.closed.send_modify(|closed| *closed += 1);
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
            // Signed in after all: paired, or Spotify took the key.
            if event["logged_in"] == true && state.error.as_deref().is_some_and(is_setup_error) {
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
        // Commands fail while it is not signed in; what to do about that
        // stays shown over their errors.
        "error" if state.error.as_deref().is_some_and(is_setup_error) => false,
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

/// Whether `state` shows a seek to `target_ms` carried out, `waited`
/// after it was first asked: `song`, when given, is playing, and its
/// position is near the target, not still where an earlier song was.
fn seek_landed(state: &LocalState, target_ms: u32, song: Option<&str>, waited: Duration) -> bool {
    if state.loading {
        return false;
    }
    let Some(track) = &state.track else {
        return false;
    };
    if song.is_some_and(|song| track.uri != song) {
        return false;
    }
    let position = state.position_now();
    let waited = u32::try_from(waited.as_millis()).unwrap_or(u32::MAX);
    position.saturating_add(SEEK_GRACE_MS) >= target_ms
        && position
            <= target_ms
                .saturating_add(waited)
                .saturating_add(SEEK_GRACE_MS)
}

/// Whether Spotify's `active` device is one other than Soloist's
/// `device_id`. `None` when that says nothing: no device is active, and
/// Soloist stays as it was; Soloist's id is not known; or this computer was
/// just asked to play, and Spotify has not caught up yet.
fn plays_elsewhere(device_id: &str, active: Option<&str>, claimed: bool) -> Option<bool> {
    match active {
        _ if device_id.is_empty() => None,
        Some(id) if id == device_id => Some(false),
        Some(_) if claimed => None,
        Some(_) => Some(true),
        None => None,
    }
}

/// [`apply_event`], while `elsewhere` says whether another device is
/// active. Then Soloist's events keep its last song, but not as playing or
/// paused, until an event says it is active again.
fn apply_event_from(
    state: &mut LocalState,
    event: &Value,
    now_ms: i64,
    elsewhere: &std::sync::atomic::AtomicBool,
) -> bool {
    use std::sync::atomic::Ordering;
    let mut changed = apply_event(state, event, now_ms);
    if event["is_active"] == true {
        elsewhere.store(false, Ordering::SeqCst);
    } else if elsewhere.load(Ordering::SeqCst) {
        changed |= deactivate(state);
    }
    changed
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
        // A song restored while it is not signed in solves nothing.
        if !state.error.as_deref().is_some_and(is_setup_error) {
            state.error = None;
        }
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

    /// Settings reads what Soloist is doing from local playback: its
    /// error while it runs, the engine's while it does not.
    #[test]
    fn the_status_follows_local_playback() {
        use crate::backend::LocalPlayback;
        let ready = LocalPlayback::Ready {
            device_id: "soloist".into(),
        };
        assert_eq!(status(false, true, &ready, None), Status::Off);
        assert_eq!(status(true, true, &ready, None), Status::Starting);
        assert_eq!(status(true, false, &ready, None), Status::Playing);
        assert_eq!(
            status(true, false, &LocalPlayback::Connecting, None),
            Status::Starting
        );
        assert_eq!(
            status(true, false, &LocalPlayback::Unavailable, None),
            Status::NoPlayback
        );
        let pairing = pairing_needed("Spotifast (soloist)");
        assert_eq!(
            status(true, false, &ready, Some(&pairing)),
            Status::WaitingForPairing
        );
        assert_eq!(
            status(
                true,
                false,
                &ready,
                Some("Spotify Soloist stopped unexpectedly")
            ),
            Status::Failed("Spotify Soloist stopped unexpectedly".into())
        );
        let rejected = exit_message(Some(1), Some(Refusal::KeyRejected), true).unwrap();
        assert_eq!(
            status(true, false, &LocalPlayback::Failed(rejected.clone()), None),
            Status::Failed(rejected.clone())
        );
        assert!(is_key_suspect(&rejected));
        assert!(!is_key_suspect("Spotify Soloist stopped unexpectedly"));
        let exited = exit_message(Some(1), None, true).unwrap();
        assert!(!is_key_suspect(&exited));
    }

    /// Only what Soloist says about its key points at the key: exiting
    /// because another Soloist holds the data folder does not.
    #[test]
    fn an_exit_is_explained_by_what_soloist_said() {
        assert_eq!(
            refusal("Error: another session is running for data directory \"/data/soloist\"."),
            Some(Refusal::DataDirInUse)
        );
        assert_eq!(
            refusal("2026-10-26 13:15:04.404: login failed: make sure --api-key is valid"),
            Some(Refusal::KeyRejected)
        );
        assert_eq!(
            refusal("Error: --api-key is required"),
            Some(Refusal::KeyRejected)
        );
        assert_eq!(refusal("2026-10-26 13:15:04.357: shutting down"), None);
        // Logged for one login attempt, not on the way out: a later exit
        // for another reason must not blame the key.
        assert_eq!(
            refusal("login failed while API key is configured (action_id=7)"),
            None
        );

        let in_use = exit_message(Some(1), Some(Refusal::DataDirInUse), true).unwrap();
        assert!(in_use.starts_with(EXITED_STARTING));
        assert!(in_use.contains("another Soloist"));
        assert!(!in_use.contains("API key"));
        assert!(!is_key_suspect(&in_use));

        let unexplained = exit_message(Some(1), None, true).unwrap();
        assert!(!unexplained.contains("API key"));

        // A stored session Spotify refuses ends Soloist after it ran.
        let refused = exit_message(Some(1), Some(Refusal::KeyRejected), false).unwrap();
        assert!(is_key_suspect(&refused));
        assert_eq!(exit_message(Some(0), None, false), None);
        assert!(exit_message(Some(0), None, true).is_some());
        assert!(
            exit_message(Some(EXIT_EXPIRED), Some(Refusal::KeyRejected), true)
                .unwrap()
                .contains("expired")
        );
    }

    /// A new Soloist waits for the one stopped to make way for it: Soloist
    /// locks its data folder until its process has gone, and a settings
    /// change used to start the next one before then.
    #[tokio::test(start_paused = true)]
    async fn the_next_soloist_waits_for_the_last_to_exit() {
        let (exited_tx, exited) = watch::channel(false);
        *LAST_STARTED.lock().unwrap() = Some(exited);
        let wait = tokio::spawn(wait_for_last_started());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!wait.is_finished());
        exited_tx.send(true).unwrap();
        wait.await.unwrap();

        // One killed before the supervisor took it on leaves nothing to
        // wait for.
        let (exited_tx, exited) = watch::channel(false);
        *LAST_STARTED.lock().unwrap() = Some(exited);
        drop(exited_tx);
        let started = tokio::time::Instant::now();
        wait_for_last_started().await;
        assert_eq!(started.elapsed(), Duration::ZERO);

        // One that never exits holds the next up only so long.
        let (_exited_tx, exited) = watch::channel(false);
        *LAST_STARTED.lock().unwrap() = Some(exited);
        let started = tokio::time::Instant::now();
        wait_for_last_started().await;
        assert_eq!(started.elapsed(), EXIT_WAIT);
        assert!(LAST_STARTED.lock().unwrap().is_none());
    }

    /// Soloist may refuse the key and keep running. A later sign-in
    /// clears that, and only that.
    #[test]
    fn a_key_refused_while_running_lasts_until_soloist_signs_in() {
        let said = Said::new();
        said.refusal.send_replace(Some(Refusal::KeyRejected));
        assert!(said.key_rejected());
        said.signed_in();
        assert!(!said.key_rejected());
        said.refusal.send_replace(Some(Refusal::DataDirInUse));
        said.signed_in();
        assert_eq!(*said.refusal.borrow(), Some(Refusal::DataDirInUse));

        let signed_in = json!({ "type": "auth_state", "logged_in": true });
        let mut state = LocalState {
            error: Some(key_rejected()),
            ..LocalState::default()
        };
        // A command failing meanwhile does not hide what to fix.
        let failed = json!({ "type": "error", "message": "command requires authentication" });
        assert!(!apply_event(&mut state, &failed, 0));
        assert_eq!(state.error, Some(key_rejected()));
        // Nor does the song it restores.
        assert!(apply_event(&mut state, &track_event(), 0));
        assert_eq!(state.error, Some(key_rejected()));
        assert!(apply_event(&mut state, &signed_in, 0));
        assert_eq!(state.error, None);
        let mut state = LocalState {
            error: Some("Spotify Soloist stopped unexpectedly".into()),
            ..LocalState::default()
        };
        assert!(!apply_event(&mut state, &signed_in, 0));
        assert!(state.error.is_some());
    }

    /// The reason is read once both output streams have closed, so a last
    /// line that arrives just after the exit still counts.
    #[tokio::test(start_paused = true)]
    async fn the_reason_waits_for_the_output_to_end() {
        let said = Said::new();
        let late = said.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            late.refusal.send_replace(Some(Refusal::DataDirInUse));
            late.closed.send_modify(|closed| *closed += 2);
        });
        assert_eq!(said.refusal().await, Some(Refusal::DataDirInUse));

        // Output that never closes holds it up only briefly.
        let started = tokio::time::Instant::now();
        assert_eq!(Said::new().refusal().await, None);
        assert!(started.elapsed() >= OUTPUT_DRAIN);
    }

    #[test]
    fn a_load_is_summarised_for_the_log() {
        let spec =
            |context: Option<&str>, uris: &[&str], offset_uri: Option<&str>, index| LoadSpec {
                context_uri: context.map(str::to_string),
                uris: uris.iter().map(|uri| uri.to_string()).collect(),
                offset_uri: offset_uri.map(str::to_string),
                offset_index: index,
                position_ms: 0,
                play: true,
                shuffle: Some(true),
                repeat: None,
                autoplay: false,
            };
        let liked = spec(
            Some("spotify:user:me:collection"),
            &[],
            Some("spotify:track:a"),
            None,
        );
        assert_eq!(
            load_summary(&liked),
            "spotify:user:me:collection at spotify:track:a, position 0 ms, shuffle Some(true), repeat None, playing"
        );
        let list = spec(None, &["spotify:track:a", "spotify:track:b"], None, Some(1));
        assert_eq!(
            load_summary(&list),
            "2 songs at #1, position 0 ms, shuffle Some(true), repeat None, playing"
        );
    }

    #[test]
    fn an_api_key_stays_out_of_debug_output() {
        let key = ApiKey("0123456789abcdef".into());
        assert_eq!(format!("{key:?}"), "ApiKey(..)");
    }

    #[test]
    fn the_audio_settings_replace_their_own_prefs_and_keep_soloists() {
        let prefs = "storage.size=1024\naudio.normalize_v2=true\naudio.play_bitrate_enumeration=5\naudio.gapless_migrated_to_ps=true\n";
        assert_eq!(
            prefs_with(prefs, 160, false),
            "storage.size=1024\naudio.gapless_migrated_to_ps=true\n\
             audio.normalize_v2=false\n\
             audio.play_bitrate_enumeration=3\n\
             audio.play_bitrate_non_metered_enumeration=3\n\
             audio.play_bitrate_non_metered_migrated=true\n"
        );
        assert!(prefs_with("", 96, true).contains("audio.play_bitrate_enumeration=2\n"));
        // Never lossless: anything else is 320 kbps.
        assert!(prefs_with("", 320, true).contains("audio.play_bitrate_enumeration=4\n"));
        assert!(prefs_with("", 1411, true).contains("audio.play_bitrate_enumeration=4\n"));
    }

    #[test]
    fn the_audio_settings_reach_every_prefs_file() {
        let dir =
            std::env::temp_dir().join(format!("spotifast-soloist-prefs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let user = dir.join("settings/Users/someone-user");
        std::fs::create_dir_all(&user).unwrap();
        std::fs::write(user.join("prefs"), "audio.normalize_v2=false\nother=1\n").unwrap();
        write_prefs(&dir, 96, true).unwrap();
        for file in [dir.join("settings/prefs"), user.join("prefs")] {
            let prefs = std::fs::read_to_string(&file).unwrap();
            assert!(prefs.contains("audio.normalize_v2=true\n"), "{prefs}");
            assert!(
                prefs.contains("audio.play_bitrate_enumeration=2\n"),
                "{prefs}"
            );
        }
        assert!(
            std::fs::read_to_string(user.join("prefs"))
                .unwrap()
                .starts_with("other=1\n")
        );
        std::fs::remove_dir_all(&dir).unwrap();
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
        // Asked again, the saved fingerprint spares the download.
        let program = find_binary_in(&dir).unwrap();
        assert!(program.parent().unwrap().join(ETAG_FILE).is_file());
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
    fn a_seek_counts_once_its_song_reports_a_position_near_it() {
        let song = "spotify:track:2JRo0gjbX4GrCqBYdRohoo";
        let waited = Duration::from_secs(1);
        let mut state = LocalState::default();
        assert!(!seek_landed(&state, 90_000, None, waited), "nothing plays");
        apply_event(&mut state, &track_event(), 0);
        state.loading = true;
        state.position_ms = 90_000;
        assert!(
            !seek_landed(&state, 90_000, Some(song), waited),
            "still loading"
        );
        state.loading = false;
        state.position_ms = 0;
        assert!(
            !seek_landed(&state, 90_000, Some(song), waited),
            "the seek was dropped"
        );
        state.position_ms = 150_000;
        assert!(
            !seek_landed(&state, 90_000, Some(song), waited),
            "still where an earlier play was"
        );
        state.position_ms = 89_500;
        assert!(seek_landed(&state, 90_000, Some(song), waited));
        assert!(
            seek_landed(&state, 90_000, None, waited),
            "a context's song"
        );
        assert!(
            !seek_landed(&state, 90_000, Some("spotify:track:other"), waited),
            "another song"
        );
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
    fn only_another_active_device_takes_playback_from_soloist() {
        assert_eq!(plays_elsewhere("solo", Some("phone"), false), Some(true));
        assert_eq!(plays_elsewhere("solo", Some("solo"), false), Some(false));
        assert_eq!(plays_elsewhere("solo", None, false), None);
        assert_eq!(plays_elsewhere("", Some("phone"), false), None);
    }

    #[test]
    fn a_play_asked_here_outweighs_spotify_still_naming_another_device() {
        // Spotify has not caught up with the play yet: nothing changes.
        assert_eq!(plays_elsewhere("solo", Some("phone"), true), None);
        // It has: Soloist plays.
        assert_eq!(plays_elsewhere("solo", Some("solo"), true), Some(false));
    }

    #[test]
    fn soloist_events_do_not_make_it_the_player_while_another_device_plays() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let elsewhere = AtomicBool::new(false);
        let mut state = LocalState::default();
        apply_event_from(&mut state, &track_event(), 0, &elsewhere);
        let paused = json!({ "type": "playback_changed", "status": "paused" });
        apply_event_from(&mut state, &paused, 0, &elsewhere);
        assert!(state.is_active());

        elsewhere.store(true, Ordering::SeqCst);
        apply_event_from(&mut state, &paused, 0, &elsewhere);
        assert_eq!(state.playback, Playback::Stopped);
        assert!(!state.is_active());
        assert!(state.track.is_some());

        let active = json!({ "type": "playback_state", "status": "playing", "is_active": true });
        apply_event_from(&mut state, &active, 0, &elsewhere);
        assert!(!elsewhere.load(Ordering::SeqCst));
        assert_eq!(state.playback, Playback::Playing);
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
