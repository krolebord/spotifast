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
//! Soloist is not redistributable: it is found on this computer, never
//! bundled. Its API key is the user's own, read from an owner-only file and
//! handed to the process at startup; it is never logged.

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
/// How long pairing may take once the account was handed over.
const PAIR_TIMEOUT: Duration = Duration::from_secs(20);
/// ZeroConf browse length when looking for our own Soloist.
const BROWSE: Duration = Duration::from_secs(3);
/// The Soloist device id, remembered next to its stored session.
const DEVICE_ID_FILE: &str = "spotifast_device_id";
/// Volume drags are forwarded at most this often.
const PREVIEW_INTERVAL: Duration = Duration::from_millis(80);

/// Where Soloist lives and keeps its state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SoloistConfig {
    /// A path set in Settings, or `None` to look in the usual places.
    pub binary: Option<String>,
    /// Owner-only file holding the user's Soloist API key.
    pub key_file: PathBuf,
    /// Soloist's own data directory: device identity and stored session.
    pub data_dir: PathBuf,
    pub cache_dir: Option<PathBuf>,
    pub cache_mb: Option<u64>,
}

/// The Soloist executable: the configured path, else the first `soloist`
/// on `PATH` or in the usual install folders. Only reads file metadata.
pub fn find_binary(configured: Option<&str>) -> Option<PathBuf> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    if let Some(path) = configured.map(str::trim).filter(|path| !path.is_empty()) {
        let path = match (path.strip_prefix("~/"), &home) {
            (Some(rest), Some(home)) => home.join(rest),
            _ => PathBuf::from(path),
        };
        return path.is_file().then_some(path);
    }
    let mut dirs: Vec<PathBuf> = std::env::var_os("PATH")
        .map(|path| std::env::split_paths(&path).collect())
        .unwrap_or_default();
    if let Some(home) = &home {
        dirs.push(home.join(".local/bin"));
        dirs.push(home.join("bin"));
        dirs.push(home.join("soloist"));
        dirs.push(home.join("Applications/soloist"));
    }
    dirs.push("/opt/homebrew/bin".into());
    dirs.push("/usr/local/bin".into());
    dirs.into_iter()
        .map(|dir| dir.join(BINARY))
        .find(|path| path.is_file())
}

/// A running Soloist and the connection to its local API.
pub struct Soloist {
    commands: mpsc::UnboundedSender<Value>,
    device_id: String,
    stop: watch::Sender<bool>,
    ended: watch::Receiver<bool>,
    last_preview: Mutex<Option<Instant>>,
}

impl Soloist {
    /// Starts Soloist, connects to its API and makes sure it is signed in
    /// as the playback account. Its events update `state` from then on.
    pub async fn start(
        config: &SoloistConfig,
        device_name: &str,
        initial_volume: u16,
        credentials: &librespot_core::authentication::Credentials,
        state: Arc<Mutex<LocalState>>,
        notify: Notify,
    ) -> Result<Self> {
        let binary = find_binary(config.binary.as_deref()).ok_or_else(|| {
            anyhow!(
                "Spotify Soloist was not found. Install it from Spotify for Developers, or switch playback back to librespot in Settings."
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
            .kill_on_drop(true);
        if let Some(cache) = &config.cache_dir {
            command.arg("--cache-dir").arg(cache);
            if let Some(mb) = config.cache_mb {
                command.arg("--cache-size").arg(mb.max(100).to_string());
            }
        }
        log::info!("starting Soloist from {}", binary.display());
        let mut child = command.spawn().context("unable to start Spotify Soloist")?;
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
        let reader_state = Arc::clone(&state);
        let reader_notify = Arc::clone(&notify);
        let reader_ended = ended_tx.clone();
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
                    let _ = auth_tx.send(Some(event["logged_in"].as_bool().unwrap_or(false)));
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
                    reader_notify(EngineEvent::State(snapshot));
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
                            "This Spotify Soloist build has expired. Download a newer build from Spotify for Developers."
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
            device_id: String::new(),
            stop,
            ended,
            last_preview: Mutex::new(None),
        };
        match soloist
            .sign_in(config, device_name, credentials, &mut auth)
            .await
        {
            Ok(device_id) => {
                soloist.device_id = device_id;
                Ok(soloist)
            }
            Err(error) => {
                soloist.shutdown();
                Err(error)
            }
        }
    }

    /// Waits for a stored session, pairs over ZeroConf without one, and
    /// returns Soloist's Connect device id.
    async fn sign_in(
        &self,
        config: &SoloistConfig,
        device_name: &str,
        credentials: &librespot_core::authentication::Credentials,
        auth: &mut watch::Receiver<Option<bool>>,
    ) -> Result<String> {
        let restored = tokio::time::timeout(
            RESTORE_WAIT,
            auth.wait_for(|logged_in| *logged_in == Some(true)),
        )
        .await
        .is_ok_and(|seen| seen.is_ok());
        let id_file = config.data_dir.join(DEVICE_ID_FILE);
        let remembered = tokio::fs::read_to_string(&id_file)
            .await
            .ok()
            .map(|id| id.trim().to_string())
            .filter(|id| !id.is_empty());

        let device_id = if restored {
            match remembered {
                Some(id) => id,
                None => {
                    let name = device_name.to_string();
                    match tokio::task::spawn_blocking(move || own_receiver(&name)).await {
                        Ok(Ok((_, info))) => info.device_id,
                        // Playback still works through Soloist's own API;
                        // only loads that start partway into a list need
                        // the id, and they fall back to the list's start.
                        Ok(Err(error)) => {
                            log::warn!("Soloist's Connect device id is unknown: {error:#}");
                            String::new()
                        }
                        Err(error) => {
                            log::warn!("Soloist's Connect device id is unknown: {error}");
                            String::new()
                        }
                    }
                }
            }
        } else {
            log::info!("Soloist has no stored session; pairing it with the playback account");
            let name = device_name.to_string();
            let handed = crate::zeroconf::Credentials::from_playback(credentials)?;
            let device_id = tokio::task::spawn_blocking(move || -> Result<String> {
                let (receiver, info) = own_receiver(&name)?;
                let http = reqwest::blocking::Client::builder().no_proxy().build()?;
                crate::zeroconf::add_user(&http, &receiver, &info, &handed, "Spotifast")?;
                Ok(info.device_id)
            })
            .await
            .context("pairing stopped")?
            .map_err(|error| {
                anyhow!(
                    "Could not pair Soloist ({error:#}). Select \"{device_name}\" once in the Spotify app on a phone on the same network, then reconnect."
                )
            })?;
            tokio::time::timeout(
                PAIR_TIMEOUT,
                auth.wait_for(|logged_in| *logged_in == Some(true)),
            )
            .await
            .map_err(|_| anyhow!("Soloist did not sign in after pairing"))?
            .map_err(|_| anyhow!("Soloist closed its connection while pairing"))?;
            self.send("get_state", json!({}))?;
            device_id
        };
        if !device_id.is_empty() {
            let _ = tokio::fs::write(&id_file, &device_id).await;
        }
        Ok(device_id)
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

    /// Stops the Soloist process.
    pub fn shutdown(&self) {
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

/// Finds the Soloist this process started among ZeroConf receivers: the
/// one with our device name, preferring this computer's own addresses.
fn own_receiver(name: &str) -> Result<(crate::zeroconf::Receiver, crate::zeroconf::Info)> {
    let found = crate::zeroconf::discover(BROWSE)?;
    let mut candidates: Vec<_> = found
        .into_iter()
        .filter(|receiver| !receiver.cast && receiver.name == name)
        .collect();
    let local = local_addresses();
    candidates.sort_by_key(|receiver| !local.contains(&receiver.address));
    let http = reqwest::blocking::Client::builder().no_proxy().build()?;
    for receiver in candidates {
        if let Ok(info) = crate::zeroconf::get_info(&http, &receiver)
            && info.remote_name == name
            && !info.device_id.trim().is_empty()
        {
            return Ok((receiver, info));
        }
    }
    bail!("no Spotify Connect receiver named \"{name}\" answered on this network")
}

/// Loopback and the address this computer reaches the network from. Finding
/// the latter sends nothing: a UDP socket only picks its route on connect.
fn local_addresses() -> Vec<std::net::IpAddr> {
    let mut addresses = vec![
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
    ];
    if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0")
        && socket.connect("192.0.2.1:9").is_ok()
        && let Ok(local) = socket.local_addr()
    {
        addresses.push(local.ip());
    }
    addresses
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
                    "This Spotify Soloist build has expired. Download a newer build from Spotify for Developers."
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
        let _ = tokio::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .await;
        if tokio::time::timeout(Duration::from_secs(3), child.wait())
            .await
            .is_ok()
        {
            return;
        }
    }
    let _ = child.kill().await;
}

/// Soloist's output goes to the log, with the API key masked.
async fn forward_log(output: impl tokio::io::AsyncRead + Unpin, key: String) {
    let mut lines = tokio::io::BufReader::new(output).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        log::info!("soloist: {}", line.replace(&key, "[api key]"));
    }
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
