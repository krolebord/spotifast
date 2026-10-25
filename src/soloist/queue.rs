//! Soloist's queue, edited over Spotify Connect.
//!
//! Soloist's own API appends songs and nothing else: it cannot clear,
//! reorder or remove one, and it queues no episodes. Spotify's Connect state
//! service does all of that for any device. `add_to_queue` appends, and
//! `set_queue` replaces the upcoming list as a whole, given the queue
//! revision it was read at; a stale revision is refused. The idle librespot
//! device beside Soloist is told the active device's state, revision
//! included, so this reads Soloist's queue there.
//!
//! The app edits the queue as a clear followed by the songs again, as it
//! does librespot's. The writer takes such a run of commands together,
//! works out the queue they leave, appends the songs Soloist lacks, and
//! then puts the whole list in order with one `set_queue`.
//!
//! Only Soloist's queue is written here. While another device plays, the
//! app queues there itself, as with librespot; a run that still reaches
//! this writer is refused, and the app is told.

use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use librespot_connect::ClusterState;
use librespot_core::session::Session;
use librespot_protocol::player::{PlayerState, ProvidedTrack};
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};

use crate::player::{EngineEvent, Notify};

/// How long to wait for the rest of a run of commands sent together.
const GATHER: Duration = Duration::from_millis(50);
/// How long Spotify gets to report a change before the next one is read.
const SETTLE: Duration = Duration::from_secs(4);
/// `set_queue` attempts, each against a freshly read revision.
const ATTEMPTS: usize = 3;
/// The provider Spotify gives songs the listener queued.
const QUEUED: &str = "queue";

/// Another device plays: Soloist's queue is not the one playing.
#[derive(Debug)]
struct NotActive;

impl std::fmt::Display for NotActive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Soloist is not the active device")
    }
}

impl std::error::Error for NotActive {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Remove every queued song; the context's own songs stay.
    Clear,
    /// Queue a song or episode after the ones already queued.
    Add(String),
}

/// Applies queue commands to Soloist, one run after another.
pub struct Writer {
    ops: mpsc::UnboundedSender<Op>,
}

impl Writer {
    /// `states` and `cluster` come from the librespot device beside Soloist,
    /// whose Connect id is `to`. The task ends with the writer.
    pub fn spawn(
        session: Session,
        to: String,
        states: watch::Receiver<Option<PlayerState>>,
        cluster: watch::Receiver<ClusterState>,
        notify: Notify,
    ) -> Self {
        let (ops, receiver) = mpsc::unbounded_channel();
        let remote = Remote {
            session,
            to,
            states,
            cluster,
            notify,
        };
        tokio::spawn(run(remote, receiver));
        Self { ops }
    }

    pub fn send(&self, op: Op) -> Result<()> {
        self.ops
            .send(op)
            .map_err(|_| anyhow!("Soloist's queue writer has stopped"))
    }
}

struct Remote {
    session: Session,
    to: String,
    states: watch::Receiver<Option<PlayerState>>,
    cluster: watch::Receiver<ClusterState>,
    notify: Notify,
}

async fn run(mut remote: Remote, mut receiver: mpsc::UnboundedReceiver<Op>) {
    while let Some(op) = receiver.recv().await {
        let mut ops = vec![op];
        tokio::time::sleep(GATHER).await;
        while let Ok(op) = receiver.try_recv() {
            ops.push(op);
        }
        if let Err(error) = remote.apply(&ops).await {
            log::warn!("Soloist's queue was not updated: {error:#}");
            // Only a refusal is certain; anything else may still land.
            if error.is::<NotActive>() {
                (remote.notify)(EngineEvent::Error(
                    "Soloist isn't playing, so the queue wasn't changed.".into(),
                ));
            }
        }
    }
}

impl Remote {
    async fn apply(&mut self, ops: &[Op]) -> Result<()> {
        let state = self.state()?;
        let want = desired(&queued_uris(&state), ops);
        let adds = missing(&queued_uris(&state), &want);
        log::info!(
            "Soloist queue: {} commands leave {} songs queued; {} queued now, {} to add",
            ops.len(),
            want.len(),
            queued_uris(&state).len(),
            adds.len()
        );
        for uri in &adds {
            log::info!("Soloist queue: add_to_queue {uri}");
            let revision = self.revision();
            self.command(&json!({
                "command": { "endpoint": "add_to_queue", "track": { "uri": uri } }
            }))
            .await?;
            self.settle(&revision).await;
        }
        for attempt in 1..=ATTEMPTS {
            let state = self.state()?;
            if !missing(&queued_uris(&state), &want).is_empty() {
                // An append Spotify has not reported yet.
                if attempt == ATTEMPTS {
                    bail!("Spotify did not report the queued songs");
                }
                self.settle(&state.queue_revision).await;
                continue;
            }
            let Some(body) = set_queue(&state, &want) else {
                return Ok(());
            };
            log::info!(
                "Soloist queue: set_queue with {} next and {} previous songs at revision {}",
                state.next_tracks.len(),
                state.prev_tracks.len(),
                state.queue_revision
            );
            match self.command(&body).await {
                Ok(()) => {
                    self.settle(&state.queue_revision).await;
                    return Ok(());
                }
                Err(error) if attempt == ATTEMPTS => return Err(error),
                Err(error) => {
                    log::debug!("Soloist's queue moved on, reading it again: {error:#}");
                    self.settle(&state.queue_revision).await;
                }
            }
        }
        Ok(())
    }

    /// Soloist's player state, once Spotify reports Soloist as active.
    fn state(&self) -> Result<PlayerState> {
        if self.cluster.borrow().active_device_id.as_deref() != Some(self.to.as_str()) {
            return Err(NotActive.into());
        }
        self.states
            .borrow()
            .clone()
            .ok_or_else(|| anyhow!("Soloist's queue is not known yet"))
    }

    fn revision(&self) -> String {
        self.states
            .borrow()
            .as_ref()
            .map(|state| state.queue_revision.clone())
            .unwrap_or_default()
    }

    /// Waits until the queue revision is no longer `revision`, or a while.
    async fn settle(&mut self, revision: &str) {
        let states = &mut self.states;
        let changed = async {
            loop {
                let current = states
                    .borrow_and_update()
                    .as_ref()
                    .map(|state| state.queue_revision.clone());
                if current
                    .as_deref()
                    .is_some_and(|current| current != revision)
                {
                    return;
                }
                if states.changed().await.is_err() {
                    return;
                }
            }
        };
        let _ = tokio::time::timeout(SETTLE, changed).await;
    }

    async fn command(&self, body: &Value) -> Result<()> {
        super::connect_command(&self.session, self.session.device_id(), &self.to, body).await
    }
}

/// The songs the listener queued, in the order they will play.
fn queued_uris(state: &PlayerState) -> Vec<String> {
    state
        .next_tracks
        .iter()
        .filter(|track| track.provider == QUEUED)
        .map(|track| track.uri.clone())
        .collect()
}

/// The queue `ops` leave, starting from `queued`.
fn desired(queued: &[String], ops: &[Op]) -> Vec<String> {
    let mut want = queued.to_vec();
    for op in ops {
        match op {
            Op::Clear => want.clear(),
            Op::Add(uri) => want.push(uri.clone()),
        }
    }
    want
}

/// The songs in `want` beyond the copies already in `queued`, in order.
fn missing(queued: &[String], want: &[String]) -> Vec<String> {
    let mut available = queued.to_vec();
    want.iter()
        .filter(|uri| match available.iter().position(|have| have == *uri) {
            Some(index) => {
                available.remove(index);
                false
            }
            None => true,
        })
        .cloned()
        .collect()
}

/// The `set_queue` that leaves `want` queued, or `None` when it already is.
/// Every song in `want` is already queued; each keeps its own entry, and the
/// context's songs keep theirs, after the queue.
fn set_queue(state: &PlayerState, want: &[String]) -> Option<Value> {
    if queued_uris(state) == want {
        return None;
    }
    let mut available: Vec<&ProvidedTrack> = state
        .next_tracks
        .iter()
        .filter(|track| track.provider == QUEUED)
        .collect();
    let mut next: Vec<Value> = Vec::with_capacity(state.next_tracks.len());
    for uri in want {
        let index = available.iter().position(|track| &track.uri == uri)?;
        next.push(entry(available.remove(index)));
    }
    next.extend(
        state
            .next_tracks
            .iter()
            .filter(|track| track.provider != QUEUED)
            .map(entry),
    );
    let prev: Vec<Value> = state.prev_tracks.iter().map(entry).collect();
    Some(json!({ "command": {
        "endpoint": "set_queue",
        "next_tracks": next,
        "prev_tracks": prev,
        "queue_revision": state.queue_revision,
    }}))
}

fn entry(track: &ProvidedTrack) -> Value {
    json!({
        "uri": track.uri,
        "uid": track.uid,
        "provider": track.provider,
        "metadata": track.metadata,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn track(uri: &str, uid: &str, provider: &str) -> ProvidedTrack {
        ProvidedTrack {
            uri: uri.into(),
            uid: uid.into(),
            provider: provider.into(),
            ..ProvidedTrack::default()
        }
    }

    fn state(next: Vec<ProvidedTrack>) -> PlayerState {
        PlayerState {
            next_tracks: next,
            prev_tracks: vec![track("spotify:track:p", "p0", "context")],
            queue_revision: "41".into(),
            ..PlayerState::default()
        }
    }

    fn uris(list: &[&str]) -> Vec<String> {
        list.iter().map(|uri| (*uri).to_string()).collect()
    }

    #[test]
    fn a_clear_and_re_add_leaves_the_songs_in_their_new_order() {
        let ops = [
            Op::Clear,
            Op::Add("spotify:track:b".into()),
            Op::Add("spotify:track:a".into()),
        ];
        let want = desired(&uris(&["spotify:track:a", "spotify:track:b"]), &ops);
        assert_eq!(want, uris(&["spotify:track:b", "spotify:track:a"]));
        assert!(missing(&uris(&["spotify:track:a", "spotify:track:b"]), &want).is_empty());
    }

    #[test]
    fn appends_need_only_the_new_songs_and_count_repeats() {
        let queued = uris(&["spotify:track:a"]);
        let want = desired(
            &queued,
            &[
                Op::Add("spotify:track:a".into()),
                Op::Add("spotify:episode:e".into()),
            ],
        );
        assert_eq!(
            missing(&queued, &want),
            uris(&["spotify:track:a", "spotify:episode:e"])
        );
    }

    #[test]
    fn set_queue_reorders_queued_entries_and_keeps_the_context() {
        let state = state(vec![
            track("spotify:track:a", "q1", QUEUED),
            track("spotify:track:b", "q2", QUEUED),
            track("spotify:track:c", "c1", "context"),
        ]);
        let body = set_queue(&state, &uris(&["spotify:track:b", "spotify:track:a"])).unwrap();
        let command = &body["command"];
        assert_eq!(command["endpoint"], "set_queue");
        assert_eq!(command["queue_revision"], "41");
        let next: Vec<(&str, &str)> = command["next_tracks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| (t["uri"].as_str().unwrap(), t["uid"].as_str().unwrap()))
            .collect();
        assert_eq!(
            next,
            [
                ("spotify:track:b", "q2"),
                ("spotify:track:a", "q1"),
                ("spotify:track:c", "c1"),
            ]
        );
        assert_eq!(command["prev_tracks"][0]["uid"], "p0");
    }

    #[test]
    fn set_queue_drops_removed_songs_and_keeps_one_of_two_copies() {
        let state = state(vec![
            track("spotify:track:a", "q1", QUEUED),
            track("spotify:track:b", "q2", QUEUED),
            track("spotify:track:a", "q3", QUEUED),
            track("spotify:track:c", "c1", "context"),
        ]);
        let body = set_queue(&state, &uris(&["spotify:track:a"])).unwrap();
        let next: Vec<&str> = body["command"]["next_tracks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["uid"].as_str().unwrap())
            .collect();
        assert_eq!(next, ["q1", "c1"]);
    }

    #[test]
    fn clearing_removes_every_queued_song() {
        let state = state(vec![
            track("spotify:track:a", "q1", QUEUED),
            track("spotify:track:c", "c1", "context"),
        ]);
        let want = desired(&queued_uris(&state), &[Op::Clear]);
        let body = set_queue(&state, &want).unwrap();
        assert_eq!(body["command"]["next_tracks"][0]["uid"], "c1");
        assert_eq!(body["command"]["next_tracks"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn nothing_is_sent_when_the_queue_is_already_as_wanted() {
        let state = state(vec![
            track("spotify:track:a", "q1", QUEUED),
            track("spotify:track:c", "c1", "context"),
        ]);
        assert!(set_queue(&state, &uris(&["spotify:track:a"])).is_none());
        // A song not queued yet cannot be placed.
        assert!(set_queue(&state, &uris(&["spotify:track:z"])).is_none());
    }
}
