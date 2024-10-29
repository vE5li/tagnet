//! The daemon ingest bus.
//!
//! Historically the runtime had a single `mpsc` channel carrying
//! `(Change, ChangeOrigin)` from every producer (the UI-facing [`Api`], the
//! sync-directory watcher, and inbound peer sessions) to the single
//! `handle_changes` task that is the sole DB writer.
//!
//! [`DaemonMessage`] widens that one channel into an enum so the *same* ordered
//! FIFO can also carry a request-reply message ([`DaemonMessage::Fetch`]) used
//! by `tagnet-cli edit` to pull a file's bytes from a peer on demand. Keeping it
//! on the existing channel (rather than a second channel + `select!`) preserves
//! the order of a producer's messages relative to each other — e.g. an edit
//! enqueued just before a fetch of the same file — and reuses the existing
//! clone/drain/shutdown wiring.
//!
//! The reply travels back out-of-band via the `oneshot` carried in the `Fetch`
//! variant, mirroring the `SyncDirectoryCommand::ReadFile { respond_to }` idiom.
//! This is what lets the control layer stay decoupled from the peer/sync
//! machinery: it only ever holds an [`Api`](crate::api::Api), which enqueues a
//! `Fetch` and awaits the `oneshot`; the recursive fetch engine that talks to
//! peers lives entirely in `handle_changes`/the peer sessions.

use tagnet_core::{
    FileId,
    state::{Change, ChangeOrigin},
};
use tokio::sync::oneshot;

/// A message on the daemon ingest bus.
///
/// One ordered channel, two message kinds: the historical fire-and-forget
/// change, and a request-reply fetch.
pub enum DaemonMessage {
    /// A mutation to apply. Fire-and-forget: no reply.
    Change(Change, ChangeOrigin),
    /// An on-demand request for a file's bytes (used by `tagnet-cli edit` when
    /// the file is not present locally). `handle_changes` seeds a local-origin
    /// entry in the pending-fetch table, floods `Sync::FetchRequest` to peers,
    /// and resolves `respond_to` when a matching `FetchFound` arrives (or with
    /// an error on timeout / exhaustion).
    Fetch {
        file_id: FileId,
        /// The BLAKE3 hex digest the requester expects; a peer's bytes are only
        /// accepted if they hash to this. Gates correctness across the flood
        /// and removes any need for divergence handling.
        expected_hash: String,
        respond_to: oneshot::Sender<Result<Vec<u8>, FetchError>>,
    },
}

impl DaemonMessage {
    /// Convenience constructor for the common fire-and-forget change case.
    pub fn change(change: Change, origin: ChangeOrigin) -> Self {
        DaemonMessage::Change(change, origin)
    }
}

/// Why an on-demand fetch failed.
#[derive(Debug, Clone)]
pub enum FetchError {
    /// No connected peer (directly or transitively) reported holding content
    /// that matches `expected_hash` before the timeout.
    NotAvailable,
    /// The fetch did not complete within the overall deadline.
    TimedOut,
    /// The runtime is shutting down; the request cannot be served.
    ShuttingDown,
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::NotAvailable => {
                formatter.write_str("file not available from any connected peer")
            }
            FetchError::TimedOut => formatter.write_str("fetch timed out"),
            FetchError::ShuttingDown => formatter.write_str("runtime is shutting down"),
        }
    }
}

impl std::error::Error for FetchError {}
