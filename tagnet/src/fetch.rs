//! On-demand recursive file fetch across the live peer tree.
//!
//! Used by `tagnet-cli edit <uuid>` when a file's bytes are not present
//! locally. A [`Sync::FetchRequest`](tagnet_core::state::Sync::FetchRequest) is
//! flooded across the tree of *live* peer connections, which is assumed acyclic
//! (there is no seen-set / TTL — a node simply never forwards a request back to
//! the peer it arrived from). Modelled as a "call stack across machines":
//!
//! - Each node, on receiving a `FetchRequest` it cannot satisfy locally,
//!   forwards it to all its neighbours **except the one it came from** and
//!   records a [`PendingFetch`] so it knows where to return the answer.
//! - The **first** `FetchFound` whose hash matches wins and unwinds back toward
//!   the origin immediately; the pending entry is removed and any later replies
//!   for the same `request_id` are dropped.
//! - A node returns `FetchMissing` to its parent only once **every** child it
//!   forwarded to has returned `FetchMissing` or timed out.
//!
//! Every hop arms its own timeout so a single dead branch cannot stall the
//! chain; the originating CLI also applies an overall deadline.
//!
//! The pending table is shared (`Arc<Mutex<..>>`) between `handle_changes`
//! (which seeds *local-origin* requests coming off the ingest bus) and the peer
//! session tasks (which seed *relayed* requests and deliver replies). This is
//! the only cross-session coordination the feature introduces; the control
//! layer never touches it — it only enqueues a `DaemonMessage::Fetch` and awaits
//! the `oneshot`.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use tagnet_core::{
    FileId, RequestId,
    state::{Frame, Sync as SyncMessage},
};
use tokio::sync::{Mutex, RwLock, oneshot};

use crate::{bus::FetchError, configuration::RuntimeConfiguration};

/// How long any single hop waits for its children before concluding the subtree
/// does not have the content. The originating CLI applies its own (larger)
/// overall deadline on top of this.
pub const HOP_TIMEOUT: Duration = Duration::from_secs(8);

/// Where the answer to a pending fetch must be delivered.
enum ReplyTarget {
    /// This request originated locally (from the ingest bus / CLI). Resolve the
    /// waiting caller directly.
    Local(oneshot::Sender<Result<Vec<u8>, FetchError>>),
    /// This request was relayed from a peer; the answer must be sent back to
    /// that peer (identified by public key).
    Peer(String),
}

/// A fetch this node is currently awaiting answers for.
struct PendingFetch {
    file_id: FileId,
    expected_hash: String,
    reply_to: ReplyTarget,
    /// Public keys of the peers we forwarded the request to and have not yet
    /// heard a terminal reply from. When this drains to empty (and no child
    /// answered `Found`), we report `Missing` to `reply_to`.
    children_outstanding: HashSet<String>,
}

/// The fetch subsystem: the shared table of in-flight fetches (keyed by
/// `request_id`) plus the peer runtime it routes frames through.
///
/// Cheap to clone (both fields are `Arc`s); every peer session and
/// `handle_changes` holds a clone so they can seed and resolve requests against
/// the same table. Owning `runtime_configuration` here (rather than passing it
/// into each call) lets the engine operations be plain methods.
#[derive(Clone)]
pub struct PendingFetches {
    inner: Arc<Mutex<HashMap<RequestId, PendingFetch>>>,
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
}

/// Reference to a peer's outbound frame queue plus its public key.
struct PeerOutbound {
    public_key: String,
    sender: tokio::sync::mpsc::UnboundedSender<Frame>,
}

impl PendingFetches {
    pub fn new(runtime_configuration: Arc<RwLock<RuntimeConfiguration>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            runtime_configuration,
        }
    }

    /// Snapshot every connected peer's outbound sender, optionally excluding one
    /// public key (the peer a relayed request came from — never echo it back).
    async fn connected_peers(&self, exclude: Option<&str>) -> Vec<PeerOutbound> {
        let runtime = self.runtime_configuration.read().await;
        runtime
            .peers
            .iter()
            .filter(|(public_key, _)| exclude != Some(public_key.as_str()))
            .filter_map(|(public_key, runtime_peer)| {
                runtime_peer.outbound.as_ref().map(|sender| PeerOutbound {
                    public_key: public_key.clone(),
                    sender: sender.clone(),
                })
            })
            .collect()
    }

    /// Resolve a single peer's outbound sender by public key.
    async fn peer_outbound(
        &self,
        public_key: &str,
    ) -> Option<tokio::sync::mpsc::UnboundedSender<Frame>> {
        let runtime = self.runtime_configuration.read().await;
        runtime
            .peers
            .get(public_key)
            .and_then(|runtime_peer| runtime_peer.outbound.clone())
    }

    /// Begin a fetch that originated locally (from the ingest bus / CLI).
    ///
    /// `have_local` is a hash-gated check against local sync directories: if it
    /// returns `Some(bytes)`, the file is already here and we resolve
    /// immediately. Otherwise we flood `FetchRequest` to all connected peers and
    /// register a pending entry; if there are no peers, we resolve
    /// `NotAvailable` at once.
    pub async fn start_local_fetch(
        &self,
        file_id: FileId,
        expected_hash: String,
        have_local: Option<Vec<u8>>,
        respond_to: oneshot::Sender<Result<Vec<u8>, FetchError>>,
    ) {
        if let Some(bytes) = have_local {
            let _ = respond_to.send(Ok(bytes));
            return;
        }

        let peers = self.connected_peers(None).await;
        if peers.is_empty() {
            let _ = respond_to.send(Err(FetchError::NotAvailable));
            return;
        }

        let request_id = RequestId::new();
        let children: HashSet<String> = peers.iter().map(|peer| peer.public_key.clone()).collect();

        {
            let mut table = self.inner.lock().await;
            table.insert(
                request_id,
                PendingFetch {
                    file_id,
                    expected_hash: expected_hash.clone(),
                    reply_to: ReplyTarget::Local(respond_to),
                    children_outstanding: children,
                },
            );
        }

        let request = Frame::Sync(SyncMessage::FetchRequest {
            request_id,
            file_id,
            expected_hash,
        });
        for peer in &peers {
            let _ = peer.sender.send(request.clone());
        }

        self.arm_hop_timeout(request_id);
    }

    /// Handle an inbound `FetchRequest` relayed from `from_public_key`.
    ///
    /// If `have_local` matches, answer `FetchFound` straight back to the sender.
    /// Otherwise forward to all peers except the sender and register a pending
    /// entry keyed by the request's own `request_id`. With no other peers to
    /// try, answer `FetchMissing` immediately.
    pub async fn handle_incoming_request(
        &self,
        from_public_key: &str,
        request_id: RequestId,
        file_id: FileId,
        expected_hash: String,
        have_local: Option<Vec<u8>>,
    ) {
        if let Some(content) = have_local {
            let content_hash = expected_hash.clone();
            if let Some(sender) = self.peer_outbound(from_public_key).await {
                let _ = sender.send(Frame::Sync(SyncMessage::FetchFound {
                    request_id,
                    file_id,
                    content,
                    content_hash,
                }));
            }
            return;
        }

        let peers = self.connected_peers(Some(from_public_key)).await;
        if peers.is_empty() {
            if let Some(sender) = self.peer_outbound(from_public_key).await {
                let _ = sender.send(Frame::Sync(SyncMessage::FetchMissing { request_id }));
            }
            return;
        }

        let children: HashSet<String> = peers.iter().map(|peer| peer.public_key.clone()).collect();
        {
            let mut table = self.inner.lock().await;
            // A duplicate request_id (would only happen on a cyclic graph, which
            // we assume away) is ignored: keep the first registration.
            table.entry(request_id).or_insert(PendingFetch {
                file_id,
                expected_hash: expected_hash.clone(),
                reply_to: ReplyTarget::Peer(from_public_key.to_owned()),
                children_outstanding: children,
            });
        }

        let request = Frame::Sync(SyncMessage::FetchRequest {
            request_id,
            file_id,
            expected_hash,
        });
        for peer in &peers {
            let _ = peer.sender.send(request.clone());
        }

        self.arm_hop_timeout(request_id);
    }

    /// Handle an inbound `FetchFound`. First-wins: resolve and remove the
    /// pending entry, delivering the bytes to its `reply_to`. Late duplicates
    /// find no entry and are dropped.
    pub async fn handle_incoming_found(
        &self,
        request_id: RequestId,
        file_id: FileId,
        content: Vec<u8>,
        content_hash: String,
    ) {
        let entry = {
            let mut table = self.inner.lock().await;
            table.remove(&request_id)
        };
        let Some(entry) = entry else {
            return;
        };

        // Defensive: only accept content that matches what this request expected.
        if content_hash != entry.expected_hash {
            log::warn!(
                "FetchFound for {} carried hash {} but {} was expected; dropping",
                request_id.to_string(),
                content_hash,
                entry.expected_hash
            );
            return;
        }

        match entry.reply_to {
            ReplyTarget::Local(sender) => {
                let _ = sender.send(Ok(content));
            }
            ReplyTarget::Peer(parent_public_key) => {
                if let Some(sender) = self.peer_outbound(&parent_public_key).await {
                    let _ = sender.send(Frame::Sync(SyncMessage::FetchFound {
                        request_id,
                        file_id,
                        content,
                        content_hash,
                    }));
                }
            }
        }
    }

    /// Handle an inbound `FetchMissing` from `from_public_key`. Removes that
    /// child from the pending entry; if it was the last outstanding child,
    /// reports `Missing` upward.
    pub async fn handle_incoming_missing(&self, from_public_key: &str, request_id: RequestId) {
        let resolved = {
            let mut table = self.inner.lock().await;
            match table.get_mut(&request_id) {
                Some(entry) => {
                    entry.children_outstanding.remove(from_public_key);
                    if entry.children_outstanding.is_empty() {
                        table.remove(&request_id)
                    } else {
                        None
                    }
                }
                None => None,
            }
        };

        if let Some(entry) = resolved {
            self.report_missing(request_id, entry).await;
        }
    }

    /// Deliver a negative answer to a pending entry's `reply_to`.
    async fn report_missing(&self, request_id: RequestId, entry: PendingFetch) {
        log::debug!(
            "Reporting fetch {} for file {} as missing",
            request_id.to_string(),
            entry.file_id.to_string()
        );
        match entry.reply_to {
            ReplyTarget::Local(sender) => {
                let _ = sender.send(Err(FetchError::NotAvailable));
            }
            ReplyTarget::Peer(parent_public_key) => {
                if let Some(sender) = self.peer_outbound(&parent_public_key).await {
                    let _ = sender.send(Frame::Sync(SyncMessage::FetchMissing { request_id }));
                }
            }
        }
    }

    /// Spawn a task that, after [`HOP_TIMEOUT`], forces resolution of
    /// `request_id` as missing if it is still pending (a child never answered).
    fn arm_hop_timeout(&self, request_id: RequestId) {
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(HOP_TIMEOUT).await;
            let entry = {
                let mut table = this.inner.lock().await;
                table.remove(&request_id)
            };
            if let Some(entry) = entry {
                log::debug!(
                    "Fetch {} timed out at a hop; reporting missing upward",
                    request_id.to_string()
                );
                this.report_missing(request_id, entry).await;
            }
        });
    }
}
