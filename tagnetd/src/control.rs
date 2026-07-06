//! Daemon local control endpoint (portability plan section 7).
//!
//! On Linux the sync engine and the DB live in a long-running (systemd) daemon,
//! while the UI is a *separate* process. Because [`FileDatabase`] is
//! single-owner, the UI cannot open the DB itself — it must ask the daemon.
//! This module is that channel.
//!
//! ## Transport
//!
//! A **Unix-domain-socket** control listener at
//! [`paths::control_socket_path`](crate::paths::control_socket_path) (the fixed
//! `/run/tagnet/tagnet.sock`) — **not** a TCP port. Security is entirely
//! filesystem permissions: the runtime directory `/run/tagnet` is created
//! `0700` and owned by the service user (via `RuntimeDirectory=tagnet` in the
//! systemd unit), so only that user can connect and nothing is exposed on any
//! network interface. There is therefore **no auth handshake** for local
//! control (unlike the ed25519 peer handshake on the peer-sync port).
//!
//! The existing WebSocket/`Frame` framing (tokio + tokio-tungstenite) is reused
//! over the [`UnixListener`], so the networking code stays unified. The wire
//! payloads, however, are a **distinct message category** from the peer
//! `Change`/`Sync` protocol: peer framing is about *sync*; control framing
//! ([`ControlFrame`]) carries the section-5 API requests/responses/events.
//!
//! ## Relationship to the peer-sync port
//!
//! The `0.0.0.0:{listen_port}` listener is unchanged — it remains the remote
//! peer-sync port. UI control is **never** routed through it. A local UI client
//! is conceptually "another kind of client" of the daemon: it issues
//! queries/commands and subscribes to the change stream, reusing the same
//! broadcast plumbing (`event_sender` / [`Api::subscribe`]) that
//! `forward_to_peers` uses for peers.
//!
//! ## Two halves
//!
//! - [`serve_control`] is the **daemon side**: accepts connections, decodes
//!   [`ControlRequest`]s, dispatches them to the in-process [`Api`], and streams
//!   [`ApiEvent`]s back as [`ControlFrame::Event`].
//! - [`IpcClientBackend`] is the **client side** (portability plan section 6's
//!   IPC-client backend): it connects to the socket, serialises API calls, and
//!   returns results/events. It implements [`TransportBackend`], so the Dart UI
//!   (via `flutter_rust_bridge`) and the `tagnet` CLI talk to it exactly as they
//!   would the in-process backend.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use serde::{Deserialize, Serialize};
use tagnet_core::{FileId, FileInfo, TagId};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::{Mutex, oneshot},
};
use tokio_tungstenite::{
    WebSocketStream, tungstenite::client::IntoClientRequest, tungstenite::protocol::Message,
};
use tokio_util::sync::CancellationToken;

use crate::{
    api::{Api, ApiError, ApiEvent},
    database::{SubtagRule, Tag},
    transport::{EventStream, TransportBackend},
};

// --- Wire protocol (plan 5.3/5.4/5.5 over the control socket) ----------------

/// A UI-facing API call, sent by a control client to the daemon.
///
/// One variant per [`Api`] method. Requests are matched to their
/// [`ControlResponse`] by the [`ControlFrame::Request`] `id`, so multiple calls
/// may be in flight on one connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlRequest {
    // Reads (plan 5.3).
    ListTags,
    ListFiles,
    /// Resolve a full-or-short file id prefix to a single `FileId`. Answered
    /// with [`ControlResponse::FileId`] (or an `Error`).
    ResolveFileId {
        prefix: String,
    },
    /// Resolve a full-or-short tag id prefix to a single `TagId`. Answered with
    /// [`ControlResponse::TagId`] (or an `Error`).
    ResolveTagId {
        prefix: String,
    },
    TagsForFile {
        file_id: FileId,
        subtag_rule: SubtagRule,
    },
    FilesForTag {
        tag_id: TagId,
        subtag_rule: SubtagRule,
    },
    SubtagsForTag {
        tag_id: TagId,
        subtag_rule: SubtagRule,
    },
    TagsForTag {
        tag_id: TagId,
        subtag_rule: SubtagRule,
    },
    // Writes (plan 5.4).
    CreateTag {
        name: String,
        color: String,
    },
    DeleteTag {
        tag_id: TagId,
    },
    RenameTag {
        tag_id: TagId,
        name: String,
    },
    SetTagColor {
        tag_id: TagId,
        color: String,
    },
    UploadFile {
        path_name: String,
        content: Vec<u8>,
        tags: Vec<TagId>,
    },
    /// Replace an existing file's content. Answered with
    /// [`ControlResponse::Ok`].
    EditFile {
        file_id: FileId,
        content: Vec<u8>,
    },
    /// Fetch a file's bytes on demand (from a peer if not local). Answered with
    /// [`ControlResponse::FileContent`] or an error. `expected_hash` gates which
    /// content is accepted.
    FetchFile {
        file_id: FileId,
        expected_hash: String,
    },
    /// Resolve a file's absolute on-disk path if present locally. Answered with
    /// [`ControlResponse::LocalPath`].
    LocalPathForFile {
        file_id: FileId,
    },
    DeleteFile {
        file_id: FileId,
    },
    MoveFile {
        file_id: FileId,
        logical_path: String,
    },
    TagFile {
        tag_id: TagId,
        file_id: FileId,
    },
    UntagFile {
        tag_id: TagId,
        file_id: FileId,
    },
    TagTag {
        parent_id: TagId,
        subtag_id: TagId,
    },
    UntagTag {
        parent_id: TagId,
        subtag_id: TagId,
    },
    /// Subscribe to the event stream. After this is accepted, the daemon starts
    /// emitting [`ControlFrame::Event`]s on this connection; the response is
    /// [`ControlResponse::Subscribed`].
    Subscribe,
}

/// The result of a [`ControlRequest`], returned as [`ControlFrame::Response`].
///
/// Every variant is either the success payload of the matching request or the
/// single serialisable [`ApiError`] (plan 5.5). The client maps these back onto
/// the [`TransportBackend`] return types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    Tags(Vec<Tag>),
    Files(Vec<FileInfo>),
    TagIds(Vec<TagId>),
    FileIds(Vec<FileId>),
    TagId(TagId),
    FileId(FileId),
    /// The bytes of a fetched file (answer to [`ControlRequest::FetchFile`]).
    FileContent(Vec<u8>),
    /// A file's absolute on-disk path, or `None` if not present locally (answer
    /// to [`ControlRequest::LocalPathForFile`]).
    LocalPath(Option<PathBuf>),
    /// A write/command that returns no payload succeeded.
    Ok,
    /// The subscription was established; events will follow on this connection.
    Subscribed,
    /// The request failed. Carries the single UI-facing error type.
    Error(ApiError),
}

/// Every control-socket message, in either direction.
///
/// This is the control counterpart to the peer [`Frame`](tagnet_core::state::Frame):
/// same WebSocket text framing, disjoint message set. `Request`/`Response`
/// carry a correlation `id`; `Event` is unsolicited (pushed after `Subscribe`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlFrame {
    /// Client -> daemon: an API call tagged with a per-connection request id.
    Request { id: u64, request: ControlRequest },
    /// Daemon -> client: the reply to the request with matching `id`.
    Response { id: u64, response: ControlResponse },
    /// Daemon -> client: an unsolicited event on a subscribed connection
    /// (plan 5.5).
    Event(ApiEvent),
}

// --- Daemon side -------------------------------------------------------------

/// Bind the control socket and serve control clients until `shutdown` fires
/// (portability plan section 7).
///
/// Binds a [`UnixListener`] at `socket_path`, removing any stale socket file
/// left by a previous run first (a leftover socket makes `bind` fail with
/// `AddrInUse`). Each accepted connection is handled on its own task by
/// [`handle_control_connection`]; all share the one in-process [`Api`].
///
/// This is wired into the section-3 shutdown path by the caller: it runs inside
/// the runtime driver's `select!` and returns when `shutdown` is cancelled, at
/// which point the socket file is removed on the way out.
pub async fn serve_control(
    api: Api,
    socket_path: PathBuf,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    // A leftover socket file (unclean shutdown) would make bind() fail with
    // AddrInUse. It is safe to remove: the runtime directory (/run/tagnet) is
    // owned by the single service user, and a second live daemon for the same
    // user is not a supported configuration. (systemd's RuntimeDirectory
    // normally clears this on start; removing it here also covers non-systemd
    // launches.)
    if socket_path.exists()
        && let Err(error) = tokio::fs::remove_file(&socket_path).await
    {
        log::warn!(
            "Failed to remove stale control socket {}: {error}",
            socket_path.display()
        );
    }

    let listener = UnixListener::bind(&socket_path)?;
    log::info!("Control socket listening on {}", socket_path.display());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                log::info!("Shutdown requested; stopping control socket");
                break;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _address)) => {
                        tokio::spawn(handle_control_connection(
                            api.clone(),
                            stream,
                            shutdown.child_token(),
                        ));
                    }
                    Err(error) => {
                        log::warn!("Control socket accept error: {error}");
                        break;
                    }
                }
            }
        }
    }

    // Best-effort cleanup so the next daemon start binds cleanly.
    let _ = tokio::fs::remove_file(&socket_path).await;
    Ok(())
}

/// Serve a single control client for the life of its connection.
///
/// Completes the WebSocket handshake over the Unix stream, then loops:
/// - decode inbound [`ControlFrame::Request`]s, dispatch to the [`Api`], and
///   reply with a [`ControlFrame::Response`];
/// - on [`ControlRequest::Subscribe`], subscribe to the [`Api`] event stream
///   and forward every [`ApiEvent`] as a [`ControlFrame::Event`].
///
/// Only one subscription per connection is needed (the UI subscribes once);
/// a second `Subscribe` simply replaces the stream.
async fn handle_control_connection(api: Api, stream: UnixStream, shutdown: CancellationToken) {
    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws_stream) => ws_stream,
        Err(error) => {
            log::warn!("Control WebSocket handshake failed: {error}");
            return;
        }
    };
    log::debug!("Control client connected");

    let (mut outgoing, mut incoming) = ws_stream.split();

    // Populated once the client sends `Subscribe`. `None` until then so an
    // un-subscribed connection never wakes on the event branch.
    let mut events: Option<EventStream> = None;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                log::debug!("Shutdown requested; closing control client");
                break;
            }
            // Forward live events to a subscribed client. `recv` on a `None`
            // stream is never polled because of the `if let` guard.
            event = async { events.as_mut().unwrap().recv().await }, if events.is_some() => {
                match event {
                    Some(event) => {
                        if let Err(error) =
                            send_control(&mut outgoing, &ControlFrame::Event(event)).await
                        {
                            log::debug!("Failed to push control event: {error}");
                            break;
                        }
                    }
                    None => {
                        // The runtime's event bus closed (shutdown).
                        break;
                    }
                }
            }
            inbound = incoming.next() => {
                let Some(message) = inbound else {
                    log::debug!("Control client closed the connection");
                    break;
                };
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        log::debug!("Control client read error: {error}");
                        break;
                    }
                };
                // Ignore non-data frames (ping/pong/close handled by the lib).
                if message.is_ping() || message.is_pong() || message.is_close() {
                    continue;
                }
                let text = message.to_string();
                let frame: ControlFrame = match serde_json::from_str(&text) {
                    Ok(frame) => frame,
                    Err(error) => {
                        log::warn!("Malformed control frame: {error}");
                        continue;
                    }
                };
                let ControlFrame::Request { id, request } = frame else {
                    // Clients only ever send `Request`s; ignore anything else.
                    log::warn!("Control client sent a non-request frame; ignoring");
                    continue;
                };

                let response = dispatch(&api, request, &mut events).await;
                if let Err(error) =
                    send_control(&mut outgoing, &ControlFrame::Response { id, response }).await
                {
                    log::debug!("Failed to send control response: {error}");
                    break;
                }
            }
        }
    }

    log::debug!("Control client disconnected");
}

/// Execute one [`ControlRequest`] against the in-process [`Api`] and produce a
/// [`ControlResponse`].
///
/// Most reads are synchronous on [`Api`] (each opens its own short-lived
/// read-only handle) and writes enqueue a `Change` and return immediately.
/// `FetchFile` and `LocalPathForFile` are genuinely async (a channel round-trip
/// into the daemon), so this function is `async`. Nothing holds a
/// `&FileDatabase` across an `.await`. `Subscribe` mutates the caller's `events`
/// slot.
async fn dispatch(
    api: &Api,
    request: ControlRequest,
    events: &mut Option<EventStream>,
) -> ControlResponse {
    match request {
        ControlRequest::ListTags => match api.list_tags() {
            Ok(tags) => ControlResponse::Tags(tags),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::ListFiles => match api.list_files() {
            Ok(files) => ControlResponse::Files(files),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::ResolveFileId { prefix } => match api.resolve_file_id(&prefix) {
            Ok(file_id) => ControlResponse::FileId(file_id),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::ResolveTagId { prefix } => match api.resolve_tag_id(&prefix) {
            Ok(tag_id) => ControlResponse::TagId(tag_id),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagsForFile {
            file_id,
            subtag_rule,
        } => match api.tags_for_file(file_id, subtag_rule) {
            Ok(tag_ids) => ControlResponse::TagIds(tag_ids),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::FilesForTag {
            tag_id,
            subtag_rule,
        } => match api.files_for_tag(tag_id, subtag_rule) {
            Ok(file_ids) => ControlResponse::FileIds(file_ids),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::SubtagsForTag {
            tag_id,
            subtag_rule,
        } => match api.subtags_for_tag(tag_id, subtag_rule) {
            Ok(tag_ids) => ControlResponse::TagIds(tag_ids),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagsForTag {
            tag_id,
            subtag_rule,
        } => match api.tags_for_tag(tag_id, subtag_rule) {
            Ok(tag_ids) => ControlResponse::TagIds(tag_ids),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::CreateTag { name, color } => match api.create_tag(name, color) {
            Ok(tag_id) => ControlResponse::TagId(tag_id),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::DeleteTag { tag_id } => match api.delete_tag(tag_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::RenameTag { tag_id, name } => match api.rename_tag(tag_id, name) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::SetTagColor { tag_id, color } => match api.set_tag_color(tag_id, color) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::UploadFile {
            path_name,
            content,
            tags,
        } => match api.upload_file(path_name, content, tags) {
            Ok(file_id) => ControlResponse::FileId(file_id),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::EditFile { file_id, content } => match api.edit_file(file_id, content) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::FetchFile {
            file_id,
            expected_hash,
        } => match api.fetch_file(file_id, expected_hash).await {
            Ok(content) => ControlResponse::FileContent(content),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::LocalPathForFile { file_id } => {
            match api.local_path_for_file(file_id).await {
                Ok(path) => ControlResponse::LocalPath(path),
                Err(error) => ControlResponse::Error(error),
            }
        }
        ControlRequest::DeleteFile { file_id } => match api.delete_file(file_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::MoveFile {
            file_id,
            logical_path,
        } => match api.move_file(file_id, logical_path) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagFile { tag_id, file_id } => match api.tag_file(tag_id, file_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::UntagFile { tag_id, file_id } => match api.untag_file(tag_id, file_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagTag {
            parent_id,
            subtag_id,
        } => match api.tag_tag(parent_id, subtag_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::UntagTag {
            parent_id,
            subtag_id,
        } => match api.untag_tag(parent_id, subtag_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::Subscribe => {
            *events = Some(EventStream::InProcess(api.subscribe()));
            ControlResponse::Subscribed
        }
    }
}

async fn send_control(
    outgoing: &mut SplitSink<WebSocketStream<UnixStream>, Message>,
    frame: &ControlFrame,
) -> Result<(), String> {
    let text = serde_json::to_string(frame).map_err(|error| format!("serialize: {error}"))?;
    outgoing
        .send(Message::text(text))
        .await
        .map_err(|error| format!("send: {error}"))
}

// --- Client side (plan section 6 IPC-client backend) -------------------------

/// The IPC-client transport backend (portability plan section 6).
///
/// A thin embedded Rust client that connects to the daemon's control socket,
/// serialises [`TransportBackend`] calls into [`ControlRequest`]s, and awaits
/// the matching [`ControlResponse`]. It is the Linux-daemon counterpart to
/// `InProcessBackend`; the Dart UI (and the `tagnet` CLI) never learn which they
/// hold.
///
/// A single background reader task owns the socket's read half and demultiplexes
/// inbound frames: [`ControlFrame::Response`]s are matched to waiters by `id`;
/// [`ControlFrame::Event`]s are pushed onto a broadcast channel that
/// [`subscribe`](TransportBackend::subscribe) taps.
#[derive(Clone)]
pub struct IpcClientBackend {
    inner: Arc<IpcClientInner>,
}

struct IpcClientInner {
    /// Write half of the control socket, behind a mutex so concurrent API
    /// calls serialise their frames without interleaving bytes.
    writer: Mutex<SplitSink<WebSocketStream<UnixStream>, Message>>,
    /// Correlation-id -> oneshot for the response of an in-flight request.
    pending: Mutex<HashMap<u64, oneshot::Sender<ControlResponse>>>,
    /// Monotonic request-id source.
    next_id: AtomicU64,
    /// Broadcast of events received on this connection. `subscribe` taps it.
    events: tokio::sync::broadcast::Sender<ApiEvent>,
}

impl IpcClientBackend {
    /// Connect to the daemon's default control socket
    /// ([`paths::control_socket_path`](crate::paths::control_socket_path)).
    pub async fn connect_default() -> Result<Self, ApiError> {
        Self::connect(crate::paths::control_socket_path()).await
    }

    /// Connect to the daemon control socket at `socket_path`.
    ///
    /// Establishes the WebSocket handshake over the Unix stream (the daemon
    /// speaks WS to reuse the peer framing code) and spawns the demultiplexing
    /// reader task. Errors are surfaced as [`ApiError::Transport`] so the UI
    /// sees the single API error type.
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self, ApiError> {
        let socket_path = socket_path.as_ref();
        let stream = UnixStream::connect(socket_path).await.map_err(|error| {
            ApiError::Transport(format!(
                "connect to control socket {}: {error}",
                socket_path.display()
            ))
        })?;

        // tokio-tungstenite needs a client request even over a Unix socket;
        // the URI is a placeholder the daemon ignores (there is no routing).
        let request = "ws://localhost/"
            .into_client_request()
            .map_err(|error| ApiError::Transport(format!("build ws request: {error}")))?;
        let (ws_stream, _response) = tokio_tungstenite::client_async(request, stream)
            .await
            .map_err(|error| ApiError::Transport(format!("control ws handshake: {error}")))?;

        let (outgoing, mut incoming) = ws_stream.split();

        let (events, _) = tokio::sync::broadcast::channel(1024);
        let inner = Arc::new(IpcClientInner {
            writer: Mutex::new(outgoing),
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            events: events.clone(),
        });

        // Reader task: demultiplex responses (to waiters) and events (to the
        // broadcast). Ends when the socket closes, waking any pending waiter
        // with a dropped sender (surfaced as a Transport error).
        let reader_inner = inner.clone();
        tokio::spawn(async move {
            while let Some(message) = incoming.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        log::debug!("Control client read error: {error}");
                        break;
                    }
                };
                if message.is_ping() || message.is_pong() || message.is_close() {
                    continue;
                }
                let text = message.to_string();
                let frame: ControlFrame = match serde_json::from_str(&text) {
                    Ok(frame) => frame,
                    Err(error) => {
                        log::warn!("Malformed control frame from daemon: {error}");
                        continue;
                    }
                };
                match frame {
                    ControlFrame::Response { id, response } => {
                        if let Some(sender) = reader_inner.pending.lock().await.remove(&id) {
                            let _ = sender.send(response);
                        } else {
                            log::warn!("Control response for unknown request id {id}");
                        }
                    }
                    ControlFrame::Event(event) => {
                        // Best-effort (plan 5.5): if no one is subscribed, drop.
                        let _ = reader_inner.events.send(event);
                    }
                    ControlFrame::Request { .. } => {
                        log::warn!("Daemon sent a request frame to a client; ignoring");
                    }
                }
            }
            // Socket closed: fail every outstanding request so callers unblock.
            reader_inner.pending.lock().await.clear();
            log::debug!("Control client reader task ended");
        });

        let client = Self { inner };

        // Subscribe to the daemon's event stream **once, up front**, for the
        // whole life of the connection. The daemon only forwards `ApiEvent`s to
        // a client after it receives a `Subscribe` request (see `dispatch`);
        // without this, the reader task above would never observe any
        // `ControlFrame::Event`, so `TransportBackend::subscribe` (which just
        // taps the local broadcast fed by that reader) would stay silent and
        // the UI would never live-update. This is the IPC-path counterpart to
        // the in-process backend, where `subscribe` reaches the live bus
        // directly. Repeated UI-side `subscribe` calls then share this one
        // daemon subscription.
        match client.call(ControlRequest::Subscribe).await? {
            ControlResponse::Subscribed => {}
            other => return Err(unexpected(other)),
        }

        Ok(client)
    }

    /// Send a request and await its response, correlating by a fresh id.
    async fn call(&self, request: ControlRequest) -> Result<ControlResponse, ApiError> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.inner.pending.lock().await.insert(id, sender);

        let frame = ControlFrame::Request { id, request };
        let text = serde_json::to_string(&frame)
            .map_err(|error| ApiError::Transport(format!("serialize request: {error}")))?;
        {
            let mut writer = self.inner.writer.lock().await;
            writer
                .send(Message::text(text))
                .await
                .map_err(|error| ApiError::Transport(format!("send request: {error}")))?;
        }

        receiver.await.map_err(|_| {
            ApiError::Transport("control connection closed before response".to_owned())
        })
    }
}

/// Collapse a [`ControlResponse`] error variant into the `Result` the
/// [`TransportBackend`] surface expects; otherwise report the unexpected shape.
fn unexpected(response: ControlResponse) -> ApiError {
    match response {
        ControlResponse::Error(error) => error,
        other => ApiError::Transport(format!("unexpected control response: {other:?}")),
    }
}

// The `TransportBackend` trait declares each method as
// `-> impl Future<...> + Send`; a plain `async fn` in the impl satisfies that
// (matching `InProcessBackend`). Each call maps 1:1 onto a `ControlRequest`
// and pattern-matches the expected `ControlResponse`, treating anything else
// (including `ControlResponse::Error`) via `unexpected`.
impl TransportBackend for IpcClientBackend {
    async fn list_tags(&self) -> Result<Vec<Tag>, ApiError> {
        match self.call(ControlRequest::ListTags).await? {
            ControlResponse::Tags(tags) => Ok(tags),
            other => Err(unexpected(other)),
        }
    }

    async fn list_files(&self) -> Result<Vec<FileInfo>, ApiError> {
        match self.call(ControlRequest::ListFiles).await? {
            ControlResponse::Files(files) => Ok(files),
            other => Err(unexpected(other)),
        }
    }

    async fn resolve_file_id(&self, prefix: String) -> Result<FileId, ApiError> {
        match self.call(ControlRequest::ResolveFileId { prefix }).await? {
            ControlResponse::FileId(file_id) => Ok(file_id),
            other => Err(unexpected(other)),
        }
    }

    async fn resolve_tag_id(&self, prefix: String) -> Result<TagId, ApiError> {
        match self.call(ControlRequest::ResolveTagId { prefix }).await? {
            ControlResponse::TagId(tag_id) => Ok(tag_id),
            other => Err(unexpected(other)),
        }
    }

    async fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self
            .call(ControlRequest::TagsForFile {
                file_id,
                subtag_rule,
            })
            .await?
        {
            ControlResponse::TagIds(tag_ids) => Ok(tag_ids),
            other => Err(unexpected(other)),
        }
    }

    async fn files_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<FileId>, ApiError> {
        match self
            .call(ControlRequest::FilesForTag {
                tag_id,
                subtag_rule,
            })
            .await?
        {
            ControlResponse::FileIds(file_ids) => Ok(file_ids),
            other => Err(unexpected(other)),
        }
    }

    async fn subtags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self
            .call(ControlRequest::SubtagsForTag {
                tag_id,
                subtag_rule,
            })
            .await?
        {
            ControlResponse::TagIds(tag_ids) => Ok(tag_ids),
            other => Err(unexpected(other)),
        }
    }

    async fn tags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self
            .call(ControlRequest::TagsForTag {
                tag_id,
                subtag_rule,
            })
            .await?
        {
            ControlResponse::TagIds(tag_ids) => Ok(tag_ids),
            other => Err(unexpected(other)),
        }
    }

    async fn create_tag(&self, name: String, color: String) -> Result<TagId, ApiError> {
        match self.call(ControlRequest::CreateTag { name, color }).await? {
            ControlResponse::TagId(tag_id) => Ok(tag_id),
            other => Err(unexpected(other)),
        }
    }

    async fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        match self.call(ControlRequest::DeleteTag { tag_id }).await? {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn rename_tag(&self, tag_id: TagId, name: String) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::RenameTag { tag_id, name })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn set_tag_color(&self, tag_id: TagId, color: String) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::SetTagColor { tag_id, color })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn upload_file(
        &self,
        path_name: String,
        content: Vec<u8>,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        match self
            .call(ControlRequest::UploadFile {
                path_name,
                content,
                tags,
            })
            .await?
        {
            ControlResponse::FileId(file_id) => Ok(file_id),
            other => Err(unexpected(other)),
        }
    }

    async fn edit_file(&self, file_id: FileId, content: Vec<u8>) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::EditFile { file_id, content })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<Vec<u8>, ApiError> {
        match self
            .call(ControlRequest::FetchFile {
                file_id,
                expected_hash,
            })
            .await?
        {
            ControlResponse::FileContent(content) => Ok(content),
            other => Err(unexpected(other)),
        }
    }

    async fn local_path_for_file(&self, file_id: FileId) -> Result<Option<PathBuf>, ApiError> {
        match self
            .call(ControlRequest::LocalPathForFile { file_id })
            .await?
        {
            ControlResponse::LocalPath(path) => Ok(path),
            other => Err(unexpected(other)),
        }
    }

    async fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        match self.call(ControlRequest::DeleteFile { file_id }).await? {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn move_file(&self, file_id: FileId, logical_path: String) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::MoveFile {
                file_id,
                logical_path,
            })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::TagFile { tag_id, file_id })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::UntagFile { tag_id, file_id })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn tag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::TagTag {
                parent_id,
                subtag_id,
            })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn untag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::UntagTag {
                parent_id,
                subtag_id,
            })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    fn subscribe(&self) -> EventStream {
        EventStream::Ipc(self.inner.events.subscribe())
    }
}
