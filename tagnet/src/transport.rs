//! Transport abstraction (portability plan section 6).
//!
//! The UI always talks to the same logical [section-5 API](crate::api). Only
//! the transport underneath differs:
//!
//! - **In-process** (Android, and optional single-process desktop): calls
//!   straight into [`Api`](crate::api::Api) / the change pipeline.
//! - **IPC-client** (Linux daemon mode): a thin embedded Rust client that
//!   connects to the daemon's control socket (section 7), serialises API calls,
//!   and returns results/events.
//!
//! This module defines the transport-agnostic surface as the
//! [`TransportBackend`] trait and provides the **in-process** implementation
//! ([`InProcessBackend`]). The IPC-client backend is deferred to section 7
//! (the daemon control socket it would talk to does not exist yet); see the
//! `Backend::Ipc` note below.
//!
//! `flutter_rust_bridge` always targets [`Backend`] on both platforms. On
//! Android it wraps the in-process backend; on Linux it will wrap the
//! IPC-client backend. The Dart UI never knows which — a single UI codebase is
//! preserved.
//!
//! ## Async surface
//!
//! Every method is `async`, even the reads which are synchronous on
//! [`Api`](crate::api::Api). This is deliberate: the IPC-client backend is
//! inherently asynchronous (a socket round-trip), so the shared trait must be
//! async for both. The in-process backend simply completes immediately.
//!
//! ## Dispatch
//!
//! [`Backend`] is an `enum` rather than a `dyn TransportBackend`. `async fn` in
//! traits is not yet dyn-compatible without extra machinery, and the set of
//! backends is small, closed, and known at compile time. The enum gives static
//! dispatch and lets the event stream carry a concrete, `Send` type across the
//! FFI boundary.

use std::future::Future;
use std::path::PathBuf;

use tagnet_core::{FileId, TagId, state::Change};
use tokio::sync::broadcast;

use crate::{
    api::{Api, ApiError, ApiEvent},
    database::{SubtagRule, Tag},
};
use tagnet_core::FileInfo;

/// The transport-agnostic UI-facing API (portability plan section 6).
///
/// This mirrors [`Api`](crate::api::Api) method-for-method, but every operation
/// is `async` so both the in-process backend (immediate) and the future
/// IPC-client backend (socket round-trip) can implement it behind one surface.
///
/// Implemented by [`InProcessBackend`] today and dispatched through the
/// [`Backend`] enum.
///
/// The returned futures are declared `+ Send` (rather than plain `async fn`)
/// so callers — notably `flutter_rust_bridge`, which spawns them on a
/// multi-threaded runtime — can move them across threads.
pub trait TransportBackend {
    // --- Read API (plan 5.3) -------------------------------------------------

    /// List all tags.
    fn list_tags(&self) -> impl Future<Output = Result<Vec<Tag>, ApiError>> + Send;

    /// List every currently-known file with its latest version info.
    fn list_files(&self) -> impl Future<Output = Result<Vec<FileInfo>, ApiError>> + Send;

    /// List the tags applied to `file_id`.
    fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> impl Future<Output = Result<Vec<TagId>, ApiError>> + Send;

    /// List the files carrying `tag_id` (the v1 single-tag "search").
    fn files_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> impl Future<Output = Result<Vec<FileId>, ApiError>> + Send;

    // --- Write API (plan 5.4) ------------------------------------------------

    /// Create a tag; returns the freshly-minted id.
    fn create_tag(
        &self,
        name: String,
        color: String,
    ) -> impl Future<Output = Result<TagId, ApiError>> + Send;

    /// Delete a tag.
    fn delete_tag(&self, tag_id: TagId) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Upload a file (in-memory `Vec<u8>` in v1); returns the freshly-minted id.
    fn upload_file(
        &self,
        path_name: String,
        content: Vec<u8>,
        tags: Vec<TagId>,
    ) -> impl Future<Output = Result<FileId, ApiError>> + Send;

    /// Replace the content of an existing file.
    fn edit_file(
        &self,
        file_id: FileId,
        content: Vec<u8>,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Fetch a file's bytes on demand (from a peer if not present locally).
    /// `expected_hash` gates which content is accepted.
    fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> impl Future<Output = Result<Vec<u8>, ApiError>> + Send;

    /// Resolve a file's absolute on-disk path if present locally, else `None`.
    fn local_path_for_file(
        &self,
        file_id: FileId,
    ) -> impl Future<Output = Result<Option<PathBuf>, ApiError>> + Send;

    /// Delete a file.
    fn delete_file(&self, file_id: FileId) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Apply `tag_id` to `file_id`.
    fn tag_file(
        &self,
        tag_id: TagId,
        file_id: FileId,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Remove `tag_id` from `file_id`.
    fn untag_file(
        &self,
        tag_id: TagId,
        file_id: FileId,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    // --- Event stream (plan 5.5) ---------------------------------------------

    /// Subscribe to the live change stream. Returns an [`EventStream`] whose
    /// [`recv`](EventStream::recv) yields [`ApiEvent`]s.
    fn subscribe(&self) -> EventStream;
}

/// The transport-agnostic event stream returned by
/// [`TransportBackend::subscribe`].
///
/// It normalises the two delivery mechanisms behind one type so the UI (and
/// `flutter_rust_bridge`) sees a single stream shape regardless of transport.
/// Poll it with [`EventStream::recv`].
pub enum EventStream {
    /// In-process delivery: a direct subscription to the runtime's broadcast
    /// bus. Each item is a raw [`Change`] the runtime applied; [`recv`] wraps
    /// it in [`ApiEvent::Changed`].
    InProcess(broadcast::Receiver<Change>),
    /// IPC delivery (section 7): a subscription to the control client's
    /// broadcast of [`ApiEvent`]s decoded off the control socket. The
    /// [`ApiEvent`]s are already fully-formed (the daemon sends
    /// [`ApiEvent::Changed`] per change; a reconnecting client would receive
    /// [`ApiEvent::Resynced`]).
    Ipc(broadcast::Receiver<ApiEvent>),
}

impl EventStream {
    /// Await the next event.
    ///
    /// Returns:
    /// - `Some(ApiEvent::Changed(_))` for each applied change,
    /// - `Some(ApiEvent::Resynced)` when the subscriber lagged past the
    ///   channel capacity (plan 5.5: the UI should re-fetch state), and
    /// - `None` once the stream is permanently closed (runtime shut down).
    pub async fn recv(&mut self) -> Option<ApiEvent> {
        match self {
            EventStream::InProcess(receiver) => match receiver.recv().await {
                Ok(change) => Some(ApiEvent::Changed(change)),
                // A slow subscriber fell behind: surface a resync request so
                // the UI re-fetches current state (plan 5.5) rather than
                // silently dropping changes.
                Err(broadcast::error::RecvError::Lagged(_)) => Some(ApiEvent::Resynced),
                // Sender dropped: the runtime is gone, the stream is done.
                Err(broadcast::error::RecvError::Closed) => None,
            },
            EventStream::Ipc(receiver) => match receiver.recv().await {
                // Already-decoded `ApiEvent`s arrive off the control socket.
                Ok(event) => Some(event),
                // The local client fell behind the daemon's event feed: same
                // remedy as in-process — ask the UI to re-fetch state.
                Err(broadcast::error::RecvError::Lagged(_)) => Some(ApiEvent::Resynced),
                // The control connection dropped (reader task ended).
                Err(broadcast::error::RecvError::Closed) => None,
            },
        }
    }
}

/// In-process transport backend (plan section 6).
///
/// Thinnest possible wrapper over [`Api`](crate::api::Api): every call
/// delegates directly, completing immediately. Used on Android (single
/// process) and for single-process desktop.
///
/// The wrapped reads perform blocking SQLite work; in-process that is
/// acceptable because each read opens and drops its own short-lived read-only
/// handle (see [`Api`](crate::api::Api) docs) and does not hold it across an
/// `.await`.
#[derive(Clone)]
pub struct InProcessBackend {
    api: Api,
}

impl InProcessBackend {
    /// Wrap an [`Api`](crate::api::Api) handle produced by
    /// [`run`](crate::run).
    pub fn new(api: Api) -> Self {
        Self { api }
    }

    /// Borrow the underlying [`Api`](crate::api::Api).
    pub fn api(&self) -> &Api {
        &self.api
    }
}

impl TransportBackend for InProcessBackend {
    async fn list_tags(&self) -> Result<Vec<Tag>, ApiError> {
        self.api.list_tags()
    }

    async fn list_files(&self) -> Result<Vec<FileInfo>, ApiError> {
        self.api.list_files()
    }

    async fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        self.api.tags_for_file(file_id, subtag_rule)
    }

    async fn files_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<FileId>, ApiError> {
        self.api.files_for_tag(tag_id, subtag_rule)
    }

    async fn create_tag(&self, name: String, color: String) -> Result<TagId, ApiError> {
        self.api.create_tag(name, color)
    }

    async fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        self.api.delete_tag(tag_id)
    }

    async fn upload_file(
        &self,
        path_name: String,
        content: Vec<u8>,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        self.api.upload_file(path_name, content, tags)
    }

    async fn edit_file(&self, file_id: FileId, content: Vec<u8>) -> Result<(), ApiError> {
        self.api.edit_file(file_id, content)
    }

    async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<Vec<u8>, ApiError> {
        self.api.fetch_file(file_id, expected_hash).await
    }

    async fn local_path_for_file(&self, file_id: FileId) -> Result<Option<PathBuf>, ApiError> {
        self.api.local_path_for_file(file_id).await
    }

    async fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        self.api.delete_file(file_id)
    }

    async fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.api.tag_file(tag_id, file_id)
    }

    async fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.api.untag_file(tag_id, file_id)
    }

    fn subscribe(&self) -> EventStream {
        EventStream::InProcess(self.api.subscribe())
    }
}

/// The transport-agnostic handle `flutter_rust_bridge` targets on every
/// platform (plan section 6).
///
/// An `enum` over the concrete backends, forwarding the whole
/// [`TransportBackend`] surface to whichever variant is present. The Dart UI
/// holds one `Backend` and never learns which transport backs it.
///
/// [`Backend::InProcess`] is used on Android / single-process desktop;
/// [`Backend::Ipc`] connects to the daemon control socket (section 7) on the
/// Linux daemon topology.
#[derive(Clone)]
pub enum Backend {
    /// In-process backend (Android / single-process desktop).
    InProcess(InProcessBackend),
    /// IPC-client backend talking to the daemon control socket (section 7).
    Ipc(crate::control::IpcClientBackend),
}

impl Backend {
    /// Build an in-process backend from an [`Api`](crate::api::Api) handle.
    pub fn in_process(api: Api) -> Self {
        Backend::InProcess(InProcessBackend::new(api))
    }

    /// Connect an IPC-client backend to the daemon's default control socket
    /// (section 7).
    pub async fn ipc_default() -> Result<Self, ApiError> {
        Ok(Backend::Ipc(
            crate::control::IpcClientBackend::connect_default().await?,
        ))
    }
}

impl TransportBackend for Backend {
    async fn list_tags(&self) -> Result<Vec<Tag>, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.list_tags().await,
            Backend::Ipc(backend) => backend.list_tags().await,
        }
    }

    async fn list_files(&self) -> Result<Vec<FileInfo>, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.list_files().await,
            Backend::Ipc(backend) => backend.list_files().await,
        }
    }

    async fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.tags_for_file(file_id, subtag_rule).await,
            Backend::Ipc(backend) => backend.tags_for_file(file_id, subtag_rule).await,
        }
    }

    async fn files_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<FileId>, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.files_for_tag(tag_id, subtag_rule).await,
            Backend::Ipc(backend) => backend.files_for_tag(tag_id, subtag_rule).await,
        }
    }

    async fn create_tag(&self, name: String, color: String) -> Result<TagId, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.create_tag(name, color).await,
            Backend::Ipc(backend) => backend.create_tag(name, color).await,
        }
    }

    async fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.delete_tag(tag_id).await,
            Backend::Ipc(backend) => backend.delete_tag(tag_id).await,
        }
    }

    async fn upload_file(
        &self,
        path_name: String,
        content: Vec<u8>,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.upload_file(path_name, content, tags).await,
            Backend::Ipc(backend) => backend.upload_file(path_name, content, tags).await,
        }
    }

    async fn edit_file(&self, file_id: FileId, content: Vec<u8>) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.edit_file(file_id, content).await,
            Backend::Ipc(backend) => backend.edit_file(file_id, content).await,
        }
    }

    async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<Vec<u8>, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.fetch_file(file_id, expected_hash).await,
            Backend::Ipc(backend) => backend.fetch_file(file_id, expected_hash).await,
        }
    }

    async fn local_path_for_file(&self, file_id: FileId) -> Result<Option<PathBuf>, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.local_path_for_file(file_id).await,
            Backend::Ipc(backend) => backend.local_path_for_file(file_id).await,
        }
    }

    async fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.delete_file(file_id).await,
            Backend::Ipc(backend) => backend.delete_file(file_id).await,
        }
    }

    async fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.tag_file(tag_id, file_id).await,
            Backend::Ipc(backend) => backend.tag_file(tag_id, file_id).await,
        }
    }

    async fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.untag_file(tag_id, file_id).await,
            Backend::Ipc(backend) => backend.untag_file(tag_id, file_id).await,
        }
    }

    fn subscribe(&self) -> EventStream {
        match self {
            Backend::InProcess(backend) => backend.subscribe(),
            Backend::Ipc(backend) => backend.subscribe(),
        }
    }
}
