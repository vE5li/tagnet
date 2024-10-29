//! UI-facing API (portability plan section 5).
//!
//! This is the single, transport-agnostic API surface the UI talks to. It is
//! deliberately a **v1**: every operation maps 1:1 onto capabilities that
//! already exist in [`FileDatabase`](crate::database::FileDatabase) and the
//! change pipeline. See the portability plan section 5.6 for explicit
//! non-goals.
//!
//! ## Architecture (plan 5.1)
//!
//! The API is split into a read half and a write half because the core
//! enforces a single-writer model:
//!
//! - **Reads** open their own read-only [`FileDatabase`] handle from
//!   `main_db_path`, exactly as peer sessions do. A `&FileDatabase` is never
//!   held across an `.await`.
//! - **Writes** are expressed as [`Change`] values and pushed onto the ingest
//!   bus (`change_sender`). The single `handle_changes` task remains the only
//!   DB writer and performs idempotent persistence plus peer forwarding. This
//!   API adds no business logic and never writes the DB directly.
//!
//! Both process topologies (in-process on Android, IPC-to-daemon on Linux)
//! wrap this same [`Api`] handle; the Dart UI never knows which.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use tagnet_core::{
    FileId, FileInfo, LogicalPath, TagId,
    state::{Change, ChangeOrigin},
};
use tokio::sync::{broadcast, mpsc::UnboundedSender, oneshot};

use crate::bus::{DaemonMessage, FetchError};
use crate::database::{DatabaseError, FileDatabase, SubtagRule, Tag};
use crate::directory_manager::SyncDirectoryCommand;

/// Errors surfaced to the UI (plan 5.5).
///
/// A single serializable error type so the transport can carry one shape over
/// the wire. It wraps the crate's hand-rolled [`DatabaseError`] (which has no
/// `Display` and flattens most SQL failures) rather than leaking it raw, and
/// adds UI-facing variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApiError {
    /// Unknown `FileId`/`TagId`.
    NotFound,
    /// A caller-supplied argument was invalid (e.g. empty tag name).
    InvalidArgument(String),
    /// A database-layer failure.
    Database(DatabaseError),
    /// IPC-only: socket/protocol failure. Never produced in-process.
    Transport(String),
    /// An unexpected internal failure (e.g. a change could not be enqueued
    /// because the runtime is shutting down).
    Internal(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::NotFound => write!(formatter, "not found"),
            ApiError::InvalidArgument(message) => {
                write!(formatter, "invalid argument: {message}")
            }
            ApiError::Database(error) => write!(formatter, "database error: {error:?}"),
            ApiError::Transport(message) => write!(formatter, "transport error: {message}"),
            ApiError::Internal(message) => write!(formatter, "internal error: {message}"),
        }
    }
}

impl std::error::Error for ApiError {}

impl From<DatabaseError> for ApiError {
    fn from(error: DatabaseError) -> Self {
        match error {
            DatabaseError::MissingFile | DatabaseError::MissingTag => ApiError::NotFound,
            DatabaseError::InvalidTagName => {
                ApiError::InvalidArgument("invalid tag name".to_owned())
            }
            DatabaseError::InvalidColor => ApiError::InvalidArgument("invalid color".to_owned()),
            other => ApiError::Database(other),
        }
    }
}

impl From<FetchError> for ApiError {
    fn from(error: FetchError) -> Self {
        match error {
            // No peer had the content: surface as a plain not-found to the UI.
            FetchError::NotAvailable => ApiError::NotFound,
            FetchError::TimedOut | FetchError::ShuttingDown => {
                ApiError::Internal(error.to_string())
            }
        }
    }
}

/// A live update delivered on the API event stream (plan 5.5).
///
/// Delivery is **best-effort**, mirroring the in-process ingest bus. There is
/// no per-event replay or buffering. On (re)connection over IPC the transport
/// emits [`ApiEvent::Resynced`] first; the UI responds by re-fetching current
/// state via the read API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApiEvent {
    /// The stream (re)started; the UI should re-fetch current state. Produced
    /// by the transport layer on connect/reconnect, not by the change bus.
    Resynced,
    /// A change was applied to the store.
    Changed(Change),
}

/// The transport-agnostic UI-facing API handle.
///
/// Cheap to clone. Holds the pieces needed to serve reads (the DB path),
/// serve writes (the ingest-bus sender), and produce the event stream (a
/// broadcast subscription source). Constructed by [`run`](crate::run) and
/// wrapped by each transport backend (plan section 6).
#[derive(Clone)]
pub struct Api {
    main_db_path: PathBuf,
    change_sender: UnboundedSender<DaemonMessage>,
    /// Direct handle to the sync-directory manager, used only for read-only
    /// path lookups (`local_path_for_file`). Writes still go via
    /// `change_sender` and the `handle_changes` pipeline.
    command_sender: UnboundedSender<SyncDirectoryCommand>,
    events: broadcast::Sender<Change>,
}

impl Api {
    /// Build an API handle from the runtime's shared pieces.
    ///
    /// - `main_db_path`: the main DB path; each read opens its own read-only
    ///   handle on it (SQLite serialises file-level access).
    /// - `change_sender`: the ingest bus every mutation is pushed onto.
    /// - `command_sender`: the sync-directory manager command channel, used for
    ///   read-only path lookups.
    /// - `events`: the broadcast channel `handle_changes` publishes applied
    ///   changes to.
    pub fn new(
        main_db_path: PathBuf,
        change_sender: UnboundedSender<DaemonMessage>,
        command_sender: UnboundedSender<SyncDirectoryCommand>,
        events: broadcast::Sender<Change>,
    ) -> Self {
        Self {
            main_db_path,
            change_sender,
            command_sender,
            events,
        }
    }

    /// Open a fresh read-only DB handle for a single read call.
    ///
    /// `FileDatabase` is `Send + !Sync`; we never share one across `.await`,
    /// so each read opens its own handle and drops it before returning.
    fn open_read(&self) -> Result<FileDatabase, ApiError> {
        FileDatabase::initialize(&self.main_db_path).map_err(ApiError::from)
    }

    /// Enqueue a locally-originated change onto the ingest bus.
    ///
    /// `directory_path` in the [`ChangeOrigin::Local`] is a sentinel that must
    /// not match any configured sync directory, so `handle_changes` dispatches
    /// the change to every matching sync directory rather than skipping one as
    /// the "source". An empty path never matches a real sync-directory path.
    fn enqueue(&self, change: Change) -> Result<(), ApiError> {
        self.change_sender
            .send(DaemonMessage::Change(
                change,
                ChangeOrigin::Local {
                    directory_path: PathBuf::new(),
                },
            ))
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))
    }

    // --- Read API (plan 5.3) -------------------------------------------------

    /// List all tags. Backed by `FileDatabase::get_all_tags`.
    pub fn list_tags(&self) -> Result<Vec<Tag>, ApiError> {
        let database = self.open_read()?;
        Ok(database.get_all_tags()?.into_iter().collect())
    }

    /// List every currently-known file with its latest version info. Backed by
    /// `FileDatabase::get_all_files`.
    pub fn list_files(&self) -> Result<Vec<FileInfo>, ApiError> {
        let database = self.open_read()?;
        Ok(database.get_all_files()?)
    }

    /// List the tags applied to `file_id`. `subtag_rule` controls whether the
    /// tag hierarchy is walked. Backed by `FileDatabase::tag_ids_for_file`.
    pub fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        let database = self.open_read()?;
        Ok(database
            .tag_ids_for_file(file_id, subtag_rule)?
            .into_iter()
            .collect())
    }

    /// List the files carrying `tag_id`. This is the v1 "search": single-tag
    /// only. `subtag_rule` controls hierarchy traversal. Backed by
    /// `FileDatabase::file_ids_for_tag`.
    pub fn files_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<FileId>, ApiError> {
        let database = self.open_read()?;
        Ok(database
            .file_ids_for_tag(tag_id, subtag_rule)?
            .into_iter()
            .collect())
    }

    // --- Write API (plan 5.4) ------------------------------------------------

    /// Create a tag. Mints a fresh `TagId` and enqueues `Change::TagAdded`;
    /// the id is returned immediately (persistence is asynchronous — observe
    /// the event stream for confirmation).
    pub fn create_tag(&self, name: String, color: String) -> Result<TagId, ApiError> {
        if name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("tag name is empty".to_owned()));
        }
        let tag_id = TagId::new();
        self.enqueue(Change::TagAdded {
            tag_id,
            tag_name: name,
            metadata: None,
        })?;
        Ok(tag_id)
    }

    /// Delete a tag. Enqueues `Change::TagRemoved`.
    pub fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        self.enqueue(Change::TagRemoved { tag_id })
    }

    /// Upload a file. `content` is passed in memory as `Vec<u8>` (v1; streaming
    /// large media is deferred). Mints a fresh `FileId`, computes the blake3
    /// content hash exactly as the directory manager does, and enqueues
    /// `Change::FileAdded`. Returns the id immediately.
    pub fn upload_file(
        &self,
        path_name: String,
        content: Vec<u8>,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        if path_name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("path is empty".to_owned()));
        }
        let file_id = FileId::new();
        let content_hash = blake3::hash(&content).to_hex().to_string();
        // A caller-supplied name is a logical path directly (the user is naming
        // the file, not pointing at an on-disk location in a sync directory).
        self.enqueue(Change::FileAdded {
            file_id,
            logical_path: LogicalPath::new(path_name),
            content,
            content_hash,
            tags,
        })?;
        Ok(file_id)
    }

    /// Replace the content of an existing file. Computes the blake3 hash (as the
    /// directory manager does) and enqueues `Change::FileChanged`. Fire-and-
    /// forget: returns once enqueued, persistence is asynchronous.
    ///
    /// Used by `tagnet-cli edit` to write back edited bytes.
    pub fn edit_file(&self, file_id: FileId, content: Vec<u8>) -> Result<(), ApiError> {
        let content_hash = blake3::hash(&content).to_hex().to_string();
        self.enqueue(Change::FileChanged {
            file_id,
            content,
            content_hash,
        })
    }

    /// Delete a file. Enqueues `Change::FileDeleted`.
    pub fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        self.enqueue(Change::FileDeleted { file_id })
    }

    /// The overall deadline a caller waits for an on-demand fetch to complete.
    /// Must exceed [`crate::fetch::HOP_TIMEOUT`] so intermediate hops can time
    /// out and report before this outer deadline fires.
    const FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    /// Fetch a file's bytes on demand, from a peer if not present locally.
    ///
    /// Enqueues a [`DaemonMessage::Fetch`] onto the ingest bus; `handle_changes`
    /// checks the local sync directories first (hash-gated) and, failing that,
    /// floods a recursive `Sync::FetchRequest` across the live peer tree. Awaits
    /// the reply with an overall timeout. `expected_hash` gates which content is
    /// accepted; the caller obtains it from the file's known metadata
    /// (`FileInfo::content_hash`).
    pub async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<Vec<u8>, ApiError> {
        let (respond_to, response) = oneshot::channel();
        self.change_sender
            .send(DaemonMessage::Fetch {
                file_id,
                expected_hash,
                respond_to,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;

        match tokio::time::timeout(Self::FETCH_TIMEOUT, response).await {
            Ok(Ok(Ok(bytes))) => Ok(bytes),
            Ok(Ok(Err(fetch_error))) => Err(fetch_error.into()),
            // The responder was dropped without sending — treat as shutdown.
            Ok(Err(_recv_error)) => Err(ApiError::Internal(FetchError::ShuttingDown.to_string())),
            Err(_elapsed) => Err(ApiError::Internal(FetchError::TimedOut.to_string())),
        }
    }

    /// Resolve `file_id` to the absolute on-disk path where its bytes currently
    /// live locally, or `None` if no sync directory holds it. Read-only.
    ///
    /// Used by `tagnet-cli edit` to detect the "already local" case and open the
    /// real file in place (the watcher then propagates the save).
    pub async fn local_path_for_file(&self, file_id: FileId) -> Result<Option<PathBuf>, ApiError> {
        let (respond_to, response) = oneshot::channel();
        self.command_sender
            .send(SyncDirectoryCommand::LocalPath {
                file_id,
                respond_to,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;
        response
            .await
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))
    }

    /// Apply `tag_id` to `file_id`. Enqueues `Change::FileTagged`.
    pub fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.enqueue(Change::FileTagged {
            file_id,
            tag_id,
            metadata: None,
        })
    }

    /// Remove `tag_id` from `file_id`. Enqueues `Change::FileUntagged`.
    pub fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.enqueue(Change::FileUntagged { file_id, tag_id })
    }

    // --- Event stream (plan 5.5) ---------------------------------------------

    /// Subscribe to the live change stream.
    ///
    /// Yields every [`Change`] applied by `handle_changes` after this call.
    /// Delivery is best-effort: a slow subscriber that lags beyond the channel
    /// capacity observes a `RecvError::Lagged`, which the transport layer maps
    /// onto an [`ApiEvent::Resynced`] so the UI re-fetches state.
    pub fn subscribe(&self) -> broadcast::Receiver<Change> {
        self.events.subscribe()
    }
}
