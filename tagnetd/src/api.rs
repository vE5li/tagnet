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

use crate::bus::{DaemonMessage, FetchError, Ingest};
use crate::database::{DatabaseError, FileDatabase, SubtagRule, Tag};
use crate::directory_manager::SyncDirectoryCommand;
use crate::fetch::PendingFetches;
use crate::transfer::ChunkSource;
use std::sync::Arc;

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
    /// A short-id prefix matched more than one file, so it could not be
    /// resolved to a single id. Carries the ambiguous prefix.
    Ambiguous(String),
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
            ApiError::Ambiguous(prefix) => {
                write!(
                    formatter,
                    "ambiguous id prefix '{prefix}': matches multiple files"
                )
            }
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
            DatabaseError::AmbiguousIdPrefix(prefix) => ApiError::Ambiguous(prefix),
            DatabaseError::InvalidTagName => {
                ApiError::InvalidArgument("invalid tag name".to_owned())
            }
            DatabaseError::InvalidColor => ApiError::InvalidArgument("invalid color".to_owned()),
            DatabaseError::CantTagItself => {
                ApiError::InvalidArgument("a tag cannot be its own subtag".to_owned())
            }
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
    /// A file this connection was temporarily providing (an upload/edit) has
    /// been handed off (a peer completed pulling it); the client may release
    /// the local file. Produced by the control layer, not the change bus.
    ProviderReleased { file_id: FileId },
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
    /// Fetch/transfer subsystem, used by the control layer to register a
    /// temporary chunk provider for an upload/edit (the client serves the bytes
    /// on demand).
    pending_fetches: PendingFetches,
    /// Directory for daemon-owned temp files produced by `fetch_file`. A
    /// completed fetch materializes here and the path is handed to the caller
    /// with move semantics. See [`crate::paths::Paths::fetch_temp_dir`].
    fetch_temp_dir: PathBuf,
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
        pending_fetches: PendingFetches,
        fetch_temp_dir: PathBuf,
    ) -> Self {
        Self {
            main_db_path,
            change_sender,
            command_sender,
            events,
            pending_fetches,
            fetch_temp_dir,
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
                Ingest::from_change(change),
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

    /// Resolve a full-or-short file id `prefix` (as displayed by `list_files`'s
    /// short ids, or a pasted full id) to a single [`FileId`]. Backed by
    /// `FileDatabase::resolve_file_id_prefix`.
    ///
    /// Returns [`ApiError::NotFound`] if nothing matches and
    /// [`ApiError::Ambiguous`] if more than one file matches.
    pub fn resolve_file_id(&self, prefix: &str) -> Result<FileId, ApiError> {
        let database = self.open_read()?;
        Ok(database.resolve_file_id_prefix(prefix)?)
    }

    /// Resolve a full-or-short tag id `prefix` (as displayed by `list_tags`'s
    /// short ids, or a pasted full id) to a single [`TagId`]. The tag
    /// counterpart of [`resolve_file_id`](Self::resolve_file_id). Backed by
    /// `FileDatabase::resolve_tag_id_prefix`.
    ///
    /// Returns [`ApiError::NotFound`] if nothing matches and
    /// [`ApiError::Ambiguous`] if more than one tag matches.
    pub fn resolve_tag_id(&self, prefix: &str) -> Result<TagId, ApiError> {
        let database = self.open_read()?;
        Ok(database.resolve_tag_id_prefix(prefix)?)
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

    /// List the subtags of `tag_id` (its children in the tag hierarchy).
    /// `subtag_rule` controls whether the hierarchy is walked transitively.
    /// Backed by `FileDatabase::subtag_ids_for_tag`.
    pub fn subtags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        let database = self.open_read()?;
        Ok(database
            .subtag_ids_for_tag(tag_id, subtag_rule)?
            .into_iter()
            .collect())
    }

    /// List the tags applied to `tag_id` (the tags it is a subtag of) — the tag
    /// analogue of [`tags_for_file`](Self::tags_for_file). `subtag_rule`
    /// controls whether the hierarchy is walked transitively. Backed by
    /// `FileDatabase::tag_ids_for_subtag`.
    pub fn tags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        let database = self.open_read()?;
        Ok(database
            .tag_ids_for_subtag(tag_id, subtag_rule)?
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
        // A locally-originated mutation is stamped with our wall clock now; the
        // timestamp then rides the change unchanged to peers for LWW.
        // Hex form (matches the CLI's default and the Flutter app's palette),
        // so tags created with an empty color render consistently everywhere.
        let color = if color.trim().is_empty() {
            "#F44336".to_owned()
        } else {
            color
        };
        let tag_id = TagId::new();
        self.enqueue(Change::TagAdded {
            tag_id,
            tag_name: name,
            color,
            metadata: None,
            modified_at: crate::database::now_millis(),
        })?;
        Ok(tag_id)
    }

    /// Delete a tag. Enqueues `Change::TagRemoved`.
    pub fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        self.enqueue(Change::TagRemoved { tag_id })
    }

    /// Rename a tag. Enqueues `Change::TagRenamed`, stamped with our wall clock
    /// now for last-writer-wins reconciliation.
    pub fn rename_tag(&self, tag_id: TagId, name: String) -> Result<(), ApiError> {
        if name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("tag name is empty".to_owned()));
        }
        self.enqueue(Change::TagRenamed {
            tag_id,
            tag_name: name,
            modified_at: crate::database::now_millis(),
        })
    }

    /// Change a tag's color. Enqueues `Change::TagRecolored` carrying the full
    /// new color, stamped with our wall clock now for last-writer-wins.
    pub fn set_tag_color(&self, tag_id: TagId, color: String) -> Result<(), ApiError> {
        if color.trim().is_empty() {
            return Err(ApiError::InvalidArgument("color is empty".to_owned()));
        }
        self.enqueue(Change::TagRecolored {
            tag_id,
            color,
            modified_at: crate::database::now_millis(),
        })
    }

    /// Upload a file whose bytes the client provides on demand.
    ///
    /// The client has already computed `content_hash` (by streaming its own
    /// file) and will serve the bytes chunk-by-chunk as a temporary provider;
    /// no bytes are passed here. Mints a `FileId`, records the file + version,
    /// and announces a metadata-only `FileMetadataAdded` to peers, which then
    /// pull the content from the provider the control layer registers.
    pub fn upload_file(
        &self,
        path_name: String,
        content_hash: String,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        if path_name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("path is empty".to_owned()));
        }
        let file_id = FileId::new();
        self.change_sender
            .send(DaemonMessage::AnnounceProvided {
                file_id,
                logical_path: Some(LogicalPath::new(path_name)),
                content_hash,
                tags,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;
        Ok(file_id)
    }

    /// Register a temporary chunk provider for a file the client is serving on
    /// demand. Delegates to the transfer subsystem's provider registry.
    pub async fn register_provider(
        &self,
        file_id: FileId,
        content_hash: String,
        source: Arc<dyn ChunkSource>,
    ) {
        self.pending_fetches
            .register_provider(file_id, content_hash, source)
            .await;
    }

    /// Remove a temporary provider (the client released the file).
    pub async fn unregister_provider(&self, file_id: FileId, content_hash: &str) {
        self.pending_fetches
            .unregister_provider(file_id, content_hash)
            .await;
    }

    /// Replace the content of an existing file, provided on demand by the client
    /// (see [`Self::upload_file`]). Records the new version and announces a
    /// metadata-only `FileMetadataChanged` to peers, which pull from the
    /// provider.
    pub fn edit_file(&self, file_id: FileId, content_hash: String) -> Result<(), ApiError> {
        self.change_sender
            .send(DaemonMessage::AnnounceProvided {
                file_id,
                logical_path: None,
                content_hash,
                tags: Vec::new(),
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))
    }

    /// Delete a file. Enqueues `Change::FileDeleted`.
    pub fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        self.enqueue(Change::FileDeleted { file_id })
    }

    /// Move (rename) a file to a new logical path. Enqueues `Change::FileMoved`;
    /// each receiving sync directory derives its own physical placement.
    pub fn move_file(&self, file_id: FileId, logical_path: String) -> Result<(), ApiError> {
        if logical_path.trim().is_empty() {
            return Err(ApiError::InvalidArgument("path is empty".to_owned()));
        }
        self.enqueue(Change::FileMoved {
            file_id,
            logical_path: LogicalPath::new(logical_path),
        })
    }

    /// The overall deadline a caller waits for an on-demand fetch to complete.
    /// Must exceed [`crate::fetch::HOP_TIMEOUT`] so intermediate hops can time
    /// out and report before this outer deadline fires.
    const FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    /// Fetch a file's content on demand, from a peer if not present locally,
    /// and return the path to a **daemon-owned temp file** holding it.
    ///
    /// Enqueues a [`DaemonMessage::Fetch`] onto the ingest bus; `handle_changes`
    /// checks the local sync directories first (hash-gated) and, failing that,
    /// floods a recursive `Sync::FetchRequest` across the live peer tree. Awaits
    /// the reply with an overall timeout. `expected_hash` gates which content is
    /// accepted; the caller obtains it from the file's known metadata
    /// (`FileInfo::content_hash`).
    ///
    /// The returned path lives under [`crate::paths::Paths::fetch_temp_dir`] and
    /// is handed to the caller with **move semantics**: the caller must consume
    /// it (rename into place or delete). The whole file is never buffered into
    /// memory — a peer transfer already lands as a temp file on disk, and a
    /// locally-held copy is streamed into the fetch temp dir.
    pub async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<PathBuf, ApiError> {
        let (respond_to, response) = oneshot::channel();
        self.change_sender
            .send(DaemonMessage::Fetch {
                file_id,
                expected_hash,
                respond_to,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;

        let content = match tokio::time::timeout(Self::FETCH_TIMEOUT, response).await {
            Ok(Ok(Ok(file_bytes))) => file_bytes,
            Ok(Ok(Err(fetch_error))) => return Err(fetch_error.into()),
            // The responder was dropped without sending — treat as shutdown.
            Ok(Err(_recv_error)) => {
                return Err(ApiError::Internal(FetchError::ShuttingDown.to_string()));
            }
            Err(_elapsed) => return Err(ApiError::Internal(FetchError::TimedOut.to_string())),
        };

        // Materialize into a fresh daemon-owned temp file the caller consumes.
        let dest = self.fetch_temp_dir.join(uuid::Uuid::new_v4().to_string());
        content.materialize_to(&dest).await.map_err(|error| {
            ApiError::Internal(format!("failed to stage fetched file: {error}"))
        })?;
        Ok(dest)
    }

    /// Resolve `file_id` to the absolute on-disk path where its bytes currently
    /// live locally, or `None` if no sync directory holds it. Read-only.
    ///
    /// Used by `tagnet edit` to detect the "already local" case and open the
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
            modified_at: crate::database::now_millis(),
        })
    }

    /// Remove `tag_id` from `file_id`. Enqueues `Change::FileUntagged`.
    pub fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.enqueue(Change::FileUntagged {
            file_id,
            tag_id,
            modified_at: crate::database::now_millis(),
        })
    }

    /// Make `subtag_id` a subtag (child) of `parent_id` in the tag hierarchy.
    /// Enqueues `Change::TagTagged`.
    ///
    /// A tag cannot be its own subtag; that is rejected here (with
    /// [`ApiError::InvalidArgument`]) rather than only being caught by the
    /// database inside the change pipeline, so the caller learns immediately.
    pub fn tag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        if parent_id == subtag_id {
            return Err(ApiError::InvalidArgument(
                "a tag cannot be its own subtag".to_owned(),
            ));
        }
        self.enqueue(Change::TagTagged {
            taggee_id: subtag_id,
            tag_id: parent_id,
            metadata: None,
            modified_at: crate::database::now_millis(),
        })
    }

    /// Remove `subtag_id` as a subtag of `parent_id`. Enqueues
    /// `Change::TagUntagged`.
    pub fn untag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        self.enqueue(Change::TagUntagged {
            taggee_id: subtag_id,
            tag_id: parent_id,
            modified_at: crate::database::now_millis(),
        })
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
