use rusqlite::{
    ToSql,
    types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod tag {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    use crate::{FileId, TagId};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum WeakType {
        String,
        Float,
        // Timestamp,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum WeakData {
        String(String),
        Float(f64),
        // Timestamp(),
    }

    pub enum QueryError {
        NotFound,
        WrongType,
    }

    // - Metadata fields cannot be nested
    // e.g.: "file_name: String, folder_name: String"
    // TODO: Maybe use `Cow<'json, str>`
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct MetadataFormat(HashMap<String, WeakType>);

    impl MetadataFormat {
        // TODO: Make this a from.
        pub fn new(string: &str) -> Self {
            todo!()
        }

        pub fn value_map(
            &self,
            values: &MetadataValues,
        ) -> Result<HashMap<String, WeakData>, QueryError> {
            todo!()
        }

        pub fn query_value(
            &self,
            values: &MetadataValues,
            key: &str,
        ) -> Result<WeakData, QueryError> {
            todo!()
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct MetadataValues(HashMap<String, WeakData>);

    pub struct TagMetadata {
        file_id: FileId,
        tag_id: TagId, // <-- Tag has the `MetadataFormat`.
        data: MetadataValues,
    }
}

pub mod state {
    use std::path::PathBuf;

    use serde::{Deserialize, Serialize};

    use crate::{
        FileId, LogicalPath, RequestId, TagId,
        tag::{MetadataFormat, MetadataValues},
    };

    pub enum ChangeOrigin {
        Local { directory_path: PathBuf },
        Peer { public_key: String },
    }

    /// Anything a client can request the server to do. Add/edit/remove files and tags (including
    /// tag metadata), tag files or tags.
    ///
    /// The Server is the only entity that has knowledge of the complete state. It doesn't try to
    /// keep every client informed of the entire state, it only synchronizes the state that is:
    /// - Configured to be synced to the client
    /// - Allowed to be accessed by the user
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Change {
        // The client can't know the Id of the file initially.
        FileAdded {
            file_id: FileId,
            /// The file's logical identity. Receivers store this in the main
            /// database and each derive their own on-disk placement from it.
            logical_path: LogicalPath,
            // encoding: ,
            content: Vec<u8>,
            /// BLAKE3 hex digest of `content`. The receiver records this in
            /// `file_versions` so the version chain is authoritative without
            /// the receiver having to re-hash the bytes.
            content_hash: String,
            // TODO: Bundle metadata with the tag.
            tags: Vec<TagId>,
        },
        FileMoved {
            file_id: FileId,
            /// The file's new logical identity. As with `FileAdded`, each
            /// receiving sync directory derives its own physical placement.
            logical_path: LogicalPath,
        },
        FileChanged {
            file_id: FileId,
            // encoding: ,
            content: Vec<u8>,
            /// BLAKE3 hex digest of `content`. See `FileAdded::content_hash`.
            content_hash: String,
        },
        FileDeleted {
            file_id: FileId,
        },
        TagAdded {
            tag_id: TagId,
            tag_name: String,
            // color,
            metadata: Option<MetadataFormat>,
        },
        TagRenamed {
            tag_id: TagId,
            tag_name: String,
        },
        TagChanged {
            tag_id: TagId,
            metadata: Option<MetadataValues>,
        },
        TagRemoved {
            tag_id: TagId,
        },
        FileTagged {
            file_id: FileId,
            tag_id: TagId,
            metadata: Option<MetadataValues>,
        },
        FileTagChanged {
            file_id: FileId,
            tag_id: TagId,
            metadata: Option<MetadataValues>,
        },
        FileUntagged {
            file_id: FileId,
            tag_id: TagId,
        },
        TagTagged {
            taggee_id: TagId,
            tag_id: TagId,
            metadata: Option<MetadataValues>,
        },
        TagTagChanged {
            taggee_id: TagId,
            tag_id: TagId,
            metadata: Option<MetadataValues>,
        },
        TagUntagged {
            taggee_id: TagId,
            tag_id: TagId,
        },
    }

    /// One file's full version history as announced in a `Sync::Manifest`.
    ///
    /// `history` is ordered oldest-to-newest: `history[0]` is `version_number`
    /// 1, the last entry is the current version. Each entry pairs the
    /// per-file monotonic `version_number` with the BLAKE3 `content_hash` that
    /// was recorded for it.
    ///
    /// `latest_observed_at` is the wall-clock timestamp (unix millis) of the
    /// latest version on the announcing side. The receiver uses it only as a
    /// tiebreaker when histories have diverged (neither side's latest hash
    /// appears in the other's history).
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ManifestEntry {
        pub file_id: FileId,
        pub history: Vec<(i64, String)>,
        pub latest_observed_at: i64,
    }

    /// Reconciliation messages exchanged between peers, independent of the
    /// live `Change` stream.
    ///
    /// Flow at connection time:
    /// 1. After the public-key handshake, both sides send their `Manifest`
    ///    unprompted.
    /// 2. The receiver compares each entry against its local `file_versions`
    ///    table. For entries where it determines the peer has bytes it
    ///    doesn't, it sends back `Request`.
    /// 3. The peer answers each `Request` with a `Change::FileAdded` carrying
    ///    the current bytes and `content_hash` (re-using the live wire
    ///    format), or `NotFound` if the file is no longer locally available.
    ///
    /// The `Fetch*` variants are a distinct, on-demand mechanism (used by
    /// `tagnet-cli edit`): a recursive request for a *specific* file's bytes
    /// that floods across an assumed-acyclic tree of live peer connections.
    /// Each node forwards a `FetchRequest` to all neighbours except the one it
    /// arrived from; the first `FetchFound` (whose hash matches
    /// `expected_hash`) unwinds back along the request path to the origin. A
    /// node reports `FetchMissing` only once every child it forwarded to has
    /// reported `FetchMissing` (or timed out). The `request_id` correlates
    /// replies with the pending request at each hop. Kept separate from the
    /// manifest-driven `Request`/`NotFound` above so that reconciliation is
    /// untouched.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Sync {
        Manifest {
            entries: Vec<ManifestEntry>,
        },
        Request {
            file_id: FileId,
        },
        NotFound {
            file_id: FileId,
        },

        /// Ask peers for the bytes of `file_id` whose content hashes to
        /// `expected_hash`. Flooded across the live-connection tree.
        FetchRequest {
            request_id: RequestId,
            file_id: FileId,
            expected_hash: String,
        },
        /// A peer holds bytes for `file_id` matching the request's
        /// `expected_hash`. Unwinds back toward the origin.
        FetchFound {
            request_id: RequestId,
            file_id: FileId,
            content: Vec<u8>,
            content_hash: String,
        },
        /// This subtree does not have the requested content (all children
        /// exhausted or timed out).
        FetchMissing {
            request_id: RequestId,
        },
    }

    /// Top-level wire message wrapper. Every WebSocket text frame between
    /// peers, after the initial plaintext-public-key handshake, is a JSON
    /// `Frame`.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Frame {
        Change(Change),
        Sync(Sync),
    }
}

/// A file as presented to the UI: its id, managed relative path, and the
/// content hash + number of its latest recorded version.
///
/// Produced by `FileDatabase::get_all_files` and returned by the UI-facing
/// read API (portability plan section 5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub file_id: FileId,
    /// The file's logical path: its human-readable identity (possibly nested,
    /// e.g. `foo/bar/name.txt`), independent of where any sync directory stores
    /// the bytes on disk. Mirrors `FileDatabase.files.logical_path`.
    pub logical_path: LogicalPath,
    pub content_hash: String,
    pub version_number: i64,
}

macro_rules! make_id_type {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            pub fn from_string(uuid: &str) -> Option<Self> {
                Some(Self(Uuid::try_from(uuid).ok()?))
            }

            pub fn to_string(&self) -> String {
                self.0.to_string()
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }

        impl ToSql for $name {
            fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
                Ok(self.0.to_string().into())
            }
        }

        impl FromSql for $name {
            fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                // FIX: Don't unwrap.
                Ok(Self(Uuid::try_from(value.as_str()?).unwrap()))
            }
        }
    };
}

make_id_type!(FileId);
make_id_type!(PreviewId);
make_id_type!(TagId);

/// A transient identifier for a single on-demand fetch traversal
/// (`Sync::Fetch*`). Unlike [`FileId`]/[`TagId`] it is never persisted to the
/// database — it exists only to correlate a `FetchFound`/`FetchMissing` reply
/// with the pending request it answers, across relaying peers ("call stack
/// across machines"). It therefore deliberately omits the SQL trait impls that
/// [`make_id_type!`] provides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct RequestId(Uuid);

impl RequestId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

/// A file's **logical** path: its human-readable identity within tagnet's
/// namespace (possibly nested, e.g. `foo/bar/name.txt`). This is what is shown
/// to users, advertised to peers, and stored in the main `FileDatabase`
/// (`files.logical_path`). It is independent of where any individual sync
/// directory stores the bytes on disk.
///
/// Deliberately *not* interchangeable with [`PhysicalPath`]: the only way to
/// obtain a `LogicalPath` from a `PhysicalPath` is [`PhysicalPath::into_logical`]
/// (the ingestion boundary), and the only way to obtain a `PhysicalPath` from a
/// `LogicalPath` is a `SyncType`-aware placement decision that lives in the
/// `tagnet` crate (`physical_for`). Keeping them distinct makes the
/// logical-vs-physical confusion a compile error rather than a convention.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct LogicalPath(String);

/// A file's **physical** path: where its bytes live on disk *relative to a
/// particular sync directory's root*. For a `TagBased` directory this equals
/// the logical path; for a `Universal` directory it is the file's `file_id`
/// (files are stored under their id on disk). It also serves as the reverse
/// index for filesystem events (path -> file_id), so it must always reflect the
/// actual on-disk name. Stored in `SyncDirectoryDatabase` (`files.physical_path`).
///
/// See [`LogicalPath`] for why the two are not interchangeable.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct PhysicalPath(String);

impl LogicalPath {
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl PhysicalPath {
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    /// The single blessed **ingestion** conversion: a concrete on-disk relative
    /// path becomes a file's logical identity. This is the only way to turn a
    /// `PhysicalPath` into a `LogicalPath`, and is appropriate exactly when a
    /// file first enters tagnet's namespace from disk (upload/add, or a move
    /// *into* a sync directory), where the physical location *defines* the
    /// logical path.
    pub fn into_logical(self) -> LogicalPath {
        LogicalPath(self.0)
    }
}

impl std::fmt::Display for LogicalPath {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::fmt::Display for PhysicalPath {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl ToSql for LogicalPath {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.0.as_str().into())
    }
}

impl FromSql for LogicalPath {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(Self(value.as_str()?.to_owned()))
    }
}

impl ToSql for PhysicalPath {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.0.as_str().into())
    }
}

impl FromSql for PhysicalPath {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(Self(value.as_str()?.to_owned()))
    }
}
