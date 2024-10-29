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
        FileId, TagId,
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
            path: String,
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
            path: String,
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
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Sync {
        Manifest { entries: Vec<ManifestEntry> },
        Request { file_id: FileId },
        NotFound { file_id: FileId },
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
