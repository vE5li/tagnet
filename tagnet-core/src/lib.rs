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
    use serde::{Deserialize, Serialize};

    use crate::{
        FileId, TagId,
        tag::{MetadataFormat, MetadataValues},
    };

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
            display_name: String,
            file_path: Option<String>,
            // encoding: ,
            content: Vec<u8>,
        },
        FileMoved {
            file_id: FileId,
            display_name: String,
            path: Option<String>,
        },
        FileChanged {
            file_id: FileId,
            // encoding: ,
            content: Vec<u8>,
        },
        FileDeleted {
            file_id: FileId,
        },
        TagAdded {
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
