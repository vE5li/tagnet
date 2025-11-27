use std::path::Path;

use rusqlite::{
    Connection, ToSql,
    types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef},
};
use serde::{Deserialize, Serialize};
use tagnet_core::{FileId, TagId, tag::MetadataFormat};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    File,
    Tag,
}

impl ToSql for EntryType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            EntryType::File => Ok(0.into()),
            EntryType::Tag => Ok(1.into()),
        }
    }
}

impl FromSql for EntryType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_i64()? {
            0 => Ok(Self::File),
            1 => Ok(Self::Tag),
            invalid => panic!("invalid entry type {}", invalid),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct Tag {
    pub id: TagId,
    pub name: String,
    pub color: String,
    pub metadata: Option<MetadataFormat>,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct File {
    pub id: FileId,
    pub path: String,
    pub display_name: String,
    // pub last_modified: String,
    // pub content_length: String,
    // pub content_type: String,
    // pub has_preview: bool,
    // pub preview_id: Option<PreviewId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DatabaseError {
    UnableToOpenOrCreate,
    FailedToExecuteCommand,
    NonUtf8FilePath,
    MissingFile,
    MissingTag,
    InvalidTagName,
    InvalidColor,
    CantTagItself,
}

#[derive(Debug)]
pub struct DatabaseHandle {
    connection: Connection,
}

pub fn initialize(database_path: impl AsRef<Path>) -> Result<DatabaseHandle, DatabaseError> {
    let connection =
        Connection::open(database_path).map_err(|_| DatabaseError::UnableToOpenOrCreate)?;

    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS files (
            id              TEXT PRIMARY KEY,
            display_name    TEXT NOT NULL,
            path            TEXT
        )",
            (), // empty list of parameters.
        )
        .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS tags (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL UNIQUE,
            color       TEXT NOT NULL,
            metadata    TEXT
        )",
            (), // empty list of parameters.
        )
        .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS entries (
            id          TEXT PRIMARY KEY,
            tag_id      TEXT NOT NULL,
            target_id   TEXT NOT NULL,
            type        INTEGER,
            UNIQUE (tag_id, target_id, type)
        )",
            (), // empty list of parameters.
        )
        .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

    Ok(DatabaseHandle { connection })
}

impl DatabaseHandle {
    /// Add a new file.
    pub fn add_file(
        &self,
        display_name: String,
        file_path: Option<String>,
    ) -> Result<FileId, DatabaseError> {
        let file_id = FileId::new();

        self.connection
            .execute(
                "INSERT INTO files (id, display_name, path) VALUES (?1, ?2, ?3)",
                (FileId::new(), display_name, file_path.unwrap_or_default()),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(file_id)
    }
}

/*
    /// Add a new tag.
 pub fn add_tag(
        &self,
        name: impl Into<String>,
        color: impl Into<String>,
    ) -> Result<TagId, DatabaseError> {
        let name = name.into();
        let color = color.into();

        // TODO: Check that the tag name is not only numbers.
        if name.is_empty() {
            return Err(DatabaseError::InvalidTagName);
        }

        // TODO: Check that the color is valid.
        if color.is_empty() {
            return Err(DatabaseError::InvalidColor);
        }

        self.connection
            .execute(
                "INSERT INTO tags (name, color) VALUES (?1, ?2)",
                [&name, &color],
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(TagId(self.connection.last_insert_rowid()))
    }

    pub fn remove_tag(&self, tag_id: TagId) -> Result<(), DatabaseError> {
        self.connection
            .execute("DELETE FROM tags WHERE id = ?1", [&tag_id])
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        self.connection
            .execute(
                "DELETE FROM entries WHERE tag_id = ?1 OR (target_id = ?1 AND type = 1)",
                [&tag_id],
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    /// Tag a file with the provided tag.
    pub fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "INSERT INTO entries (tag_id, target_id, type) VALUES (?1, ?2, ?3)",
                (&tag_id, &file_id, EntryType::File),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    // pub fn get_preview(
    //     &self,
    //     preview_id: PreviewId,
    //     preview_size: PreviewSize,
    //     // TODO: Not a DatabaseError, might also be a client (Nextcloud) error.
    // ) -> Result<String, DatabaseError> {
    //     let column = match preview_size {
    //         PreviewSize::Small => "small",
    //         PreviewSize::Medium => "medium",
    //         PreviewSize::Big => "big",
    //     };
    //
    //     let query = format!("SELECT {column} FROM previews WHERE id = ?1");
    //     let mut statement = self
    //         .connection
    //         .prepare(&query)
    //         .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
    //
    //     let preview = statement
    //         .query_map([preview_id], |row| row.get(0))
    //         .map_err(|_| DatabaseError::FailedToExecuteCommand)?
    //         .map(|preview| preview.unwrap())
    //         .next()
    //         .ok_or(DatabaseError::MissingFile)?;
    //
    //     Ok(preview)
    // }
    //
    // pub fn set_preview(
    //     &self,
    //     file_id: FileId,
    //     previews: (String, String, String),
    // ) -> Result<PreviewId, DatabaseError> {
    //     self.connection
    //         .execute(
    //             "INSERT INTO previews (small, medium, big) VALUES (?1, ?2, ?3)",
    //             previews,
    //         )
    //         .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
    //
    //     let preview_id = PreviewId(self.connection.last_insert_rowid());
    //
    //     self.connection
    //         .execute(
    //             "UPDATE files SET preview_id = ?2 WHERE id = ?1",
    //             (file_id, Some(preview_id)),
    //         )
    //         .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
    //
    //     Ok(preview_id)
    // }

    /// Tag a tag with the provided tag.
    pub fn tag_tag(&self, tag_id: TagId, subtag_id: TagId) -> Result<(), DatabaseError> {
        if tag_id == subtag_id {
            return Err(DatabaseError::CantTagItself);
        }

        self.connection
            .execute(
                "INSERT INTO entries (tag_id, target_id, type) VALUES (?1, ?2, ?3)",
                (&tag_id, &subtag_id, EntryType::Tag),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    /// Remove a tag from a file.
    pub fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "DELETE FROM entries WHERE tag_id = ?1 AND target_id = ?2 AND type = 0",
                (&tag_id, &file_id),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    /// Remove a tag from a tag.
    pub fn untag_tag(&self, tag_id: TagId, subtag_id: TagId) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "DELETE FROM entries WHERE tag_id = ?1 AND target_id = ?2 AND type = 1",
                (&tag_id, &subtag_id),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    fn file_ids_for_tag_inner(
        &self,
        tag_id: TagId,
        lookup_cache: &mut BTreeSet<TagId>,
        collected_tag_ids: &mut BTreeSet<FileId>,
        subtag_rule: SubtagRule,
    ) -> Result<(), DatabaseError> {
        enum Entry {
            File { file_id: FileId },
            Tag { tag_id: TagId },
        }

        let mut statement = self
            .connection
            .prepare("SELECT target_id, type FROM entries WHERE tag_id = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([tag_id], |row| {
                let r#type: EntryType = row.get(1)?;

                let entry = match r#type {
                    EntryType::File => Entry::File {
                        file_id: row.get(0)?,
                    },
                    EntryType::Tag => Entry::Tag {
                        tag_id: row.get(0)?,
                    },
                };

                Ok(entry)
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|entry| entry.unwrap());

        lookup_cache.insert(tag_id);

        for entry in iterator {
            match entry {
                Entry::File { file_id } => {
                    collected_tag_ids.insert(file_id);
                }
                Entry::Tag { tag_id } => {
                    if subtag_rule == SubtagRule::Include && !lookup_cache.contains(&tag_id) {
                        self.file_ids_for_tag_inner(
                            tag_id,
                            lookup_cache,
                            collected_tag_ids,
                            subtag_rule,
                        )?
                    }
                }
            }
        }

        Ok(())
    }

    /// Get all files that are tagged with the provided tag.
    pub fn file_ids_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = FileId>, DatabaseError> {
        let mut file_ids = BTreeSet::new();
        let mut lookup_cache = BTreeSet::new();

        self.file_ids_for_tag_inner(tag_id, &mut lookup_cache, &mut file_ids, subtag_rule)?;

        Ok(file_ids)
    }

    /// Get all files that are tagged with the provided tag.
    pub fn tag_ids_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = TagId>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT tag_id FROM entries WHERE target_id = ?1 AND type = 0")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut tag_ids = statement
            .query_map([file_id], |row| row.get::<_, TagId>(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|tag_id| tag_id.unwrap())
            .collect::<BTreeSet<_>>();

        if subtag_rule == SubtagRule::Include {
            let mut lookup_cache = BTreeSet::new();

            for tag_id in tag_ids.clone() {
                self.tag_ids_for_subtag_inner(
                    tag_id,
                    &mut lookup_cache,
                    &mut tag_ids,
                    subtag_rule,
                )?;
            }
        }

        Ok(tag_ids)
    }

    fn subtag_ids_for_tag_inner(
        &self,
        tag_id: TagId,
        lookup_cache: &mut BTreeSet<TagId>,
        collected_tags: &mut BTreeSet<TagId>,
        subtag_rule: SubtagRule,
    ) -> Result<(), DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT target_id FROM entries WHERE tag_id = ?1 AND type = 1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([tag_id], |row| row.get::<_, TagId>(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|entry| entry.unwrap());

        lookup_cache.insert(tag_id);

        for tag_id in iterator {
            collected_tags.insert(tag_id);

            if subtag_rule == SubtagRule::Include && !lookup_cache.contains(&tag_id) {
                self.subtag_ids_for_tag_inner(tag_id, lookup_cache, collected_tags, subtag_rule)?;
            }
        }

        Ok(())
    }

    /// Get all subtags tagged with the provided tag.
    pub fn subtag_ids_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = TagId>, DatabaseError> {
        let mut tags = BTreeSet::new();
        let mut lookup_cache = BTreeSet::new();

        self.subtag_ids_for_tag_inner(tag_id, &mut lookup_cache, &mut tags, subtag_rule)?;

        Ok(tags)
    }

    fn tag_ids_for_subtag_inner(
        &self,
        subtag_id: TagId,
        lookup_cache: &mut BTreeSet<TagId>,
        collected_tags: &mut BTreeSet<TagId>,
        subtag_rule: SubtagRule,
    ) -> Result<(), DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT tag_id FROM entries WHERE target_id = ?1 AND type = 1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([subtag_id], |row| row.get::<_, TagId>(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|entry| entry.unwrap());

        lookup_cache.insert(subtag_id);

        for tag_id in iterator {
            collected_tags.insert(tag_id);

            if subtag_rule == SubtagRule::Include && !lookup_cache.contains(&tag_id) {
                self.tag_ids_for_subtag_inner(tag_id, lookup_cache, collected_tags, subtag_rule)?;
            }
        }

        Ok(())
    }

    /// Get all tags that tag the provided tag.
    pub fn tag_ids_for_subtag(
        &self,
        subtag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = TagId>, DatabaseError> {
        let mut tags = BTreeSet::new();
        let mut lookup_cache = BTreeSet::new();

        self.tag_ids_for_subtag_inner(subtag_id, &mut lookup_cache, &mut tags, subtag_rule)?;

        Ok(tags)
    }

    /// Get all files.
    pub fn all_files(&self) -> Result<impl IntoIterator<Item = File>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, path, display_name, last_modified, content_length, content_type, has_preview, preview_id FROM files")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let file_list = statement
            .query_map([], |row| {
                Ok(File {
                    id: row.get(0)?,
                    path: row.get(1)?,
                    display_name: row.get(2)?,
                    last_modified: row.get(3)?,
                    content_length: row.get(4)?,
                    content_type: row.get(5)?,
                    has_preview: row.get(6)?,
                    preview_id: row.get(7)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|file| file.unwrap())
            .collect::<Vec<_>>();

        Ok(file_list)
    }

    /// Get all tags.
    pub fn all_tags(&self) -> Result<impl IntoIterator<Item = Tag>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, name, color FROM tags")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let tag_list = statement
            .query_map([], |row| {
                Ok(Tag {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    color: row.get(2)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|tag| tag.unwrap())
            .collect::<Vec<_>>();

        Ok(tag_list)
    }

    /// Get a file by its ID.
    pub fn file_from_id(&self, file_id: FileId) -> Result<File, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT path, display_name, last_modified, content_length, content_type, has_preview, preview_id FROM files WHERE id = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let file = statement
            .query_map([file_id], |row| {
                Ok(File {
                    id: file_id,
                    path: row.get(0)?,
                    display_name: row.get(1)?,
                    last_modified: row.get(2)?,
                    content_length: row.get(3)?,
                    content_type: row.get(4)?,
                    has_preview: row.get(5)?,
                    preview_id: row.get(6)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|file| file.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(file)
    }

    /// Get the a file by its preview ID.
    // pub fn file_from_preview_id(&self, preview_id: PreviewId) -> Result<File, DatabaseError> {
    //     let mut statement = self
    //         .connection
    //         .prepare("SELECT id, path, display_name, last_modified, content_length, content_type FROM files WHERE preview = ?1")
    //         .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
    //
    //     let file = statement
    //         .query_map([Some(preview_id)], |row| {
    //             Ok(File {
    //                 id: row.get(0)?,
    //                 path: row.get(1)?,
    //                 display_name: row.get(2)?,
    //                 last_modified: row.get(3)?,
    //                 content_length: row.get(4)?,
    //                 content_type: row.get(5)?,
    //
    //                 preview: Some(preview_id),
    //             })
    //         })
    //         .map_err(|_| DatabaseError::FailedToExecuteCommand)?
    //         .map(|file| file.unwrap())
    //         .next()
    //         .ok_or(DatabaseError::MissingFile)?;
    //
    //     Ok(file)
    // }

    /// Get the name of a tag by the ID.
    pub fn tag_from_id(&self, tag_id: TagId) -> Result<Tag, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT name, color FROM tags WHERE id = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let tag = statement
            .query_map([tag_id], |row| {
                Ok(Tag {
                    id: tag_id,
                    name: row.get(0)?,
                    color: row.get(1)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|tag| tag.unwrap())
            .next()
            .ok_or(DatabaseError::MissingTag)?;

        Ok(tag)
    }

    /// Get the ID of a file by the path.
    pub fn file_id_from_path(
        &self,
        file_path: impl AsRef<OsString>,
    ) -> Result<FileId, DatabaseError> {
        let file_path = file_path
            .as_ref()
            .to_str()
            .ok_or(DatabaseError::NonUtf8FilePath)?;

        let mut statement = self
            .connection
            .prepare("SELECT id FROM files WHERE path = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let file_id = statement
            .query_map([file_path], |row| row.get(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|id| id.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(file_id)
    }

    /// Get the ID of a tag by the name.
    pub fn tag_id_from_name(&self, name: &str) -> Result<TagId, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM tags WHERE name = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let tag_id = statement
            .query_map([name], |row| row.get(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|id| id.unwrap())
            .next()
            .ok_or(DatabaseError::MissingTag)?;

        Ok(tag_id)
    }

    pub fn update_tag_name(
        &self,
        tag_id: TagId,
        name: impl Into<String>,
    ) -> Result<(), DatabaseError> {
        let name = name.into();

        // TODO: Check that the tag name is not only numbers.
        if name.is_empty() {
            return Err(DatabaseError::InvalidTagName);
        }

        self.connection
            .execute("UPDATE tags SET name = ?2 WHERE id = ?1", (tag_id, name))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    pub fn update_tag_color(
        &self,
        tag_id: TagId,
        color: impl Into<String>,
    ) -> Result<(), DatabaseError> {
        let color = color.into();

        // TODO: Check that the tag name is not only numbers.
        if color.is_empty() {
            return Err(DatabaseError::InvalidColor);
        }

        self.connection
            .execute("UPDATE tags SET color = ?2 WHERE id = ?1", (tag_id, color))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }
}

*/

// TODO: Temporary functions to debug.
impl DatabaseHandle {
    pub fn show_files(&self) -> Result<(), DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, path, display_name FROM files")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([], |row| {
                Ok(File {
                    id: row.get(0)?,
                    path: row.get(1)?,
                    display_name: row.get(2)?,
                })
            })
            .unwrap();

        for file in iterator {
            println!("{:?}", file.unwrap());
        }

        Ok(())
    }

    // pub fn show_tags(&self) -> Result<(), DatabaseError> {
    //     let mut statement = self
    //         .connection
    //         .prepare("SELECT id, name, color FROM tags")
    //         .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
    //
    //     let iterator = statement
    //         .query_map([], |row| {
    //             Ok(Tag {
    //                 id: row.get(0)?,
    //                 name: row.get(1)?,
    //                 color: row.get(2)?,
    //             })
    //         })
    //         .unwrap();
    //
    //     for tag in iterator {
    //         println!("{:?}", tag.unwrap());
    //     }
    //
    //     Ok(())
    // }
    //
    // pub fn show_entries(&self) -> Result<(), DatabaseError> {
    //     #[derive(Debug)]
    //     #[allow(dead_code)]
    //     struct Entry {
    //         tag_id: TagId,
    //         target_id: i64,
    //         r#type: EntryType,
    //     }
    //
    //     let mut statement = self
    //         .connection
    //         .prepare("SELECT tag_id, target_id, type FROM entries")
    //         .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
    //
    //     let iterator = statement
    //         .query_map([], |row| {
    //             Ok(Entry {
    //                 tag_id: row.get(0)?,
    //                 target_id: row.get(1)?,
    //                 r#type: row.get(2)?,
    //             })
    //         })
    //         .unwrap();
    //
    //     for entry in iterator {
    //         println!("{:?}", entry.unwrap());
    //     }
    //
    //     Ok(())
    // }
}
