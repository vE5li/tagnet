use std::{collections::BTreeSet, ffi::OsString, path::Path};

use rusqlite::{
    types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef},
    Connection, ToSql,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct FileId(i64);

impl From<FileId> for i64 {
    fn from(value: FileId) -> Self {
        value.0
    }
}

impl From<i64> for FileId {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl ToSql for FileId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.0.into())
    }
}

impl FromSql for FileId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(Self(value.as_i64()?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct TagId(i64);

impl From<TagId> for i64 {
    fn from(value: TagId) -> Self {
        value.0
    }
}

impl From<i64> for TagId {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl ToSql for TagId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.0.into())
    }
}

impl FromSql for TagId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(Self(value.as_i64()?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct EntryId(i64);

impl ToSql for EntryId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.0.into())
    }
}

impl FromSql for EntryId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(Self(value.as_i64()?))
    }
}

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubtagRule {
    Include,
    Exclude,
    // TODO: Maybe?
    // Depth { depth: usize },
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct Tag {
    id: TagId,
    name: String,
    color: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct File {
    id: FileId,
    path: String,
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
            id    INTEGER PRIMARY KEY,
            path  TEXT NOT NULL UNIQUE
        )",
            (), // empty list of parameters.
        )
        .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS tags (
            id    INTEGER PRIMARY KEY,
            name  TEXT NOT NULL UNIQUE,
            color TEXT NOT NULL
        )",
            (), // empty list of parameters.
        )
        .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS entries (
            id          INTEGER PRIMARY KEY,
            tag_id      INTEGER NOT NULL,
            target_id   INTEGER NOT NULL,
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
    pub fn add_file(&self, file_path: impl AsRef<Path>) -> Result<FileId, DatabaseError> {
        let file_path = file_path
            .as_ref()
            .to_str()
            .ok_or(DatabaseError::NonUtf8FilePath)?;

        self.connection
            .execute("INSERT INTO files (path) VALUES (?1)", [&file_path])
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(FileId(self.connection.last_insert_rowid()))
    }

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

    /// Tag a file with the provided tag.
    pub fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<EntryId, DatabaseError> {
        self.connection
            .execute(
                "INSERT INTO entries (tag_id, target_id, type) VALUES (?1, ?2, ?3)",
                (&tag_id, &file_id, EntryType::File),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(EntryId(self.connection.last_insert_rowid()))
    }

    /// Tag a tag with the provided tag.
    pub fn tag_tag(&self, tag_id: TagId, subtag_id: TagId) -> Result<EntryId, DatabaseError> {
        if tag_id == subtag_id {
            return Err(DatabaseError::CantTagItself);
        }

        self.connection
            .execute(
                "INSERT INTO entries (tag_id, target_id, type) VALUES (?1, ?2, ?3)",
                (&tag_id, &subtag_id, EntryType::Tag),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(EntryId(self.connection.last_insert_rowid()))
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
            .map(|entry| entry.unwrap())
            .collect::<Vec<_>>();

        Ok(tag_list)
    }

    /// Get the path of a file by the ID.
    pub fn file_from_id(&self, file_id: FileId) -> Result<File, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT path FROM files WHERE id = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let file = statement
            .query_map([file_id], |row| {
                Ok(File {
                    id: file_id,
                    path: row.get::<_, String>(0)?.into(),
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|file| file.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(file)
    }

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

impl DatabaseHandle {
    // TODO: Temporary function to debug.
    pub fn show_files(&self) -> Result<(), DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, path FROM files")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([], |row| {
                Ok(File {
                    id: row.get(0)?,
                    path: row.get::<_, String>(1)?.into(),
                })
            })
            .unwrap();

        for file in iterator {
            println!("{:?}", file.unwrap());
        }

        Ok(())
    }
    // TODO: Temporary function to debug.
    pub fn show_tags(&self) -> Result<(), DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, name, color FROM tags")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([], |row| {
                Ok(Tag {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    color: row.get(2)?,
                })
            })
            .unwrap();

        for tag in iterator {
            println!("{:?}", tag.unwrap());
        }

        Ok(())
    }

    // TODO: Temporary function to debug.
    pub fn show_entries(&self) -> Result<(), DatabaseError> {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct Entry {
            id: EntryId,
            tag_id: TagId,
            target_id: i64,
            r#type: EntryType,
        }

        let mut statement = self
            .connection
            .prepare("SELECT id, tag_id, target_id, type FROM entries")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([], |row| {
                Ok(Entry {
                    id: row.get(0)?,
                    tag_id: row.get(1)?,
                    target_id: row.get(2)?,
                    r#type: row.get(3)?,
                })
            })
            .unwrap();

        for entry in iterator {
            println!("{:?}", entry.unwrap());
        }

        Ok(())
    }
}
