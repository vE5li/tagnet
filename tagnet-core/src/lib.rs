use std::{collections::BTreeSet, ffi::OsString, path::Path};

use rusqlite::{
    types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef},
    Connection, ToSql,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileId(i64);

impl FileId {
    // FIX: May be temporary.
    pub fn from_raw(id: i64) -> Self {
        Self(id)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TagId(i64);

impl TagId {
    // FIX: May be temporary.
    pub fn from_raw(id: i64) -> Self {
        Self(id)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

#[derive(Debug)]
pub enum DatabaseError {
    UnableToOpenOrCreate,
    FailedToExecuteCommand,
    NonUtf8FilePath,
    MissingFile,
    MissingTag,
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
            name  TEXT NOT NULL UNIQUE
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
            type        INTEGER
        )",
            (), // empty list of parameters.
        )
        .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

    Ok(DatabaseHandle { connection })
}

impl DatabaseHandle {
    /// Add a new file.
    pub fn add_file(&self, file_path: impl AsRef<Path>) -> Result<FileId, DatabaseError> {
        let file_name = file_path
            .as_ref()
            .to_str()
            .ok_or(DatabaseError::NonUtf8FilePath)?;

        self.connection
            .execute("INSERT INTO files (path) VALUES (?1)", [&file_name])
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(FileId(self.connection.last_insert_rowid()))
    }

    /// Add a new tag.
    pub fn add_tag(&self, name: impl Into<String>) -> Result<TagId, DatabaseError> {
        self.connection
            .execute("INSERT INTO tags (name) VALUES (?1)", [&name.into()])
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
    pub fn tag_tag(&self, tag_id: TagId, other_tag_id: TagId) -> Result<EntryId, DatabaseError> {
        self.connection
            .execute(
                "INSERT INTO entries (tag_id, target_id, type) VALUES (?1, ?2, ?3)",
                (&tag_id, &other_tag_id, EntryType::Tag),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(EntryId(self.connection.last_insert_rowid()))
    }

    fn files_for_tag_inner(
        &self,
        tag_id: TagId,
        files: &mut BTreeSet<FileId>,
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

        for entry in iterator {
            match entry {
                Entry::File { file_id } => {
                    files.insert(file_id);
                }
                Entry::Tag { tag_id } => {
                    if subtag_rule == SubtagRule::Include {
                        self.files_for_tag_inner(tag_id, files, subtag_rule)?
                    }
                }
            }
        }

        Ok(())
    }

    /// Get all files that are tagged with the provided tag.
    pub fn files_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = FileId>, DatabaseError> {
        let mut files = BTreeSet::new();
        self.files_for_tag_inner(tag_id, &mut files, subtag_rule)?;
        Ok(files)
    }

    fn tags_for_tag_inner(
        &self,
        tag_id: TagId,
        tags: &mut BTreeSet<TagId>,
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

        for tag_id in iterator {
            tags.insert(tag_id);

            if subtag_rule == SubtagRule::Include {
                self.tags_for_tag_inner(tag_id, tags, subtag_rule)?;
            }
        }

        Ok(())
    }

    /// Get all tags that are tagged with the provided tag.
    pub fn tags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = TagId>, DatabaseError> {
        let mut tags = BTreeSet::new();
        self.tags_for_tag_inner(tag_id, &mut tags, subtag_rule)?;
        Ok(tags)
    }

    /// Get the path of a file by the ID.
    pub fn file_path(&self, file_id: FileId) -> Result<OsString, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT path FROM files WHERE id = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let file_path = statement
            .query_map([file_id], |row| row.get::<_, String>(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|path| path.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(file_path.into())
    }

    /// Get the name of a tag by the ID.
    pub fn tag_name(&self, tag_id: TagId) -> Result<String, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT name FROM tags WHERE id = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let tag_name = statement
            .query_map([tag_id], |row| row.get::<_, String>(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|name| name.unwrap())
            .next()
            .ok_or(DatabaseError::MissingTag)?;

        Ok(tag_name)
    }
}

impl DatabaseHandle {
    // TODO: Temporary function to debug.
    pub fn show_files(&self) -> Result<(), DatabaseError> {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct File {
            id: FileId,
            path: String,
        }

        let mut statement = self
            .connection
            .prepare("SELECT id, path FROM files")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([], |row| {
                Ok(File {
                    id: row.get(0)?,
                    path: row.get(1)?,
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
        #[derive(Debug)]
        #[allow(dead_code)]
        struct Tag {
            id: TagId,
            name: String,
        }

        let mut statement = self
            .connection
            .prepare("SELECT id, name FROM tags")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([], |row| {
                Ok(Tag {
                    id: row.get(0)?,
                    name: row.get(1)?,
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
