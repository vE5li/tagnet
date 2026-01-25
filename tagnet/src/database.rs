use std::{collections::BTreeSet, fs::File, path::Path};

use rusqlite::{
    Connection, ToSql,
    types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef},
};
use serde::{Deserialize, Serialize};
use tagnet_core::{FileId, TagId, tag::MetadataFormat};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubtagRule {
    Include,
    Exclude,
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

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct Tag {
    pub id: TagId,
    pub name: String,
    pub color: String,
    pub metadata: Option<MetadataFormat>,
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
pub struct FileDatabase {
    connection: Connection,
}

impl FileDatabase {
    pub fn initialize(database_path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let connection =
            Connection::open(database_path).map_err(|_| DatabaseError::UnableToOpenOrCreate)?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS files (
            id              TEXT PRIMARY KEY,
            path            TEXT NOT NULL
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

        Ok(Self { connection })
    }

    /// Add a new file.
    pub fn add_file(&self, file_id: FileId, path: String) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "INSERT INTO files (id, path) VALUES (?1, ?2)",
                (file_id, path),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    pub fn update_file_path(&self, file_id: FileId, path: String) -> Result<(), DatabaseError> {
        self.connection
            .execute("UPDATE files SET path = ?2 WHERE id = ?1", (file_id, path))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    pub fn remove_file(&self, file_id: FileId) -> Result<(), DatabaseError> {
        self.connection
            .execute("DELETE FROM files WHERE id = ?1", [file_id])
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    /// Add a new tag.
    pub fn add_tag(
        &self,
        tag_id: TagId,
        name: impl Into<String>,
        color: impl Into<String>,
    ) -> Result<(), DatabaseError> {
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
                "INSERT INTO tags (id, name, color) VALUES (?1, ?2, ?3)",
                (tag_id, &name, &color),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
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

    /// Get all tags.
    pub fn get_all_tags(&self) -> Result<impl IntoIterator<Item = Tag>, DatabaseError> {
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
                    metadata: None,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|tag| tag.unwrap())
            .collect::<Vec<_>>();

        Ok(tag_list)
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
                    metadata: None,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|tag| tag.unwrap())
            .next()
            .ok_or(DatabaseError::MissingTag)?;

        Ok(tag)
    }

    /// Get the ID of a file by the path.
    pub fn file_id_from_path(&self, file_path: impl AsRef<Path>) -> Result<FileId, DatabaseError> {
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
}

// TODO: Temporary functions to debug.
impl FileDatabase {
    pub fn show_content(&self, include_raw: bool) -> Result<(), DatabaseError> {
        #[derive(Debug, Serialize, Deserialize)]
        #[allow(dead_code)]
        pub struct File {
            pub id: FileId,
            pub path: String,
        }

        #[derive(Debug, Serialize, Deserialize)]
        #[allow(dead_code)]
        pub struct Tag {
            pub id: TagId,
            pub name: String,
            pub color: String,
        }

        #[derive(Debug)]
        #[allow(dead_code)]
        struct Entry {
            tag_id: TagId,
            target_id_as_file_id: FileId,
            target_id_as_tag_id: TagId,
            r#type: EntryType,
        }

        let mut statement = self
            .connection
            .prepare("SELECT id, path FROM files")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let files = statement
            .query_map([], |row| {
                Ok(File {
                    id: row.get(0)?,
                    path: row.get(1)?,
                })
            })
            .unwrap()
            .collect::<Vec<_>>();

        let mut statement = self
            .connection
            .prepare("SELECT id, name, color FROM tags")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let tags = statement
            .query_map([], |row| {
                Ok(Tag {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    color: row.get(2)?,
                })
            })
            .unwrap()
            .collect::<Vec<_>>();

        let mut statement = self
            .connection
            .prepare("SELECT tag_id, target_id, type FROM entries")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let entries = statement
            .query_map([], |row| {
                Ok(Entry {
                    tag_id: row.get(0)?,
                    target_id_as_file_id: row.get(1)?,
                    target_id_as_tag_id: row.get(1)?,
                    r#type: row.get(2)?,
                })
            })
            .unwrap()
            .collect::<Vec<_>>();

        if include_raw {
            for file in &files {
                println!("{:?}", file.as_ref().unwrap());
            }

            for tag in &tags {
                println!("{:?}", tag.as_ref().unwrap());
            }

            for entry in &entries {
                println!("{:?}", entry.as_ref().unwrap());
            }
        }

        for file in &files {
            let file = file.as_ref().unwrap();

            let tags = entries
                .iter()
                .filter_map(|entry| {
                    let entry = entry.as_ref().unwrap();
                    (entry.r#type == EntryType::File && entry.target_id_as_file_id == file.id)
                        .then_some(entry)
                })
                .map(|entry| {
                    tags.iter()
                        .find(|tag| {
                            let tag = tag.as_ref().unwrap();
                            tag.id == entry.tag_id
                        })
                        .unwrap()
                        .as_ref()
                        .unwrap()
                })
                .map(|tag| format!("\x1B[0;31m{}\x1B[0m", tag.name))
                .collect::<Vec<String>>();

            println!(
                "\x1B[0;34m{}\x1B[0m | \x1B[0;35m{}\x1B[0m | {}",
                file.id.to_string(),
                file.path,
                tags.join(", ")
            );
        }

        for tag in &tags {
            let tag = tag.as_ref().unwrap();

            let tags = entries
                .iter()
                .filter_map(|entry| {
                    let entry = entry.as_ref().unwrap();
                    (entry.r#type == EntryType::Tag && entry.target_id_as_tag_id == tag.id)
                        .then_some(entry)
                })
                .map(|entry| {
                    tags.iter()
                        .find(|tag| {
                            let tag = tag.as_ref().unwrap();
                            tag.id == entry.tag_id
                        })
                        .unwrap()
                        .as_ref()
                        .unwrap()
                })
                .map(|tag| format!("\x1B[0;32m{}\x1B[0m", tag.name))
                .collect::<Vec<String>>();

            println!(
                "\x1B[0;36m{}\x1B[0m | \x1B[0;33m{}\x1B[0m | {}",
                tag.id.to_string(),
                tag.name,
                tags.join(", ")
            );
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SyncDirectoryFile {
    pub file_id: FileId,
    pub path: String,
    pub content_hash: String,
}

#[derive(Debug)]
pub struct SyncDirectoryDatabase {
    connection: Connection,
}

impl SyncDirectoryDatabase {
    pub fn initialize(database_path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let connection =
            Connection::open(database_path).map_err(|_| DatabaseError::UnableToOpenOrCreate)?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS files (
            id              TEXT PRIMARY KEY,
            path            TEXT NOT NULL,
            content_hash    TEXT
        )",
                (), // empty list of parameters.
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(Self { connection })
    }

    /// Add a new file.
    pub fn add_file(
        &self,
        file_id: FileId,
        path: impl AsRef<str>,
        content_hash: String,
    ) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "INSERT INTO files (id, path, content_hash) VALUES (?1, ?2, ?3)",
                (file_id, path.as_ref(), content_hash),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    pub fn update_file_content_hash(
        &self,
        file_id: FileId,
        content_hash: String,
    ) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "UPDATE files SET content_hash = ?2 WHERE id = ?1",
                (file_id, content_hash),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    pub fn update_file_path(
        &self,
        file_id: FileId,
        path: impl AsRef<str>,
    ) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "UPDATE files SET path = ?2 WHERE id = ?1",
                (file_id, path.as_ref()),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    pub fn remove_file_by_id(&self, file_id: FileId) -> Result<(), DatabaseError> {
        self.connection
            .execute("DELETE FROM files WHERE id = ?1", [file_id])
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    // pub fn remove_file_by_path(&self, path: impl AsRef<str>) -> Result<(), DatabaseError> {
    //     self.connection
    //         .execute("DELETE FROM files WHERE path = ?1", [path.as_ref()])
    //         .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
    //
    //     Ok(())
    // }

    pub fn get_file(&self, file_id: FileId) -> Result<SyncDirectoryFile, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT path, content_hash FROM files WHERE id = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let file = statement
            .query_map([file_id], |row| {
                Ok(SyncDirectoryFile {
                    file_id,
                    path: row.get(0)?,
                    content_hash: row.get(1)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|preview| preview.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(file)
    }

    pub fn get_file_id(&self, path: impl AsRef<str>) -> Result<FileId, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM files WHERE path = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let id = statement
            .query_map([path.as_ref()], |row| row.get(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|preview| preview.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(id)
    }

    pub fn get_all_files(&self) -> Result<Vec<SyncDirectoryFile>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, path, content_hash FROM files")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(statement
            .query_map([], |row| {
                Ok(SyncDirectoryFile {
                    file_id: row.get(0)?,
                    path: row.get(1)?,
                    content_hash: row.get(2)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|file| file.unwrap())
            .collect())
    }

    pub fn get_all_files_at(
        &self,
        path: impl AsRef<str>,
    ) -> Result<Vec<SyncDirectoryFile>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, path, content_hash FROM files WHERE path LIKE ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let matcher = format!("{}%", path.as_ref());

        Ok(statement
            .query_map([matcher], |row| {
                Ok(SyncDirectoryFile {
                    file_id: row.get(0)?,
                    path: row.get(1)?,
                    content_hash: row.get(2)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|file| file.unwrap())
            .collect())
    }
}

// TODO: Temporary functions to debug.
impl SyncDirectoryDatabase {
    pub fn show_files(&self) -> Result<(), DatabaseError> {
        #[derive(Debug, Serialize, Deserialize)]
        #[allow(dead_code)]
        pub struct File {
            pub id: FileId,
            pub path: String,
            pub content_hash: String,
        }

        let mut statement = self
            .connection
            .prepare("SELECT id, path, content_hash FROM files")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([], |row| {
                Ok(File {
                    id: row.get(0)?,
                    path: row.get(1)?,
                    content_hash: row.get(2)?,
                })
            })
            .unwrap();

        for file in iterator {
            println!("{:?}", file.unwrap());
        }

        Ok(())
    }
}
