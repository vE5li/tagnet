use std::{
    collections::BTreeSet,
    fs::File,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{
    Connection, ToSql,
    types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef},
};
use serde::{Deserialize, Serialize};
use tagnet_core::{FileId, FileInfo, LogicalPath, PhysicalPath, TagId, tag::MetadataFormat};

/// A single recorded version of a file's content.
///
/// Rows in `file_versions` are append-only. The `version_number` is a per-file
/// monotonically increasing counter (starts at 1) that defines ordering between
/// versions of the same file. `observed_at` is the unix-millis wall-clock time
/// at which we recorded the version and is metadata only — do not use it for
/// ordering.
#[derive(Debug, Clone)]
pub struct FileVersion {
    pub file_id: FileId,
    pub content_hash: String,
    pub observed_at: i64,
    pub version_number: i64,
    pub origin: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct Tag {
    pub id: TagId,
    pub name: String,
    pub color: String,
    pub metadata: Option<MetadataFormat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
                // `logical_path` is the file's logical identity: its
                // human-readable path/name (possibly nested, e.g.
                // `foo/bar/name.txt`), independent of where any individual sync
                // directory stores the bytes on disk. This is what `list_files`
                // reports and what is advertised to peers. Contrast with
                // `SyncDirectoryDatabase.files.physical_path`, which is the
                // on-disk location within a particular sync directory.
                "CREATE TABLE IF NOT EXISTS files (
            id              TEXT PRIMARY KEY,
            logical_path    TEXT NOT NULL
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

        // Append-only log of content hashes per file. The latest row per
        // `file_id` (highest `version_number`) is the current version.
        //
        // - `version_number` is a per-file monotonic counter starting at 1. It
        //   is what we order by; do not order by `observed_at`.
        // - `observed_at` is unix-millis wall-clock at insert time, kept for
        //   debugging / UI.
        // - `origin` is `'local'` for now. When cross-peer conflict resolution
        //   lands it will hold the originating peer's public key.
        //
        // Intentionally no `FOREIGN KEY` on `file_id`: a version may be
        // recorded by `SyncDirectoryManager` before the corresponding row in
        // `files` exists (which is inserted later, asynchronously, by
        // `handle_changes`). The same ordering will apply when peer-originated
        // versions land. A FK here would fight the message-passing
        // architecture.
        //
        // TODO: When a file is deleted (`FileDatabase::remove_file`), we
        // currently leave its `file_versions` rows behind as a history audit
        // trail. If/when that history grows unwieldy, add a cleanup pass or
        // make `remove_file` cascade the delete here.
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS file_versions (
            file_id         TEXT    NOT NULL,
            content_hash    TEXT    NOT NULL,
            observed_at     INTEGER NOT NULL,
            version_number  INTEGER NOT NULL,
            origin          TEXT    NOT NULL,
            PRIMARY KEY (file_id, version_number)
        )",
                (),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        connection
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_file_versions_latest
                    ON file_versions(file_id, version_number DESC)",
                (),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(Self { connection })
    }

    /// Append a new version row for `file_id`.
    ///
    /// The `version_number` is computed as `MAX(version_number) + 1` for this
    /// file (starting at 1) inside a transaction so concurrent calls cannot
    /// collide on the PK. Returns the newly assigned `version_number`.
    ///
    /// `origin` is `"local"` when the version was observed on disk by this
    /// daemon; it will later be a peer's public key when the version came in
    /// over the wire.
    pub fn record_version(
        &mut self,
        file_id: FileId,
        content_hash: &str,
        origin: &str,
    ) -> Result<i64, DatabaseError> {
        let observed_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or(0);

        let transaction = self
            .connection
            .transaction()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        // `MAX(version_number)` returns NULL when there are no rows for this
        // file_id, which rusqlite refuses to deserialize into a plain `i64`.
        // Pull it as `Option<i64>` and default to 0 here instead.
        let current_max: Option<i64> = transaction
            .query_row(
                "SELECT MAX(version_number) FROM file_versions WHERE file_id = ?1",
                [file_id],
                |row| row.get(0),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        let next_version_number: i64 = current_max.unwrap_or(0) + 1;

        transaction
            .execute(
                "INSERT INTO file_versions
                    (file_id, content_hash, observed_at, version_number, origin)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    file_id,
                    content_hash,
                    observed_at,
                    next_version_number,
                    origin,
                ),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        transaction
            .commit()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(next_version_number)
    }

    /// Return the most recent recorded `content_hash` for every file that has
    /// at least one row in `file_versions`. Used at startup by
    /// `SyncDirectoryManager` to detect files that changed on disk while the
    /// daemon was offline.
    ///
    /// One row per `file_id`; files with no recorded version are absent from
    /// the result.
    pub fn latest_content_hashes(
        &self,
    ) -> Result<std::collections::HashMap<FileId, String>, DatabaseError> {
        // The DESC index on (file_id, version_number) lets SQLite answer this
        // efficiently: for each file_id, take the row with the highest
        // version_number.
        let mut statement = self
            .connection
            .prepare(
                "SELECT file_id, content_hash
                 FROM file_versions AS outer
                 WHERE version_number = (
                     SELECT MAX(version_number)
                     FROM file_versions AS inner
                     WHERE inner.file_id = outer.file_id
                 )",
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut hashes = std::collections::HashMap::new();
        let rows = statement
            .query_map([], |row| {
                let file_id: FileId = row.get(0)?;
                let content_hash: String = row.get(1)?;
                Ok((file_id, content_hash))
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        for row in rows {
            let (file_id, content_hash) = row.map_err(|_| DatabaseError::FailedToExecuteCommand)?;
            hashes.insert(file_id, content_hash);
        }

        Ok(hashes)
    }

    /// Return the most recent recorded version for `file_id`, or `None` if the
    /// file has never had a version recorded.
    pub fn latest_version(&self, file_id: FileId) -> Result<Option<FileVersion>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT content_hash, observed_at, version_number, origin
                 FROM file_versions
                 WHERE file_id = ?1
                 ORDER BY version_number DESC
                 LIMIT 1",
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut rows = statement
            .query_map([file_id], |row| {
                Ok(FileVersion {
                    file_id,
                    content_hash: row.get(0)?,
                    observed_at: row.get(1)?,
                    version_number: row.get(2)?,
                    origin: row.get(3)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        match rows.next() {
            Some(Ok(version)) => Ok(Some(version)),
            Some(Err(_)) => Err(DatabaseError::FailedToExecuteCommand),
            None => Ok(None),
        }
    }

    /// Return the full version history for `file_id`, ordered oldest-first by
    /// `version_number`. Each tuple is `(version_number, content_hash)`. An
    /// empty vec means the file has no recorded versions.
    ///
    /// Used to build `state::ManifestEntry::history`.
    pub fn version_history(&self, file_id: FileId) -> Result<Vec<(i64, String)>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT version_number, content_hash
                 FROM file_versions
                 WHERE file_id = ?1
                 ORDER BY version_number ASC",
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut history = Vec::new();
        let rows = statement
            .query_map([file_id], |row| {
                let version_number: i64 = row.get(0)?;
                let content_hash: String = row.get(1)?;
                Ok((version_number, content_hash))
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        for row in rows {
            history.push(row.map_err(|_| DatabaseError::FailedToExecuteCommand)?);
        }
        Ok(history)
    }

    /// Return every `file_id` that currently has at least one row in the
    /// `files` table (i.e. not deleted) together with its full version
    /// history.
    ///
    /// Files with rows in `file_versions` but no row in `files` (a deleted
    /// file whose history we kept around) are excluded — we don't announce
    /// them during reconciliation. When the tombstone design lands, this is
    /// where deletes will start being included again.
    pub fn manifest_entries(
        &self,
    ) -> Result<Vec<(FileId, Vec<(i64, String)>, i64)>, DatabaseError> {
        // First fetch the file_ids we still know about, then for each fetch
        // its history. Two-stage to keep the SQL straightforward; manifest
        // construction is a one-shot at connect time so the N+1 here is
        // acceptable.
        let mut id_statement = self
            .connection
            .prepare("SELECT id FROM files")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        let file_ids: Vec<FileId> = id_statement
            .query_map([], |row| row.get::<_, FileId>(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut entries = Vec::with_capacity(file_ids.len());
        for file_id in file_ids {
            let history = self.version_history(file_id)?;
            // Files in `files` should always have at least one version
            // (every add/change path records one), but be defensive.
            if history.is_empty() {
                log::warn!(
                    "File {} has no recorded versions; skipping manifest entry",
                    file_id.to_string()
                );
                continue;
            }
            let latest_observed_at = self
                .latest_version(file_id)?
                .map(|version| version.observed_at)
                .unwrap_or(0);
            entries.push((file_id, history, latest_observed_at));
        }
        Ok(entries)
    }

    /// Cheap existence check for a `file_id` in the `files` table. Used by
    /// `handle_changes` to decide whether an inbound `FileAdded` should be
    /// treated as new or as an idempotent re-announcement.
    pub fn file_exists(&self, file_id: FileId) -> Result<bool, DatabaseError> {
        let count: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM files WHERE id = ?1",
                [file_id],
                |row| row.get(0),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        Ok(count > 0)
    }

    /// Add a new file.
    pub fn add_file(
        &self,
        file_id: FileId,
        logical_path: &LogicalPath,
    ) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "INSERT INTO files (id, logical_path) VALUES (?1, ?2)",
                (file_id, logical_path),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    pub fn update_file_logical_path(
        &self,
        file_id: FileId,
        logical_path: &LogicalPath,
    ) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "UPDATE files SET logical_path = ?2 WHERE id = ?1",
                (file_id, logical_path),
            )
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

    /// Get the ID of a file by its logical path.
    pub fn file_id_from_logical_path(
        &self,
        logical_path: &LogicalPath,
    ) -> Result<FileId, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM files WHERE logical_path = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let file_id = statement
            .query_map([logical_path], |row| row.get(0))
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

    /// Get the logical path for `file_id`.
    ///
    /// The inverse of `file_id_from_logical_path`. Errors with `MissingFile` if
    /// the file has no row in `files` (unknown or deleted).
    pub fn logical_path_for_file_id(&self, file_id: FileId) -> Result<LogicalPath, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT logical_path FROM files WHERE id = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let logical_path = statement
            .query_map([file_id], |row| row.get(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|logical_path| logical_path.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(logical_path)
    }

    /// List every currently-known file (i.e. with a row in `files`) together
    /// with its latest version's content hash and version number.
    ///
    /// Files are joined to the row in `file_versions` with the highest
    /// `version_number`, using the DESC index on
    /// `(file_id, version_number)`. Files without any recorded version are
    /// excluded (they should not occur in practice, since every add/change
    /// path records a version, but the inner join makes this defensive).
    pub fn get_all_files(&self) -> Result<Vec<FileInfo>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT f.id, f.logical_path, v.content_hash, v.version_number
                 FROM files AS f
                 JOIN file_versions AS v
                   ON v.file_id = f.id
                  AND v.version_number = (
                      SELECT MAX(version_number)
                      FROM file_versions AS inner
                      WHERE inner.file_id = f.id
                  )",
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut files = Vec::new();
        let rows = statement
            .query_map([], |row| {
                Ok(FileInfo {
                    file_id: row.get(0)?,
                    logical_path: row.get(1)?,
                    content_hash: row.get(2)?,
                    version_number: row.get(3)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        for row in rows {
            files.push(row.map_err(|_| DatabaseError::FailedToExecuteCommand)?);
        }

        Ok(files)
    }
}

// TODO: Temporary functions to debug.
impl FileDatabase {
    pub fn show_content(&self, include_raw: bool) -> Result<(), DatabaseError> {
        #[derive(Debug, Serialize, Deserialize)]
        #[allow(dead_code)]
        pub struct File {
            pub id: FileId,
            pub logical_path: LogicalPath,
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
            .prepare("SELECT id, logical_path FROM files")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let files = statement
            .query_map([], |row| {
                Ok(File {
                    id: row.get(0)?,
                    logical_path: row.get(1)?,
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
                .filter_map(|entry| {
                    // The tag row may be absent (e.g. a file tagged with a tag
                    // this node has never received — tags aren't synced yet).
                    // This is debug output, so skip dangling references rather
                    // than panicking and taking down the change handler.
                    tags.iter()
                        .find(|tag| tag.as_ref().unwrap().id == entry.tag_id)
                        .map(|tag| tag.as_ref().unwrap())
                })
                .map(|tag| format!("\x1B[0;31m{}\x1B[0m", tag.name))
                .collect::<Vec<String>>();

            println!(
                "\x1B[0;34m{}\x1B[0m | \x1B[0;35m{}\x1B[0m | {}",
                file.id.to_string(),
                file.logical_path,
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
                .filter_map(|entry| {
                    // See note above: skip dangling tag references instead of
                    // panicking.
                    tags.iter()
                        .find(|tag| tag.as_ref().unwrap().id == entry.tag_id)
                        .map(|tag| tag.as_ref().unwrap())
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
    /// The file's physical path on disk, relative to this sync directory's
    /// root. For a `TagBased` directory this equals the file's logical path;
    /// for a `Universal` directory it is the file's `file_id` (files are stored
    /// under their id on disk). This also serves as the reverse index for
    /// filesystem events (path -> file_id), so it must always reflect the actual
    /// on-disk name. It is NOT the value to advertise to peers or show to users;
    /// for that use the logical path from `FileDatabase`
    /// (`FileDatabase.files.logical_path`).
    pub physical_path: PhysicalPath,
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
                // `physical_path` is where the bytes live on disk relative to
                // this sync directory's root, and doubles as the reverse index
                // for filesystem events (path -> file_id). For TagBased it
                // equals the logical path; for Universal it is the `file_id`.
                // The logical/human name lives in `FileDatabase.files`.
                "CREATE TABLE IF NOT EXISTS files (
            id              TEXT PRIMARY KEY,
            physical_path   TEXT NOT NULL
        )",
                (), // empty list of parameters.
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(Self { connection })
    }

    /// Add a new file.
    ///
    /// Note: content hashes are stored in `FileDatabase::file_versions`, not
    /// here. After calling this you typically also want to call
    /// `FileDatabase::record_version`.
    pub fn add_file(
        &self,
        file_id: FileId,
        physical_path: &PhysicalPath,
    ) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "INSERT INTO files (id, physical_path) VALUES (?1, ?2)",
                (file_id, physical_path),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    pub fn update_file_physical_path(
        &self,
        file_id: FileId,
        physical_path: &PhysicalPath,
    ) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "UPDATE files SET physical_path = ?2 WHERE id = ?1",
                (file_id, physical_path),
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

    // pub fn remove_file_by_physical_path(&self, physical_path: impl AsRef<str>) -> Result<(), DatabaseError> {
    //     self.connection
    //         .execute("DELETE FROM files WHERE physical_path = ?1", [physical_path.as_ref()])
    //         .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
    //
    //     Ok(())
    // }

    pub fn get_file(&self, file_id: FileId) -> Result<SyncDirectoryFile, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT physical_path FROM files WHERE id = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let file = statement
            .query_map([file_id], |row| {
                Ok(SyncDirectoryFile {
                    file_id,
                    physical_path: row.get(0)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|preview| preview.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(file)
    }

    pub fn get_file_id(&self, physical_path: &PhysicalPath) -> Result<FileId, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM files WHERE physical_path = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let id = statement
            .query_map([physical_path], |row| row.get(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|preview| preview.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(id)
    }

    pub fn get_all_files(&self) -> Result<Vec<SyncDirectoryFile>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, physical_path FROM files")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(statement
            .query_map([], |row| {
                Ok(SyncDirectoryFile {
                    file_id: row.get(0)?,
                    physical_path: row.get(1)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|file| file.unwrap())
            .collect())
    }

    pub fn get_all_files_at(
        &self,
        physical_path: &PhysicalPath,
    ) -> Result<Vec<SyncDirectoryFile>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, physical_path FROM files WHERE physical_path LIKE ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let matcher = format!("{}%", physical_path.as_str());

        Ok(statement
            .query_map([matcher], |row| {
                Ok(SyncDirectoryFile {
                    file_id: row.get(0)?,
                    physical_path: row.get(1)?,
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
            pub physical_path: PhysicalPath,
        }

        let mut statement = self
            .connection
            .prepare("SELECT id, physical_path FROM files")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([], |row| {
                Ok(File {
                    id: row.get(0)?,
                    physical_path: row.get(1)?,
                })
            })
            .unwrap();

        for file in iterator {
            println!("{:?}", file.unwrap());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_db() -> FileDatabase {
        FileDatabase::initialize(":memory:").expect("open in-memory db")
    }

    #[test]
    fn logical_path_for_file_id_roundtrips() {
        let database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("photos/cat.jpg"))
            .unwrap();

        assert_eq!(
            database.logical_path_for_file_id(file_id).unwrap(),
            LogicalPath::new("photos/cat.jpg")
        );
    }

    #[test]
    fn logical_path_for_file_id_missing_is_not_found() {
        let database = memory_db();
        let missing = FileId::new();
        assert!(matches!(
            database.logical_path_for_file_id(missing),
            Err(DatabaseError::MissingFile)
        ));
    }

    #[test]
    fn get_all_files_reports_latest_version() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"))
            .unwrap();

        // Two versions; get_all_files must report the latest (higher
        // version_number), not the first.
        let v1 = database
            .record_version(file_id, "hash-v1", "local")
            .unwrap();
        let v2 = database
            .record_version(file_id, "hash-v2", "local")
            .unwrap();
        assert!(v2 > v1);

        let files = database.get_all_files().unwrap();
        assert_eq!(files.len(), 1);
        let info = &files[0];
        assert_eq!(info.file_id, file_id);
        assert_eq!(info.logical_path, LogicalPath::new("a.txt"));
        assert_eq!(info.content_hash, "hash-v2");
        assert_eq!(info.version_number, v2);
    }

    #[test]
    fn get_all_files_excludes_files_without_versions() {
        let database = memory_db();
        // A file row with no recorded version: the inner join drops it.
        database
            .add_file(FileId::new(), &LogicalPath::new("orphan.txt"))
            .unwrap();

        assert!(database.get_all_files().unwrap().is_empty());
    }

    #[test]
    fn get_all_files_empty_when_no_files() {
        let database = memory_db();
        assert!(database.get_all_files().unwrap().is_empty());
    }
}
