use std::{
    cell::RefCell,
    collections::HashMap,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use notify::{RecursiveMode, Watcher};
use tagnet_core::{
    FileId, PhysicalPath, TagId,
    state::{Change, ChangeOrigin},
};
use walkdir::WalkDir;

use crate::{
    bus::DaemonMessage,
    configuration::{Configuration, SyncType},
    database::{DatabaseError, SyncDirectoryDatabase, SyncDirectoryFile},
    paths::Paths,
    watcher::{DebouncedEventKind, WatchDispatcher},
};

#[derive(Debug, Clone, Copy)]
enum SyncDirectoryError {
    UnmonitoredDirectory,
    FailedToOpenFile,
    FailedToReadFile,
    MissingTrackedFile,
    FailedAddingFile,
    FailedChangingFile,
    FailedMovingFile,
    FailedRemovingFile,
}

pub enum SyncDirectoryCommand {
    CreateFile {
        file_id: FileId,
        /// Where to place the bytes on disk within the target sync directory.
        /// Already resolved from the file's logical path by the caller via
        /// `SyncType::physical_for`, so the handler stores it verbatim.
        physical_path: PhysicalPath,
        content: Vec<u8>,
        // Maybe a bit weird to have it like this? Not sure.
        // We currently need that to check which directory this event was meant for.
        sync_directory_path: PathBuf,
    },
    ChangeFile {
        file_id: FileId,
        content: Vec<u8>,
        // Maybe a bit weird to have it like this? Not sure.
        // We currently need that to check which directory this event was meant for.
        sync_directory_path: PathBuf,
    },
    MoveFile {
        file_id: FileId,
        /// The new on-disk location within the target sync directory, already
        /// resolved from the file's new logical path via `SyncType::physical_for`.
        physical_path: PhysicalPath,
        // Maybe a bit weird to have it like this? Not sure.
        // We currently need that to check which directory this event was meant for.
        sync_directory_path: PathBuf,
    },
    RemoveFile {
        file_id: FileId,
        // Maybe a bit weird to have it like this? Not sure.
        // We currently need that to check which directory this event was meant for.
        sync_directory_path: PathBuf,
    },
    /// Read the bytes for `file_id` from whichever sync directory currently
    /// holds it and respond on `respond_to`. Used by peer connection tasks to
    /// fulfil a `Sync::Request`.
    ///
    /// The response is `Some((relative_path, content, content_hash))` if at
    /// least one sync directory has the file, `None` otherwise. The hash is
    /// recomputed from the bytes (cheap with BLAKE3) rather than trusting the
    /// per-sync-directory DB.
    /// Resolve `file_id` to the **absolute** on-disk path where its bytes
    /// currently live in the first sync directory that holds it, without reading
    /// the content. Responds with `None` if no sync directory has the file.
    /// Used by `tagnet edit` to open a locally-present file in place (so the
    /// filesystem watcher picks up the save and propagates it).
    LocalPath {
        file_id: FileId,
        respond_to: tokio::sync::oneshot::Sender<Option<PathBuf>>,
    },
    ReadFile {
        file_id: FileId,
        respond_to: tokio::sync::oneshot::Sender<Option<(PhysicalPath, Vec<u8>, String)>>,
    },
}

struct RichSyncDirectory {
    path: PathBuf,
    sync_type: SyncType,
    database: SyncDirectoryDatabase,
}

pub struct SyncDirectoryManager {
    sync_directories: Vec<RichSyncDirectory>,
    change_sender: tokio::sync::mpsc::UnboundedSender<DaemonMessage>,
    _dispatcher: WatchDispatcher,
    watcher_events: tokio::sync::mpsc::UnboundedReceiver<DebouncedEventKind>,
    command_receiver: tokio::sync::mpsc::UnboundedReceiver<SyncDirectoryCommand>,
    // TODO: Make this a more robust messaging framework instead of a ref cell.
    skip_queue: RefCell<Vec<DebouncedEventKind>>,
}

impl SyncDirectoryManager {
    pub async fn new(
        configuration: Configuration,
        paths: &Paths,
        change_sender: tokio::sync::mpsc::UnboundedSender<DaemonMessage>,
        command_receiver: tokio::sync::mpsc::UnboundedReceiver<SyncDirectoryCommand>,
    ) -> Self {
        let (mut dispatcher, watcher_events) = WatchDispatcher::new()
            .await
            .expect("Failed to set up debouncer");

        let sync_directories = configuration
            .sync_directories
            .iter()
            .filter_map(|sync_directory| {
                let path = sync_directory.path.clone();

                log::debug!(
                    "Setting up sync directory at {}",
                    sync_directory.path.to_string_lossy()
                );

                if let Err(error) = std::fs::create_dir_all(&path) {
                    log::error!("Failed to create sync directory: {}", error);
                    return None;
                }

                if let Err(error) = dispatcher
                    .watcher()
                    .watch(path.as_ref(), RecursiveMode::Recursive)
                {
                    log::error!("Failed to set up watcher for sync directory: {}", error);
                    return None;
                }

                let database_path = paths.sync_directory_db_path(&path);

                let database = match SyncDirectoryDatabase::initialize(database_path) {
                    Ok(database) => database,
                    Err(error) => {
                        log::error!("Failed to set up sync directory database: {:?}", error);
                        return None;
                    }
                };

                Some(RichSyncDirectory {
                    path,
                    sync_type: sync_directory.sync_type.clone(),
                    database,
                })
            })
            .collect::<Vec<_>>();

        Self {
            sync_directories,
            change_sender,
            _dispatcher: dispatcher,
            watcher_events,
            command_receiver,
            skip_queue: Default::default(),
        }
    }

    fn send_change(
        &self,
        sync_directory: &RichSyncDirectory,
        change: Change,
    ) -> Result<(), SyncDirectoryError> {
        let change_origin = ChangeOrigin::Local {
            directory_path: sync_directory.path.clone(),
        };

        // FIX: Put this into a queue for proper retry handling instead.
        let _ = self
            .change_sender
            .send(DaemonMessage::Change(change, change_origin));

        Ok(())
    }

    fn add_file(
        &self,
        sync_directory: &RichSyncDirectory,
        path: impl AsRef<Path>,
        content: Vec<u8>,
        content_hash: String,
        tags: Vec<TagId>,
    ) -> Result<(), SyncDirectoryError> {
        let file_id = FileId::new();

        // Ingestion boundary: this file's on-disk relative path within the sync
        // directory *is* its logical identity. `add_file` is only used by
        // TagBased directories (Universal uses `upload_file`), so the physical
        // and logical paths coincide here; we still derive them explicitly.
        let logical_path = PhysicalPath::new(path.as_ref().to_string_lossy()).into_logical();
        let physical_path = sync_directory
            .sync_type
            .physical_for(&logical_path, file_id);

        sync_directory
            .database
            .add_file(file_id, &physical_path)
            .map_err(|_| SyncDirectoryError::FailedAddingFile)?;

        self.send_change(
            sync_directory,
            Change::FileAdded {
                file_id,
                logical_path,
                content,
                content_hash,
                tags,
            },
        )
    }

    fn upload_file(
        &self,
        sync_directory: &RichSyncDirectory,
        path: impl AsRef<Path>,
        content: Vec<u8>,
        content_hash: String,
        tags: Vec<TagId>,
    ) -> Result<(), SyncDirectoryError> {
        let file_id = FileId::new();

        // Ingestion boundary: the on-disk relative path becomes the file's
        // logical identity. (For a Universal directory that relative path is
        // itself the file's `file_id`, so an uploaded Universal file's logical
        // path is its id until a real name is supplied elsewhere.)
        let logical_path = PhysicalPath::new(path.as_ref().to_string_lossy()).into_logical();

        if let Err(error) = self.send_change(
            sync_directory,
            Change::FileAdded {
                file_id,
                logical_path,
                content,
                content_hash,
                tags,
            },
        ) {
            log::error!(
                "Failed to add file {}: {:?}",
                path.as_ref().to_string_lossy(),
                error
            );
            return Err(error);
        }

        let full_path = sync_directory.path.join(path);

        log::info!(
            "File {} was uploaded. Removing the local file",
            full_path.to_string_lossy()
        );

        self.skip_queue
            .borrow_mut()
            .push(DebouncedEventKind::Remove {
                file_name: full_path.clone(),
            });

        // After the file was uploaded we want to remove it from this directory.
        // TODO: Don't unwrap.
        std::fs::remove_file(full_path).unwrap();

        Ok(())
    }

    fn update_file_content(
        &self,
        sync_directory: &RichSyncDirectory,
        file_id: FileId,
        content: Vec<u8>,
        content_hash: String,
    ) -> Result<(), SyncDirectoryError> {
        self.send_change(
            sync_directory,
            Change::FileChanged {
                file_id,
                content,
                content_hash,
            },
        )
    }

    /// Handle a file being moved/renamed *within* a sync directory on this
    /// device. The new on-disk relative path is an ingestion boundary: it
    /// defines the file's new logical identity. We update our own physical
    /// record and announce the new logical path to peers via `FileMoved`.
    ///
    /// Only meaningful for `TagBased` directories (a Universal directory has no
    /// user-visible on-disk names to move); callers guarantee that.
    fn move_file_within_directory(
        &self,
        sync_directory: &RichSyncDirectory,
        file_id: FileId,
        new_relative_path: impl AsRef<Path>,
    ) -> Result<(), SyncDirectoryError> {
        let logical_path =
            PhysicalPath::new(new_relative_path.as_ref().to_string_lossy()).into_logical();
        let physical_path = sync_directory
            .sync_type
            .physical_for(&logical_path, file_id);

        sync_directory
            .database
            .update_file_physical_path(file_id, &physical_path)
            .map_err(|_| SyncDirectoryError::FailedChangingFile)?;

        self.send_change(
            sync_directory,
            Change::FileMoved {
                file_id,
                logical_path,
            },
        )
    }

    fn remove_file_by_id(
        &self,
        sync_directory: &RichSyncDirectory,
        file_id: FileId,
    ) -> Result<(), SyncDirectoryError> {
        sync_directory
            .database
            .remove_file_by_id(file_id)
            .map_err(|_| SyncDirectoryError::FailedRemovingFile)?;

        self.send_change(sync_directory, Change::FileDeleted { file_id })
    }

    fn get_all_files(
        &self,
        sync_directory: &RichSyncDirectory,
    ) -> Result<Vec<SyncDirectoryFile>, SyncDirectoryError> {
        sync_directory
            .database
            .get_all_files()
            .map_err(|_| SyncDirectoryError::MissingTrackedFile)
    }

    fn get_all_files_at(
        &self,
        sync_directory: &RichSyncDirectory,
        physical_path: impl AsRef<Path>,
    ) -> Result<Vec<SyncDirectoryFile>, SyncDirectoryError> {
        let physical_path = PhysicalPath::new(physical_path.as_ref().to_string_lossy());
        sync_directory
            .database
            .get_all_files_at(&physical_path)
            .map_err(|_| SyncDirectoryError::MissingTrackedFile)
    }

    /// Compute a content-addressed hash of `content` for use in
    /// `file_versions.content_hash`.
    ///
    /// Uses BLAKE3 (cryptographic, 256-bit) returned as a 64-char lowercase
    /// hex string. Two files with the same bytes always produce the same
    /// string; collisions are not a practical concern.
    fn calculate_content_hash(content: &[u8]) -> String {
        blake3::hash(content).to_hex().to_string()
    }

    fn get_file_content(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<(Vec<u8>, String), SyncDirectoryError> {
        let mut file =
            std::fs::File::open(path).map_err(|_| SyncDirectoryError::FailedToOpenFile)?;
        let mut content = Vec::new();

        file.read_to_end(&mut content)
            .map_err(|_| SyncDirectoryError::FailedToReadFile)?;

        let content_hash = Self::calculate_content_hash(&content);

        Ok((content, content_hash))
    }

    fn sync_directory_for_path(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<&RichSyncDirectory, SyncDirectoryError> {
        self.sync_directories
            .iter()
            .find(|sync_directory| path.as_ref().starts_with(&sync_directory.path))
            .ok_or(SyncDirectoryError::UnmonitoredDirectory)
    }

    fn get_file_id(
        &self,
        sync_directory: &RichSyncDirectory,
        physical_path: impl AsRef<Path>,
    ) -> Result<FileId, SyncDirectoryError> {
        let physical_path = PhysicalPath::new(physical_path.as_ref().to_string_lossy());
        sync_directory
            .database
            .get_file_id(&physical_path)
            .map_err(|_| SyncDirectoryError::MissingTrackedFile)
    }

    fn intial_sync_universal(
        &self,
        sync_directory: &RichSyncDirectory,
        files: Vec<SyncDirectoryFile>,
        last_known_hashes: &HashMap<FileId, String>,
    ) {
        for sync_file in files {
            let full_path = sync_directory.path.join(sync_file.file_id.to_string());

            log::debug!("Checking file {}", full_path.to_string_lossy());

            if !full_path.exists() {
                log::info!(
                    "File {} was deleted without monitoring. Syncing deletion",
                    full_path.to_string_lossy()
                );

                if let Err(error) = self.remove_file_by_id(sync_directory, sync_file.file_id) {
                    log::error!(
                        "Failed to remove file {}: {:?}",
                        full_path.to_string_lossy(),
                        error
                    );
                }

                continue;
            }

            let (content, content_hash) = match self.get_file_content(&full_path) {
                Ok((content, content_hash)) => (content, content_hash),
                Err(error) => {
                    log::error!("Failed to read file content: {:?}", error);
                    continue;
                }
            };

            let last_known_hash = last_known_hashes.get(&sync_file.file_id);
            if last_known_hash.map(String::as_str) != Some(content_hash.as_str()) {
                log::info!(
                    "File {} was changed without monitoring. Syncing change",
                    full_path.to_string_lossy()
                );

                if let Err(error) = self.update_file_content(
                    sync_directory,
                    sync_file.file_id,
                    content,
                    content_hash,
                ) {
                    log::error!(
                        "Failed to update file {}: {:?}",
                        full_path.to_string_lossy(),
                        error
                    );
                }
            }
        }

        for entry in WalkDir::new(&sync_directory.path)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
        {
            let Ok(relative_path) = entry.path().strip_prefix(&sync_directory.path) else {
                panic!("Walkdir returned a path outside of the sync directory");
            };

            // FIX: Maybe this should not use to_string_lossy but rather to utf8 since a valid uuid
            // will always ve valid utf8?
            if let Some(file_id) = FileId::from_string(&relative_path.to_string_lossy()) {
                match sync_directory.database.get_file(file_id) {
                    // File is already tracked.
                    Ok(_) => {
                        log::debug!(
                            "File {} is already tracked",
                            relative_path.to_string_lossy()
                        );
                        continue;
                    }
                    Err(DatabaseError::MissingFile) => {
                        // Fall through.
                    }
                    Err(error) => {
                        panic!("Database error: {:?}", error);
                    }
                }
            }

            log::info!(
                "File {} was added without monitoring. Uploading file",
                entry.path().to_string_lossy()
            );

            let (content, content_hash) = match self.get_file_content(entry.path()) {
                Ok(content_and_hash) => content_and_hash,
                Err(error) => {
                    log::error!("Failed to read added file: {:?}", error);
                    continue;
                }
            };

            self.upload_file(
                sync_directory,
                relative_path,
                content,
                content_hash,
                Vec::new(),
            )
            .unwrap();
        }
    }

    fn intial_sync_tagged(
        &self,
        sync_directory: &RichSyncDirectory,
        files: Vec<SyncDirectoryFile>,
        tags: &[TagId],
        last_known_hashes: &HashMap<FileId, String>,
    ) {
        for sync_file in files {
            let full_path = sync_directory.path.join(sync_file.physical_path.as_str());

            log::debug!("Checking file {}", full_path.to_string_lossy());

            if !full_path.exists() {
                log::info!(
                    "File {} was deleted without monitoring. Syncing deletion",
                    full_path.to_string_lossy()
                );

                if let Err(error) = self.remove_file_by_id(sync_directory, sync_file.file_id) {
                    log::error!(
                        "Failed to remove file {}: {:?}",
                        full_path.to_string_lossy(),
                        error
                    );
                }

                continue;
            }

            let (content, content_hash) = match self.get_file_content(&full_path) {
                Ok((content, content_hash)) => (content, content_hash),
                Err(error) => {
                    log::error!("Failed to read file content: {:?}", error);
                    continue;
                }
            };

            let last_known_hash = last_known_hashes.get(&sync_file.file_id);
            if last_known_hash.map(String::as_str) != Some(content_hash.as_str()) {
                log::info!(
                    "File {} was changed without monitoring. Syncing change",
                    full_path.to_string_lossy()
                );

                if let Err(error) = self.update_file_content(
                    sync_directory,
                    sync_file.file_id,
                    content,
                    content_hash,
                ) {
                    log::error!(
                        "Failed to update file {}: {:?}",
                        full_path.to_string_lossy(),
                        error
                    );
                }
            }
        }

        for entry in WalkDir::new(&sync_directory.path)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
        {
            let Ok(relative_path) = entry.path().strip_prefix(&sync_directory.path) else {
                log::error!("Walkdir returned a path outside of the sync directory");
                continue;
            };

            match sync_directory
                .database
                .get_file_id(&PhysicalPath::new(relative_path.to_string_lossy()))
            {
                // File is already tracked.
                Ok(_) => {
                    log::debug!(
                        "File {} is already tracked",
                        relative_path.to_string_lossy()
                    );
                }
                Err(DatabaseError::MissingFile) => {
                    log::info!(
                        "File {} was added without monitoring. Syncing addition",
                        entry.path().to_string_lossy()
                    );

                    let (content, content_hash) = match self.get_file_content(entry.path()) {
                        Ok((content, content_hash)) => (content, content_hash),
                        Err(error) => {
                            log::error!("Failed to read added file: {:?}", error);
                            continue;
                        }
                    };

                    if let Err(error) = self.add_file(
                        sync_directory,
                        relative_path,
                        content,
                        content_hash,
                        tags.to_vec(),
                    ) {
                        log::error!(
                            "Failed to add file {}: {:?}",
                            relative_path.to_string_lossy(),
                            error
                        );
                    }
                }
                Err(error) => {
                    panic!("Database error: {:?}", error);
                }
            }
        }
    }

    fn run_initial_sync(&mut self, last_known_hashes: &HashMap<FileId, String>) {
        for sync_directory in &self.sync_directories {
            log::debug!(
                "Checking for missed updates at {}",
                sync_directory.path.to_string_lossy()
            );

            let files = match self.get_all_files(sync_directory) {
                Ok(files) => files,
                Err(error) => {
                    log::error!("Failed to get list of tracked files: {:?}", error);
                    continue;
                }
            };

            match &sync_directory.sync_type {
                SyncType::Universal => {
                    self.intial_sync_universal(sync_directory, files, last_known_hashes)
                }
                SyncType::TagBased { tags } => {
                    self.intial_sync_tagged(sync_directory, files, tags, last_known_hashes)
                }
            }
        }
    }

    fn try_remove_empty_directory(&self, directory_path: impl AsRef<Path>) {
        if let Ok(mut read_dir) = directory_path.as_ref().read_dir()
            && read_dir.next().is_none()
        {
            log::info!(
                "Removing empty directory {}",
                directory_path.as_ref().to_string_lossy()
            );

            std::fs::remove_dir(directory_path).expect("failed to remove empty directory");
        }
    }

    fn handle_command(&mut self, command: SyncDirectoryCommand) -> Result<(), SyncDirectoryError> {
        match command {
            SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;

                // `physical_path` was already resolved from the file's logical
                // path via `SyncType::physical_for` by the caller, so it is the
                // correct on-disk name for this directory type (the file_id for
                // Universal, the logical path for TagBased). Store the bytes and
                // record the physical path verbatim.
                let file_path = sync_directory.path.join(physical_path.as_str());

                log::info!("Adding file at {}", file_path.to_string_lossy());

                // FIX: Don't unwrap.
                std::fs::create_dir_all(file_path.parent().unwrap())
                    .expect("failed to create file subdirectory");
                let mut file = std::fs::File::create(&file_path).expect("failed to create file");
                file.write_all(&content).unwrap();

                sync_directory
                    .database
                    .add_file(file_id, &physical_path)
                    .map_err(|_| SyncDirectoryError::FailedAddingFile)?;

                self.skip_queue
                    .borrow_mut()
                    .push(DebouncedEventKind::Create {
                        file_name: file_path.clone(),
                    });
            }
            SyncDirectoryCommand::ChangeFile {
                file_id,
                content,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;

                let physical_path = match &sync_directory.sync_type {
                    SyncType::Universal => PhysicalPath::new(file_id.to_string()),
                    SyncType::TagBased { .. } => {
                        sync_directory
                            .database
                            .get_file(file_id)
                            .map_err(|_| SyncDirectoryError::FailedChangingFile)?
                            .physical_path
                    }
                };
                let file_path = sync_directory.path.join(physical_path.as_str());

                log::info!("Modifying file at {}", file_path.to_string_lossy());

                // FIX: Don't unwrap.
                let mut file = std::fs::File::create(&file_path).expect("failed to create file");
                file.write_all(&content).unwrap();

                self.skip_queue
                    .borrow_mut()
                    .push(DebouncedEventKind::Modify {
                        file_name: file_path.clone(),
                    });
            }
            SyncDirectoryCommand::MoveFile {
                file_id,
                physical_path,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;

                match &sync_directory.sync_type {
                    SyncType::Universal => {
                        // Universal directories store files under their `file_id`
                        // on disk, so a logical rename never moves any bytes: the
                        // resolved `physical_path` is still the `file_id` and this
                        // is a no-op DB write kept for symmetry.
                        sync_directory
                            .database
                            .update_file_physical_path(file_id, &physical_path)
                            .map_err(|_| SyncDirectoryError::FailedMovingFile)?;
                    }
                    SyncType::TagBased { .. } => {
                        let file = sync_directory
                            .database
                            .get_file(file_id)
                            .map_err(|_| SyncDirectoryError::FailedMovingFile)?;

                        let old_file_path = sync_directory.path.join(file.physical_path.as_str());
                        let new_file_path = sync_directory.path.join(physical_path.as_str());

                        log::info!(
                            "Moving file from {} to {}",
                            old_file_path.to_string_lossy(),
                            new_file_path.to_string_lossy()
                        );

                        sync_directory
                            .database
                            .update_file_physical_path(file_id, &physical_path)
                            .map_err(|_| SyncDirectoryError::FailedMovingFile)?;

                        // TODO: Don't unwrap
                        std::fs::create_dir_all(new_file_path.parent().unwrap())
                            .expect("failed to create new directory");
                        std::fs::rename(&old_file_path, &new_file_path)
                            .expect("failed to move file");

                        // If the moved file was in a directory that is now empty, we want to remove the
                        // directory as well.
                        self.try_remove_empty_directory(old_file_path.parent().unwrap());

                        self.skip_queue.borrow_mut().push(DebouncedEventKind::Move {
                            from: Some(old_file_path),
                            to: Some(new_file_path),
                        });
                    }
                };
            }
            SyncDirectoryCommand::RemoveFile {
                file_id,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;
                let file = sync_directory
                    .database
                    .get_file(file_id)
                    .map_err(|_| SyncDirectoryError::FailedRemovingFile)?;

                log::info!(
                    "Removing file {} from {}",
                    file.physical_path,
                    sync_directory.path.to_string_lossy()
                );

                let file_path = match &sync_directory.sync_type {
                    SyncType::Universal => sync_directory.path.join(file_id.to_string()),
                    SyncType::TagBased { .. } => {
                        sync_directory.path.join(file.physical_path.as_str())
                    }
                };

                std::fs::remove_file(&file_path).expect("failed to remove file");

                // If the removed file was in a directory that is now empty, we want to remove the
                // directory as well.
                if let SyncType::TagBased { .. } = &sync_directory.sync_type
                    && let Some(directory) = PathBuf::from(file.physical_path.as_str()).parent()
                {
                    let directory_path = sync_directory.path.join(directory);
                    self.try_remove_empty_directory(directory_path);
                }

                sync_directory
                    .database
                    .remove_file_by_id(file_id)
                    .map_err(|_| SyncDirectoryError::FailedRemovingFile)?;

                self.skip_queue
                    .borrow_mut()
                    .push(DebouncedEventKind::Remove {
                        file_name: file_path,
                    });
            }
            SyncDirectoryCommand::LocalPath {
                file_id,
                respond_to,
            } => {
                // Resolve the absolute on-disk path of the first sync directory
                // that has `file_id`, without reading the bytes. We do not
                // verify the file exists on disk here; the DB row is treated as
                // authoritative (mirrors how `ReadFile` trusts it before the
                // read). The caller opens the path directly.
                let mut response: Option<PathBuf> = None;
                for sync_directory in &self.sync_directories {
                    let file = match sync_directory.database.get_file(file_id) {
                        Ok(file) => file,
                        Err(_) => continue,
                    };
                    let absolute_path = sync_directory.path.join(file.physical_path.as_str());
                    response = Some(absolute_path);
                    break;
                }
                let _ = respond_to.send(response);
            }
            SyncDirectoryCommand::ReadFile {
                file_id,
                respond_to,
            } => {
                // Walk our sync directories looking for the first one that
                // claims to have `file_id` in its database. For TagBased
                // directories the on-disk path is the recorded relative path;
                // for Universal directories the file is stored under its
                // `file_id`. If a database row points at a missing file on
                // disk we log and continue to the next directory.
                let mut response: Option<(PhysicalPath, Vec<u8>, String)> = None;
                for sync_directory in &self.sync_directories {
                    let file = match sync_directory.database.get_file(file_id) {
                        Ok(file) => file,
                        Err(_) => continue,
                    };
                    // `physical_path`/`absolute_path` describe where the bytes
                    // live in *this* sync directory. For Universal directories
                    // the file is stored under its `file_id`; for TagBased it is
                    // stored under its recorded relative path. This is the
                    // *physical* path only: the caller substitutes the logical
                    // (human-readable) name from the main database before sending
                    // to a peer, since the per-directory DB may only hold the
                    // physical name (the `file_id` for Universal).
                    let physical_path = file.physical_path.clone();
                    let absolute_path = sync_directory.path.join(physical_path.as_str());
                    let content = match self.get_file_content(&absolute_path) {
                        Ok((bytes, _hash)) => bytes,
                        Err(error) => {
                            log::warn!(
                                "ReadFile: {} reported {} but read failed: {:?}",
                                sync_directory.path.to_string_lossy(),
                                absolute_path.to_string_lossy(),
                                error
                            );
                            continue;
                        }
                    };
                    let content_hash = Self::calculate_content_hash(&content);
                    response = Some((physical_path, content, content_hash));
                    break;
                }
                let _ = respond_to.send(response);
            }
        }

        Ok(())
    }

    fn handle_event(&self, event: DebouncedEventKind) -> Result<(), SyncDirectoryError> {
        match event {
            DebouncedEventKind::Create { file_name } => {
                let sync_directory = self.sync_directory_for_path(&file_name)?;
                let (content, content_hash) = self.get_file_content(&file_name)?;
                let sync_relative_path = file_name.strip_prefix(&sync_directory.path).unwrap();

                match &sync_directory.sync_type {
                    SyncType::Universal => {
                        self.upload_file(
                            sync_directory,
                            sync_relative_path,
                            content,
                            content_hash,
                            Vec::new(),
                        )?;
                    }
                    SyncType::TagBased { tags } => {
                        self.add_file(
                            sync_directory,
                            sync_relative_path,
                            content,
                            content_hash,
                            tags.to_vec(),
                        )?;
                    }
                }
            }
            DebouncedEventKind::Move { from, to } => {
                let any_path = from.as_ref().or(to.as_ref()).unwrap();
                let sync_directory = self.sync_directory_for_path(any_path)?;

                if let Some(from) = &from
                    && let Some(to) = &to
                {
                    // Move within the directory.

                    if let SyncType::Universal = sync_directory.sync_type {
                        // A Universal directory stores files under their `file_id`
                        // on disk; a user renaming that UUID file locally has no
                        // logical meaning and would break the id<->file mapping.
                        // (A *logical* rename of a Universal file arrives instead
                        // as a `FileMoved` change and is handled in `handle_command`.)
                        panic!(
                            "figure out what should happen here. We don't really want to propagate this. Maybe we just undo the operation?"
                        );
                    };

                    let relative_from = from.strip_prefix(&sync_directory.path).unwrap();
                    let Ok(relative_to) = to.strip_prefix(&sync_directory.path) else {
                        let _sync_directory_to = self.sync_directory_for_path(to)?;

                        // FIX: Special case for when `to` is a *different sync directory*.
                        panic!("implement special case");
                    };

                    if let Ok(file_id) = self.get_file_id(sync_directory, relative_from) {
                        self.move_file_within_directory(sync_directory, file_id, relative_to)?;
                    } else {
                        for sync_file in self.get_all_files_at(sync_directory, relative_from)? {
                            let path = PathBuf::from(sync_file.physical_path.as_str());
                            let relative_path = path.strip_prefix(relative_from).unwrap();
                            let new_path = relative_to.join(relative_path);

                            self.move_file_within_directory(
                                sync_directory,
                                sync_file.file_id,
                                new_path,
                            )?;
                        }
                    }
                } else if let Some(from) = from {
                    // File was moved outside of the synced directory.

                    let relative_from = from.strip_prefix(&sync_directory.path).unwrap();

                    if let Ok(file_id) = self.get_file_id(sync_directory, relative_from) {
                        self.remove_file_by_id(sync_directory, file_id)?;
                    } else {
                        for sync_file in self.get_all_files_at(sync_directory, relative_from)? {
                            self.remove_file_by_id(sync_directory, sync_file.file_id)?;
                        }
                    }
                } else if let Some(to) = to {
                    // File was moved here from outside of the synced directory.

                    if to.is_file() {
                        let (content, content_hash) = self.get_file_content(&to)?;
                        let sync_relative_path = to.strip_prefix(&sync_directory.path).unwrap();

                        match &sync_directory.sync_type {
                            SyncType::Universal => {
                                self.upload_file(
                                    sync_directory,
                                    sync_relative_path,
                                    content,
                                    content_hash,
                                    Vec::new(),
                                )?;
                            }
                            SyncType::TagBased { tags } => {
                                self.add_file(
                                    sync_directory,
                                    sync_relative_path,
                                    content,
                                    content_hash,
                                    tags.to_vec(),
                                )?;
                            }
                        }
                    } else if to.is_dir() {
                        for entry in WalkDir::new(&to)
                            .into_iter()
                            .filter_map(|entry| entry.ok())
                            .filter(|entry| entry.file_type().is_file())
                        {
                            let (content, content_hash) = self.get_file_content(entry.path())?;
                            let sync_relative_path =
                                entry.path().strip_prefix(&sync_directory.path).unwrap();

                            match &sync_directory.sync_type {
                                SyncType::Universal => {
                                    self.upload_file(
                                        sync_directory,
                                        sync_relative_path,
                                        content,
                                        content_hash,
                                        Vec::new(),
                                    )?;
                                }
                                SyncType::TagBased { tags } => {
                                    self.add_file(
                                        sync_directory,
                                        sync_relative_path,
                                        content,
                                        content_hash,
                                        tags.to_vec(),
                                    )?;
                                }
                            }
                        }
                    } else {
                        log::warn!(
                            "A file that is not a regular file or a directory was detected. This is unsupported at the moment"
                        );
                    }
                } else {
                    log::error!("Received an empty move. This should never happen");
                }
            }
            DebouncedEventKind::Modify { file_name } => {
                let sync_directory = self.sync_directory_for_path(&file_name)?;
                let (content, content_hash) = self.get_file_content(&file_name)?;
                let sync_relative_path = file_name.strip_prefix(&sync_directory.path).unwrap();
                let file_id = self.get_file_id(sync_directory, sync_relative_path)?;

                self.update_file_content(sync_directory, file_id, content, content_hash)?;
            }
            DebouncedEventKind::Remove { file_name } => {
                let sync_directory = self.sync_directory_for_path(&file_name)?;
                let sync_relative_path = file_name.strip_prefix(&sync_directory.path).unwrap();
                let file_id = self.get_file_id(sync_directory, sync_relative_path)?;

                self.remove_file_by_id(sync_directory, file_id)?;
            }
        }

        Ok(())
    }

    /// Run the directory manager.
    ///
    /// `last_known_hashes` is the last-known content hash per `FileId` as
    /// observed by previous runs of the daemon, loaded once from the main
    /// DB's `file_versions` table at startup. Used exclusively during
    /// `run_initial_sync` to decide whether an on-disk file changed while the
    /// daemon was offline; it is dropped once the initial sync finishes.
    /// Shutdown-safety invariant: this loop is cancelled abruptly on shutdown
    /// (`handle_sync_directories` drops the manager when its `CancellationToken`
    /// fires, which runs `WatchDispatcher::drop` to stop the watcher). Because
    /// cancellation can only interrupt this future at an `.await` point, and the
    /// only `.await`s in this loop are the two `recv()` calls in the `select!`
    /// below, a shutdown can only ever land *between* whole events — never
    /// midway through `handle_command`/`handle_event`. Those handlers are fully
    /// synchronous today, so each one runs to completion atomically.
    ///
    /// DANGER: do not introduce an `.await` inside `handle_command`,
    /// `handle_event`, or anything they call. Doing so creates a new
    /// cancellation point mid-handler: a shutdown could then abandon a
    /// sync-directory DB write or a partial file mirror halfway through,
    /// leaving on-disk state inconsistent. If any of that work must become
    /// async, first make shutdown cooperative here (observe cancellation at the
    /// top of the loop and return normally) instead of relying on abrupt drop.
    pub async fn run(&mut self, last_known_hashes: HashMap<FileId, String>) {
        self.run_initial_sync(&last_known_hashes);

        log::info!("Directories are fully synced");

        loop {
            tokio::select! {
                command = self.command_receiver.recv() => {
                    let Some(command) = command else {
                        // TODO: Maybe this is an error?
                        break;
                    };

                    if let Err(error) = self.handle_command(command) {
                        log::error!("Failed to handle command: {:?}", error);
                    }
                },
                watcher_event = self.watcher_events.recv() => {
                    let Some(event) = watcher_event else {
                        // TODO: Maybe this is an error?
                        break;
                    };

                    log::debug!("Received event: {:?}", event);

                    {
                        let mut skip_queue = self.skip_queue.borrow_mut();

                        if let Some(index) = skip_queue.iter().position(|skip_event| *skip_event == event) {
                            log::debug!("Event was scheduled to be skipped by the directory manager");
                            skip_queue.remove(index);
                            continue;
                        }
                    }

                    if let Err(error) = self.handle_event(event) {
                        log::error!("Failed to handle event: {:?}", error);
                    }
                },
            }
        }
    }
}
