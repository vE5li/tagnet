use std::{
    cell::RefCell,
    hash::{DefaultHasher, Hash, Hasher},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use base64::{Engine, prelude::BASE64_STANDARD};
use notify::{RecursiveMode, Watcher};
use tagnet_core::{
    FileId, TagId,
    state::{Change, ChangeOrigin},
};
use walkdir::WalkDir;

use crate::{
    configuration::{Configuration, SyncType},
    database::{DatabaseError, SyncDirectoryDatabase, SyncDirectoryFile},
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
        file_name: PathBuf,
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
        path: PathBuf,
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
}

struct RichSyncDirectory {
    path: PathBuf,
    sync_type: SyncType,
    database: SyncDirectoryDatabase,
}

pub struct SyncDirectoryManager {
    sync_directories: Vec<RichSyncDirectory>,
    change_sender: tokio::sync::mpsc::UnboundedSender<(Change, ChangeOrigin)>,
    _dispatcher: WatchDispatcher,
    watcher_events: tokio::sync::mpsc::UnboundedReceiver<DebouncedEventKind>,
    command_receiver: tokio::sync::mpsc::UnboundedReceiver<SyncDirectoryCommand>,
    // TODO: Make this a more robust messaging framework instead of a ref cell.
    skip_queue: RefCell<Vec<DebouncedEventKind>>,
}

impl SyncDirectoryManager {
    pub async fn new(
        configuration: Configuration,
        change_sender: tokio::sync::mpsc::UnboundedSender<(Change, ChangeOrigin)>,
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

                // TODO: Improve the name selection.
                let database_name = format!(
                    "/home/lucas/.tagnet/{}.db",
                    path.file_name().unwrap().to_string_lossy()
                );

                let database = match SyncDirectoryDatabase::initialize(database_name) {
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
        let _ = self.change_sender.send((change, change_origin));

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

        sync_directory
            .database
            .add_file(file_id, path.as_ref().to_string_lossy(), content_hash)
            .map_err(|_| SyncDirectoryError::FailedAddingFile)?;

        self.send_change(
            sync_directory,
            Change::FileAdded {
                file_id,
                path: path.as_ref().to_string_lossy().to_string(),
                content,
                tags,
            },
        )
    }

    fn upload_file(
        &self,
        sync_directory: &RichSyncDirectory,
        path: impl AsRef<Path>,
        content: Vec<u8>,
        tags: Vec<TagId>,
    ) -> Result<(), SyncDirectoryError> {
        let file_id = FileId::new();

        if let Err(error) = self.send_change(
            sync_directory,
            Change::FileAdded {
                file_id,
                path: path.as_ref().to_string_lossy().to_string(),
                content,
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
        sync_directory
            .database
            .update_file_content_hash(file_id, content_hash)
            .map_err(|_| SyncDirectoryError::FailedChangingFile)?;

        self.send_change(sync_directory, Change::FileChanged { file_id, content })
    }

    fn update_file_path(
        &self,
        sync_directory: &RichSyncDirectory,
        file_id: FileId,
        path: impl AsRef<Path>,
    ) -> Result<(), SyncDirectoryError> {
        sync_directory
            .database
            .update_file_path(file_id, path.as_ref().to_string_lossy())
            .map_err(|_| SyncDirectoryError::FailedChangingFile)?;

        self.send_change(
            sync_directory,
            Change::FileMoved {
                file_id,
                path: path.as_ref().to_string_lossy().to_string(),
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
        path: impl AsRef<Path>,
    ) -> Result<Vec<SyncDirectoryFile>, SyncDirectoryError> {
        sync_directory
            .database
            .get_all_files_at(path.as_ref().to_string_lossy())
            .map_err(|_| SyncDirectoryError::MissingTrackedFile)
    }

    fn calculate_content_hash(content: &[u8]) -> String {
        let mut hasher = DefaultHasher::new();
        content.hash(&mut hasher);
        BASE64_STANDARD.encode(hasher.finish().to_le_bytes())
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
        path: impl AsRef<Path>,
    ) -> Result<FileId, SyncDirectoryError> {
        sync_directory
            .database
            .get_file_id(path.as_ref().to_string_lossy())
            .map_err(|_| SyncDirectoryError::MissingTrackedFile)
    }

    fn intial_sync_universal(
        &self,
        sync_directory: &RichSyncDirectory,
        files: Vec<SyncDirectoryFile>,
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

            if content_hash != sync_file.content_hash {
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

            let content = match self.get_file_content(entry.path()) {
                Ok((content, _)) => content,
                Err(error) => {
                    log::error!("Failed to read added file: {:?}", error);
                    continue;
                }
            };

            self.upload_file(sync_directory, relative_path, content, Vec::new())
                .unwrap();
        }
    }

    fn intial_sync_tagged(
        &self,
        sync_directory: &RichSyncDirectory,
        files: Vec<SyncDirectoryFile>,
        tags: &[TagId],
    ) {
        for sync_file in files {
            let full_path = sync_directory.path.join(sync_file.path);

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

            if content_hash != sync_file.content_hash {
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
                .get_file_id(relative_path.to_string_lossy())
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

    fn run_initial_sync(&mut self) {
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
                SyncType::Universal => self.intial_sync_universal(sync_directory, files),
                SyncType::TagBased { tags } => self.intial_sync_tagged(sync_directory, files, tags),
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
                file_name,
                content,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;

                let file_name = match &sync_directory.sync_type {
                    SyncType::Universal => file_id.to_string().into(),
                    SyncType::TagBased { .. } => file_name,
                };
                let file_path = sync_directory.path.join(&file_name);

                log::info!("Adding file at {}", file_path.to_string_lossy());

                // FIX: Don't unwrap.
                std::fs::create_dir_all(file_path.parent().unwrap())
                    .expect("failed to create file subdirectory");
                let mut file = std::fs::File::create(&file_path).expect("failed to create file");
                file.write_all(&content).unwrap();

                let content_hash = Self::calculate_content_hash(&content);

                sync_directory
                    .database
                    .add_file(file_id, file_name.to_string_lossy(), content_hash)
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

                let file_name = match &sync_directory.sync_type {
                    SyncType::Universal => file_id.to_string(),
                    SyncType::TagBased { .. } => {
                        sync_directory
                            .database
                            .get_file(file_id)
                            .map_err(|_| SyncDirectoryError::FailedChangingFile)?
                            .path
                    }
                };
                let file_path = sync_directory.path.join(&file_name);

                log::info!("Modifying file at {}", file_path.to_string_lossy());

                // FIX: Don't unwrap.
                let mut file = std::fs::File::create(&file_path).expect("failed to create file");
                file.write_all(&content).unwrap();

                let content_hash = Self::calculate_content_hash(&content);

                sync_directory
                    .database
                    .update_file_content_hash(file_id, content_hash)
                    .map_err(|_| SyncDirectoryError::FailedAddingFile)?;

                self.skip_queue
                    .borrow_mut()
                    .push(DebouncedEventKind::Modify {
                        file_name: file_path.clone(),
                    });
            }
            SyncDirectoryCommand::MoveFile {
                file_id,
                path,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;

                match &sync_directory.sync_type {
                    SyncType::Universal => {
                        // For a universal sync directory, we don't need to update anything apart from
                        // the database.
                        sync_directory
                            .database
                            .update_file_path(file_id, path.to_string_lossy())
                            .map_err(|_| SyncDirectoryError::FailedMovingFile)?;
                    }
                    SyncType::TagBased { .. } => {
                        let file = sync_directory
                            .database
                            .get_file(file_id)
                            .map_err(|_| SyncDirectoryError::FailedMovingFile)?;

                        let old_file_path = sync_directory.path.join(&file.path);
                        let new_file_path = sync_directory.path.join(&path);

                        log::info!(
                            "Moving file from {} to {}",
                            old_file_path.to_string_lossy(),
                            new_file_path.to_string_lossy()
                        );

                        sync_directory
                            .database
                            .update_file_path(file_id, path.to_string_lossy())
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
                    file.path,
                    sync_directory.path.to_string_lossy()
                );

                let file_path = match &sync_directory.sync_type {
                    SyncType::Universal => sync_directory.path.join(file_id.to_string()),
                    SyncType::TagBased { .. } => sync_directory.path.join(&file.path),
                };

                std::fs::remove_file(&file_path).expect("failed to remove file");

                // If the removed file was in a directory that is now empty, we want to remove the
                // directory as well.
                if let SyncType::TagBased { .. } = &sync_directory.sync_type
                    && let Some(directory) = PathBuf::from(file.path).parent()
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
                        self.upload_file(sync_directory, sync_relative_path, content, Vec::new())?;
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
                        self.update_file_path(sync_directory, file_id, relative_to)?;
                    } else {
                        for sync_file in self.get_all_files_at(sync_directory, relative_from)? {
                            let path = PathBuf::from(sync_file.path);
                            let relative_path = path.strip_prefix(relative_from).unwrap();
                            let new_path = relative_to.join(relative_path);

                            self.update_file_path(sync_directory, sync_file.file_id, new_path)?;
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

    pub async fn run(&mut self) {
        self.run_initial_sync();

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
