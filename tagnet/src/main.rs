use std::{
    fs::File,
    hash::{DefaultHasher, Hash, Hasher},
    io::{Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use base64::{Engine, prelude::BASE64_STANDARD};
use futures_util::StreamExt;

use notify::{RecursiveMode, Watcher};
use rusqlite::Connection;
use tagnet_core::{FileId, state::Change};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use walkdir::WalkDir;

use crate::{
    configuration::{Configuration, Peer, SyncDirectory, SyncType},
    database::{DatabaseError, FileDatabase, SyncDirectoryDatabase, SyncDirectoryFile},
    watcher::{DebouncedEventKind, WatchDispatcher},
};

mod configuration;
mod database;
mod watcher;

// ## On the server:
//
// /home/foo/cloud
// -> configured as "any file". Special type of sync connection that stores all
//    files by their hash, without any subdirectories.
//
// lucas@computer
// lucas@laptop
// -> configure connections with a user specific (likely during connection buildup) to enforce
//    visibility.

// ## On the client:
//
// /home/foo/some-tag
// /home/foo/other-tag
// -> configured as "everything with tag x". Regular two way sync.
//
// admin@central
// -> single connectino to an authorized account. This way everything will be forwarded.

#[derive(Debug, Clone, Copy)]
enum FooBarError {
    UnmonitoredDirectory,
    FailedToOpenFile,
    FailedToReadFile,
    MissingTrackedFile,
    FailedAddingFile,
    FailedUpdatingFile,
    FailedRemovingFile,
}

async fn handle_connection(
    sender: tokio::sync::mpsc::UnboundedSender<Change>,
    raw_stream: TcpStream,
    address: SocketAddr,
) {
    log::debug!("Incoming TCP connection from: {:?}", address);

    let Ok(ws_stream) = tokio_tungstenite::accept_async(raw_stream).await else {
        log::error!("Error during the websocket handshake occurred");
        return;
    };

    log::debug!("WebSocket connection established: {:?}", address);

    let (outgoing, mut incoming) = ws_stream.split();

    while let Some(Ok(message)) = incoming.next().await {
        let text = message.to_string();
        let change = serde_json::from_str(&text).expect("Failed to deserialize");

        sender.send(change);
    }
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    env_logger::init();

    let configuration = Configuration::new();

    let listener = TcpListener::bind("127.0.0.1:9001")
        .await
        .expect("Failed to bind");

    let (change_sender, change_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (skip_sender, skip_receiver) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(handle_sync_directories(
        configuration.sync_directories,
        change_sender.clone(),
        skip_receiver,
    ));

    tokio::spawn(handle_changes(
        configuration.peers,
        change_receiver,
        skip_sender,
    ));

    while let Ok((stream, address)) = listener.accept().await {
        tokio::spawn(handle_connection(change_sender.clone(), stream, address));
    }

    Ok(())
}

async fn handle_sync_directories(
    sync_directories: Vec<SyncDirectory>,
    change_sender: UnboundedSender<Change>,
    skip_events: UnboundedReceiver<DebouncedEventKind>,
) {
    SyncDirectoryManager::new(sync_directories, change_sender, skip_events)
        .await
        .run()
        .await;
}

async fn handle_changes(
    peers: Vec<Peer>,
    mut change_receiver: tokio::sync::mpsc::UnboundedReceiver<Change>,
    skip_sender: UnboundedSender<DebouncedEventKind>,
) {
    let database = FileDatabase::initialize("test.db").expect("Failed to open database file");

    while let Some(change) = change_receiver.recv().await {
        match change {
            Change::FileAdded {
                file_id,
                path,
                content,
            } => {
                // FIX: Don't unwrap.
                let mut file = std::fs::File::create(file_id.to_string()).unwrap();
                file.write_all(&content).unwrap();

                database
                    .add_file(file_id, path)
                    .expect("Failed to add file to database");

                // TODO: We need to know where this change comes from.
                // If it does not come from this system we also need to create the file in the sync
                // directories *and* ignore the event.
            }
            Change::FileMoved { file_id, path } => {
                database
                    .update_file_path(file_id, path)
                    .expect("Failed to update file path");
            }
            Change::FileChanged { file_id, content } => {
                // FIX: Don't unwrap.
                let mut file = std::fs::File::create(file_id.to_string()).unwrap();
                file.write_all(&content).unwrap();
            }
            Change::FileDeleted { file_id } => {
                std::fs::remove_file(file_id.to_string()).expect("Failed to remove file");

                database
                    .remove_file(file_id)
                    .expect("Failed to remove file");
            }
            Change::TagAdded {
                tag_name: _,
                metadata: _,
            } => todo!(),
            Change::TagRenamed {
                tag_id: _,
                tag_name: _,
            } => todo!(),
            Change::TagChanged {
                tag_id: _,
                metadata: _,
            } => todo!(),
            Change::TagRemoved { tag_id: _ } => todo!(),
            Change::FileTagged {
                file_id: _,
                tag_id: _,
                metadata: _,
            } => todo!(),
            Change::FileTagChanged {
                file_id: _,
                tag_id: _,
                metadata: _,
            } => todo!(),
            Change::FileUntagged {
                file_id: _,
                tag_id: _,
            } => todo!(),
            Change::TagTagged {
                taggee_id: _,
                tag_id: _,
                metadata: _,
            } => todo!(),
            Change::TagTagChanged {
                taggee_id: _,
                tag_id: _,
                metadata: _,
            } => todo!(),
            Change::TagUntagged {
                taggee_id: _,
                tag_id: _,
            } => todo!(),
        }

        println!("\n-- FILE DATABASE --");
        database.show_files().unwrap();
        // handle.show_tags().unwrap();
        // handle.show_entries().unwrap();
        // handle.show_previews().unwrap();
    }
}

struct RichSyncDirectory {
    path: PathBuf,
    sync_type: SyncType,
    database: SyncDirectoryDatabase,
}

struct SyncDirectoryManager {
    sync_directories: Vec<RichSyncDirectory>,
    change_sender: tokio::sync::mpsc::UnboundedSender<Change>,
    _dispatcher: WatchDispatcher,
    watcher_events: tokio::sync::mpsc::UnboundedReceiver<DebouncedEventKind>,
    skip_events: tokio::sync::mpsc::UnboundedReceiver<DebouncedEventKind>,
}

impl SyncDirectoryManager {
    pub async fn new(
        sync_directories: Vec<SyncDirectory>,
        change_sender: tokio::sync::mpsc::UnboundedSender<Change>,
        skip_events: tokio::sync::mpsc::UnboundedReceiver<DebouncedEventKind>,
    ) -> Self {
        let (mut dispatcher, watcher_events) = WatchDispatcher::new()
            .await
            .expect("Failed to set up debouncer");

        let sync_directories = sync_directories
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
                let database_name = format!("{}.db", path.file_name().unwrap().to_string_lossy());

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
            skip_events,
        }
    }

    fn add_file(
        &self,
        sync_directory: &RichSyncDirectory,
        path: impl AsRef<Path>,
        content: Vec<u8>,
        content_hash: String,
    ) -> Result<(), FooBarError> {
        let file_id = FileId::new();

        sync_directory
            .database
            .add_file(file_id, path.as_ref().to_string_lossy(), content_hash)
            .map_err(|_| FooBarError::FailedAddingFile)?;

        // FIX: Put this into a queue for proper retry handling instead.
        let _ = self.change_sender.send(Change::FileAdded {
            file_id,
            path: path.as_ref().to_string_lossy().to_string(),
            content,
        });

        Ok(())
    }

    fn update_file_content(
        &self,
        sync_directory: &RichSyncDirectory,
        file_id: FileId,
        content: Vec<u8>,
        content_hash: String,
    ) -> Result<(), FooBarError> {
        sync_directory
            .database
            .update_file_content_hash(file_id, content_hash)
            .map_err(|_| FooBarError::FailedUpdatingFile)?;

        // FIX: Put this into a queue for proper retry handling instead.
        let _ = self
            .change_sender
            .send(Change::FileChanged { file_id, content });

        Ok(())
    }

    fn update_file_path(
        &self,
        sync_directory: &RichSyncDirectory,
        file_id: FileId,
        path: impl AsRef<Path>,
    ) -> Result<(), FooBarError> {
        sync_directory
            .database
            .update_file_path(file_id, path.as_ref().to_string_lossy())
            .map_err(|_| FooBarError::FailedUpdatingFile)?;

        // FIX: Put this into a queue for proper retry handling instead.
        let _ = self.change_sender.send(Change::FileMoved {
            file_id,
            path: path.as_ref().to_string_lossy().to_string(),
        });

        Ok(())
    }

    fn remove_file_by_id(
        &self,
        sync_directory: &RichSyncDirectory,
        file_id: FileId,
    ) -> Result<(), FooBarError> {
        sync_directory
            .database
            .remove_file_by_id(file_id)
            .map_err(|_| FooBarError::FailedRemovingFile)?;

        // FIX: Put this into a queue for proper retry handling instead.
        let _ = self.change_sender.send(Change::FileDeleted { file_id });

        Ok(())
    }

    fn get_all_files(
        &self,
        sync_directory: &RichSyncDirectory,
    ) -> Result<Vec<SyncDirectoryFile>, FooBarError> {
        sync_directory
            .database
            .get_all_files()
            .map_err(|_| FooBarError::MissingTrackedFile)
    }

    fn get_all_files_at(
        &self,
        sync_directory: &RichSyncDirectory,
        path: impl AsRef<Path>,
    ) -> Result<Vec<SyncDirectoryFile>, FooBarError> {
        sync_directory
            .database
            .get_all_files_at(path.as_ref().to_string_lossy())
            .map_err(|_| FooBarError::MissingTrackedFile)
    }

    fn get_file_content(&self, path: impl AsRef<Path>) -> Result<(Vec<u8>, String), FooBarError> {
        let mut file = std::fs::File::open(path).map_err(|_| FooBarError::FailedToOpenFile)?;
        let mut content = Vec::new();

        file.read_to_end(&mut content)
            .map_err(|_| FooBarError::FailedToReadFile)?;

        let mut hasher = DefaultHasher::new();
        content.hash(&mut hasher);
        let content_hash = BASE64_STANDARD.encode(hasher.finish().to_le_bytes());

        Ok((content, content_hash))
    }

    fn sync_directory_for_path(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<&RichSyncDirectory, FooBarError> {
        self.sync_directories
            .iter()
            .find(|sync_directory| path.as_ref().starts_with(&sync_directory.path))
            .ok_or(FooBarError::UnmonitoredDirectory)
    }

    fn get_file_id(
        &self,
        sync_directory: &RichSyncDirectory,
        path: impl AsRef<Path>,
    ) -> Result<FileId, FooBarError> {
        sync_directory
            .database
            .get_file_id(path.as_ref().to_string_lossy())
            .map_err(|_| FooBarError::MissingTrackedFile)
    }

    fn run_initial_sync(&self) {
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

                        if let Err(error) =
                            self.add_file(sync_directory, relative_path, content, content_hash)
                        {
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
    }

    fn handle_event(&self, event: DebouncedEventKind) -> Result<(), FooBarError> {
        match event {
            DebouncedEventKind::Create { file_name } => {
                let sync_directory = self.sync_directory_for_path(&file_name)?;
                let (content, content_hash) = self.get_file_content(&file_name)?;
                let sync_relative_path = file_name.strip_prefix(&sync_directory.path).unwrap();

                self.add_file(sync_directory, sync_relative_path, content, content_hash)?;
            }
            DebouncedEventKind::Move { from, to } => {
                let any_path = from.as_ref().or(to.as_ref()).unwrap();
                let sync_directory = self.sync_directory_for_path(any_path)?;

                if let Some(from) = &from
                    && let Some(to) = &to
                {
                    // Move within the directory.

                    let relative_from = from.strip_prefix(&sync_directory.path).unwrap();
                    let relative_to = to.strip_prefix(&sync_directory.path).unwrap();

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
                    // Files was moved here from outside of the synced directory.

                    let relative_from = from.strip_prefix(&sync_directory.path).unwrap();

                    if let Ok(file_id) = self.get_file_id(sync_directory, relative_from) {
                        self.remove_file_by_id(sync_directory, file_id)?;
                    } else {
                        for sync_file in self.get_all_files_at(sync_directory, relative_from)? {
                            self.remove_file_by_id(sync_directory, sync_file.file_id)?;
                        }
                    }
                } else if let Some(to) = to {
                    // Files was moved outside of the synced directory.

                    if to.is_file() {
                        let (content, content_hash) = self.get_file_content(&to)?;
                        let sync_relative_path = to.strip_prefix(&sync_directory.path).unwrap();

                        self.add_file(sync_directory, sync_relative_path, content, content_hash)?;
                    } else if to.is_dir() {
                        for entry in WalkDir::new(&to)
                            .into_iter()
                            .filter_map(|entry| entry.ok())
                            .filter(|entry| entry.file_type().is_file())
                        {
                            let (content, content_hash) = self.get_file_content(entry.path())?;
                            let sync_relative_path =
                                entry.path().strip_prefix(&sync_directory.path).unwrap();

                            self.add_file(
                                sync_directory,
                                sync_relative_path,
                                content,
                                content_hash,
                            )?;
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

        // TODO: Check for skip entries that are too old somewhere.
        // Likely also needs to save the timestamp for that.
        let mut skip_queue = Vec::new();

        loop {
            tokio::select! {
                skip_event = self.skip_events.recv() => {
                    let Some(event) = skip_event else {
                        // TODO: Maybe this is an error?
                        break;
                    };

                    skip_queue.push(event);
                },
                watcher_event = self.watcher_events.recv() => {
                    let Some(event) = watcher_event else {
                        // TODO: Maybe this is an error?
                        break;
                    };

                    log::debug!("Received event: {:?}", event);

                    if let Some(index) = skip_queue.iter().position(|skip_event| *skip_event == event) {
                        log::debug!("Event was emitted locally and will thus be ignored");
                        skip_queue.remove(index);
                    }

                    if let Err(error) = self.handle_event(event) {
                        log::error!("Failed to handle event: {:?}", error);
                    }
                },
            }
        }
    }
}
