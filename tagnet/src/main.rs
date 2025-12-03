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
    sync::mpsc::UnboundedSender,
};
use walkdir::WalkDir;

use crate::{
    configuration::{Configuration, SyncType},
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

async fn handle_sync_directories(sender: UnboundedSender<Change>) {
    SyncDirectoryManager::new(sender).await.run().await;
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    env_logger::init();

    let listener = TcpListener::bind("127.0.0.1:9001")
        .await
        .expect("Failed to bind");

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(handle_sync_directories(sender.clone()));
    tokio::spawn(handle_changes(receiver));

    while let Ok((stream, address)) = listener.accept().await {
        tokio::spawn(handle_connection(sender.clone(), stream, address));
    }

    Ok(())
}

async fn handle_changes(mut receiver: tokio::sync::mpsc::UnboundedReceiver<Change>) {
    let database = FileDatabase::initialize("test.db").expect("Failed to open database file");

    while let Some(change) = receiver.recv().await {
        match change {
            Change::FileAdded {
                file_id,
                path,
                content,
            } => {
                // let path = file_path.as_ref().map(|file_path| {
                //     PathBuf::try_from(file_path).expect("Failed to parse file buffer")
                // });

                // FIX: Don't unwrap.
                let mut file = std::fs::File::create(file_id.to_string()).unwrap();
                file.write_all(&content).unwrap();

                database
                    .add_file(file_id, path)
                    .expect("Failed to add file to database");
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
            Change::TagAdded { tag_name, metadata } => todo!(),
            Change::TagRenamed { tag_id, tag_name } => todo!(),
            Change::TagChanged { tag_id, metadata } => todo!(),
            Change::TagRemoved { tag_id } => todo!(),
            Change::FileTagged {
                file_id,
                tag_id,
                metadata,
            } => todo!(),
            Change::FileTagChanged {
                file_id,
                tag_id,
                metadata,
            } => todo!(),
            Change::FileUntagged { file_id, tag_id } => todo!(),
            Change::TagTagged {
                taggee_id,
                tag_id,
                metadata,
            } => todo!(),
            Change::TagTagChanged {
                taggee_id,
                tag_id,
                metadata,
            } => todo!(),
            Change::TagUntagged { taggee_id, tag_id } => todo!(),
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
    sender: tokio::sync::mpsc::UnboundedSender<Change>,
    dispatcher: WatchDispatcher,
    watcher_events: tokio::sync::mpsc::UnboundedReceiver<DebouncedEventKind>,
}

impl SyncDirectoryManager {
    pub async fn new(sender: tokio::sync::mpsc::UnboundedSender<Change>) -> Self {
        let configuration = Configuration::new();

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
            sender,
            dispatcher,
            watcher_events,
        }
    }

    fn add_file(
        &self,
        sync_directory: &RichSyncDirectory,
        path: impl AsRef<Path>,
        content: Vec<u8>,
        content_hash: String,
    ) {
        let file_id = FileId::new();

        // TODO: Don't unwrap.
        sync_directory
            .database
            .add_file(file_id, path.as_ref().to_string_lossy(), content_hash)
            .unwrap();

        // FIX:Handle send error.
        let _ = self.sender.send(Change::FileAdded {
            file_id,
            path: path.as_ref().to_string_lossy().to_string(),
            content,
        });
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

    fn update_file_content(
        &self,
        sync_directory: &RichSyncDirectory,
        file_id: FileId,
        content: Vec<u8>,
        content_hash: String,
    ) {
        // TODO: Don't panic
        sync_directory
            .database
            .update_file_content_hash(file_id, content_hash)
            .expect("Failed to pudate file content");

        // FIX:Handle send error.
        let _ = self.sender.send(Change::FileChanged { file_id, content });
    }

    fn update_file_path(
        &self,
        sync_directory: &RichSyncDirectory,
        file_id: FileId,
        path: impl AsRef<Path>,
    ) {
        // TODO: Don't panic
        sync_directory
            .database
            .update_file_path(file_id, path.as_ref().to_string_lossy())
            .expect("Failed to update file path");

        // FIX:Handle send error.
        let _ = self.sender.send(Change::FileMoved {
            file_id,
            path: path.as_ref().to_string_lossy().to_string(),
        });
    }

    fn remove_file_by_id(&self, sync_directory: &RichSyncDirectory, file_id: FileId) {
        // FIX: Don't unwrap.
        sync_directory.database.remove_file_by_id(file_id).unwrap();

        // FIX:Handle send error.
        let _ = self.sender.send(Change::FileDeleted { file_id });
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

                    self.remove_file_by_id(sync_directory, sync_file.file_id);
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

                    self.update_file_content(
                        sync_directory,
                        sync_file.file_id,
                        content,
                        content_hash,
                    );
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

                        self.add_file(sync_directory, relative_path, content, content_hash);
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

                self.add_file(sync_directory, sync_relative_path, content, content_hash);
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
                        self.update_file_path(sync_directory, file_id, relative_to);
                    } else {
                        for sync_file in self.get_all_files_at(sync_directory, relative_from)? {
                            let path = PathBuf::from(sync_file.path);
                            let relative_path = path.strip_prefix(relative_from).unwrap();
                            let new_path = relative_to.join(relative_path);

                            self.update_file_path(sync_directory, sync_file.file_id, new_path);
                        }
                    }
                } else if let Some(from) = from {
                    // Files was moved here from outside of the synced directory.

                    let relative_from = from.strip_prefix(&sync_directory.path).unwrap();

                    if let Ok(file_id) = self.get_file_id(sync_directory, relative_from) {
                        self.remove_file_by_id(sync_directory, file_id);
                    } else {
                        for sync_file in self.get_all_files_at(sync_directory, relative_from)? {
                            self.remove_file_by_id(sync_directory, sync_file.file_id);
                        }
                    }
                } else if let Some(to) = to {
                    // Files was moved outside of the synced directory.

                    if to.is_file() {
                        let (content, content_hash) = self.get_file_content(&to)?;
                        let sync_relative_path = to.strip_prefix(&sync_directory.path).unwrap();

                        self.add_file(sync_directory, sync_relative_path, content, content_hash);
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
                            );
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

                self.update_file_content(sync_directory, file_id, content, content_hash);
            }
            DebouncedEventKind::Remove { file_name } => {
                let sync_directory = self.sync_directory_for_path(&file_name)?;
                let sync_relative_path = file_name.strip_prefix(&sync_directory.path).unwrap();
                let file_id = self.get_file_id(sync_directory, sync_relative_path)?;

                self.remove_file_by_id(sync_directory, file_id);
            }
        }

        Ok(())
    }

    pub async fn run(&mut self) {
        self.run_initial_sync();

        log::info!("Directories are fully synced");

        while let Some(event) = self.watcher_events.recv().await {
            log::debug!("Received event: {:?}", event);

            // FIX: Don't drop events until they are processed correctly.
            if let Err(error) = self.handle_event(event) {
                log::error!("Failed to handle event: {:?}", error);
            }
        }
    }
}
