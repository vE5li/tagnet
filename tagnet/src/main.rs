use std::{
    hash::{DefaultHasher, Hash, Hasher},
    io::{Read, Write},
    net::SocketAddr,
    path::PathBuf,
    time::Duration,
};

use base64::{Engine, prelude::BASE64_STANDARD};
use futures_util::StreamExt;

use notify::{RecursiveMode, Watcher};
use rusqlite::Connection;
use tagnet_core::{FileId, state::Change};
use tokio::net::{TcpListener, TcpStream};
use walkdir::WalkDir;

use crate::{
    configuration::{Configuration, SyncType},
    database::{DatabaseError, FileDatabase, SyncDirectoryDatabase},
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

    let listener = TcpListener::bind("127.0.0.1:9001")
        .await
        .expect("Failed to bind");

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(setup(sender.clone()));

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

pub async fn setup(sender: tokio::sync::mpsc::UnboundedSender<Change>) {
    let configuration = Configuration::new();

    let (mut dispatcher, mut watcher_events) = WatchDispatcher::new()
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

    for sync_directory in &sync_directories {
        log::debug!(
            "Checking for missed updates at {}",
            sync_directory.path.to_string_lossy()
        );

        // TODO: Don't unwrap.
        let files = sync_directory.database.get_all_files().unwrap();

        for sync_file in files {
            log::debug!("Checking file {}", sync_file.path);

            let full_path = sync_directory.path.join(sync_file.path);

            if !full_path.exists() {
                log::info!(
                    "File {} was deleted without monitoring. Syncing deletion",
                    full_path.to_string_lossy()
                );

                // FIX: Don't unwrap.
                sync_directory
                    .database
                    .remove_file_by_id(sync_file.file_id)
                    .unwrap();

                // FIX:Handle send error.
                let _ = sender.send(Change::FileDeleted {
                    file_id: sync_file.file_id,
                });

                continue;
            }

            // FIX: Don't panic anywhere, just continue.
            let mut file = std::fs::File::open(&full_path).expect("File cannot be opened");
            let mut content = Vec::new();
            file.read_to_end(&mut content).expect("Failed to read file");

            let mut hasher = DefaultHasher::new();
            content.hash(&mut hasher);
            let content_hash = BASE64_STANDARD.encode(hasher.finish().to_le_bytes());

            if content_hash != sync_file.content_hash {
                log::info!(
                    "File {} was changed without monitoring. Syncing change",
                    full_path.to_string_lossy()
                );

                sync_directory
                    .database
                    .update_file_content_hash(sync_file.file_id, content_hash)
                    .expect("Failed to add file to database");

                // FIX:Handle send error.
                let _ = sender.send(Change::FileChanged {
                    file_id: sync_file.file_id,
                    content,
                });
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

                    // Try to read the file
                    // FIX: Don't panic anywhere, just continue.
                    let mut file = std::fs::File::open(entry.path()).expect("File doesn't exist");
                    let mut content = Vec::new();
                    file.read_to_end(&mut content).expect("Failed to read file");

                    let mut hasher = DefaultHasher::new();
                    content.hash(&mut hasher);
                    let content_hash = BASE64_STANDARD.encode(hasher.finish().to_le_bytes());

                    let file_id = FileId::new();

                    // TODO: Don't unwrap.
                    sync_directory
                        .database
                        .add_file(file_id, relative_path.to_string_lossy(), content_hash)
                        .unwrap();

                    println!("\n-- SYNC DATABASE --");
                    sync_directory.database.show_files().unwrap();

                    // FIX:Handle send error.
                    let _ = sender.send(Change::FileAdded {
                        file_id,
                        path: relative_path.to_string_lossy().to_string(),
                        content,
                    });
                }
                Err(error) => {
                    panic!("Database error: {:?}", error);
                }
            }
        }
    }

    log::info!("Directories are fully synced");

    while let Some(event) = watcher_events.recv().await {
        log::debug!("Received event: {:?}", event);

        match event {
            DebouncedEventKind::Create { file_name } => {
                let Some(sync_directory) = sync_directories
                    .iter()
                    .find(|sync_directory| file_name.starts_with(&sync_directory.path))
                else {
                    log::error!("Got a change from an unmonitored directory");
                    continue;
                };

                // Try to read the file
                // FIX: Don't panic anywhere, just continue.
                let mut file = std::fs::File::open(&file_name).expect("File doesn't exist");
                let mut content = Vec::new();
                file.read_to_end(&mut content).expect("Failed to read file");

                let mut hasher = DefaultHasher::new();
                content.hash(&mut hasher);
                let content_hash = BASE64_STANDARD.encode(hasher.finish().to_le_bytes());

                // FIX: Don't panic.
                let display_name = file_name.file_name().expect("File is actually a directory");
                let sync_relative_path = file_name.strip_prefix(&sync_directory.path).unwrap();

                let file_id = FileId::new();

                // TODO: Don't unwrap.
                sync_directory
                    .database
                    .add_file(file_id, sync_relative_path.to_string_lossy(), content_hash)
                    .unwrap();

                println!("\n-- SYNC DATABASE --");
                sync_directory.database.show_files().unwrap();

                // FIX:Handle send error.
                let _ = sender.send(Change::FileAdded {
                    file_id,
                    path: sync_relative_path.to_string_lossy().to_string(),
                    content,
                });
            }
            DebouncedEventKind::Move { from, to } => {
                let any_path = from.as_ref().or(to.as_ref()).unwrap();

                let Some(sync_directory) = sync_directories
                    .iter()
                    .find(|sync_directory| any_path.starts_with(&sync_directory.path))
                else {
                    log::error!("Got a change from an unmonitored directory");
                    continue;
                };

                if let Some(from) = &from
                    && let Some(to) = &to
                {
                    // Move within the directory.

                    if to.is_file() {
                        let relative_from = from.strip_prefix(&sync_directory.path).unwrap();
                        let relative_to = to.strip_prefix(&sync_directory.path).unwrap();

                        // FIX: Don't unwrap.
                        let file_id = sync_directory
                            .database
                            .get_file_id(relative_from.to_string_lossy())
                            .unwrap();

                        sync_directory
                            .database
                            .update_file_path(file_id, relative_to.to_string_lossy())
                            .unwrap();

                        println!("\n-- SYNC DATABASE --");
                        sync_directory.database.show_files().unwrap();

                        // FIX:Handle send error.
                        let _ = sender.send(Change::FileMoved {
                            file_id,
                            path: relative_to.to_string_lossy().to_string(),
                        });
                    } else if to.is_dir() {
                        // TODO:
                    } else {
                        log::warn!(
                            "A file that is not a regular file or a directory was detected. This is unsupported at the moment"
                        );
                    }
                } else if let Some(from) = from {
                    // Files was moved here from outside of the synced directory.

                    // TODO: Check doesn't work like that if the file is no longer there.
                    // if from.is_file() {
                    //     // FIX: Don't unwrap.
                    //     let file_id = sync_directory
                    //         .database
                    //         .get_file_id(from.to_string_lossy())
                    //         .unwrap();
                    //
                    //     sync_directory
                    //         .database
                    //         .update_file_path(file_id, from.to_string_lossy())
                    //         .unwrap();
                    // } else if from.is_dir() {
                    //     // TODO:
                    // } else {
                    //     log::warn!(
                    //         "A file that is not a regular file or a directory was detected. This is unsupported at the moment"
                    //     );
                    // }
                } else if let Some(to) = to {
                    // Files was moved outside of the synced directory.

                    if to.is_file() {
                        // Try to read the file
                        // FIX: Don't panic anywhere, just continue.
                        let mut file = std::fs::File::open(&to).expect("File doesn't exist");
                        let mut content = Vec::new();
                        file.read_to_end(&mut content).expect("Failed to read file");

                        let mut hasher = DefaultHasher::new();
                        content.hash(&mut hasher);
                        let content_hash = BASE64_STANDARD.encode(hasher.finish().to_le_bytes());

                        let sync_relative_path = to.strip_prefix(&sync_directory.path).unwrap();

                        let file_id = FileId::new();

                        // TODO: Don't unwrap.
                        sync_directory
                            .database
                            .add_file(file_id, sync_relative_path.to_string_lossy(), content_hash)
                            .unwrap();

                        println!("\n-- SYNC DATABASE --");
                        sync_directory.database.show_files().unwrap();

                        // FIX:Handle send error.
                        let _ = sender.send(Change::FileAdded {
                            file_id,
                            path: sync_relative_path.to_string_lossy().to_string(),
                            content,
                        });
                    } else if to.is_dir() {
                        // TODO:
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
                let Some(sync_directory) = sync_directories
                    .iter()
                    .find(|sync_directory| file_name.starts_with(&sync_directory.path))
                else {
                    log::error!("Got a change from an unmonitored directory");
                    continue;
                };

                let mut file = std::fs::File::open(&file_name).expect("File doesn't exist");
                let mut content = Vec::new();
                file.read_to_end(&mut content).expect("Failed to read file");

                let mut hasher = DefaultHasher::new();
                content.hash(&mut hasher);
                let content_hash = BASE64_STANDARD.encode(hasher.finish().to_le_bytes());

                let sync_relative_path = file_name.strip_prefix(&sync_directory.path).unwrap();

                // FIX: Don't unwrap.
                let file_id = sync_directory
                    .database
                    .get_file_id(sync_relative_path.to_string_lossy())
                    .unwrap();

                sync_directory
                    .database
                    .update_file_content_hash(file_id, content_hash)
                    .expect("Failed to add file to database");

                println!("\n-- SYNC DATABASE --");
                sync_directory.database.show_files().unwrap();

                // FIX:Handle send error.
                let _ = sender.send(Change::FileChanged { file_id, content });
            }
            DebouncedEventKind::Remove { file_name } => {
                let Some(sync_directory) = sync_directories
                    .iter()
                    .find(|sync_directory| file_name.starts_with(&sync_directory.path))
                else {
                    log::error!("Got a change from an unmonitored directory");
                    continue;
                };

                let sync_relative_path = file_name.strip_prefix(&sync_directory.path).unwrap();

                let file_id = sync_directory
                    .database
                    .get_file_id(sync_relative_path.to_string_lossy())
                    .unwrap();

                sync_directory
                    .database
                    .remove_file_by_path(sync_relative_path.to_string_lossy())
                    .unwrap();

                println!("\n-- SYNC DATABASE --");
                sync_directory.database.show_files().unwrap();

                // FIX:Handle send error.
                let _ = sender.send(Change::FileDeleted { file_id });
            }
        }
    }
}
