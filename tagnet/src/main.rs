use std::{ffi::OsString, io::Write, net::SocketAddr, path::PathBuf};

use futures_util::StreamExt;

use clap::{Parser, Subcommand};
use tagnet_core::{
    FileId, TagId,
    state::{Change, ChangeOrigin},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

use crate::{
    configuration::{Configuration, RuntimeConfiguration, SyncType},
    database::FileDatabase,
    directory_manager::SyncDirectoryManager,
};

mod configuration;
mod database;
mod directory_manager;
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
    sender: tokio::sync::mpsc::UnboundedSender<(Change, ChangeOrigin)>,
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

    // TODO: Do tagnet handshake to determine the public key.
    let public_key = "FIX ME".to_owned();

    while let Some(Ok(message)) = incoming.next().await {
        let text = message.to_string();
        let change = serde_json::from_str(&text).expect("Failed to deserialize");

        sender.send((
            change,
            ChangeOrigin::Peer {
                public_key: public_key.clone(),
            },
        ));
    }
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Arguments {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    // FIX: Remove, for development only.
    Reset { configuration_file: PathBuf },
    Generate { file_name: PathBuf },
    Run { configuration_file: PathBuf },
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    env_logger::init();

    let arguments = Arguments::parse();

    match arguments.command {
        // FIX: Remove, for development only.
        Commands::Reset { configuration_file } => {
            log::info!("Re-creating /home/lucas/.tagnet");
            std::fs::remove_dir_all("/home/lucas/.tagnet").unwrap();
            std::fs::create_dir("/home/lucas/.tagnet").unwrap();

            let configuration = Configuration::new(configuration_file);
            for sync_directory in configuration.sync_directories {
                if let SyncType::Universal = sync_directory.sync_type {
                    log::info!("Re-creating {}", sync_directory.path.to_string_lossy());
                    std::fs::remove_dir_all(&sync_directory.path).unwrap();
                    std::fs::create_dir(&sync_directory.path).unwrap();
                }
            }

            let database = FileDatabase::initialize("/home/lucas/.tagnet/main.db")
                .expect("Failed to open database file");

            database
                .add_tag(
                    TagId::from_string("e1de1ee0-3dec-47b2-8e95-842c0acc0dfd").unwrap(),
                    "screenshots",
                    "red",
                )
                .unwrap();
            database
                .add_tag(
                    TagId::from_string("ca39bd61-1b06-4907-b36f-e7a968793e48").unwrap(),
                    "computer",
                    "red",
                )
                .unwrap();
            database
                .add_tag(
                    TagId::from_string("5a0e2939-f881-4c55-a349-cbb91c082057").unwrap(),
                    "image",
                    "red",
                )
                .unwrap();

            database.show_tags().unwrap();
        }
        Commands::Generate { file_name } => {
            let configuration = Configuration::new_example();
            configuration.write_to_file(file_name);
        }
        Commands::Run { configuration_file } => {
            let configuration = Configuration::new(configuration_file);
            let runtime_configuration = RuntimeConfiguration::new(&configuration);

            let listener = TcpListener::bind("127.0.0.1:9001")
                .await
                .expect("Failed to bind");

            let (change_sender, change_receiver) = tokio::sync::mpsc::unbounded_channel();
            let (command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();

            tokio::spawn(handle_sync_directories(
                configuration.clone(),
                change_sender.clone(),
                command_receiver,
            ));

            tokio::spawn(handle_changes(
                configuration,
                change_receiver,
                command_sender,
            ));

            while let Ok((stream, address)) = listener.accept().await {
                tokio::spawn(handle_connection(change_sender.clone(), stream, address));
            }
        }
    }

    Ok(())
}

async fn handle_sync_directories(
    configuration: Configuration,
    change_sender: UnboundedSender<(Change, ChangeOrigin)>,
    command_receiver: UnboundedReceiver<SyncDirectoryCommand>,
) {
    SyncDirectoryManager::new(configuration, change_sender, command_receiver)
        .await
        .run()
        .await;
}

pub enum SyncDirectoryCommand {
    CreateFile {
        file_id: FileId,
        file_name: OsString,
        content: Vec<u8>,
        // Maybe a bit weird to have it like this? Not sure.
        // We currently need that to check which directory this event was meant for.
        sync_directory_path: PathBuf,
    },
    ModifyFile {
        file_id: FileId,
        content: Vec<u8>,
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

fn contains_all_tags(sync_directory_tags: &[TagId], file_tags: &[TagId]) -> bool {
    sync_directory_tags
        .iter()
        .all(|tag_id| file_tags.contains(tag_id))
}

async fn handle_changes(
    configuration: Configuration,
    mut change_receiver: tokio::sync::mpsc::UnboundedReceiver<(Change, ChangeOrigin)>,
    command_sender: UnboundedSender<SyncDirectoryCommand>,
) {
    let database = FileDatabase::initialize("/home/lucas/.tagnet/main.db")
        .expect("Failed to open database file");

    while let Some((change, change_origin)) = change_receiver.recv().await {
        match change {
            // Change::Copy {
            //   path,
            //   content,
            // }
            Change::FileAdded {
                file_id,
                path,
                content,
                tags,
            } => {
                database
                    .add_file(file_id, path.clone())
                    .expect("Failed to add file to database");

                tags.iter().for_each(|tag_id| {
                    database
                        .tag_file(*tag_id, file_id)
                        .expect("failed to tag added file");
                });

                for sync_directory in &configuration.sync_directories {
                    if let ChangeOrigin::Local { directory_path } = &change_origin
                        && directory_path == &sync_directory.path
                        && let SyncType::TagBased { .. } = &sync_directory.sync_type
                    {
                        // If the file came from a tag based sync directory, we don't need to take
                        // any action.
                        continue;
                    };

                    if let SyncType::TagBased {
                        tags: sync_directory_tags,
                    } = &sync_directory.sync_type
                        && !contains_all_tags(sync_directory_tags, &tags)
                    {
                        // If the directory is tag based and the file *does not* have all the
                        // tags the sync directory does, skip this sync directory.
                        continue;
                    }

                    // This means the event didn't originate from this sync directory itself and
                    // the tags match, thus we may want to apply the change.
                    // TODO: Handle result.
                    let _ = command_sender.send(SyncDirectoryCommand::CreateFile {
                        file_id,
                        file_name: path.clone().into(),
                        content: content.clone(),
                        sync_directory_path: sync_directory.path.clone(),
                    });
                }

                // TODO: Iterate special directories and if the origing is in an upload directory, delete
                // the file.

                for peer in &configuration.peers {
                    if let ChangeOrigin::Peer { public_key } = &change_origin
                        && public_key == &peer.public_key
                    {
                        // Nothing to do, the change originates from this peer.
                        continue;
                    }

                    // TODO: Inform this peer.
                }
            }
            Change::FileMoved { file_id, path } => {
                database
                    .update_file_path(file_id, path)
                    .expect("Failed to update file path");

                // TODO: In checks, assert that this event did not come from an upload or copy sync
                // directory.
            }
            Change::FileChanged { file_id, content } => {
                // TODO: Don't unwrap.
                // TODO: Should this be include? Currently this WILL NOT WORK since add file
                // doesn't consider subtags. We would need to get a list of *all* tags (incuding
                // subdags) when adding the file to make it work.
                // -> Maybe make it configurable in the config, per-sync directory.
                let file_tags = database
                    .tag_ids_for_file(file_id, database::SubtagRule::Exclude)
                    .expect("failed to get file tags")
                    .into_iter()
                    .collect::<Vec<TagId>>();

                for sync_directory in &configuration.sync_directories {
                    if let ChangeOrigin::Local { directory_path } = &change_origin
                        && directory_path == &sync_directory.path
                    {
                        // If the file is already modified in the origin, we don't need to take
                        // any action.
                        continue;
                    };

                    if let SyncType::TagBased {
                        tags: sync_directory_tags,
                    } = &sync_directory.sync_type
                        && !contains_all_tags(sync_directory_tags, &file_tags)
                    {
                        // If the directory is tag based and the file *does not* have all the
                        // tags the sync directory does, skip this sync directory.
                        continue;
                    }

                    // This means the event didn't originate from this sync directory itself and
                    // the tags match, thus we may want to apply the change.
                    // TODO: Handle result.
                    let _ = command_sender.send(SyncDirectoryCommand::ModifyFile {
                        file_id,
                        content: content.clone(),
                        sync_directory_path: sync_directory.path.clone(),
                    });
                }

                for peer in &configuration.peers {
                    if let ChangeOrigin::Peer { public_key } = &change_origin
                        && public_key == &peer.public_key
                    {
                        // Nothing to do, the change originates from this peer.
                        continue;
                    }

                    // TODO: Inform this peer.
                }
            }
            Change::FileDeleted { file_id } => {
                // TODO: Don't unwrap.
                // TODO: Should this be include? Currently this WILL NOT WORK since add file
                // doesn't consider subtags. We would need to get a list of *all* tags (incuding
                // subdags) when adding the file to make it work.
                // -> Maybe make it configurable in the config, per-sync directory.
                let file_tags = database
                    .tag_ids_for_file(file_id, database::SubtagRule::Exclude)
                    .expect("failed to get file tags")
                    .into_iter()
                    .collect::<Vec<TagId>>();

                database
                    .remove_file(file_id)
                    .expect("Failed to remove file from database");

                for sync_directory in &configuration.sync_directories {
                    if let ChangeOrigin::Local { directory_path } = &change_origin
                        && directory_path == &sync_directory.path
                    {
                        // If the file came from this directory, it is already removed. We
                        // can just skip this directory.
                        continue;
                    };

                    if let SyncType::TagBased {
                        tags: sync_directory_tags,
                    } = &sync_directory.sync_type
                        && !contains_all_tags(sync_directory_tags, &file_tags)
                    {
                        // If the directory is tag based and the file *does not* have all the
                        // tags the sync directory does, skip this sync directory.
                        continue;
                    }

                    // This means the event didn't originate from this sync directory itself, thus
                    // we may want to apply it.
                    // TODO: Handle result.
                    let _ = command_sender.send(SyncDirectoryCommand::RemoveFile {
                        file_id,
                        sync_directory_path: sync_directory.path.clone(),
                    });
                }

                for peer in &configuration.peers {
                    if let ChangeOrigin::Peer { public_key } = &change_origin
                        && public_key == &peer.public_key
                    {
                        // Nothing to do, the change originates from this peer.
                        continue;
                    }

                    // TODO: Inform this peer.
                }
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
        database.show_tags().unwrap();
        // handle.show_entries().unwrap();
        // handle.show_previews().unwrap();
    }
}
