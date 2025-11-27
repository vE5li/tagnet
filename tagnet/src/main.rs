use std::{io::Write, net::SocketAddr, path::PathBuf};

use futures_util::StreamExt;

use tagnet_core::state::Change;
use tokio::net::{TcpListener, TcpStream};

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
    println!("Incoming TCP connection from: {:?}", address);

    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the websocket handshake occurred");

    println!("WebSocket connection established: {:?}", address);

    let (outgoing, mut incoming) = ws_stream.split();

    while let Some(Ok(message)) = incoming.next().await {
        let text = message.to_string();
        let change = serde_json::from_str(&text).expect("Failed to deserialize");

        sender.send(change);
    }
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let listener = TcpListener::bind("127.0.0.1:9001")
        .await
        .expect("Failed to bind");

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(configuration::setup(sender.clone()));

    tokio::spawn(handle_changes(receiver));

    while let Ok((stream, address)) = listener.accept().await {
        tokio::spawn(handle_connection(sender.clone(), stream, address));
    }

    Ok(())
}

async fn handle_changes(mut receiver: tokio::sync::mpsc::UnboundedReceiver<Change>) {
    let handle = database::initialize("test.db").expect("Failed to open database file");

    while let Some(change) = receiver.recv().await {
        match change {
            Change::FileAdded {
                file_id,
                display_name,
                file_path,
                content,
            } => {
                // let path = file_path.as_ref().map(|file_path| {
                //     PathBuf::try_from(file_path).expect("Failed to parse file buffer")
                // });

                // FIX: Don't unwrap.
                let mut file = std::fs::File::create(file_id.to_string()).unwrap();
                file.write_all(&content).unwrap();

                handle
                    .add_file(display_name, file_path)
                    .expect("Failed to add file to database");
            }
            Change::FileMoved {
                file_id,
                display_name,
                path,
            } => todo!(),
            Change::FileChanged { file_id, content } => todo!(),
            Change::FileDeleted { file_id } => todo!(),
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

        println!("\n\n-- DEBUG --");
        handle.show_files().unwrap();
        // handle.show_tags().unwrap();
        // handle.show_entries().unwrap();
        // handle.show_previews().unwrap();
    }
}
