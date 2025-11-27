use notify::Watcher;
use std::{net::IpAddr, path::PathBuf, time::Duration};

use crate::watcher::{
    DebouncedEvent, create_dispatcher,
    notify::{EventKind, RecursiveMode},
};
use tagnet_core::{TagId, state::Change};

struct Peer {
    address: IpAddr,
    port: u16,
    user: String,
}

enum SyncType {
    Universal,
    TagBased { tags: Vec<TagId> },
}

struct SyncDirectory {
    path: PathBuf,
    sync_type: SyncType,
}

struct Configuration {
    peers: Vec<Peer>,
    sync_directories: Vec<SyncDirectory>,
}

impl Configuration {
    pub fn new() -> Self {
        Configuration {
            peers: Vec::new(),
            sync_directories: vec![SyncDirectory {
                path: "/home/lucas/testcloud".into(),
                sync_type: SyncType::Universal,
            }],
        }
    }
}

pub async fn setup(sender: tokio::sync::mpsc::UnboundedSender<Change>) {
    let configuration = Configuration::new();

    let (mut dispatcher, mut file_events) = create_dispatcher(Duration::from_secs(1))
        .await
        .expect("Failed to set up debouncer");

    configuration
        .sync_directories
        .iter()
        .map(|sync_directory| sync_directory.path.clone())
        .for_each(|path| {
            // FIX: Don't panic here.
            std::fs::create_dir_all(&path).expect("Failed to create directory");

            dispatcher
                .watcher()
                .watch(path.as_ref(), RecursiveMode::Recursive)
                .unwrap();
        });

    while let Some(event) = file_events.recv().await {
        println!("{:?}", event);
        // Send events to sender.
    }
}
