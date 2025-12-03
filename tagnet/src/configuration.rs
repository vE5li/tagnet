use notify::Watcher;
use std::{io::Read, net::IpAddr, path::PathBuf, time::Duration};

use crate::watcher::{
    DebouncedEvent, DebouncedEventKind, create_dispatcher,
    notify::{EventKind, RecursiveMode},
};
use tagnet_core::{FileId, TagId, state::Change};

pub struct Peer {
    pub address: IpAddr,
    pub port: u16,
    pub user: String,
}

#[derive(Debug, Clone)]
pub enum SyncType {
    Universal,
    TagBased { tags: Vec<TagId> },
}

pub struct SyncDirectory {
    pub path: PathBuf,
    pub sync_type: SyncType,
}

pub struct Configuration {
    pub peers: Vec<Peer>,
    pub sync_directories: Vec<SyncDirectory>,
}

impl Configuration {
    pub fn new() -> Self {
        // TODO: We need to make sure that sync directories are not nested.
        Configuration {
            peers: Vec::new(),
            sync_directories: vec![
                SyncDirectory {
                    path: "/home/lucas/testcloud".into(),
                    sync_type: SyncType::Universal,
                },
                SyncDirectory {
                    path: "/home/lucas/testcloud-2".into(),
                    sync_type: SyncType::Universal,
                },
            ],
        }
    }
}
