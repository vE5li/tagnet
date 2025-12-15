use std::{net::IpAddr, path::PathBuf};

use tagnet_core::TagId;

pub struct Peer {
    pub address: IpAddr,
    pub port: u16,
    pub user: String,
    pub public_key: String,
    pub synchronized_tags: Vec<TagId>,
    pub synchronizes_all: bool,
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
