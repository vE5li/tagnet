use std::{
    collections::HashMap,
    io::Write,
    net::IpAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tagnet_core::TagId;

use crate::configuration;

#[derive(Debug, Serialize, Deserialize)]
pub struct Peer {
    /// IP address and port of the peer. None to let the peer establish the connection.
    pub address: Option<(IpAddr, u16)>,
    pub user: String,
    pub public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncType {
    Universal,
    TagBased { tags: Vec<TagId> },
    // Upload,
    // Copy,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncDirectory {
    pub path: PathBuf,
    pub sync_type: SyncType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Configuration {
    /// Synchronized directories on the device itself.
    pub sync_directories: Vec<SyncDirectory>,

    /// Port to listen on. None for not listening.
    pub listen_port: Option<u16>,
    pub peers: Vec<Peer>,
}

impl Configuration {
    // TODO: Return a result
    pub fn new(configuration_file: impl AsRef<Path>) -> Self {
        // TODO: Don't unwrap.
        let file_content = std::fs::read_to_string(configuration_file.as_ref()).unwrap();
        let configuration = serde_json::from_str(&file_content).unwrap();

        // TODO: We need to make sure that sync directories are not nested.
        // TODO: Make sure that public keys are unique.

        configuration
    }

    pub fn new_example() -> Self {
        Configuration {
            peers: vec![Peer {
                address: Some(("192.168.188.10".parse().unwrap(), 2468)),
                user: "test".to_owned(),
                public_key: "public-key".to_owned(),
            }],
            listen_port: Some(3468),
            sync_directories: vec![
                SyncDirectory {
                    path: "/home/lucas/testcloud".into(),
                    sync_type: SyncType::Universal,
                },
                SyncDirectory {
                    path: "/home/lucas/testcloud-2".into(),
                    sync_type: SyncType::TagBased { tags: Vec::new() },
                },
            ],
        }
    }

    // TODO: Return a result
    pub fn write_to_file(&self, file_name: impl AsRef<Path>) {
        let json = serde_json::to_string_pretty(self).unwrap();

        // TODO: Don't unwrap.
        let mut file = std::fs::File::create(file_name.as_ref()).unwrap();
        file.write_all(json.as_bytes()).unwrap();
    }

    pub fn get_external_sync_type(&self) -> SyncType {
        let mut all_synced_tags = Vec::new();

        for sync_directory in &self.sync_directories {
            match &sync_directory.sync_type {
                // If any of the sync directories want to save all files, we don't need the list of
                // tags.
                SyncType::Universal => return SyncType::Universal,
                SyncType::TagBased { tags } => all_synced_tags.extend_from_slice(tags),
            }
        }

        SyncType::TagBased {
            tags: all_synced_tags,
        }
    }
}

pub struct ConnectionStatistics {
    // pub last_connected: Option<()>,
    // pub data_sent: usize,
    // pub data_received: usize,
    // pub last_synced_file: Option<String>,
}

pub struct RuntimePeer {
    pub sync_type: Option<SyncType>,
    pub statistics: ConnectionStatistics,
}

// TODO: Probably make this Default instead.
impl RuntimePeer {
    pub fn new() -> Self {
        Self {
            // No sync type set yet.
            sync_type: None,
            statistics: ConnectionStatistics {},
        }
    }
}

pub struct RuntimeConfiguration {
    pub peers: HashMap<String, RuntimePeer>,
}

impl RuntimeConfiguration {
    pub fn new(configuration: &Configuration) -> Self {
        let peers = configuration
            .peers
            .iter()
            .map(|peer| (peer.public_key.clone(), RuntimePeer::new()))
            .collect();

        Self { peers }
    }
}
