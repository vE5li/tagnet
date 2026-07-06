use std::{
    collections::HashMap,
    io::Write,
    net::IpAddr,
    path::{Path, PathBuf},
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use tagnet_core::{FileId, LogicalPath, PhysicalPath, TagId, state::Frame};
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    /// IP address and port of the peer. None to let the peer establish the connection.
    pub address: Option<(IpAddr, u16)>,
    /// Human-readable label for this peer, used only to make log messages
    /// readable. Peer identity is always established via `public_key`.
    pub name: String,
    pub public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncType {
    Universal,
    TagBased { tags: Vec<TagId> },
}

impl SyncType {
    /// Decide where a file with the given logical path is stored on disk within
    /// a sync directory of this type — the `LogicalPath -> PhysicalPath`
    /// placement decision.
    ///
    /// - `Universal`: files are stored under their `file_id` on disk, so the
    ///   physical path is the id regardless of the logical name.
    /// - `TagBased`: the on-disk layout mirrors the logical namespace, so the
    ///   physical path equals the logical path.
    ///
    /// This is the only sanctioned way to turn a `LogicalPath` into a
    /// `PhysicalPath`; keeping it here (rather than in `tagnet-core`) is why the
    /// core newtypes expose no direct conversion in this direction.
    pub fn physical_for(&self, logical_path: &LogicalPath, file_id: FileId) -> PhysicalPath {
        match self {
            SyncType::Universal => PhysicalPath::new(file_id.to_string()),
            SyncType::TagBased { .. } => PhysicalPath::new(logical_path.as_str()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpecialType {
    Upload,
    Copy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncDirectory {
    pub path: PathBuf,
    pub sync_type: SyncType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Configuration {
    /// Synchronized directories on the device itself.
    pub sync_directories: Vec<SyncDirectory>,
    /// Directories for special features, such as uploading one-time sending.
    // pub special_directories: Vec<SyncDirectory>,

    /// Port to listen on. None for not listening.
    pub listen_port: Option<u16>,
    pub peers: Vec<Peer>,
}

/// Why a [`Configuration`] could not be produced from its serialized form.
///
/// Frontends that build a [`Configuration`] at runtime (e.g. the Android app,
/// which generates the JSON on first launch and passes it through the bridge)
/// must not panic on malformed input — a panic crashes the app. They use
/// [`Configuration::from_str`], which surfaces failures as this error.
#[derive(Debug)]
pub enum ConfigurationError {
    /// The configuration file on disk could not be read.
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The bytes were not valid configuration JSON.
    Parse(serde_json::Error),
}

impl std::fmt::Display for ConfigurationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigurationError::Read { path, source } => {
                write!(
                    formatter,
                    "failed to read configuration file {}: {source}",
                    path.display()
                )
            }
            ConfigurationError::Parse(error) => {
                write!(formatter, "failed to parse configuration JSON: {error}")
            }
        }
    }
}

impl std::error::Error for ConfigurationError {}

impl Configuration {
    // TODO: Return a result
    pub fn new(configuration_file: impl AsRef<Path>) -> Self {
        // TODO: Don't unwrap.
        let file_content = std::fs::read_to_string(configuration_file.as_ref()).unwrap();

        // TODO: We need to make sure that sync directories are not nested.
        // TODO: Make sure that public keys are unique.

        serde_json::from_str(&file_content).unwrap()
    }

    /// Read and parse a [`Configuration`] from a file, returning a [`Result`]
    /// instead of panicking (the fallible counterpart to
    /// [`Configuration::new`]).
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigurationError> {
        let path = path.as_ref();
        let contents =
            std::fs::read_to_string(path).map_err(|source| ConfigurationError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        Self::from_str(&contents)
    }

    pub fn new_example() -> Self {
        Configuration {
            peers: vec![Peer {
                address: Some(("192.168.188.10".parse().unwrap(), 2468)),
                name: "test".to_owned(),
                public_key: "public-key".to_owned(),
            }],
            listen_port: Some(3468),
            sync_directories: vec![
                SyncDirectory {
                    path: "/tmp/tagnet-testcloud".into(),
                    sync_type: SyncType::Universal,
                },
                SyncDirectory {
                    path: "/tmp/tagnet-testcloud-2".into(),
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

    /// Human-readable name for the peer with the given public key, falling back
    /// to the public key itself when the peer is unknown. Used purely for logs.
    pub fn peer_name<'a>(&'a self, public_key: &'a str) -> &'a str {
        self.peers
            .iter()
            .find(|peer| peer.public_key == public_key)
            .map(|peer| peer.name.as_str())
            .unwrap_or(public_key)
    }

    pub fn get_external_sync_type(&self) -> SyncType {
        let mut all_synced_tags = Vec::new();

        for sync_directory in &self.sync_directories {
            match &sync_directory.sync_type {
                // If any of the sync directories want to save all files, we don't need the list of
                // tags.
                SyncType::Universal => return SyncType::Universal,
                SyncType::TagBased { tags } => all_synced_tags.extend_from_slice(tags),
                // SyncType::Upload || SyncType::Copy => {},
            }
        }

        SyncType::TagBased {
            tags: all_synced_tags,
        }
    }
}

impl std::str::FromStr for Configuration {
    type Err = ConfigurationError;

    /// Parse a [`Configuration`] from a JSON string, returning a [`Result`]
    /// instead of panicking.
    ///
    /// This is the non-file, non-panicking entry point required by the
    /// portability plan (section 8): frontends without a shell/filesystem
    /// contract (Android) generate the configuration JSON in memory and parse
    /// it here. [`Configuration::new`] remains the file-reading desktop path.
    fn from_str(json: &str) -> Result<Self, ConfigurationError> {
        // TODO: We need to make sure that sync directories are not nested.
        // TODO: Make sure that public keys are unique.
        serde_json::from_str(json).map_err(ConfigurationError::Parse)
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
    /// Sender into the outbound WebSocket task for this peer.
    /// `None` when no connection is currently established.
    ///
    /// Carries `Frame` (not raw `Change`) because reconciliation messages
    /// (`Sync::Request`, `Sync::NotFound`, ...) share the same outbound queue
    /// as live changes. `forward_to_peers` wraps in `Frame::Change`.
    pub outbound: Option<UnboundedSender<Frame>>,
}

impl Default for RuntimePeer {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimePeer {
    pub fn new() -> Self {
        Self {
            // No sync type set yet.
            sync_type: None,
            statistics: ConnectionStatistics {},
            outbound: None,
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
