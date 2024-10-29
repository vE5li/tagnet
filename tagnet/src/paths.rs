//! Central place for resolving on-disk locations under the tagnet data
//! directory (`$HOME/.tagnet`). Keeping this in one module avoids re-reading
//! `HOME` and re-building paths by hand in every call site.

use std::path::PathBuf;

/// The tagnet data directory: `$HOME/.tagnet`.
pub fn tagnet_data_dir() -> PathBuf {
    PathBuf::from(
        std::env::var("TAGNET_DATA_DIR").expect("TAGNET_DATA_DIR environment variable not set"),
    )
}

/// This machine's long-lived identity key.
pub fn identity_path() -> PathBuf {
    PathBuf::from(
        std::env::var("TAGNET_PRIVATE_KEY_FILE")
            .expect("TAGNET_PRIVATE_KEY_FILE environment variable not set"),
    )
}

/// The main `FileDatabase` shared across the daemon.
pub fn main_db_path() -> PathBuf {
    tagnet_data_dir().join("main.db")
}

/// The per-sync-directory `SyncDirectoryDatabase`, named after the directory
/// it tracks (e.g. a directory `testcloud` maps to `testcloud.db`).
pub fn sync_directory_db_path(sync_directory: &std::path::Path) -> PathBuf {
    let name = sync_directory
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unnamed".to_owned());
    tagnet_data_dir().join(format!("{name}.db"))
}
