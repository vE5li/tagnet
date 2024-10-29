//! `tagnet-cli`: the first consumer of the IPC-client backend
//! (portability plan section 7).
//!
//! Historically this tool opened a WebSocket to the daemon's **peer-sync** port
//! and hand-built a `Change::FileAdded`, duplicating what `api::Api::upload_file`
//! now does — and it was broken end-to-end because it never performed the peer
//! handshake the daemon requires on that port.
//!
//! It now talks to the daemon's **local control socket** (a Unix domain socket
//! at the fixed path `/run/tagnet/tagnet.sock`) via the section-6
//! [`IpcClientBackend`] and calls the section-5 API. This both fixes the tool
//! and validates the IPC-client backend with a minimal, UI-free consumer. The
//! control socket is **not** the peer-sync port; local control is never routed
//! through the network listener.

use std::{collections::HashMap, path::PathBuf, process::ExitCode};

use clap::{Parser, Subcommand};
use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_FULL};
use owo_colors::OwoColorize;
use tagnet::{control::IpcClientBackend, database::SubtagRule, transport::TransportBackend};
use tagnet_core::{FileId, TagId};

/// Number of leading characters needed to uniquely identify `target` among
/// `all` ids (jj-style short change ids).
///
/// This is intentionally simple (O(n * len) per id) — correctness first, we can
/// optimize with a shared prefix trie later once the behaviour is validated.
fn unique_prefix_length(target: &str, all: &[String]) -> usize {
    for length in 1..=target.len() {
        let prefix = &target[..length];
        let collisions = all
            .iter()
            .filter(|other| other.as_str() != target && other.starts_with(prefix))
            .count();
        if collisions == 0 {
            return length;
        }
    }
    target.len()
}

/// Render an id with its unique prefix highlighted and the remainder dimmed,
/// mirroring how `jj` displays change ids.
fn highlight_id(id: &str, prefix_length: usize) -> String {
    let (unique, rest) = id.split_at(prefix_length.min(id.len()));
    format!("{}{}", unique.magenta().bold(), rest.bright_black())
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Arguments {
    /// Path to the daemon's control socket. Defaults to the fixed
    /// `/run/tagnet/tagnet.sock`; override only for non-standard launches.
    #[arg(long, global = true)]
    socket: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Upload a file's contents to the daemon, optionally tagging it.
    Upload {
        /// File on disk to read and upload.
        path: PathBuf,
        /// Tag ids (UUIDs) to apply to the uploaded file.
        #[arg(long = "tag", value_name = "TAG_ID")]
        tags: Vec<String>,
    },
    /// List all tags known to the daemon.
    ListTags,
    /// List all files known to the daemon.
    ListFiles,
    /// Create a tag; prints the newly-minted tag id.
    CreateTag {
        name: String,
        #[arg(long, default_value = "red")]
        color: String,
    },
    /// List the files carrying a tag (the v1 single-tag search).
    FilesForTag { tag_id: String },
    /// Edit a file in `$EDITOR`, fetching it from a peer first if it is not
    /// present locally, and writing back any changes.
    Edit {
        /// The file id (UUID) to edit.
        uuid: String,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = Arguments::parse();

    // Connect the IPC-client backend to the daemon's control socket.
    let backend = match &arguments.socket {
        Some(path) => IpcClientBackend::connect(path).await,
        None => IpcClientBackend::connect_default().await,
    };
    let backend = match backend {
        Ok(backend) => backend,
        Err(error) => {
            eprintln!("Failed to connect to the tagnet daemon control socket: {error}");
            eprintln!("Is the daemon running? (tagnet run <config>)");
            return ExitCode::FAILURE;
        }
    };

    match run(&backend, arguments.command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(backend: &IpcClientBackend, command: Commands) -> Result<(), String> {
    match command {
        Commands::Upload { path, tags } => {
            let content = std::fs::read(&path)
                .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
            let path_name = path
                .file_name()
                .ok_or_else(|| format!("{} has no file name", path.display()))?
                .to_string_lossy()
                .to_string();
            let tags = parse_tag_ids(&tags)?;

            let file_id = backend
                .upload_file(path_name, content, tags)
                .await
                .map_err(|error| error.to_string())?;
            println!("Uploaded as file {}", file_id.to_string());
        }
        Commands::ListTags => {
            let tags = backend
                .list_tags()
                .await
                .map_err(|error| error.to_string())?;
            if tags.is_empty() {
                println!("(no tags)");
            } else {
                let ids: Vec<String> = tags.iter().map(|tag| tag.id.to_string()).collect();

                let mut table = Table::new();
                table
                    .load_preset(UTF8_FULL)
                    .set_content_arrangement(ContentArrangement::Dynamic)
                    .set_header(vec!["Tag ID", "Name", "Color"]);

                for tag in &tags {
                    let id = tag.id.to_string();
                    let prefix_length = unique_prefix_length(&id, &ids);
                    table.add_row(vec![
                        Cell::new(highlight_id(&id, prefix_length)),
                        Cell::new(&tag.name),
                        Cell::new(&tag.color),
                    ]);
                }

                println!("{table}");
            }
        }
        Commands::ListFiles => {
            let files = backend
                .list_files()
                .await
                .map_err(|error| error.to_string())?;
            if files.is_empty() {
                println!("(no files)");
            } else {
                let ids: Vec<String> =
                    files.iter().map(|file| file.file_id.to_string()).collect();

                // Map tag ids to names so we can show human-readable tags per
                // file. Fetched once for the whole listing.
                let tag_names: HashMap<TagId, String> = backend
                    .list_tags()
                    .await
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .map(|tag| (tag.id, tag.name))
                    .collect();

                let mut table = Table::new();
                table
                    .load_preset(UTF8_FULL)
                    .set_content_arrangement(ContentArrangement::Dynamic)
                    .set_header(vec!["File ID", "Path", "Version", "Tags"]);

                for file in &files {
                    let id = file.file_id.to_string();
                    let prefix_length = unique_prefix_length(&id, &ids);

                    // Simple per-file lookup for now; can be batched later if it
                    // becomes a bottleneck.
                    let tag_ids = backend
                        .tags_for_file(file.file_id, SubtagRule::Exclude)
                        .await
                        .map_err(|error| error.to_string())?;
                    let tags = tag_ids
                        .iter()
                        .map(|tag_id| {
                            tag_names
                                .get(tag_id)
                                .cloned()
                                .unwrap_or_else(|| tag_id.to_string())
                        })
                        .collect::<Vec<_>>()
                        .join(", ");

                    table.add_row(vec![
                        Cell::new(highlight_id(&id, prefix_length)),
                        Cell::new(&file.logical_path),
                        Cell::new(format!("v{}", file.version_number)),
                        Cell::new(tags),
                    ]);
                }

                println!("{table}");
            }
        }
        Commands::CreateTag { name, color } => {
            let tag_id = backend
                .create_tag(name, color)
                .await
                .map_err(|error| error.to_string())?;
            println!("{}", tag_id.to_string());
        }
        Commands::FilesForTag { tag_id } => {
            let tag_id =
                TagId::from_string(&tag_id).ok_or_else(|| format!("invalid tag id: {tag_id}"))?;
            let file_ids = backend
                .files_for_tag(tag_id, SubtagRule::Exclude)
                .await
                .map_err(|error| error.to_string())?;
            if file_ids.is_empty() {
                println!("(no files)");
            }
            for file_id in file_ids {
                println!("{}", file_id.to_string());
            }
        }
        Commands::Edit { uuid } => {
            let file_id =
                FileId::from_string(&uuid).ok_or_else(|| format!("invalid file id: {uuid}"))?;
            edit_file(backend, file_id).await?;
        }
    }
    Ok(())
}

/// The `edit` flow.
///
/// - If the daemon reports the file is present in a local sync directory, open
///   that real file directly in `$EDITOR`. The daemon's filesystem watcher
///   picks up the save and propagates a `FileChanged` on its own — no explicit
///   write-back, no temp file.
/// - Otherwise fetch the bytes from a peer, drop them in a temp file, open the
///   editor, and — only if the content actually changed — write the new bytes
///   back with `edit_file`.
async fn edit_file(backend: &IpcClientBackend, file_id: FileId) -> Result<(), String> {
    if let Some(path) = backend
        .local_path_for_file(file_id)
        .await
        .map_err(|error| error.to_string())?
    {
        open_in_editor(&path)?;
        return Ok(());
    }

    // Not local: we need the expected content hash to fetch. It comes from the
    // file's known metadata; if the daemon has never heard of this file there is
    // nothing to fetch.
    let files = backend
        .list_files()
        .await
        .map_err(|error| error.to_string())?;
    let expected_hash = files
        .into_iter()
        .find(|file| file.file_id == file_id)
        .map(|file| file.content_hash)
        .ok_or_else(|| format!("unknown file id: {}", file_id.to_string()))?;

    let original = backend
        .fetch_file(file_id, expected_hash)
        .await
        .map_err(|error| error.to_string())?;

    // Write to a temp file, edit, read back. `into_temp_path` keeps the file on
    // disk (deleted when the returned handle drops) while we shell out.
    let temp = tempfile::NamedTempFile::new()
        .map_err(|error| format!("failed to create temp file: {error}"))?;
    std::fs::write(temp.path(), &original)
        .map_err(|error| format!("failed to write temp file: {error}"))?;

    open_in_editor(temp.path())?;

    let edited = std::fs::read(temp.path())
        .map_err(|error| format!("failed to read temp file back: {error}"))?;

    if edited == original {
        println!("No changes");
        return Ok(());
    }

    backend
        .edit_file(file_id, edited)
        .await
        .map_err(|error| error.to_string())?;
    println!("Edited file {}", file_id.to_string());
    Ok(())
}

/// Open `path` in the user's `$EDITOR` (falling back to `vi`), blocking until it
/// exits. A non-zero editor exit is treated as an abort.
fn open_in_editor(path: &std::path::Path) -> Result<(), String> {
    let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_owned());
    let status = std::process::Command::new(&editor)
        .arg(path)
        .status()
        .map_err(|error| format!("failed to launch editor '{editor}': {error}"))?;
    if !status.success() {
        return Err(format!("editor '{editor}' exited without success"));
    }
    Ok(())
}

fn parse_tag_ids(raw: &[String]) -> Result<Vec<TagId>, String> {
    raw.iter()
        .map(|value| TagId::from_string(value).ok_or_else(|| format!("invalid tag id: {value}")))
        .collect()
}
