//! `tagnet` (CLI client): the first consumer of the IPC-client backend
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
use serde::Serialize;
use serde_json::json;
use tagnet_core::{FileId, FileInfo, TagId};
use tagnetd::{
    control::IpcClientBackend,
    database::{SubtagRule, Tag},
    transport::TransportBackend,
};

/// How command results are rendered to stdout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    /// Human-friendly tables and prose.
    Human,
    /// Machine-readable JSON (one value per command, pretty-printed).
    Json,
}

/// A serializable tag row, shared by every command that prints tags. Mirrors
/// the human [`tag_table`] columns: the tag's id, name, colour, and the names
/// of the tags applied to it.
#[derive(Debug, Serialize)]
struct TagRow {
    id: TagId,
    name: String,
    color: String,
    tags: Vec<String>,
}

/// A serializable file row, shared by every command that prints files. Mirrors
/// the human [`file_table`] columns plus the raw fields useful to scripts.
#[derive(Debug, Serialize)]
struct FileRow {
    id: FileId,
    path: String,
    version: i64,
    content_hash: String,
    tags: Vec<String>,
}

impl TagRow {
    /// Build a row from a tag and its applied-tag names (see [`tags_by_tag`]).
    fn new(tag: &Tag, tags: Vec<String>) -> Self {
        Self {
            id: tag.id,
            name: tag.name.clone(),
            color: tag.color.clone(),
            tags,
        }
    }
}

impl FileRow {
    /// Build a row from a file's info and its tag names (see [`tags_by_file`]).
    fn new(file: &FileInfo, tags: Vec<String>) -> Self {
        Self {
            id: file.file_id,
            path: file.logical_path.to_string(),
            version: file.version_number,
            content_hash: file.content_hash.clone(),
            tags,
        }
    }
}

/// Print a serializable value as pretty JSON to stdout.
fn print_json(value: &impl Serialize) {
    // Serialization of our own owned types cannot fail; fall back to a minimal
    // error object rather than panicking if it ever somehow does.
    match serde_json::to_string_pretty(value) {
        Ok(text) => println!("{text}"),
        Err(error) => println!("{{\"error\":\"failed to serialize output: {error}\"}}"),
    }
}

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

/// Translate the `--include-subtags` (or `--recursive`) flag into a
/// [`SubtagRule`]: set means walk the tag hierarchy transitively, unset means
/// direct relationships only.
fn subtag_rule(include: bool) -> SubtagRule {
    if include {
        SubtagRule::Include
    } else {
        SubtagRule::Exclude
    }
}

/// The single tag table used by *every* command that prints a set of tags
/// (`list-tags`, `tags-for-file`, `subtags`).
///
/// Short-id prefixes are highlighted the way `jj`/`git` show change ids. The
/// prefix length is computed against `tags`, so pass the full set you intend to
/// display; the highlighted prefix is a valid lookup key for the tag commands.
///
/// The `Tags` column shows the tags applied to each tag (the tags it is a
/// subtag of), the tag analogue of the file table's per-file tags.
/// `tags_by_tag` supplies those names; a tag absent from the map renders with
/// an empty column.
fn tag_table(tags: &[Tag], tags_by_tag: &HashMap<TagId, Vec<String>>) -> Table {
    let ids: Vec<String> = tags.iter().map(|tag| tag.id.to_string()).collect();

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["Tag ID", "Name", "Color", "Tags"]);

    for tag in tags {
        let id = tag.id.to_string();
        let prefix_length = unique_prefix_length(&id, &ids);
        let tags_column = tags_by_tag
            .get(&tag.id)
            .map(|names| names.join(", "))
            .unwrap_or_default();
        table.add_row(vec![
            Cell::new(highlight_id(&id, prefix_length)),
            Cell::new(&tag.name),
            Cell::new(&tag.color),
            Cell::new(tags_column),
        ]);
    }

    table
}

/// The single file table used by *every* command that prints a set of files
/// (`list-files`, `files-for-tag`).
///
/// The short-id prefix comes from the daemon-computed `short_id_length` (unique
/// against *all* files, so it is a valid global lookup key). `tags_by_file`
/// supplies the human-readable tag names shown per file; a file absent from the
/// map renders with an empty tag column.
fn file_table(files: &[FileInfo], tags_by_file: &HashMap<FileId, Vec<String>>) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["File ID", "Path", "Version", "Tags"]);

    for file in files {
        let id = file.file_id.to_string();
        let tags = tags_by_file
            .get(&file.file_id)
            .map(|names| names.join(", "))
            .unwrap_or_default();

        table.add_row(vec![
            Cell::new(highlight_id(&id, file.short_id_length)),
            Cell::new(&file.logical_path),
            Cell::new(format!("v{}", file.version_number)),
            Cell::new(tags),
        ]);
    }

    table
}

/// Emit a set of tags in the selected [`OutputMode`]: the shared [`tag_table`]
/// (or `(no tags)`) for humans, or a JSON array of [`TagRow`]s for scripts.
fn emit_tags(output: OutputMode, tags: &[Tag], tags_by_tag: &HashMap<TagId, Vec<String>>) {
    match output {
        OutputMode::Human => {
            if tags.is_empty() {
                println!("(no tags)");
            } else {
                println!("{}", tag_table(tags, tags_by_tag));
            }
        }
        OutputMode::Json => {
            let rows: Vec<TagRow> = tags
                .iter()
                .map(|tag| TagRow::new(tag, tags_by_tag.get(&tag.id).cloned().unwrap_or_default()))
                .collect();
            print_json(&rows);
        }
    }
}

/// Emit a set of files in the selected [`OutputMode`]: the shared [`file_table`]
/// (or `(no files)`) for humans, or a JSON array of [`FileRow`]s for scripts.
fn emit_files(output: OutputMode, files: &[FileInfo], tags_by_file: &HashMap<FileId, Vec<String>>) {
    match output {
        OutputMode::Human => {
            if files.is_empty() {
                println!("(no files)");
            } else {
                println!("{}", file_table(files, tags_by_file));
            }
        }
        OutputMode::Json => {
            let rows: Vec<FileRow> = files
                .iter()
                .map(|file| {
                    FileRow::new(
                        file,
                        tags_by_file.get(&file.file_id).cloned().unwrap_or_default(),
                    )
                })
                .collect();
            print_json(&rows);
        }
    }
}

/// Fetch the tag-id → name map once, for turning per-file/per-relationship tag
/// ids into human-readable names in the shared tables.
async fn tag_name_map(backend: &IpcClientBackend) -> Result<HashMap<TagId, String>, String> {
    Ok(backend
        .list_tags()
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|tag| (tag.id, tag.name))
        .collect())
}

/// Build the per-file tag-name lists shown in [`file_table`], one lookup per
/// file. `tag_names` maps ids to names (see [`tag_name_map`]); an unknown id
/// falls back to its stringified form. `rule` controls whether the tag
/// hierarchy is walked (see `--include-subtags`).
async fn tags_by_file(
    backend: &IpcClientBackend,
    files: &[FileInfo],
    tag_names: &HashMap<TagId, String>,
    rule: SubtagRule,
) -> Result<HashMap<FileId, Vec<String>>, String> {
    let mut map = HashMap::with_capacity(files.len());
    for file in files {
        let tag_ids = backend
            .tags_for_file(file.file_id, rule)
            .await
            .map_err(|error| error.to_string())?;
        let names = tag_ids
            .iter()
            .map(|tag_id| {
                tag_names
                    .get(tag_id)
                    .cloned()
                    .unwrap_or_else(|| tag_id.to_string())
            })
            .collect();
        map.insert(file.file_id, names);
    }
    Ok(map)
}

/// Build the per-tag applied-tag name lists shown in [`tag_table`], one lookup
/// per tag. The tag analogue of [`tags_by_file`]. `tag_names` maps ids to names
/// (see [`tag_name_map`]); an unknown id falls back to its stringified form.
/// `rule` controls whether the tag hierarchy is walked (see `--include-subtags`).
async fn tags_by_tag(
    backend: &IpcClientBackend,
    tags: &[Tag],
    tag_names: &HashMap<TagId, String>,
    rule: SubtagRule,
) -> Result<HashMap<TagId, Vec<String>>, String> {
    let mut map = HashMap::with_capacity(tags.len());
    for tag in tags {
        let applied_ids = backend
            .tags_for_tag(tag.id, rule)
            .await
            .map_err(|error| error.to_string())?;
        let names = applied_ids
            .iter()
            .map(|tag_id| {
                tag_names
                    .get(tag_id)
                    .cloned()
                    .unwrap_or_else(|| tag_id.to_string())
            })
            .collect();
        map.insert(tag.id, names);
    }
    Ok(map)
}

/// Resolve a user-supplied file id — a full id or any unambiguous short-id
/// prefix (as shown by `list-files`) — to a full [`FileId`] via the daemon.
///
/// This is the single entry point every command that accepts a file id should
/// use, so short ids work uniformly everywhere. Resolution is done daemon-side
/// against all files, so uniqueness is re-checked at use time (a prefix that
/// was unique when displayed may since have become ambiguous).
async fn resolve_file_id(backend: &IpcClientBackend, input: &str) -> Result<FileId, String> {
    backend
        .resolve_file_id(input.to_owned())
        .await
        .map_err(|error| match error {
            tagnetd::api::ApiError::NotFound => format!("no file matches id '{input}'"),
            other => other.to_string(),
        })
}

/// Resolve a user-supplied tag id — a full id or any unambiguous short-id
/// prefix (as shown by `list-tags`) — to a full [`TagId`] via the daemon.
///
/// The tag counterpart of [`resolve_file_id`]. Every command that accepts a tag
/// id should route through this so short ids work uniformly, and so uniqueness
/// is re-checked daemon-side at use time.
async fn resolve_tag_id(backend: &IpcClientBackend, input: &str) -> Result<TagId, String> {
    backend
        .resolve_tag_id(input.to_owned())
        .await
        .map_err(|error| match error {
            tagnetd::api::ApiError::NotFound => format!("no tag matches id '{input}'"),
            other => other.to_string(),
        })
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Arguments {
    /// Path to the daemon's control socket. Defaults to the fixed
    /// `/run/tagnet/tagnet.sock`; override only for non-standard launches.
    #[arg(long, global = true)]
    socket: Option<PathBuf>,
    /// Emit machine-readable JSON instead of human-friendly tables/text.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Upload a file's contents to the daemon, optionally tagging it.
    #[command(visible_alias = "u")]
    Upload {
        /// File on disk to read and upload.
        path: PathBuf,
        /// Tags to apply to the uploaded file, each a full id or any
        /// unambiguous short-id prefix of it (as shown by `list-tags`).
        #[arg(long = "tag", value_name = "TAG_ID")]
        tags: Vec<String>,
        /// Keep the local file after uploading (by default it is deleted
        /// once the upload has succeeded).
        #[arg(long = "keep")]
        keep: bool,
    },
    /// List all tags known to the daemon.
    #[command(visible_alias = "lt")]
    ListTags {
        /// In the Tags column, also show tags reached through the tag hierarchy
        /// (subtags), not just each tag's directly-applied tags.
        #[arg(long)]
        include_subtags: bool,
    },
    /// List all files known to the daemon.
    #[command(visible_aliases = ["lf", "ls"])]
    ListFiles {
        /// In the Tags column, also show tags reached through the tag hierarchy
        /// (subtags), not just each file's directly-applied tags.
        #[arg(long)]
        include_subtags: bool,
    },
    /// Create a tag; prints the newly-minted tag id.
    #[command(visible_alias = "ct")]
    CreateTag {
        name: String,
        // Hex form (matches the Flutter app's palette, kTagColorPalette), so
        // CLI- and app-created tags render identically.
        #[arg(long, default_value = "#F44336")]
        color: String,
    },
    /// List the files carrying a tag (the v1 single-tag search).
    #[command(visible_alias = "ft")]
    FilesForTag {
        tag_id: String,
        /// Also include files carrying any subtag of this tag, walking the
        /// hierarchy transitively.
        #[arg(long)]
        include_subtags: bool,
    },
    /// Edit a file in `$EDITOR`, fetching it from a peer first if it is not
    /// present locally, and writing back any changes.
    #[command(visible_alias = "e")]
    Edit {
        /// The file to edit, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-files`).
        id: String,
    },
    /// Download a file into the downloads directory, fetching it from a peer
    /// first if it is not present locally.
    #[command(visible_alias = "d")]
    Download {
        /// The file to download, given as a full id or any unambiguous
        /// short-id prefix of it (as shown by `list-files`).
        id: String,
    },
    /// Delete a file.
    #[command(visible_alias = "df")]
    DeleteFile {
        /// The file to delete, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-files`).
        id: String,
    },
    /// Delete a tag.
    #[command(visible_alias = "dt")]
    DeleteTag {
        /// The tag to delete (a full id or any unambiguous short-id prefix of it, as shown by `list-tags`).
        tag_id: String,
    },
    /// Apply one or more tags to an existing file.
    #[command(visible_alias = "t")]
    Tag {
        /// The file to tag, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-files`).
        id: String,
        /// One or more tags to apply, each a full id or any unambiguous
        /// short-id prefix of it (as shown by `list-tags`).
        #[arg(required = true)]
        tag_ids: Vec<String>,
    },
    /// Remove one or more tags from a file.
    #[command(visible_alias = "ut")]
    Untag {
        /// The file to untag, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-files`).
        id: String,
        /// One or more tags to remove, each a full id or any unambiguous
        /// short-id prefix of it (as shown by `list-tags`).
        #[arg(required = true)]
        tag_ids: Vec<String>,
    },
    /// List the tags applied to a file.
    #[command(visible_alias = "tf")]
    TagsForFile {
        /// The file to inspect, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-files`).
        id: String,
        /// Also include tags reached through the tag hierarchy (the tags this
        /// file's tags are subtags of), walking transitively.
        #[arg(long)]
        include_subtags: bool,
    },
    /// Rename a tag.
    #[command(visible_alias = "rt")]
    RenameTag {
        /// The tag to rename (a full id or any unambiguous short-id prefix of it, as shown by `list-tags`).
        tag_id: String,
        /// The tag's new name.
        name: String,
    },
    /// Change a tag's color.
    #[command(visible_alias = "sc")]
    SetTagColor {
        /// The tag to recolor (a full id or any unambiguous short-id prefix of it, as shown by `list-tags`).
        tag_id: String,
        /// The tag's new color.
        color: String,
    },
    /// Move (rename) a file to a new logical path.
    #[command(visible_alias = "mv")]
    Move {
        /// The file to move, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-files`).
        id: String,
        /// The file's new logical path.
        path: String,
    },
    /// Make a tag a subtag of one or more parent tags.
    #[command(visible_alias = "tt")]
    TagTag {
        /// The child tag, given as a full id or any unambiguous short-id prefix
        /// of it (as shown by `list-tags`).
        child: String,
        /// One or more parent tags to nest the child under, each a full id or
        /// any unambiguous short-id prefix of it.
        #[arg(required = true)]
        parents: Vec<String>,
    },
    /// Remove a tag as a subtag of one or more parent tags.
    #[command(visible_alias = "utt")]
    UntagTag {
        /// The child tag, given as a full id or any unambiguous short-id prefix
        /// of it (as shown by `list-tags`).
        child: String,
        /// One or more parent tags to detach the child from, each a full id or
        /// any unambiguous short-id prefix of it.
        #[arg(required = true)]
        parents: Vec<String>,
    },
    /// List the subtags (children) of a tag.
    #[command(visible_alias = "st")]
    Subtags {
        /// The parent tag, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-tags`).
        tag_id: String,
        /// Walk the hierarchy transitively (include subtags of subtags).
        #[arg(long)]
        recursive: bool,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = Arguments::parse();

    let output = if arguments.json {
        OutputMode::Json
    } else {
        OutputMode::Human
    };

    // Connect the IPC-client backend to the daemon's control socket.
    let backend = match &arguments.socket {
        Some(path) => IpcClientBackend::connect(path).await,
        None => IpcClientBackend::connect_default().await,
    };
    let backend = match backend {
        Ok(backend) => backend,
        Err(error) => {
            match output {
                OutputMode::Human => {
                    eprintln!("Failed to connect to the tagnet daemon control socket: {error}");
                    eprintln!("Is the daemon running? (tagnet run <config>)");
                }
                OutputMode::Json => print_json(&json!({
                    "error": format!("failed to connect to the tagnet daemon control socket: {error}"),
                })),
            }
            return ExitCode::FAILURE;
        }
    };

    match run(&backend, arguments.command, output).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            if output == OutputMode::Json {
                print_json(&json!({ "error": error }));
            } else {
                eprintln!("Error: {error}");
            }
            ExitCode::FAILURE
        }
    }
}

async fn run(
    backend: &IpcClientBackend,
    command: Commands,
    output: OutputMode,
) -> Result<(), String> {
    match command {
        Commands::Upload { path, tags, keep } => {
            let content = std::fs::read(&path)
                .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
            let path_name = path
                .file_name()
                .ok_or_else(|| format!("{} has no file name", path.display()))?
                .to_string_lossy()
                .to_string();
            // Resolve each `--tag` argument (full id or short prefix) via the
            // daemon, so tagging on upload accepts short ids like every other
            // tag-id command.
            let mut resolved_tags = Vec::with_capacity(tags.len());
            for tag in &tags {
                resolved_tags.push(resolve_tag_id(backend, tag).await?);
            }

            let file_id = backend
                .upload_file(path_name, content, resolved_tags)
                .await
                .map_err(|error| error.to_string())?;

            if !keep {
                std::fs::remove_file(&path).map_err(|error| {
                    format!(
                        "uploaded as file {}, but failed to delete {}: {error}",
                        file_id.to_string(),
                        path.display()
                    )
                })?;
            }

            match output {
                OutputMode::Human => println!("Uploaded as file {}", file_id.to_string()),
                OutputMode::Json => print_json(&json!({ "id": file_id })),
            }
        }
        Commands::ListTags { include_subtags } => {
            let tags = backend
                .list_tags()
                .await
                .map_err(|error| error.to_string())?;
            let tag_names = tag_name_map(backend).await?;
            let tag_tags =
                tags_by_tag(backend, &tags, &tag_names, subtag_rule(include_subtags)).await?;
            emit_tags(output, &tags, &tag_tags);
        }
        Commands::ListFiles { include_subtags } => {
            let files = backend
                .list_files()
                .await
                .map_err(|error| error.to_string())?;
            let tag_names = tag_name_map(backend).await?;
            let file_tags =
                tags_by_file(backend, &files, &tag_names, subtag_rule(include_subtags)).await?;
            emit_files(output, &files, &file_tags);
        }
        Commands::CreateTag { name, color } => {
            let tag_id = backend
                .create_tag(name, color)
                .await
                .map_err(|error| error.to_string())?;
            match output {
                OutputMode::Human => println!("{}", tag_id.to_string()),
                OutputMode::Json => print_json(&json!({ "id": tag_id })),
            }
        }
        Commands::FilesForTag {
            tag_id,
            include_subtags,
        } => {
            let tag_id = resolve_tag_id(backend, &tag_id).await?;
            let file_ids: std::collections::HashSet<FileId> = backend
                .files_for_tag(tag_id, subtag_rule(include_subtags))
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .collect();
            // Render through the same file listing as `list-files`: fetch the
            // full file listing and keep the ones carrying this tag. (The read
            // API returns ids only; the daemon-computed short-id length and
            // other columns live on the full `FileInfo`.)
            let files: Vec<FileInfo> = backend
                .list_files()
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .filter(|file| file_ids.contains(&file.file_id))
                .collect();
            let tag_names = tag_name_map(backend).await?;
            // The Tags column shows each file's own direct tags, regardless of
            // how the search matched them.
            let file_tags = tags_by_file(backend, &files, &tag_names, SubtagRule::Exclude).await?;
            emit_files(output, &files, &file_tags);
        }
        Commands::Edit { id } => {
            let file_id = resolve_file_id(backend, &id).await?;
            edit_file(backend, file_id, output).await?;
        }
        Commands::Download { id } => {
            let file_id = resolve_file_id(backend, &id).await?;
            download_file(backend, file_id, output).await?;
        }
        Commands::DeleteFile { id } => {
            let file_id = resolve_file_id(backend, &id).await?;
            backend
                .delete_file(file_id)
                .await
                .map_err(|error| error.to_string())?;
            match output {
                OutputMode::Human => println!("Deleted file {}", file_id.to_string()),
                OutputMode::Json => print_json(&json!({ "deleted": file_id })),
            }
        }
        Commands::DeleteTag { tag_id } => {
            let tag_id = resolve_tag_id(backend, &tag_id).await?;
            backend
                .delete_tag(tag_id)
                .await
                .map_err(|error| error.to_string())?;
            match output {
                OutputMode::Human => println!("Deleted tag {}", tag_id.to_string()),
                OutputMode::Json => print_json(&json!({ "deleted": tag_id })),
            }
        }
        Commands::Tag { id, tag_ids } => {
            let file_id = resolve_file_id(backend, &id).await?;
            let mut applied = Vec::new();
            for tag in &tag_ids {
                let tag_id = resolve_tag_id(backend, tag).await?;
                backend
                    .tag_file(tag_id, file_id)
                    .await
                    .map_err(|error| error.to_string())?;
                if output == OutputMode::Human {
                    println!(
                        "Tagged file {} with tag {}",
                        file_id.to_string(),
                        tag_id.to_string()
                    );
                }
                applied.push(tag_id);
            }
            if output == OutputMode::Json {
                print_json(&json!({ "file": file_id, "tagged": applied }));
            }
        }
        Commands::Untag { id, tag_ids } => {
            let file_id = resolve_file_id(backend, &id).await?;
            let mut removed = Vec::new();
            for tag in &tag_ids {
                let tag_id = resolve_tag_id(backend, tag).await?;
                backend
                    .untag_file(tag_id, file_id)
                    .await
                    .map_err(|error| error.to_string())?;
                if output == OutputMode::Human {
                    println!(
                        "Removed tag {} from file {}",
                        tag_id.to_string(),
                        file_id.to_string()
                    );
                }
                removed.push(tag_id);
            }
            if output == OutputMode::Json {
                print_json(&json!({ "file": file_id, "untagged": removed }));
            }
        }
        Commands::TagsForFile {
            id,
            include_subtags,
        } => {
            let file_id = resolve_file_id(backend, &id).await?;
            let tag_ids: std::collections::HashSet<TagId> = backend
                .tags_for_file(file_id, subtag_rule(include_subtags))
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .collect();
            // Render through the same tag listing as `list-tags`: fetch all tags
            // and keep the ones applied to this file.
            let tags: Vec<Tag> = backend
                .list_tags()
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .filter(|tag| tag_ids.contains(&tag.id))
                .collect();
            let tag_names = tag_name_map(backend).await?;
            // The Tags column shows each tag's own direct tags, regardless of
            // how the command matched them.
            let tag_tags = tags_by_tag(backend, &tags, &tag_names, SubtagRule::Exclude).await?;
            emit_tags(output, &tags, &tag_tags);
        }
        Commands::RenameTag { tag_id, name } => {
            let tag_id = resolve_tag_id(backend, &tag_id).await?;
            backend
                .rename_tag(tag_id, name.clone())
                .await
                .map_err(|error| error.to_string())?;
            match output {
                OutputMode::Human => println!("Renamed tag {}", tag_id.to_string()),
                OutputMode::Json => print_json(&json!({ "id": tag_id, "name": name })),
            }
        }
        Commands::SetTagColor { tag_id, color } => {
            let tag_id = resolve_tag_id(backend, &tag_id).await?;
            backend
                .set_tag_color(tag_id, color.clone())
                .await
                .map_err(|error| error.to_string())?;
            match output {
                OutputMode::Human => println!("Recolored tag {}", tag_id.to_string()),
                OutputMode::Json => print_json(&json!({ "id": tag_id, "color": color })),
            }
        }
        Commands::Move { id, path } => {
            let file_id = resolve_file_id(backend, &id).await?;
            backend
                .move_file(file_id, path.clone())
                .await
                .map_err(|error| error.to_string())?;
            match output {
                OutputMode::Human => println!("Moved file {}", file_id.to_string()),
                OutputMode::Json => print_json(&json!({ "id": file_id, "path": path })),
            }
        }
        Commands::TagTag { child, parents } => {
            let child_id = resolve_tag_id(backend, &child).await?;
            let mut applied = Vec::new();
            for parent in &parents {
                let parent_id = resolve_tag_id(backend, parent).await?;
                backend
                    .tag_tag(parent_id, child_id)
                    .await
                    .map_err(|error| error.to_string())?;
                if output == OutputMode::Human {
                    println!(
                        "Tagged tag {} with {}",
                        child_id.to_string(),
                        parent_id.to_string()
                    );
                }
                applied.push(parent_id);
            }
            if output == OutputMode::Json {
                print_json(&json!({ "tag": child_id, "tagged": applied }));
            }
        }
        Commands::UntagTag { child, parents } => {
            let child_id = resolve_tag_id(backend, &child).await?;
            let mut removed = Vec::new();
            for parent in &parents {
                let parent_id = resolve_tag_id(backend, parent).await?;
                backend
                    .untag_tag(parent_id, child_id)
                    .await
                    .map_err(|error| error.to_string())?;
                if output == OutputMode::Human {
                    println!(
                        "Removed tag {} from {}",
                        parent_id.to_string(),
                        child_id.to_string(),
                    );
                }
                removed.push(parent_id);
            }
            if output == OutputMode::Json {
                print_json(&json!({ "tag": child_id, "untagged": removed }));
            }
        }
        Commands::Subtags { tag_id, recursive } => {
            let tag_id = resolve_tag_id(backend, &tag_id).await?;
            let subtag_ids: std::collections::HashSet<TagId> = backend
                .subtags_for_tag(tag_id, subtag_rule(recursive))
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .collect();
            // Render through the same tag listing as `list-tags`: fetch all tags
            // and keep the subtags of this tag.
            let tags: Vec<Tag> = backend
                .list_tags()
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .filter(|tag| subtag_ids.contains(&tag.id))
                .collect();
            let tag_names = tag_name_map(backend).await?;
            // The Tags column shows each tag's own direct tags, regardless of
            // how the command matched them.
            let tag_tags = tags_by_tag(backend, &tags, &tag_names, SubtagRule::Exclude).await?;
            emit_tags(output, &tags, &tag_tags);
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
async fn edit_file(
    backend: &IpcClientBackend,
    file_id: FileId,
    output: OutputMode,
) -> Result<(), String> {
    if let Some(path) = backend
        .local_path_for_file(file_id)
        .await
        .map_err(|error| error.to_string())?
    {
        open_in_editor(&path)?;
        // The watcher propagates the on-disk save; report the same shape as the
        // fetch-and-write-back path below.
        match output {
            OutputMode::Human => {}
            OutputMode::Json => print_json(&json!({ "id": file_id, "edited": true })),
        }
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
        match output {
            OutputMode::Human => println!("No changes"),
            OutputMode::Json => print_json(&json!({ "id": file_id, "edited": false })),
        }
        return Ok(());
    }

    backend
        .edit_file(file_id, edited)
        .await
        .map_err(|error| error.to_string())?;
    match output {
        OutputMode::Human => println!("Edited file {}", file_id.to_string()),
        OutputMode::Json => print_json(&json!({ "id": file_id, "edited": true })),
    }
    Ok(())
}

/// The `download` flow.
///
/// Shares its start with [`edit_file`]: locate the file's bytes — reading the
/// real file if it lives in a local sync directory, otherwise fetching them
/// from a peer — then, instead of editing, copy them into the user's downloads
/// directory.
async fn download_file(
    backend: &IpcClientBackend,
    file_id: FileId,
    output: OutputMode,
) -> Result<(), String> {
    // Pull the file's metadata once: we need its content hash to fetch (if it
    // isn't local) and its logical path to pick a sensible output filename.
    let file = backend
        .list_files()
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .find(|file| file.file_id == file_id)
        .ok_or_else(|| format!("unknown file id: {}", file_id.to_string()))?;

    let bytes = if let Some(path) = backend
        .local_path_for_file(file_id)
        .await
        .map_err(|error| error.to_string())?
    {
        std::fs::read(&path)
            .map_err(|error| format!("failed to read local file {}: {error}", path.display()))?
    } else {
        backend
            .fetch_file(file_id, file.content_hash)
            .await
            .map_err(|error| error.to_string())?
    };

    // Name the download after the file's logical path's final component, so a
    // nested `foo/bar/name.txt` lands as `name.txt`. Fall back to the file id
    // if the logical path has no usable component.
    let logical = file.logical_path.to_string();
    let file_name = logical
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or(&logical);
    let file_name = if file_name.is_empty() {
        file_id.to_string()
    } else {
        file_name.to_owned()
    };

    std::fs::write(&file_name, &bytes)
        .map_err(|error| format!("failed to write {}: {error}", file_name))?;

    match output {
        OutputMode::Human => println!("Downloaded to {}", file_name),
        OutputMode::Json => print_json(&json!({ "id": file_id, "path": file_name })),
    }
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
