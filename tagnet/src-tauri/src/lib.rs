use std::{collections::HashSet, sync::Mutex};

use regex::Regex;
use tagnet_core::{initialize, DatabaseError, DatabaseHandle, File, SubtagRule, Tag};

struct GlobalState(Mutex<DatabaseHandle>);

#[tauri::command]
fn all_tags(state: tauri::State<GlobalState>) -> Result<Vec<Tag>, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    Ok(handle.all_tags()?.into_iter().collect())
}

#[tauri::command]
fn all_files(state: tauri::State<GlobalState>) -> Result<Vec<File>, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    Ok(handle.all_files()?.into_iter().collect())
}

#[tauri::command]
fn add_tag(
    state: tauri::State<GlobalState>,
    name: &str,
    color: &str,
) -> Result<i64, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    let tag_id = handle.add_tag(name, color)?;

    Ok(tag_id.into())
}

#[tauri::command]
fn remove_tag(state: tauri::State<GlobalState>, tag_id: i64) -> Result<(), DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    handle.remove_tag(tag_id.into())
}

#[tauri::command]
fn files_for_tag(state: tauri::State<GlobalState>, tag: &str) -> Result<Vec<File>, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    let tag_id = tag
        .parse::<i64>()
        .map(Into::into)
        .or_else(|_| handle.tag_id_from_name(tag))?;

    let file_ids = handle.file_ids_for_tag(tag_id, SubtagRule::Include)?;

    file_ids
        .into_iter()
        .map(|file_id| handle.file_from_id(file_id))
        .collect()
}

#[tauri::command]
fn tags_for_file(
    state: tauri::State<GlobalState>,
    file_id: i64,
) -> Result<Vec<Tag>, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    let tag_ids = handle.tag_ids_for_file(file_id.into())?;

    tag_ids
        .into_iter()
        .map(|tag_id| handle.tag_from_id(tag_id))
        .collect()
}

#[tauri::command]
fn tags_for_selected(
    state: tauri::State<GlobalState>,
    selected_ids: Vec<i64>,
) -> Result<Vec<Tag>, DatabaseError> {
    let mut selected_ids = selected_ids.into_iter();

    let Some(first_file) = selected_ids.next() else {
        return Ok(Vec::new());
    };

    let handle = state.inner().0.lock().unwrap();

    let mut common_tag_ids = handle
        .tag_ids_for_file(first_file.into())?
        .into_iter()
        .collect::<Vec<_>>();

    for file_id in selected_ids {
        // TODO: Collecting here is most likely not the best for performance.
        let file_tag_ids = handle
            .tag_ids_for_file(file_id.into())?
            .into_iter()
            .collect::<HashSet<_>>();

        common_tag_ids.retain(|id| file_tag_ids.contains(id));
    }

    common_tag_ids
        .into_iter()
        .map(|tag_id| handle.tag_from_id(tag_id))
        .collect()
}

#[tauri::command]
fn tag_from_id(state: tauri::State<GlobalState>, tag_id: i64) -> Result<Tag, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    handle.tag_from_id(tag_id.into())
}

#[tauri::command]
fn subtags_for_tag(
    state: tauri::State<GlobalState>,
    tag_id: i64,
) -> Result<Vec<Tag>, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    let tag_ids = handle.subtag_ids_for_tag(tag_id.into(), SubtagRule::Exclude)?;

    tag_ids
        .into_iter()
        .map(|tag_id| handle.tag_from_id(tag_id))
        .collect()
}

#[tauri::command]
fn tags_for_subtag(
    state: tauri::State<GlobalState>,
    subtag_id: i64,
) -> Result<Vec<Tag>, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    let tag_ids = handle.tag_ids_for_subtag(subtag_id.into(), SubtagRule::Exclude)?;

    tag_ids
        .into_iter()
        .map(|tag_id| handle.tag_from_id(tag_id))
        .collect()
}

#[tauri::command]
fn update_tag(
    state: tauri::State<GlobalState>,
    tag_id: i64,
    name: &str,
    color: &str,
) -> Result<(), DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    handle.update_tag_name(tag_id.into(), name)?;
    handle.update_tag_color(tag_id.into(), color)?;

    Ok(())
}

#[tauri::command]
fn tag_tag(
    state: tauri::State<GlobalState>,
    tag_id: i64,
    subtag_id: i64,
) -> Result<(), DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    handle.tag_tag(tag_id.into(), subtag_id.into())?;

    Ok(())
}

#[tauri::command]
fn tag_file(
    state: tauri::State<GlobalState>,
    file_id: i64,
    tag_id: i64,
) -> Result<(), DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    handle.tag_file(tag_id.into(), file_id.into())?;

    Ok(())
}

#[tauri::command]
fn tag_selected(
    state: tauri::State<GlobalState>,
    selected_ids: Vec<i64>,
    tag_id: i64,
) -> Result<(), DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    selected_ids.into_iter().for_each(|file_id| {
        // Since the operation might fail for some files (if the file already has the tag) we just
        // discard the result in this case.
        let _ = handle.tag_file(tag_id.into(), file_id.into());
    });

    Ok(())
}

#[tauri::command]
fn untag_file(
    state: tauri::State<GlobalState>,
    file_id: i64,
    tag_id: i64,
) -> Result<(), DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    handle.untag_file(tag_id.into(), file_id.into())?;

    Ok(())
}

#[tauri::command]
fn untag_tag(
    state: tauri::State<GlobalState>,
    tag_id: i64,
    subtag_id: i64,
) -> Result<(), DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    handle.untag_tag(tag_id.into(), subtag_id.into())?;

    Ok(())
}

#[tauri::command]
fn untag_selected(
    state: tauri::State<GlobalState>,
    selected_ids: Vec<i64>,
    tag_id: i64,
) -> Result<(), DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    selected_ids.into_iter().for_each(|file_id| {
        // Since the operation might fail for some files (if the file doesn't have the tag) we just
        // discard the result in this case.
        let _ = handle.untag_file(tag_id.into(), file_id.into());
    });

    Ok(())
}

/// - Separate rules are separated by a comma
/// - The type of filter is specified by a single character before a `:`
///   - Name filters are prefixed with `n:`
///   - Tags are denoted with `t:`
///   - Regex filters are prefixed with `r:`
/// - Any filter without an explicit prefix is interpreted as a name
/// - Any filter with less than 3 characters is discarded (To search for a single character
/// file name `a` you can use `n:a`)
///
/// Examples:
/// t:dog, t:meme, war
/// t:dog, !t:meme, !war
/// t:dog, !t:meme, !n:war
#[tauri::command]
fn filter_files(
    state: tauri::State<GlobalState>,
    search_bar: &str,
) -> Result<Vec<File>, DatabaseError> {
    // TODO: Disallow `,`, `!` and `:` from tag names to make this search work.

    #[derive(Debug)]
    enum Filter<'a> {
        Tag { text: &'a str, inverted: bool },
        Regex { text: &'a str, inverted: bool },
        Name { text: &'a str, inverted: bool },
    }

    let filters = search_bar.split(",");
    let filters = filters
        .into_iter()
        .map(|filter| filter.trim())
        .filter_map(|filter| {
            let (filter, inverted) = match filter.strip_prefix("!") {
                Some(remaining) => (remaining.trim(), true),
                None => (filter, false),
            };

            // Not enough characters, discard.
            if filter.len() < 3 {
                return None;
            }

            let (filter_type, text) = filter.split_at(2);
            let text = text.trim();

            Some(match filter_type {
                "t:" => Filter::Tag { text, inverted },
                "r:" => Filter::Regex { text, inverted },
                "n:" => Filter::Name { text, inverted },
                _ => Filter::Name {
                    text: filter,
                    inverted,
                },
            })
        });

    let handle = state.inner().0.lock().unwrap();

    let mut files: Vec<File> = handle.all_files()?.into_iter().collect();

    for filter in filters {
        match filter {
            Filter::Tag { text, inverted } => {
                let Ok(tag_id) = handle.tag_id_from_name(text) else {
                    println!("Invalid tag: {text}");
                    return Ok(Vec::new());
                };

                files.retain(|file| {
                    let Ok(file_tag_ids) = handle.tag_ids_for_file(file.id) else {
                        println!("Failed to get tags for file: {}", file.path);
                        return true;
                    };

                    file_tag_ids.into_iter().find(|id| *id == tag_id).is_some() ^ inverted
                });
            }
            Filter::Regex { text, inverted } => {
                let Ok(regex) = Regex::new(text) else {
                    println!("Invalid regex: {text}");
                    return Ok(Vec::new());
                };

                files.retain(|file| regex.is_match(&file.display_name) ^ inverted);
            }
            Filter::Name { text, inverted } => {
                files.retain(|file| file.display_name.find(text).is_some() ^ inverted);
            }
        }
    }

    Ok(files)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let connection = initialize("../../test.db").unwrap();
    let connection = Mutex::new(connection);

    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .manage(GlobalState(connection))
        .invoke_handler(tauri::generate_handler![
            all_files,
            all_tags,
            files_for_tag,
            add_tag,
            remove_tag,
            tag_from_id,
            update_tag,
            subtags_for_tag,
            tags_for_subtag,
            tags_for_file,
            tags_for_selected,
            tag_file,
            tag_tag,
            tag_selected,
            untag_file,
            untag_tag,
            untag_selected,
            filter_files,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
