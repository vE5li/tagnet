use std::sync::Mutex;

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
fn remove_tag(
    state: tauri::State<GlobalState>,
    tag_id: i64,
) -> Result<(), DatabaseError> {
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
fn tags_for_file(state: tauri::State<GlobalState>, file_id: i64) -> Result<Vec<Tag>, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    let tag_ids = handle.tag_ids_for_file(file_id.into())?;

    tag_ids
        .into_iter()
        .map(|file_id| handle.tag_from_id(file_id))
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
            tag_file,
            tag_tag,
            untag_file,
            untag_tag,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
