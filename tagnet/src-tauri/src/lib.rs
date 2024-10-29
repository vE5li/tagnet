use std::sync::Mutex;

use tagnet_core::{initialize, DatabaseError, DatabaseHandle, File, SubtagRule, Tag};

struct GlobalState(Mutex<DatabaseHandle>);

#[tauri::command]
fn all_tags(state: tauri::State<GlobalState>) -> Result<Vec<Tag>, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    Ok(handle.all_tags()?.into_iter().collect())
}

#[tauri::command]
fn add_tag(state: tauri::State<GlobalState>, name: &str, color: &str) -> Result<i64, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    let tag_id = handle.add_tag(name, color)?;

    Ok(tag_id.into())
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
        .map(|file_id| Ok(handle.file_from_id(file_id)?))
        .collect()
}

#[tauri::command]
fn tag_from_id(state: tauri::State<GlobalState>, tag_id: i64) -> Result<Tag, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    handle.tag_from_id(tag_id.into())
}

#[tauri::command]
fn update_tag(state: tauri::State<GlobalState>, tag_id: i64, name: &str, color: &str) -> Result<(), DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    handle.update_tag_name(tag_id.into(), name)?;
    handle.update_tag_color(tag_id.into(), color)?;

    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let connection = initialize("../../test.db").unwrap();
    let connection = Mutex::new(connection);

    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .manage(GlobalState(connection))
        .invoke_handler(tauri::generate_handler![all_tags, files_for_tag, add_tag, tag_from_id, update_tag])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
