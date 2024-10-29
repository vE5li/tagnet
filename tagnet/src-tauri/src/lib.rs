use std::sync::Mutex;

use tagnet_core::{initialize, DatabaseError, DatabaseHandle, SubtagRule};

struct GlobalState(Mutex<DatabaseHandle>);

#[tauri::command]
fn all_tags(state: tauri::State<GlobalState>) -> Result<Vec<String>, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    handle
        .all_tags()?
        .into_iter()
        .map(|tag_id| handle.tag_name_from_id(tag_id))
        .collect()
}

#[tauri::command]
fn add_tag(state: tauri::State<GlobalState>, name: &str) -> Result<i64, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    let tag_id = handle.add_tag(name)?;

    Ok(tag_id.into())
}

#[tauri::command]
fn files_for_tag(
    state: tauri::State<GlobalState>,
    tag: &str,
) -> Result<Vec<String>, DatabaseError> {
    let handle = state.inner().0.lock().unwrap();

    let tag_id = tag
        .parse::<i64>()
        .map(Into::into)
        .or_else(|_| handle.tag_id_from_name(tag))?;

    let file_ids = handle.files_for_tag(tag_id, SubtagRule::Include)?;

    file_ids
        .into_iter()
        .map(|file_id| {
            Ok(handle
                .file_path_from_id(file_id)?
                .to_str()
                .unwrap()
                .to_owned())
        })
        .collect()
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let connection = initialize("../../test.db").unwrap();
    let connection = Mutex::new(connection);

    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .manage(GlobalState(connection))
        .invoke_handler(tauri::generate_handler![all_tags, files_for_tag, add_tag])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
