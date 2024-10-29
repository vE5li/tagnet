use std::sync::Mutex;

use tagnet_core::{initialize, DatabaseHandle, SubtagRule, TagId};

struct GlobalState(Mutex<DatabaseHandle>);

// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[tauri::command]
fn get_files_for_tag(state: tauri::State<GlobalState>, tag_id: i64) -> Vec<i64> {
    state
        .inner()
        .0
        .lock()
        .unwrap()
        .files_for_tag(TagId::from_raw(tag_id), SubtagRule::Include)
        .unwrap()
        .into_iter()
        .map(|file_id| file_id.into_raw())
        .collect()
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let connection = initialize("../../test.db").unwrap();
    let connection = Mutex::new(connection);

    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .manage(GlobalState(connection))
        .invoke_handler(tauri::generate_handler![greet, get_files_for_tag])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
