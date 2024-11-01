use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;

use crate::{DatabaseHandle, File};

const NEXTCLOUD_BASE_URL: &str = "http://central:8180";
// const USERNAME: &str = "test";
// const PASSWORD: &str = "a5vAvzJUmfFWFxtpV7db";
const USERNAME: &str = "tamy";
const PASSWORD: &str = "p%0¿æ5-Âÿñ@Êlæ!=ÐDQ²»ÓKÉó1'\"l§4èö©";

#[derive(Debug, Serialize, Deserialize)]
struct ActivityResponse {
    ocs: Ocs,
}

#[derive(Debug, Serialize, Deserialize)]
struct Ocs {
    meta: Meta,
    data: Vec<Element>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Meta {
    status: String,
    statuscode: u32,
    message: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Element {
    activity_id: u32,
    datetime: String,
    app: String,
    r#type: String,
    #[serde(default)]
    user: Option<String>,
    subject: String,
    #[serde(default)]
    subject_rich: Option<Value>, // TODO: Fix type
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    message_rich: Option<Value>, // TODO: Fix type
    #[serde(default)]
    icon: Option<String>,
    #[serde(default)]
    link: Option<String>,
    #[serde(default)]
    object_type: Option<String>,
    #[serde(default)]
    object_id: Option<u32>,
    #[serde(default)]
    object_name: Option<String>,
    #[serde(default)]
    objects: Option<Value>, // TODO: Fix type
    #[serde(default)]
    previews: Option<Value>, // TODO: Fix type
}

fn parse_xml(database_handle: &DatabaseHandle, xml: &str) {
    use xmltree::Element;

    // Parse XML to an Element tree
    let root = Element::parse(xml.as_bytes()).unwrap();

    // Iterate over each <d:response> element
    for response in root
        .children
        .iter()
        .filter(|c| c.as_element().unwrap().name == "response")
    {
        // Extract <d:href>
        if let Some(href) = response.as_element().unwrap().get_child("href") {
            let file_name = href.get_text().unwrap().as_ref().to_owned();
            let file_name = file_name.strip_prefix(&format!("/remote.php/dav/files/{}/", USERNAME)).unwrap();
            println!("Adding file: {:?}", file_name);
            database_handle.add_file(file_name);
        }

        // Extract properties within <d:propstat>
        // for propstat in response.as_element().unwrap().children.iter().filter(|c| c.as_element().unwrap().name == "propstat") {
        //     if let Some(prop) = propstat.as_element().unwrap().get_child("prop") {
        //         // Extract <d:getlastmodified>
        //         if let Some(last_modified) = prop.get_child("getlastmodified") {
        //             println!(
        //                 "Last Modified: {}",
        //                 last_modified.get_text().as_deref().unwrap_or("")
        //             );
        //         }
        //
        //         // Extract <d:quota-used-bytes>
        //         if let Some(quota_used) = prop.get_child("quota-used-bytes") {
        //             println!(
        //                 "Quota Used Bytes: {}",
        //                 quota_used.get_text().as_deref().unwrap_or("")
        //             );
        //         }
        //
        //         // Extract <d:quota-available-bytes>
        //         if let Some(quota_available) = prop.get_child("quota-available-bytes") {
        //             println!(
        //                 "Quota Available Bytes: {}",
        //                 quota_available.get_text().as_deref().unwrap_or("")
        //             );
        //         }
        //
        //         // Extract <d:getetag>
        //         if let Some(etag) = prop.get_child("getetag") {
        //             println!("ETag: {}", etag.get_text().as_deref().unwrap_or(""));
        //         }
        //
        //         // Extract <d:getcontentlength>
        //         if let Some(content_length) = prop.get_child("getcontentlength") {
        //             println!(
        //                 "Content Length: {}",
        //                 content_length.get_text().as_deref().unwrap_or("")
        //             );
        //         }
        //
        //         // Extract <d:getcontenttype>
        //         if let Some(content_type) = prop.get_child("getcontenttype") {
        //             println!(
        //                 "Content Type: {}",
        //                 content_type.get_text().as_deref().unwrap_or("")
        //             );
        //         }
        //     }
        //
        //     // Extract <d:status>
        //     if let Some(status) = propstat.as_element().unwrap().get_child("status") {
        //         println!("Status: {}", status.get_text().as_deref().unwrap_or(""));
        //     }
        // }

        // println!("---"); // Separator between responses
    }
}

pub fn sync(database_handle: &DatabaseHandle) -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Runtime::new()
        .unwrap();

    runtime.block_on(async {
        let file_list = list_files().await.unwrap();
        parse_xml(database_handle, &file_list);
    });


    // let activity_updates = get_activity(None).await?;
    // println!("{activity_updates:#?}");

    Ok(())
}

// Function to list all files using WebDAV
async fn list_files() -> Result<String, Box<dyn Error>> {
    let client = Client::new();
    let url = format!("{}/remote.php/dav/files/{}/", NEXTCLOUD_BASE_URL, USERNAME);

    // Setting Depth header to 1 for listing immediate children
    let mut headers = HeaderMap::new();
    headers.insert("Depth", HeaderValue::from_static("infinity"));
    headers.insert(AUTHORIZATION, basic_auth_header());

    let response = client
        .request(reqwest::Method::from_bytes(b"PROPFIND")?, &url)
        .headers(headers)
        .send()
        .await?;

    if response.status().is_success() {
        let body = response.text().await?;
        Ok(body)
    } else {
        panic!("Failed: {}", response.status());
        // Err(Box::new(reqwest::Error::new(
        //     response.status(),
        //     "Failed to list files",
        // )))
    }
}

// Function to get activity updates from Activity API
async fn get_activity(last_id: Option<u64>) -> Result<ActivityResponse, Box<dyn Error>> {
    let client = Client::new();
    let url = format!(
        "{}/ocs/v2.php/apps/activity/api/v2/activity",
        NEXTCLOUD_BASE_URL
    );

    let mut headers = HeaderMap::new();
    headers.insert("OCS-APIRequest", "true".parse()?);
    headers.insert("Accept", HeaderValue::from_static("application/json"));

    // We can also filter:
    // - since when
    // - number of entries to return
    // - type of the object to include in the query
    // - id of the specific object we want to query
    let mut query = vec![("sort", "asc")];

    let response = client
        .get(&url)
        .basic_auth(USERNAME, Some(PASSWORD))
        .headers(headers)
        .query(&query)
        .send()
        .await?;

    if response.status().is_success() {
        let text = response.text().await.unwrap();
        // println!("{text:?}");
        Ok(serde_json::from_str(&text)?)
    } else {
        panic!("Failed: {}", response.status());
    }
}

// Basic Authorization Header Helper
fn basic_auth_header() -> HeaderValue {
    let auth = base64::encode(format!("{}:{}", USERNAME, PASSWORD));
    HeaderValue::from_str(&format!("Basic {}", auth)).unwrap()
}
