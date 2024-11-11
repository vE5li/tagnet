use image::{DynamicImage, ImageFormat, ImageReader, RgbImage};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::error::Error;
use std::io::Read;
use std::path::Path;

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

use base64::encode;

fn image_to_data_url(image: DynamicImage) -> String {
    let mut buffer = Vec::new();
    image
        .write_to(&mut std::io::Cursor::new(&mut buffer), image::ImageFormat::Jpeg)
        .unwrap();
    let base64_string = encode(&buffer);

    format!("data:image/jpeg;base64,{}", base64_string)
}

async fn parse_xml(database_handle: &DatabaseHandle, xml: &str) {
    use xmltree::Element;

    // Parse XML to an Element tree
    let root = Element::parse(xml.as_bytes()).unwrap();
    let mut index = 0;

    // Iterate over each <d:response> element
    for response in root
        .children
        .iter()
        .filter(|c| c.as_element().unwrap().name == "response")
    {
        // Extract <d:href>
        let Some(href) = response.as_element().unwrap().get_child("href") else {
            panic!("no href");
        };

        // Extract properties within <d:propstat>
        let Some(propstat) = response
            .as_element()
            .unwrap()
            .children
            .iter()
            .find(|c| c.as_element().unwrap().name == "propstat")
        else {
            panic!("no propstat");
        };

        let Some(prop) = propstat.as_element().unwrap().get_child("prop") else {
            panic!("no prop");
        };

        let Some(last_modified) = prop.get_child("getlastmodified") else {
            panic!("no modified");
        };

        let Some(content_length) = prop
            .get_child("getcontentlength")
            .and_then(|element| element.get_text())
        else {
            // This is a directory.
            continue;
        };

        let Some(content_type) = prop
            .get_child("getcontenttype")
            .and_then(|element| element.get_text())
        else {
            // This is a directory.
            continue;
        };

        let Some(file_name) = href.get_text() else {
            panic!();
        };

        let original_path = href.get_text().unwrap();
        let path = original_path
            .strip_prefix(&format!("/remote.php/dav/files/{}/", USERNAME))
            .unwrap();

        let path = percent_encoding::percent_decode_str(path)
            .decode_utf8()
            // Files from nextcloud should always be UTF8.
            .unwrap()
            .into_owned();

        let path_2 = Path::new(&path);
        // Files from nextcloud should always be UTF8.
        let display_name = path_2
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .unwrap_or(path.as_str());
        let last_modified = last_modified.get_text().unwrap();

        let preview = get_preview(database_handle, &original_path)
            .await
            .ok()
            .map(image_to_data_url);

        println!(
            "Adding file: {display_name}. Preview File: {:?}",
            preview
        );

        database_handle.add_file(
            path_2,
            display_name,
            last_modified,
            content_length,
            content_type,
            preview,
        );
    }
}

pub fn sync(database_handle: &DatabaseHandle) -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        let file_list = list_files().await.unwrap();
        parse_xml(database_handle, &file_list).await;
    });

    // let activity_updates = get_activity(None).await?;
    // println!("{activity_updates:#?}");

    Ok(())
}

pub async fn get_preview(
    database_handle: &DatabaseHandle,
    file: &str,
) -> Result<DynamicImage, Box<dyn Error>> {
    let client = Client::new();
    // let url = format!(
    //     "{}/remote.php/dav/files/{}/{}",
    //     NEXTCLOUD_BASE_URL, USERNAME, file
    // );
    let url = format!("{}{}", NEXTCLOUD_BASE_URL, file);

    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, basic_auth_header());

    let response = client.get(&url).headers(headers).send().await?;

    if response.status().is_success() {
        let bytes = response.bytes().await?;
        let image =
            ImageReader::with_format(std::io::Cursor::new(bytes), ImageFormat::Jpeg).decode()?;

        let thumbnail = image.thumbnail(100, 100);
        Ok(thumbnail)
    } else {
        panic!("Failed: {}", response.status());
        // Err(Box::new(reqwest::Error::new(
        //     response.status(),
        //     "Failed to list files",
        // )))
    }
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
