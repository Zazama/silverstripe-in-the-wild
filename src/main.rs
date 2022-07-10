use std::fs::{File, remove_file};
use std::io::prelude::*;
use std::env::temp_dir;
use std::path::{Path};
use std::{thread, time};
use std::io::BufReader;
use std::ops::Add;
use std::sync::Arc;
use flate2::Compression;
use url::Url;
use log::{info, warn, debug, error};
use reqwest::{Response, StatusCode};
use rusqlite::{Connection, params};
use futures_util::StreamExt;
use flate2::read::{MultiGzDecoder};
use flate2::write::{GzDecoder, GzEncoder};
use rust_warc::WarcReader;
use scraper::{Html, Selector};
use serde::Deserialize;
use sha2::{Sha512, Digest};
use tokio::task::JoinHandle;

extern crate log;

const COMMON_CRAWLER_BASE_URL: &str = "https://data.commoncrawl.org/";
const SILVERSTRIPE_4_CHECK_URL_PATH_IN_RESOURCE: &str = "/vendor/silverstripe/admin/client/dist/js/bundle.js";
const SILVERSTRIPE_3_CHECK_URL_PATH: &str = "/cms/javascript/CMSMain.EditForm.js";
const SILVERSTRIPE_2_CHECK_URL_PATH: &str = "/cms/javascript/CMSMain_left.js";

const ELEMENTAL_3_CSS_URL_PATH: &str = "/elemental/css/elemental-admin.css";
const ELEMENTAL_4_CSS_URL_PATH_IN_RESOURCE: &str = "/vendor/dnadesign/silverstripe-elemental/client/dist/styles/bundle.css";

const FLUENT_3_CSS_URL_PATH: &str = "/fluent/css/fluent.css";
const FLUENT_4_CSS_URL_PATH_IN_RESOURCE: &str = "/vendor/tractorcow/silverstripe-fluent/client/dist/styles/fluent.css";

const TRANSLATABLE_CSS_3_URL_PATH: &str = "/translatable/css/CMSMain.Translatable.css";

struct SilverstripeVersion {
    major_version: u8,
    minor_version: Option<u8>,
    minor_minor_version: Option<u8>,
    hash: Option<String>
}

struct InstanceResponse {
    url: String,
    response: String
}

struct SilverstripeInstance {
    responses: Vec<InstanceResponse>,
    version: SilverstripeVersion,
    server: Option<String>,
    powered_by: Option<String>,
    dev_mode: bool,
    elemental: bool,
    fluent: bool,
    translatable: bool
}

#[derive(Deserialize)] struct Envelope { #[serde(alias = "Envelope")] envelope: PayloadMetadata }
#[derive(Deserialize)] struct PayloadMetadata { #[serde(alias = "Payload-Metadata")] payload_metadata: HTTPResponseMetadata }
#[derive(Deserialize)] struct HTTPResponseMetadata { #[serde(alias = "HTTP-Response-Metadata")] http_response_metadata: HTMLMetadata }
#[derive(Deserialize)] struct HTMLMetadata { #[serde(alias = "HTML-Metadata")] html_metadata: Head }
#[derive(Deserialize)] struct Head { #[serde(alias = "Head")] head: Metas }
#[derive(Deserialize)] struct Metas { #[serde(alias = "Metas")] metas: Vec<Meta> }
#[derive(Deserialize)] struct Meta { name: Option<String>, content: Option<String> }

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"));
    let db = Connection::open("silverstripe.db").unwrap();
    init_db(&db);

    let option1 = std::env::args().nth(1).expect("no option given, use --help for list of commands.");

    if option1.eq("--download") {
        let option2 = std::env::args().nth(2).expect("no --download <path> given, use --help for list of commands.");

        let mut file = File::open(&option2).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        let lines: Vec<String> = contents.lines().map(|x| String::from(x)).collect();

        let tasks = 3;
        let lines_chunks: Arc<Vec<Vec<String>>> = Arc::new(lines.chunks((lines.len() / tasks).max(1)).map(|v| v.to_vec()).collect());

        let mut handles: Vec<JoinHandle<()>> = vec![];

        for i in 0..tasks {
            let lines_arc = lines_chunks.clone();
            handles.push(tokio::task::spawn(async move {
                for line in lines_arc.get(i).unwrap_or(&vec![]).iter() {
                    let url = match Url::parse(&(COMMON_CRAWLER_BASE_URL.to_owned() + line)) {
                        Err(_) => {
                            warn!("Could not parse url: {}", COMMON_CRAWLER_BASE_URL.to_owned() + line);
                            continue;
                        },
                        Ok(url) => url
                    };

                    info!("Processing file {}", line);

                    get_and_insert_wat_url(&url).await;
                }
            }));
        }

        for handle in handles {
            handle.await;
        }
    } else if option1.eq("--crawl") {
        get_and_insert_instance_infos_from_urls(&db).await;
    } else if option1.eq("--help") {
        println!("--download path: Download list of .wat files found in path");
        println!("--crawl: Crawl available hosts");
    } else {
        panic!("wrong option given, use --help for list of commands.")
    }
}

async fn get_and_insert_wat_url(url: &Url) -> Option<()> {
    let db = Connection::open("silverstripe.db").unwrap();

    // Check if path has already been processed in db
    if db.prepare("SELECT * FROM paths WHERE path = ?1").unwrap().exists(params![url.path()]).unwrap() {
        info!("File already in database {}", url.as_str());
        return None;
    }

    db.execute("INSERT INTO paths (path) VALUES (?1)", params![url.path()]).unwrap();
    let path_id = db.last_insert_rowid();

    let mut response: Response;
    loop {
        response = match reqwest::get(url.as_str()).await {
            Err(e) => {
                warn!("Reqwest error, could not download url: {} {:?}", url.as_str(), e);
                return None;
            },
            Ok(res) => res
        };

        let status_code = response.status();

        if status_code == StatusCode::TOO_MANY_REQUESTS {
            info!("Rate limited, wait 60 seconds");
            tokio::time::sleep(time::Duration::from_secs(60));
            continue;
        } else if status_code != StatusCode::OK {
            warn!("Status Code error ({}), could not download url: {}", status_code.as_u16(), url.as_str());
            return None;
        }

        break;
    }

    let temporary_folder_path = temp_dir();
    let filepath = temporary_folder_path.to_str().unwrap().to_owned() + "/" + Path::new(url.path()).file_stem().unwrap().to_str().unwrap();
    debug!("Saving {} to temporary file {}", url.as_str(), filepath);
    let mut file = File::create(&filepath).unwrap();
    let mut stream = response.bytes_stream();

    while let Some(item) = stream.next().await {
        let chunk = item.or(Err(format!("Error while downloading file")));
        if chunk.is_err() {
            warn!("Chunk error, could not download url: {}", url.as_str());
            remove_file(Path::new(&filepath));
            return None;
        }

        if file.write_all(&chunk.unwrap()).is_err() {
            warn!("Write error, could not download url: {}", url.as_str());
            remove_file(Path::new(&filepath));
            return None;
        }
    }

    let read_file = File::open(&filepath).unwrap();

    let warc_reader = WarcReader::new(BufReader::with_capacity(10000, MultiGzDecoder::new(read_file)));

    'outer: for item in warc_reader {
        if item.is_err() {
            continue;
        }
        let record = item.unwrap();

        let target_domain = match Url::parse(record.header.get(&"WARC-Target-URI".into()).unwrap_or(&String::from(""))) {
            Err(_) => { continue; },
            Ok(hostname) => match hostname.domain() {
                None => { continue; },
                Some(domain) => String::from(domain)
            }
        };

        let json: Envelope = match serde_json::from_slice(&record.content) {
            Err(_) => { continue; },
            Ok(json) => json
        };

        let metas = json.envelope.payload_metadata.http_response_metadata.html_metadata.head.metas;
        for meta in metas {
            if meta.content.is_none() || meta.name.is_none() {
                continue;
            }
            if meta.name.unwrap().eq("generator") && meta.content.unwrap().to_lowercase().starts_with("silverstripe") {
                let second_target = if target_domain.starts_with("www.") { target_domain.replace("www.", "") } else { "www.".to_owned() + &target_domain };
                if db.prepare("SELECT * FROM urls WHERE url = ?1 OR url = ?2").unwrap().exists(params![target_domain, second_target]).unwrap() {
                    debug!("Url already in db: {}", target_domain);
                    remove_file(Path::new(&filepath));
                    continue 'outer;
                }

                match db.execute("INSERT INTO urls (path_id, url) VALUES (?1, ?2)", params![path_id, target_domain]) {
                    Err(_) => { error!("Unable to insert into db {} {}", path_id, target_domain) },
                    Ok(_) => { info!("Found SS site {}", target_domain) }
                }
                break;
            }
        }
    }

    remove_file(Path::new(&filepath));

    Some(())
}

async fn get_and_insert_instance_infos_from_urls(db: &Connection) {
    let mut stmt = db.prepare("SELECT url FROM urls").unwrap();
    let mapped_url_strings = stmt.query_map([], |row| {
        Ok(row.get::<_, String>(0).unwrap())
    }).unwrap();

    for url_string_res in mapped_url_strings {
        let url_string = url_string_res.unwrap();

        let instance_exists = db.prepare("SELECT * FROM instances WHERE hostname = ?1").unwrap().exists(params![&url_string]).unwrap();
        let errored_exists = db.prepare("SELECT * FROM errored WHERE hostname = ?1").unwrap().exists(params![&url_string]).unwrap();
        let skipped_exists = db.prepare("SELECT * FROM skipped WHERE hostname = ?1").unwrap().exists(params![&url_string]).unwrap();

        if errored_exists {
            info!("Skipped {} (errored)", &url_string);
            continue;
        } else if skipped_exists {
            info!("Skipped {} (skipped)", &url_string);
            continue;
        } else if instance_exists {
            info!("Skipped {} (exists)", &url_string);
            continue;
        }


        let full_url_string = "http://".to_owned() + &url_string;
        let url = match Url::parse(&full_url_string) {
            Err(_) => {
                warn!("URL parsing failed on {}", full_url_string);
                continue;
            },
            Ok(url) => url
        };

        let instance = match get_instance_info(&url).await {
            Err(_) => {
                warn!("Unable to get instance info of url {}", full_url_string);
                match db.execute("INSERT INTO errored (hostname) VALUES (?1)", params![&url_string]) {
                    Err(_) => { error!("Unable to insert into errored db {}", &url_string) },
                    Ok(_) => { }
                }
                continue;
            },
            Ok(instance) => match instance {
                 None => {
                     warn!("Unable to get instance info of url (not ss?) {}", full_url_string);
                     match db.execute("INSERT INTO skipped (hostname) VALUES (?1)", params![&url_string]) {
                         Err(_) => { error!("Unable to insert into skipped db {}", &url_string) },
                         Ok(_) => { }
                     }
                     continue;
                 },
                 Some(instance) => instance
            }
        };

        match db.execute("INSERT INTO instances (hostname, major_version, version_request_hash, elemental, fluent, translatable, dev, server, poweredBy) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
            , params![&url_string, instance.version.major_version, instance.version.hash, instance.elemental, instance.fluent, instance.translatable, instance.dev_mode, instance.server, instance.powered_by]) {
            Err(_) => {
                warn!("Unable to insert instance info of url {}", &url_string);
                continue;
            },
            Ok(_) => {
                info!("Instance info inserted: {}", &url_string);
            }
        }

        for instance_response in instance.responses {
            match db.execute("INSERT INTO responses (url, response) VALUES (?1, ?2)", params![
                &instance_response.url,
                gzip_encode_string(&instance_response.response)
            ]) {
                Err(_) => {
                    warn!("Unable to insert response of url {}", &instance_response.url);
                    continue;
                },
                Ok(_) => {}
            }
        }
    }
}

async fn get_instance_info(url: &Url) -> Result<Option<SilverstripeInstance>, String> {
    let mut response = match reqwest::get(url.as_str()).await {
        Err(_) => return Err(String::from("Instance response is invalid")),
        Ok(res) => res
    };

    // Get response headers
    let headers = response.headers().clone();
    let server_header = headers.get("server");
    let powered_by_header = headers.get("x-powered-by");
    let mut responses: Vec<InstanceResponse> = vec![];

    let response_text = match response.text().await {
        Err(_) => return Err(String::from("Unable to get response")),
        Ok(text) => text
    };

    responses.push(InstanceResponse {
        url: String::from(url.as_str()),
        response: response_text.clone()
    });
    let document = Html::parse_document(&response_text);

    // Check generator tag
    match document.select(&Selector::parse("meta[name=generator]").unwrap()).next() {
        None => {
            return Ok(None);
        },
        Some(ct) => {
            if !ct.value().attr("content").unwrap_or_default().to_lowercase().starts_with("silverstripe") {
                return Ok(None);
            }
        }
    };

    // Get SS version
    let silverstripe_version = match get_silverstripe_version(&url).await {
        None => return Ok(None),
        Some(v) => v
    };

    // Check dev mode
    let dev_mode = check_dev_mode(&url).await;

    let elemental = check_elemental(&url, silverstripe_version.major_version).await;
    let fluent = check_fluent(&url, silverstripe_version.major_version).await;
    let translatable = check_translatable(&url, silverstripe_version.major_version).await;

    Ok(Some(SilverstripeInstance {
        responses,
        version: silverstripe_version,
        server: server_header.map_or(None, |h| Some(String::from(h.to_str().unwrap_or_default()))),
        powered_by: powered_by_header.map_or(None, |h| Some(String::from(h.to_str().unwrap_or_default()))),
        dev_mode,
        elemental,
        fluent,
        translatable
    }))
}

fn gzip_encode_string(string: &str) -> String {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
    encoder.write_all(string.as_bytes());

    hex::encode(encoder.finish().unwrap_or(vec![]))
}

async fn check_translatable(url: &Url, major_version: u8) -> bool {
    if major_version == 3 {
        return check_css_response(&url.join(TRANSLATABLE_CSS_3_URL_PATH).unwrap()).await;
    }

    false
}

async fn check_fluent(url: &Url, major_version: u8) -> bool {
    if major_version == 4 {
        return check_css_response(&url.join(&String::from("/resources").add(FLUENT_4_CSS_URL_PATH_IN_RESOURCE)).unwrap()).await ||
            check_css_response(&url.join(&String::from("/_resources").add(FLUENT_4_CSS_URL_PATH_IN_RESOURCE)).unwrap()).await;
    } else if major_version == 3 {
        return check_css_response(&url.join(FLUENT_3_CSS_URL_PATH).unwrap()).await;
    }

    false
}

async fn check_elemental(url: &Url, major_version: u8) -> bool {
    if major_version == 4 {
        return check_css_response(&url.join(&String::from("/resources").add(ELEMENTAL_4_CSS_URL_PATH_IN_RESOURCE)).unwrap()).await ||
            check_css_response(&url.join(&String::from("/_resources").add(ELEMENTAL_4_CSS_URL_PATH_IN_RESOURCE)).unwrap()).await;
    } else if major_version == 3 {
        return check_css_response(&url.join(ELEMENTAL_3_CSS_URL_PATH).unwrap()).await;
    }

    false
}

async fn check_css_response(url: &Url) -> bool {
    match reqwest::get(url.as_str()).await {
        Err(_) => false,
        Ok(res) => {
            let headers = res.headers().clone();
            let content_type = headers.get("content-type");
            if content_type.is_none() || !content_type.unwrap().to_str().unwrap_or("").to_lowercase().contains("css") { return false; }
            return res.status().is_success()
        }
    }
}

async fn check_dev_mode(url: &Url) -> bool {
    match reqwest::get(url.join("/dev").unwrap().as_str()).await {
        Err(_) => false,
        Ok(res) => {
            return res.url().path().starts_with("/dev") && res.status().is_success()
        }
    }
}

async fn get_silverstripe_version(url: &Url) -> Option<SilverstripeVersion> {
    // Check SS4 resources
    if let Some(hash) = get_js_response_sha512(&url.join(String::from("/resources").add(SILVERSTRIPE_4_CHECK_URL_PATH_IN_RESOURCE).as_str()).unwrap()).await {
        return Some(SilverstripeVersion { major_version: 4, minor_version: None, minor_minor_version: None, hash: Some(hash) });
    }
    // Check SS4 _resources
    if let Some(hash) = get_js_response_sha512(&url.join(String::from("/_resources").add(SILVERSTRIPE_4_CHECK_URL_PATH_IN_RESOURCE).as_str()).unwrap()).await {
        return Some(SilverstripeVersion { major_version: 4, minor_version: None, minor_minor_version: None, hash: Some(hash) });
    }

    // Check SS3
    if let Some(hash) = get_js_response_sha512(&url.join(SILVERSTRIPE_3_CHECK_URL_PATH).unwrap()).await {
        return Some(SilverstripeVersion { major_version: 3, minor_version: None, minor_minor_version: None, hash: Some(hash) });
    }

    // Check SS2
    if let Some(hash) = get_js_response_sha512(&url.join(SILVERSTRIPE_2_CHECK_URL_PATH).unwrap()).await {
        return Some(SilverstripeVersion { major_version: 2, minor_version: None, minor_minor_version: None, hash: Some(hash) });
    }

    None
}

async fn get_js_response_sha512(url: &Url) -> Option<String> {
    match reqwest::get(url.as_str()).await {
        Err(_) => None,
        Ok(res) => {
            if !res.status().is_success() {
                return None;
            }

            let headers = res.headers().clone();
            let content_type = headers.get("content-type");
            if content_type.is_none() || !content_type.unwrap().to_str().unwrap_or("").to_lowercase().contains("script") { return None; }

            match res.text().await {
                Err(_) => None,
                Ok(text) => {
                    let mut hasher = Sha512::new();
                    hasher.update(text.as_bytes());
                    Some(hex::encode(hasher.finalize()))
                }
            }
        }
    }
}

fn init_db(db: &Connection) {
    db.execute("CREATE TABLE IF NOT EXISTS paths (id INTEGER NOT NULL PRIMARY KEY, path VARCHAR(4096) NOT NULL UNIQUE)", []).unwrap();

    db.execute("CREATE TABLE IF NOT EXISTS urls (id INTEGER NOT NULL PRIMARY KEY, path_id INTEGER NOT NULL, url VARCHAR(4096) NOT NULL UNIQUE)", []).unwrap();

    db.execute("CREATE TABLE IF NOT EXISTS responses (id INTEGER NOT NULL PRIMARY KEY, url VARCHAR(4096) NOT NULL UNIQUE, response TEXT)", []).unwrap();

    db.execute("CREATE TABLE IF NOT EXISTS instances (id INTEGER NOT NULL PRIMARY KEY, hostname VARCHAR(1024) NOT NULL UNIQUE, major_version INTEGER, version_request_hash VARCHAR(512), elemental BOOLEAN, fluent BOOLEAN, translatable BOOLEAN, dev BOOLEAN, server VARCHAR(1024), poweredBy VARCHAR(1024))", []).unwrap();

    db.execute("CREATE TABLE IF NOT EXISTS skipped (id INTEGER NOT NULL PRIMARY KEY, hostname VARCHAR(1024) NOT NULL UNIQUE)", []).unwrap();

    db.execute("CREATE TABLE IF NOT EXISTS errored (id INTEGER NOT NULL PRIMARY KEY, hostname VARCHAR(1024) NOT NULL UNIQUE)", []).unwrap();
}
