use actix_web::{error, post, get, web, http::ContentEncoding, middleware, App, HttpResponse, HttpServer, Error};
use actix_cors::Cors;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::fs;
use std::path::Path;
use chrono::Utc;
use std::io::BufReader;
use uuid::Uuid;

const MAX_FILE_SIZE: u32 = 1073741824;
const CONSUMER_CONFIG: &str = "consumers.cfg";
const EVENT_STREAM_CONFIG: &str = "event-stream.cfg";
const LOG_DIR: &str = "logs";

fn process_string_output(file_size: u64, result_str: String) -> String {
    if file_size > 0 {
        return "\n".to_owned() + &result_str.to_owned();
    }
    result_str.to_string()
}

fn get_active_file_number() -> u32 {
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .open(&EVENT_STREAM_CONFIG)
        .unwrap();
    let mut reader = BufReader::new(file);
    let mut buf: String = String::new();
    let _ = reader.read_line(&mut buf);
    buf.parse().unwrap()
}

fn get_active_file_path() -> String {
    let file_number = get_active_file_number();
    let file_name = format!("{:0>10}.evts", file_number);
    LOG_DIR.to_owned() + "/" + &file_name
}

fn get_file_size(file_path: &String) -> u64 {
    if !Path::new(&file_path).exists() {
        let _ = fs::write(&file_path, "");
    }
    fs::metadata(&file_path).unwrap().len()
}

fn bump_active_file() -> String {
    let file_number = get_active_file_number() + 1;
    let mut file = OpenOptions::new()
        .write(true)
        .append(false)
        .open(&EVENT_STREAM_CONFIG)
        .unwrap();
    let _ = file.write_all(file_number.to_string().as_bytes());
    let file_name = format!("{:0>10}.evts", file_number);
    LOG_DIR.to_owned() + "/" + &file_name
}

fn left_pad_u32(num: u32) -> String {
    format!("{:0>10}", num)
}

#[post("/")]
async fn ingest(body: web::Bytes) -> Result<HttpResponse, Error> {

    let mut file_path: String = get_active_file_path();
    let mut file_size: u64 = get_file_size(&file_path);
    if file_size >= MAX_FILE_SIZE.into() {
        file_path = bump_active_file();
        file_size = get_file_size(&file_path);
    }
    let out_str = process_string_output(file_size, String::from_utf8_lossy(&body).to_string());
   
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(&file_path)
        .unwrap();
    if let Err(_e) = file.write_all(out_str.as_bytes()) {
        return Err(error::ErrorBadRequest("File write error"));
    }

    Ok(HttpResponse::Ok().body("Ok"))
}

#[get("/")]
async fn read() -> Result<HttpResponse, Error> {

    // TODO: rewrite as websocket 

    let dt = Utc::now();
    let dt_str = dt.format("%Y-%m-%d").to_string();

    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .append(false)
        .open(&dt_str)
        .unwrap();
    let offset = 0;
    let mut reader = BufReader::new(file);
    let _ = reader.seek_relative(offset);
    let mut buf: String = String::new();
    let line_size = reader.read_line(&mut buf)?;
    let new_offset = offset + (line_size as i64);

    Ok(HttpResponse::Ok().content_type("application/json").body(&buf.to_string()))
}

#[get("/new-mac")]
async fn generate_mac() -> Result<HttpResponse, Error> {
    let uuid = Uuid::new_v4(); 
    let token = uuid.to_simple().to_string(); // 32 bytes
    let file_size = get_file_size(&CONSUMER_CONFIG.to_string());
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(&CONSUMER_CONFIG)
        .unwrap();
    let mut new_consumer = token.clone() + left_pad_u32(u32::MIN).as_str() + left_pad_u32(u32::MIN).as_str();
    new_consumer = process_string_output(file_size, new_consumer);
    let _ = file.write_all(new_consumer.as_bytes());
    Ok(HttpResponse::Ok().content_type("text/plain").body(token))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    prep_framework();
    HttpServer::new(move || {
        App::new()
            .wrap(Cors::permissive())
            .wrap(middleware::Compress::new(ContentEncoding::Br))
            .service(ingest)
            .service(read)
            .service(generate_mac)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

fn prep_framework() {
    let _ = fs::create_dir_all(&LOG_DIR);
    if !Path::new(&EVENT_STREAM_CONFIG).exists() {
        let _ = fs::write(&EVENT_STREAM_CONFIG, u32::MIN.to_string());
    }
    if !Path::new(&CONSUMER_CONFIG).exists() {
        let _ = fs::write(&CONSUMER_CONFIG, "");
    }
}