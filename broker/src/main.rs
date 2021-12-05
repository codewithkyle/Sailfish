use actix_web::{error, post, get, web, http::ContentEncoding, middleware, App, HttpResponse, HttpServer, Error, http::StatusCode};
use actix_cors::Cors;

use serde::{Serialize};

use std::fs::OpenOptions;
use std::io::prelude::*;
use std::fs;
use std::io::SeekFrom;
use std::path::Path;
use std::io::BufReader;
use uuid::Uuid;

const MAX_FILE_SIZE: u32 = 1073741824;
const CONSUMER_CONFIG: &str = "consumers.cfg";
const EVENT_STREAM_CONFIG: &str = "event-stream.cfg";
const LOG_DIR: &str = "logs";

#[derive(Serialize)]
struct Consumer {
    uid: String,
    status: u8,
    file_number: u32,
    offset: u32,
    self_offset: u32,
    actual_offset: u32,
}

struct ReadOP {
    data: String,
    size: u32,
}

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

fn get_file_path(file_number: u32) -> String {
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

fn lookup_consumer(mac: &String) -> Consumer {
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .append(false)
        .open(&CONSUMER_CONFIG)
        .unwrap();
    let mut reader = BufReader::new(file);

    let mut consumer = Consumer {
        uid: "".to_string(),
        status: 0,
        file_number: 0,
        offset: 0,
        self_offset: 0,
        actual_offset: 0,
    };

    loop {
        let mut uid_buf = vec![0u8; 32];
        let buf_size = reader.read(&mut uid_buf).unwrap();
        if buf_size == 0 {
            break;
        }
        let uid = String::from_utf8(uid_buf).unwrap().to_string();
        if uid == mac.to_owned() {
            consumer.uid = uid;

            let mut status_buf = [0u8; 1];
            let _ = reader.read(&mut status_buf);
            let is_active = u8::from_be_bytes(status_buf);
            consumer.status = is_active;

            let mut file_buf = [0u8; 4];
            let _ = reader.read(&mut file_buf);
            let file_number = u32::from_be_bytes(file_buf);
            consumer.file_number = file_number;

            let mut offset_buf = [0u8; 4];
            let _ = reader.read(&mut offset_buf);
            let offset = u32::from_be_bytes(offset_buf);
            consumer.offset = offset;

            let mut act_offset_buf = [0u8; 4];
            let _ = reader.read(&mut act_offset_buf);
            let actual_offset = u32::from_be_bytes(act_offset_buf);
            consumer.actual_offset = actual_offset;

            break;
        }

        consumer.self_offset += u32::try_from(buf_size).unwrap();
        let mut temp_buf: Vec<u8> = Vec::new();
        let skip_size = reader.read_until(b'\n', &mut temp_buf).unwrap();
        consumer.self_offset += u32::try_from(skip_size).unwrap();
    }

    return consumer;
}

fn get_consumer_status() -> String {
    let mut consumer_status = "[".to_string();
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .append(false)
        .open(&CONSUMER_CONFIG)
        .unwrap();
    let mut reader = BufReader::new(file);
    loop {
        let mut consumer = Consumer {
            uid: "".to_string(),
            file_number: 0,
            self_offset: 0,
            offset: 0,
            status: 0,
            actual_offset: 0,
        };
        let mut uid_buf = vec![0u8; 32];
        let buf_size = reader.read(&mut uid_buf).unwrap();
        if buf_size == 0 {
            break;
        }
        let uid = String::from_utf8_lossy(&uid_buf).to_string();
        consumer.uid = uid;
        let mut status_buf = [0u8; 1];
        let _ = reader.read(&mut status_buf);
        let is_active = u8::from_be_bytes(status_buf);
        consumer.status = is_active;

        let mut file_buf = [0u8; 4];
        let _ = reader.read(&mut file_buf);
        let file_number = u32::from_be_bytes(file_buf);
        consumer.file_number = file_number;

        let mut offset_buf = [0u8; 4];
        let _ = reader.read(&mut offset_buf);
        let offset = u32::from_be_bytes(offset_buf);
        consumer.offset = offset;

        let mut act_offset_buf = [0u8; 4];
        let _ = reader.read(&mut act_offset_buf);
        let actual_offset = u32::from_be_bytes(act_offset_buf);
        consumer.actual_offset = actual_offset;
       
        if consumer_status == "[" {
            consumer_status += &serde_json::to_string(&consumer).unwrap();
        }
        else { 
            consumer_status += ",";
            consumer_status += &serde_json::to_string(&consumer).unwrap();
        }

        let mut temp_buf: Vec<u8> = Vec::new();
        let _ = reader.read_until(b'\n', &mut temp_buf).unwrap();
    }
    return consumer_status + "]";
}

fn update_consumer(consumer: &Consumer) -> () {
    let mut file = OpenOptions::new()
        .read(false)
        .write(true)
        .append(false)
        .open(&CONSUMER_CONFIG)
        .unwrap();
    let _ = file.seek(SeekFrom::Start(consumer.self_offset.into()));
    let _ = file.write(&consumer.uid.clone().as_bytes()); // 32 bytes
    let _ = file.write(&consumer.status.to_be_bytes()); // 1 byte
    let _ = file.write(&consumer.file_number.to_be_bytes()); // 4 bytes
    let _ = file.write(&consumer.offset.to_be_bytes()); // 4 bytes
    let _ = file.write(&consumer.actual_offset.to_be_bytes()); // 4 bytes
}

fn read_next_line(file_path: &String, offset: &u32) -> ReadOP {
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .append(false)
        .open(&file_path)
        .unwrap();
    let mut reader = BufReader::new(file);
    let _ = reader.seek_relative(offset.to_owned() as i64);
    let mut data: String = String::new();
    let size = reader.read_line(&mut data);
    ReadOP {
        data: data,
        size: u32::try_from(size.unwrap()).unwrap(),
    }
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

#[get("/{mac}")]
async fn read_mac(path: web::Path<(String,)>) -> Result<HttpResponse, Error> {

    let mac = path.into_inner().0;
    let mut consumer = lookup_consumer(&mac);

    if consumer.uid != mac {
        return Ok(HttpResponse::build(StatusCode::NOT_FOUND).content_type("application/json").finish());
    }

    let file_path = get_file_path(consumer.file_number);
    if !Path::new(&file_path).exists() {
        consumer.status = 0;
    }
    else if get_file_size(&file_path) == (consumer.offset as u64) {
        let next_file_path = get_file_path(consumer.file_number + 1);
        if Path::new(&next_file_path).exists() {
            consumer.status = 1;
            consumer.file_number += 1;
            consumer.offset = 0;
        }
        else {
            consumer.status = 0;
        }
    }
    else {
        consumer.status = 1;
    }
   
    let mut line = ReadOP {
        data: "".to_string(),
        size: 0,
    };
    if consumer.status == 1 {
        line = read_next_line(&file_path, &consumer.offset);
        consumer.offset += line.size;
    }

    update_consumer(&consumer);

    if line.size > 0 {
        return Ok(HttpResponse::Ok().content_type("application/json").body(&line.data));
    }
    else {
        return Ok(HttpResponse::build(StatusCode::NO_CONTENT).content_type("application/json").finish());
    }
}

#[post("/{mac}")]
async fn ack_mac(path: web::Path<(String,)>) -> Result<HttpResponse, Error> {

    let mac = path.into_inner().0;
    let mut consumer = lookup_consumer(&mac);

    if consumer.uid != mac {
        return Ok(HttpResponse::build(StatusCode::NOT_FOUND).content_type("application/json").finish());
    }

    consumer.actual_offset = consumer.offset;

    update_consumer(&consumer);

    return Ok(HttpResponse::Ok().content_type("application/json").finish());
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
    let mut new_consumer = token.clone();
    new_consumer = process_string_output(file_size, new_consumer);
    let _ = file.write(&new_consumer.as_bytes());
    let _ = file.write(&0_u8.to_be_bytes()); // 1 byte
    let _ = file.write(&0_u32.to_be_bytes()); // 4 bytes
    let _ = file.write(&0_u32.to_be_bytes()); // 4 bytes
    let _ = file.write(&0_u32.to_be_bytes()); // 4 bytes

    Ok(HttpResponse::Ok().content_type("text/plain").body(token))
}

#[get("/status")]
async fn status() -> Result<HttpResponse, Error> {
    let status: String = get_consumer_status();
    Ok(HttpResponse::Ok().content_type("application/json").body(status))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    prep_framework();
    HttpServer::new(move || {
        App::new()
            .wrap(Cors::permissive())
            .wrap(middleware::Compress::new(ContentEncoding::Br))
            .service(ingest) 
            .service(generate_mac)
            .service(status)
            .service(read_mac)
            .service(ack_mac)
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