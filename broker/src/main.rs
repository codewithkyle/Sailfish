use actix_web::{error, post, get, web, http::ContentEncoding, middleware, App, HttpResponse, HttpServer, Error};
use actix_cors::Cors;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::fs;
use std::path::Path;
use chrono::Utc;
use std::io::BufReader;

const MAX_FILE_SIZE: u32 = 1073741824;

fn process_string_output(file_size:u64, result_str:String) -> String {
    if file_size > 0 {
        return "\n".to_owned() + &result_str.to_owned();
    }
    result_str.to_string()
}

fn get_active_file_number() -> u32 {
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .open("event-stream.cfg")
        .unwrap();
    let mut reader = BufReader::new(file);
    let mut buf: String = String::new();
    let _ = reader.read_line(&mut buf);
    buf.parse().unwrap()
}

fn get_active_file_name() -> String {
    let file_number = get_active_file_number();
    format!("{:0>10}.evts", file_number)
}

fn get_file_size(file_name: &String) -> u64 {
    if !Path::new(&file_name).exists() {
        let _ = fs::write(&file_name, "");
    }
    fs::metadata(&file_name).unwrap().len()
}

fn bump_active_file() -> String {
    let file_number = get_active_file_number() + 1;
    let mut file = OpenOptions::new()
        .write(true)
        .append(false)
        .open("event-stream.cfg")
        .unwrap();
    let _ = file.write_all(file_number.to_string().as_bytes());
    format!("{:0>10}.evts", file_number)
}

#[post("/")]
async fn ingest(body: web::Bytes) -> Result<HttpResponse, Error> {

    let mut file_name: String = get_active_file_name();
    let mut file_size: u64 = get_file_size(&file_name);
    if file_size >= MAX_FILE_SIZE.into() {
        file_name = bump_active_file();
        file_size = get_file_size(&file_name);
    }
    let out_str = process_string_output(file_size, String::from_utf8_lossy(&body).to_string());
   
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(&file_name)
        .unwrap();
    if let Err(_e) = file.write_all(out_str.as_bytes()) {
        return Err(error::ErrorBadRequest("File write error"));
    }

    Ok(HttpResponse::Ok().body("Ok"))
}

#[get("/")]
async fn read() -> Result<HttpResponse, Error> {

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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    prep_framework();
    HttpServer::new(move || {
        App::new()
            .wrap(Cors::permissive())
            .wrap(middleware::Compress::new(ContentEncoding::Br))
            .service(ingest)
            .service(read)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

fn prep_framework() {
    let cfg_file = "event-stream.cfg";
    if !Path::new(&cfg_file).exists() {
        let _ = fs::write(&cfg_file, u32::MIN.to_string());
    }
    let consumer_file = "consumers.cfg";
    if !Path::new(&consumer_file).exists() {
        let _ = fs::write(&consumer_file, "");
    }
}