use actix_web::{error, post, get, web, http::ContentEncoding, middleware, App, HttpResponse, HttpServer, Error};
use actix_cors::Cors;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::fs;
use std::path::Path;
use chrono::Utc;
use std::io::BufReader;
use r2d2_sqlite::{self, SqliteConnectionManager};
mod db;
use db::{Pool, Queries};

fn process_string_output(fresh_file:bool, result_str:String) -> String {
    if !fresh_file {
        return "\n".to_owned() + &result_str.to_owned();
    }
    result_str.to_string()
}

#[post("/")]
async fn ingest(body: web::Bytes) -> Result<HttpResponse, Error> {

    let dt = Utc::now();
    let dt_str = dt.format("%Y-%m-%d").to_string();

    let mut fresh_file: bool = false;
    if !Path::new(&dt_str).exists() {
        fresh_file = true;
        fs::write(&dt_str, "")?;
    }
    
    let out_str = process_string_output(fresh_file, String::from_utf8_lossy(&body).to_string());
   
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(&dt_str)
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

    let manager = SqliteConnectionManager::file("event-stream.db");
    let pool = Pool::new(manager).unwrap();

    HttpServer::new(move || {
        App::new()
            .data(pool.clone())
            .wrap(Cors::permissive())
            .wrap(middleware::Compress::new(ContentEncoding::Br))
            .service(ingest)
            .service(read)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}