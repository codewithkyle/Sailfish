use actix_web::{error, post, web, http::ContentEncoding, middleware, App, HttpResponse, HttpServer, Error};
use actix_cors::Cors;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::fs;
use std::path::Path;
use chrono::Utc;

fn replace(search: &[u8], find: u8, replace: &[u8]) -> Vec<u8> {
    let mut result = vec![];

    for &b in search {
        if b == find {
            result.extend(replace);
        } else {
            result.push(b);
        }
    }

    result
}

fn process_string_output(fresh_file:bool, result_str:&String) -> String {
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

    let find_newline = b'\n';
    let find_return = b'\r';
    let find_tab = b'\t';
    let replace_str = b"";
    let mut result = replace(&body, find_newline, replace_str);
    result = replace(&result, find_return, replace_str);
    result = replace(&result, find_tab, replace_str);
    
    let result_str = String::from_utf8_lossy(&result).to_string();
    let out_str = process_string_output(fresh_file, &result_str);
   
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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .wrap(Cors::permissive())
            .wrap(middleware::Compress::new(ContentEncoding::Br))
            .service(ingest)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}