use actix_web::{error, post, web, http::ContentEncoding, middleware, App, HttpResponse, HttpServer, Error};
use actix_cors::Cors;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::fs;
use std::path::Path;
use chrono::Utc;

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