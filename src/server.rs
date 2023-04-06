mod subjects;
mod configs;

use std::env;

use subjects::consumer::Consumer;
use subjects::producer::Producer;
use subjects::event::Event;

use actix_web::{get, put, App, HttpServer, web::{self, Bytes}, Result, HttpResponse, http::StatusCode};

#[derive(Debug, Clone)]
struct Config {
    pub lossy: bool,
}

fn write_data(token: &String, content: &[u8]) -> anyhow::Result<()> {
    let producer = Producer::hydrate(&token)?;
    producer.write(content)?;
    return Ok(());
}

fn read_data(token: &String, bump: bool) -> anyhow::Result<Event> {
    let mut consumer = Consumer::hydrate(&token)?;
    let data = consumer.read(bump)?;
    return Ok(data);
}

#[get("/{token}")]
async fn read(token: web::Path<String>, data: web::Data<Config>) -> Result<HttpResponse> {
    let mut error:String = String::new();
    let mut success = "true";
    let data = read_data(&token, data.lossy).unwrap_or_else(|e| {
        error = e.to_string();
        success = "false";
        return Event{eid: String::new(), content: Vec::new()};
    });
    if success == "false" {
        if error == "EOF" {
            return Ok(HttpResponse::build(StatusCode::NO_CONTENT)
                      .content_type("application/octet-stream")
                      .body(data.content));
        }
        return Ok(HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR)
                  .content_type("application/octet-stream")
                  .body(data.content));
    }
    return Ok(HttpResponse::build(StatusCode::OK)
              .content_type("application/octet-stream")
              .insert_header(("SF-Event-ID", data.eid))
              .body(data.content));
}

#[put("/{token}")]
async fn write(bytes: Bytes, token: web::Path<String>) -> Result<HttpResponse> {
    let mut error:String = String::new();
    let mut success = "true";
    let body = bytes.to_vec();
    write_data(&token, &body).unwrap_or_else(|e| {
        error = e.to_string();
        success = "false";
    });
    if success == "false" {
        return Ok(HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(format!("{{ \"success\": \"{}\",\"error\": \"{}\" }}", success, error)));
    }
    return Ok(HttpResponse::build(StatusCode::ACCEPTED).body(format!("{{ \"success\": \"{}\",\"error\": \"{}\" }}", success, error)));
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    let mut port:u16 = 8080;
    let mut host = "127.0.0.1";
    let mut lossy = false;

    let args = env::args().collect::<Vec<String>>();
    for i in 1..args.len() {
        match args[i].as_str() {
            "-p" | "--port" => {
                port = args[i+1].parse::<u16>().unwrap_or_else(|_| {
                    println!("Invalid port number. Valid port number is between 1 and 65535.");
                    std::process::exit(1);
                });
            }
            "-h" | "--host" => {
                host = &args[i+1];
            }
            "-l" | "--lossy" => {
                lossy = true;
            }
            _ => {}
        }
    }

    let config = web::Data::new(Config {
        lossy,
    });

    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::PayloadConfig::new(usize::MAX))
            .app_data(config.clone())
            .service(read)
            .service(write)
    })
    .bind((host, port))
    .unwrap_or_else(|e| {
        println!("Error: {}", e);
        std::process::exit(1);
    });

    println!("Listening on port {}", port);
    server.run().await
}
