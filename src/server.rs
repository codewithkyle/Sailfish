mod subjects;
mod configs;

use std::env;

use subjects::consumer::Consumer;
use subjects::producer::Producer;
use subjects::event::Event;

use actix_web::{get, put, post, App, HttpServer, web::{self, Bytes}, Result, HttpResponse, http::StatusCode};

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

fn bump(token: &String, event_id: &String) -> anyhow::Result<()> {
    let mut consumer = Consumer::hydrate(&token)?;
    consumer.bump(event_id)?;
    return Ok(());
}

#[get("/{token}")]
async fn read(token: web::Path<String>, web_data: web::Data<Config>) -> Result<HttpResponse> {
    let mut error:String = String::new();
    let mut success = "true";
    let data = read_data(&token, web_data.lossy).unwrap_or_else(|e| {
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
    
    if web_data.lossy {
        return Ok(HttpResponse::build(StatusCode::OK)
                    .content_type("application/octet-stream")
                    .body(data.content));
    }
    return Ok(HttpResponse::build(StatusCode::OK)
                .content_type("application/octet-stream")
                .insert_header(("SF-Event-ID", data.eid))
                .body(data.content));
}

#[post("/{token}/{event_id}")]
async fn post(tokens: web::Path<(String, String)>, data: web::Data<Config>) -> Result<HttpResponse> {
    if data.lossy {
        return Ok(HttpResponse::build(StatusCode::METHOD_NOT_ALLOWED).body(format!("{{ \"success\": \"{}\",\"error\": \"{}\" }}", "false", "Sailfish is running in lossy mode.")));
    }
    let mut error:String = String::new();
    let mut success = "true";
    bump(&tokens.0, &tokens.1).unwrap_or_else(|e| {
        error = e.to_string();
        success = "false";
    });
    if success == "false" {
        return Ok(HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR).body(format!("{{ \"success\": \"{}\",\"error\": \"{}\" }}", success, error)));
    }
    return Ok(HttpResponse::build(StatusCode::OK).body(format!("{{ \"success\": \"{}\",\"error\": \"{}\" }}", success, error)));
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
            .service(post)
    })
    .bind((host, port))
    .unwrap_or_else(|e| {
        println!("Error: {}", e);
        std::process::exit(1);
    });

    println!("Listening on {}:{}", host, port);
    server.run().await
}
