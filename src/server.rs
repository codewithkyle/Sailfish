mod subjects;
mod configs;

use subjects::consumer::Consumer;
use subjects::producer::Producer;
use subjects::event::Event;
use actix_web::{get, put, post, App, HttpServer, web::{self, Bytes}, Result, HttpResponse, http::StatusCode, HttpRequest};

fn write_data(token: &String, content: &[u8]) -> anyhow::Result<()> {
    let producer = Producer::hydrate(&token)?;
    producer.write(content)?;
    return Ok(());
}

fn read_data(token: &String) -> anyhow::Result<Event> {
    let mut consumer = Consumer::hydrate(&token)?;
    let data = consumer.read()?;
    return Ok(data);
}

#[get("/{token}")]
async fn read(token: web::Path<String>) -> Result<HttpResponse> {
    let mut error:String = String::new();
    let mut success = "true";
    let data = read_data(&token).unwrap_or_else(|e| {
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
    HttpServer::new(|| {
        App::new()
            .service(write)
            .app_data(web::PayloadConfig::new(usize::MAX))
            .service(read)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
