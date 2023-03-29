mod subjects;
mod configs;

use subjects::consumer::Consumer;
use subjects::producer::Producer;
use subjects::event::Event;
use actix_web::{get, put, post, App, HttpServer, web, Result, HttpResponse};

#[get("/{token}")]
async fn read(token: web::Path<String>) -> Result<HttpResponse> {
    let mut error:String = String::new();
    let mut success = "true";
    let data = read_data(&token).unwrap_or_else(|e| {
        error = e.to_string();
        success = "false";
        return Event{
            eid: "".to_string(),
            content: "".to_string(),
        };
    });
    if data.eid == "" {
        return Ok(HttpResponse::Ok().body(format!("{{ \"success\": \"{}\",\"error\": \"{}\" }}", success, error)));
    }
    return Ok(HttpResponse::Ok().body(format!("{}", data)));
}

fn write_data(token: &String, content: &str) -> anyhow::Result<()> {
    let producer = Producer::hydrate(&token)?;
    producer.write(&content)?;
    return Ok(());
}

fn read_data(token: &String) -> anyhow::Result<Event> {
    let mut consumer = Consumer::hydrate(&token)?;
    let data = consumer.read()?;
    return Ok(data);
}

#[put("/{token}")]
async fn write(req_body: String, token: web::Path<String>) -> Result<HttpResponse> {
    let mut error:String = String::new();
    let mut success = "true";
    write_data(&token, &req_body).unwrap_or_else(|e| {
        error = e.to_string();
        success = "false";
    });
    return Ok(HttpResponse::Ok().body(format!("{{ \"success\": \"{}\",\"error\": \"{}\" }}", success, error)));
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .service(write)
            .service(read)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
