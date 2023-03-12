use std::fmt::Display;

use crate::configs::{topics::{create_topic_dir, topic_exists}, producers::add_producer_to_config};

use super::{keys::generate_key, topic::Topic};

pub struct Producer {
    pub topic: String,
    pub log_file: usize,
    pub log_offset: usize,
    pub offset: u64,
    pub key: String,
}

impl Producer {
    pub fn new(topic: String) -> Self {
        let topic = topic.to_lowercase();
        if !topic_exists(&topic){
            eprintln!("Topic {} has not been created yet.", topic);
            std::process::exit(1);
        }
        let topic = Topic::hydrate(&topic);
        let key = generate_key();
        let mut producer = Producer{
            topic: topic.name,
            log_file: 0,
            log_offset: 0,
            offset: 0,
            key,
        };
        add_producer_to_config(&mut producer).unwrap_or_else(|_| {
            eprintln!("Failed to create new producer.",);
            std::process::exit(1);
        });
        return producer;
    }

    pub fn hydrate(topic: String, offset: u64, key: String) -> Self {
        todo!("Confirm the producer is in the tracker file");
        todo!("Confirm producer has topic permission");
        return Producer{
            topic,
            log_file: 0,
            log_offset: 0,
            offset,
            key,
        }
    }
}

impl Display for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return write!(f, "Producer({}, {})", self.topic, self.assemble_token());
    }
}

trait Token {
    fn assemble_token(&self) -> String;
}
impl Token for Producer {
    fn assemble_token(&self) -> String {
        return format!("{}-{}", self.offset, self.key);
    }
}
