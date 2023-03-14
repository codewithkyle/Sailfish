use std::fmt::Display;

use crate::configs::{topics::{create_topic_dir, topic_exists}, producers::add_producer_to_config, consumers::add_consumer_to_config};

use super::{keys::generate_key, topic::Topic};

pub struct Consumer {
    pub topic: String,
    pub log_file: usize,
    pub log_offset: usize,
    pub offset: u64,
    pub key: String,
}

impl Consumer {
    pub fn new(topic: String) -> Self {
        let topic = topic.to_lowercase();
        if !topic_exists(&topic){
            eprintln!("Topic {} has not been created yet.", topic);
            std::process::exit(1);
        }
        let topic = Topic::hydrate(&topic);
        let key = generate_key();
        let mut consumer = Consumer{
            topic: topic.name,
            log_file: 0,
            log_offset: 0,
            offset: 0,
            key,
        };
        add_consumer_to_config(&mut consumer).unwrap_or_else(|_| {
            eprintln!("Failed to create new consumer.",);
            std::process::exit(1);
        });
        return consumer;
    }

    pub fn hydrate(topic: String, offset: u64, key: String) -> Self {
        todo!("Confirm the consumer is in the tracker file");
        todo!("Confirm consumer has topic permission");
        return Consumer{
            topic,
            log_file: 0,
            log_offset: 0,
            offset,
            key,
        }
    }
}

impl Display for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return write!(f, "Consumer({}, {})", self.topic, self.assemble_token());
    }
}

trait Token {
    fn assemble_token(&self) -> String;
}
impl Token for Consumer {
    fn assemble_token(&self) -> String {
        return format!("{}-{}", self.offset, self.key);
    }
}
