use std::fmt::Display;

use crate::{configs::{topics::{topic_exists, write}, producers::{add_producer_to_config, get_producer, delete_producer, reroll_producer_key}}, output_error};

use super::{keys::generate_key, topic::Topic};

pub struct Producer {
    pub topic: String,
    pub offset: u64,
    pub key: String,
}

impl Producer {
    pub fn new(topic: String) -> Self {
        if !topic_exists(&topic){
            output_error(&format!("Topic {} has not been created yet.", topic));
            std::process::exit(1);
        }
        let topic = Topic::hydrate(&topic);
        let key = generate_key();
        let mut producer = Producer{
            topic: topic.name,
            offset: 0,
            key,
        };
        add_producer_to_config(&mut producer).unwrap_or_else(|_| {
            output_error("Failed to create new producer.",);
            std::process::exit(1);
        });
        return producer;
    }

    pub fn hydrate(token: &String) -> Self {
        let offset = token.split_once("-").unwrap_or_else(|| {
            output_error("Invalid token format.");
            std::process::exit(1);
        });
        let offset:u64 = offset.0.parse().unwrap_or_else(|_| {
            output_error("Invalid token format.");
            std::process::exit(1);
        });
        let producer = get_producer(offset).unwrap_or_else(|_| {
            output_error("Failed to find producer.");
            std::process::exit(1);
        });
        if &producer.key != token {
            output_error("Unauthorized.");
            std::process::exit(1);
        }
        return producer;
    }

    pub fn delete(&self) {
        delete_producer(&self).unwrap_or_else(|_| {
            output_error("Failed to delete producer.");
            std::process::exit(1);
        });
    }

    pub fn reroll(&mut self) {
        let new_key = reroll_producer_key(&self).unwrap_or_else(|_| {
            output_error("Failed to generate new producer key.");
            std::process::exit(1);
        });
        self.key = new_key;
    }

    pub fn write(&self, content: &str) {
        write(&self.topic, content).unwrap_or_else(|_| {
            output_error("Failed to write content to log file.");
            std::process::exit(1);
        });
    }
}

impl Display for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return write!(f, "{{ \"topic\": \"{}\", \"offset\": {}, \"key\": \"{}\"}}", self.topic, self.offset, self.assemble_token());
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
