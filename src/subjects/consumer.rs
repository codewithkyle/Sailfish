use std::fmt::Display;

use crate::{configs::{topics::{topic_exists, read}, consumers::{add_consumer_to_config, get_consumer, delete_consumer, reroll_consumer_key, update_consumer_in_config}}, output_error};

use super::{keys::generate_key, topic::Topic, event::Event};

pub struct Consumer {
    pub topic: String,
    pub log_file: u64,
    pub log_offset: u64,
    pub offset: u64,
    pub key: String,
}

impl Consumer {
    pub fn new(topic: String) -> Self {
        let topic = topic.to_lowercase();
        if !topic_exists(&topic){
            output_error(&format!("Topic {} has not been created yet.", topic));
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
            output_error("Failed to create new consumer.",);
            std::process::exit(1);
        });
        return consumer;
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
        let consumer = get_consumer(offset).unwrap_or_else(|_| {
            output_error("Failed to find consumer.");
            std::process::exit(1);
        });
        if consumer.key != token.split_once("-").unwrap_or(("","")).1 {
            output_error("Unauthorized.");
            std::process::exit(1);
        }
        return consumer;
    }

    pub fn delete(&self) {
        delete_consumer(&self).unwrap_or_else(|_| {
            output_error("Failed to delete consumer.");
            std::process::exit(1);
        });
    }

    pub fn reroll(&mut self) {
        let new_key = reroll_consumer_key(&self).unwrap_or_else(|_| {
            output_error("Failed to generate new consumer key.");
            std::process::exit(1);
        });
        self.key = new_key;
    }

    pub fn read(&mut self) -> Event {
        let content = read(self).unwrap_or_else(|e| {
            output_error(&e.to_string());
            std::process::exit(1);
        });
        update_consumer_in_config(self).unwrap_or_else(|_| {
            output_error("Failed to update consumer after reading log data.");
            std::process::exit(1);
        });
        return content;
    }
}

impl Display for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return write!(f, "{{ \"topic\": \"{}\", \"log_file\": {}, \"log_offset\": {}, \"offset\": {}, \"key\": \"{}\" }}", self.topic, self.log_file, self.log_offset, self.offset, self.assemble_token());
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
