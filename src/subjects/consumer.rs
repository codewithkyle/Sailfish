#![allow(unused)]

use std::fmt::Display;
use crate::configs::{topics::{topic_exists, read}, consumers::{add_consumer_to_config, get_consumer, delete_consumer, reroll_consumer_key, update_consumer_in_config}};
use super::{keys::generate_key, topic::Topic, event::Event};
use anyhow::{Result,anyhow};

pub struct Consumer {
    pub topic: String,
    pub log_file: u64,
    pub log_offset: u64,
    pub offset: u64,
    pub key: String,
}

impl Consumer {
    pub fn new(topic: String) -> Result<Self> {
        let topic = topic.to_lowercase();
        if !topic_exists(&topic){
            return Err(anyhow!("Topic {} has not been created yet.", topic));
        }
        let topic = Topic::hydrate(&topic)?;
        let key = generate_key();
        let mut consumer = Consumer{
            topic: topic.name,
            log_file: topic.first_log_file,
            log_offset: 0,
            offset: 0,
            key,
        };
        add_consumer_to_config(&mut consumer)?;
        return Ok(consumer);
    }

    pub fn hydrate(token: &String) -> Result<Self> {
        let offset = token.split_once("-").unwrap_or(("",""));
        if offset.0 == "" || offset.1 == "" {
            return Err(anyhow!("Invalid token format."));
        }
        let offset:u64 = offset.0.parse()?;
        let consumer = get_consumer(offset)?;
        if consumer.key != token.split_once("-").unwrap_or(("","")).1 {
            return Err(anyhow!("Unauthorized."));
        }
        return Ok(consumer);
    }

    pub fn delete(&self) -> Result<()> {
        delete_consumer(&self)?;
        return Ok(());
    }

    pub fn reroll(&mut self) -> Result<()> {
        let new_key = reroll_consumer_key(&self)?;
        self.key = new_key;
        return Ok(());
    }

    pub fn read(&mut self) -> Result<Event> {
        let content = read(self)?;
        update_consumer_in_config(self)?;
        return Ok(content);
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
