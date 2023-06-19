#![allow(unused)]

use std::fmt::Display;
use anyhow::{Result, anyhow};
use crate::configs::{topics::{topic_exists, write}, producers::{add_producer_to_config, get_producer, delete_producer, reroll_producer_key, producers_exists, create_producers_file}};
use super::{keys::generate_key, topic::Topic};

pub struct Producer {
    pub topic: String,
    pub offset: u64,
    pub key: String,
}

impl Producer {
    pub fn new(topic: String) -> Result<Self> {
        if !topic_exists(&topic){
            return Err(anyhow!("Topic {} has not been created yet.", topic));
        }
        let topic = Topic::hydrate(&topic)?;
        let key = generate_key();
        let mut producer = Producer{
            topic: topic.name,
            offset: 0,
            key,
        };
        if !producers_exists() {
            create_producers_file();
        }
        add_producer_to_config(&mut producer)?;
        return Ok(producer);
    }

    pub fn hydrate(token: &String) -> Result<Self> {
        let offset = token.split_once("-").unwrap_or(("", ""));
        if offset.0 == "" || offset.1 == "" {
            return Err(anyhow!("Invalid token format."));
        }
        let offset:u64 = offset.0.parse()?;
        let producer = get_producer(offset)?;
        if &producer.key != token.split_once("-").unwrap_or(("", "")).1 {
            return Err(anyhow!("Unauthorized."));
        }
        return Ok(producer);
    }

    pub fn delete(&self) -> Result<()> {
        delete_producer(&self)?;
        return Ok(());
    }

    pub fn reroll(&mut self) -> Result<()> {
        let new_key = reroll_producer_key(&self)?;
        self.key = new_key;
        return Ok(());
    }

    pub fn write(&self, content: &[u8]) -> Result<()> {
        write(&self.topic, content)?;
        return Ok(());
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
