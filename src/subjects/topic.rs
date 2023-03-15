use std::fmt::Display;

use crate::configs::topics::{create_topic_dir, add_topic_to_config, topic_exists, get_topic_from_config, delete_topic};

pub struct Topic {
    pub name: String,
    pub first_log_file: u64,
    pub curr_log_file: u64,
}

impl Topic {
    pub fn new(name: String) -> Self {
        let name = name.to_lowercase();
        if !Topic::validate(&name) {
            eprintln!("Invalid topic name.");
            std::process::exit(1);
        }
        create_topic_dir(&name);
        let topic = Topic {
            name,
            first_log_file: 0,
            curr_log_file: 0,
        };
        add_topic_to_config(&topic).unwrap_or_else(|_| {
            eprintln!("Failed to create new topic.",);
            std::process::exit(1);
        });
        return topic;
    }

    pub fn hydrate(name: &String) -> Self {
        if !topic_exists(&name) {
            eprintln!("Topic {} has not been created yet.", name);
            std::process::exit(1);
        }
        let mut topic = Topic {
            name: name.to_owned(),
            first_log_file: 0,
            curr_log_file: 0,
        };
        get_topic_from_config(&mut topic).unwrap_or_else(|_| {
            eprintln!("Failed to find topic {}.", name);
            std::process::exit(1);
        });
        return topic;
    }

    pub fn delete(&self) {
        delete_topic(&self).unwrap_or_else(|e| {
            eprintln!("Failed to delete topic {}: {}.", &self.name, e);
            std::process::exit(1);
        });
    }
}

impl Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return write!(f, "Topic({}, start file: {}, current file: {})", self.name, self.first_log_file, self.curr_log_file);
    }
}

trait Validate {
    fn validate(name: &String) -> bool;
}
impl Validate for Topic {
    fn validate(name: &String) -> bool {
        if name.contains(" ") {
            return false;
        }
        return true;
    }
}
