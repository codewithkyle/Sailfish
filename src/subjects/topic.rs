use std::fmt::Display;

use crate::{configs::topics::{create_topic_dir, add_topic_to_config, topic_exists, get_topic_from_config, delete_topic, delete_topic_dir, update_topic_in_config}, output_error};

pub struct Topic {
    pub name: String,
    pub first_log_file: u64,
    pub curr_log_file: u64,
    pub offset: u64,
}

impl Topic {
    pub fn new(name: String) -> Self {
        let name = name.to_lowercase();
        if !Topic::validate(&name) {
            output_error("Invalid topic name.");
            std::process::exit(1);
        }
        create_topic_dir(&name).unwrap_or_else(|_|{
            output_error("Failed to create topic directory.");
            std::process::exit(1);
        });
        let topic = Topic {
            name,
            first_log_file: 0,
            curr_log_file: 0,
            offset: 0,
        };
        add_topic_to_config(&topic).unwrap_or_else(|_| {
            output_error("Failed to create new topic.",);
            std::process::exit(1);
        });
        return topic;
    }

    pub fn hydrate(name: &str) -> Self {
        if !topic_exists(&name) {
            output_error(&format!("Topic {} has not been created yet.", name));
            std::process::exit(1);
        }
        let mut topic = Topic {
            name: name.to_owned(),
            first_log_file: 0,
            curr_log_file: 0,
            offset: 0,
        };
        get_topic_from_config(&mut topic).unwrap_or_else(|_| {
            output_error(&format!("Failed to find topic {}.", name));
            std::process::exit(1);
        });
        return topic;
    }

    pub fn delete(&self) {
        delete_topic(&self).unwrap_or_else(|_| {
            output_error(&format!("Failed to delete topic {}.", &self.name));
            std::process::exit(1);
        });
        delete_topic_dir(&self.name);
    }

    pub fn bump(&mut self) {
        self.curr_log_file += 1;
        update_topic_in_config(&self).unwrap_or_else(|_| {
            output_error("Failed to create new log file.");
            std::process::exit(1);
        });
    }
}

impl Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return write!(f, "{{ \"name\": \"{}\", \"first_log_file\": {}, \"curr_log_file\": {} }}", self.name, self.first_log_file, self.curr_log_file);
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
