use std::fmt::Display;

use super::keys::generate_key;

pub struct Producer {
    topic: String,
    logFile: usize,
    logOffset: usize,
    offset: usize,
    key: String,
}

impl Producer {
    pub fn new(topic: String) -> Self {
        todo!("Verify topic exists");
        let key = generate_key();
        todo!("Add producer to tracker file");
        todo!("Figure out offset");
        return Producer{
            topic,
            logFile: 0,
            logOffset: 0,
            offset: 0,
            key,
        }
    }

    pub fn hydrate(topic: String, offset: usize, key: String) -> Self {
        todo!("Confirm the producer is in the tracker file");
        todo!("Confirm producer has topic permission");
        return Producer{
            topic,
            logFile: 0,
            logOffset: 0,
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
