use std::fmt::Display;

pub struct Event {
    pub eid: String,
    pub content: String,
}

impl Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return write!(f, "{{ \"eid\": \"{}\", \"content\": \"{}\" }}", self.eid, self.content);
    }
}
