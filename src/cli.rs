mod subjects;
mod configs;

use std::str::FromStr;
use std::env;
use subjects::producer::Producer;
use subjects::topic::Topic;

enum Commands {
    Cleanup,
    Add,
    Delete,
    Reroll,
    Update,
}

enum Subject {
    Producer,
    Consumer,
    Topic,
}

impl FromStr for Commands {
    type Err = String;

    fn from_str(cmd: &str) -> Result<Self, Self::Err> {
        match cmd {
            "cleanup" => return Ok(Commands::Cleanup),
            "add" => return Ok(Commands::Add),
            "delete" => return Ok(Commands::Delete),
            "reroll" => return Ok(Commands::Reroll),
            "update" => return Ok(Commands::Update),
            _ => Err("Invalid command.".to_string()),
        }    
    }
}

impl FromStr for Subject {
    type Err = String;

    fn from_str(cmd: &str) -> Result<Self, Self::Err> {
        match cmd {
            "producer" => return Ok(Subject::Producer),
            "consumer" => return Ok(Subject::Consumer),
            "topic" => return Ok(Subject::Topic),
            _ => Err("Invalid command subject.".to_string()),
        }    
    }
}

fn main(){
    let cmd = env::args()
                .nth(1)
                .unwrap_or_else(|| {
                    eprintln!("Missing command.");
                    std::process::exit(1);
                })
                .to_lowercase();
    let cmd = Commands::from_str(&cmd)
                .unwrap_or_else(|e| {
                    eprintln!("{}", e);
                    std::process::exit(1);
                });
    match cmd {
        Commands::Cleanup => cleanup(),
        Commands::Add => add(),
        Commands::Delete => delete(),
        Commands::Update => update(),
        Commands::Reroll => reroll(),
    }
}

fn get_subject() -> Subject {
    let subject = env::args()
                    .nth(2)
                    .unwrap_or_else(|| {
                        eprintln!("Missing command subject.");
                        std::process::exit(1);
                    })
                    .to_lowercase();
    let subject = Subject::from_str(&subject)
                    .unwrap_or_else(|e| {
                        eprintln!("{}", e);
                        std::process::exit(1);
                    });
    return subject;
}

fn get_topic() -> String {
    let topic = env::args()
                    .nth(3)
                    .unwrap_or_else(|| {
                        eprintln!("Missing topic.");
                        std::process::exit(1);
                    })
                    .to_lowercase();
    return topic;
}

fn cleanup(){
    todo!("cleanup");
}

fn add() {
    let subject = get_subject();
    match subject {
        Subject::Producer => add_producer(),
        Subject::Consumer => todo!("add consumer"),
        Subject::Topic => add_topic(),
    }
}

fn add_producer(){
    let topic = get_topic();
    let producer = Producer::new(topic);
    println!("{}", producer);
}

fn add_topic() {
    let topic = get_topic();
    let topic = Topic::new(topic);
    println!("{}", topic);
}

fn delete() {
    let subject = get_subject();
    match subject {
        Subject::Producer => todo!("delete producer"),
        Subject::Consumer => todo!("delete consumer"),
        Subject::Topic => todo!("delete topic"),
    }
}

fn update() {
    let subject = get_subject();
    match subject {
        Subject::Producer => {
            eprintln!("Producers cannot be updated.");
            std::process::exit(1);
        },
        Subject::Consumer => {
            eprintln!("Consumers cannot be updated.");
            std::process::exit(1);
        },
        Subject::Topic => todo!("delete topic"),
    }
}

fn reroll() {
    let subject = get_subject();
    match subject {
        Subject::Producer => todo!("reroll producer"),
        Subject::Consumer => todo!("reroll consumer"),
        Subject::Topic => {
            eprintln!("Topics cannot be rerolled.");
            std::process::exit(1);
        },
    }
}
