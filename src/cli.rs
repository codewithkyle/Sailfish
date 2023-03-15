mod subjects;
mod configs;

use std::str::FromStr;
use std::env;
use configs::consumers::list_consumers;
use configs::producers::list_producers;
use subjects::consumer::Consumer;
use subjects::producer::Producer;
use subjects::topic::Topic;

enum Commands {
    Cleanup,
    Add,
    Delete,
    Reroll,
    Update,
    Stat,
    List,
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
            "create" => return Ok(Commands::Add),
            "remove" => return Ok(Commands::Delete),
            "delete" => return Ok(Commands::Delete),
            "reroll" => return Ok(Commands::Reroll),
            "update" => return Ok(Commands::Update),
            "stat" => return Ok(Commands::Stat),
            "list" => return Ok(Commands::List),
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
            "producers" => return Ok(Subject::Producer),
            "consumers" => return Ok(Subject::Consumer),
            "topics" => return Ok(Subject::Topic),
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
        Commands::Stat => stat_subject(),
        Commands::List => list_subject(),
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

fn get_token() -> String {
    let token = env::args()
                    .nth(3)
                    .unwrap_or_else(|| {
                        eprintln!("Missing token.");
                        std::process::exit(1);
                    })
                    .to_lowercase();
    return token;
}

fn cleanup(){
    todo!("cleanup");
}

fn add() {
    let subject = get_subject();
    match subject {
        Subject::Producer => add_producer(),
        Subject::Consumer => add_consumer(),
        Subject::Topic => add_topic(),
    }
}

fn add_consumer() {
    let topic = get_topic();
    let consumer = Consumer::new(topic);
    println!("{}", consumer);
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
        Subject::Producer => delete_producer(),
        Subject::Consumer => delete_consumer(),
        Subject::Topic => delete_topic(),
    }
}

fn delete_producer(){
    let token = get_token();
    Producer::hydrate(&token).delete();
    println!("Producer {} has been deleted.", token);
}

fn delete_consumer(){
    let token = get_token();
    Consumer::hydrate(&token).delete();
    println!("Consumer {} has been deleted.", token);
}

fn delete_topic(){
    let topic = get_topic();
    Topic::hydrate(&topic).delete();
    println!("Topic {} has been deleted.", topic);
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
        Subject::Producer => reroll_producer(),
        Subject::Consumer => reroll_consumer(),
        Subject::Topic => {
            eprintln!("Topics cannot be rerolled.");
            std::process::exit(1);
        },
    }
}

fn reroll_producer(){
    let token = get_token();
    let mut producer = Producer::hydrate(&token);
    producer.reroll();
    println!("{}", producer);
}

fn reroll_consumer(){
    let token = get_token();
    let mut consumer = Consumer::hydrate(&token);
    consumer.reroll();
    println!("{}", consumer);
}

fn stat_subject() {
    let subject = get_subject();
    match subject {
        Subject::Producer => stat_producer(),
        Subject::Consumer => stat_consumer(),
        Subject::Topic => stat_topic(),
    }
}

fn stat_producer(){
    let token = get_token();
    let producer = Producer::hydrate(&token);
    println!("{}", producer);
}

fn stat_consumer(){
    let token = get_token();
    let consumer = Consumer::hydrate(&token);
    println!("{}", consumer);
}

fn stat_topic(){
    let topic = get_topic();
    let topic = Topic::hydrate(&topic);
    println!("{}", topic);
}

fn list_subject(){
    let subject = get_subject();
    match subject {
        Subject::Producer => list_producers().unwrap_or_else(|e|{
            eprintln!("{}", e);
            std::process::exit(1);
        }),
        Subject::Consumer => list_consumers().unwrap_or_else(|e|{
            eprintln!("{}", e);
            std::process::exit(1);
        }),
        Subject::Topic => todo!("list topic"),
    }
}
