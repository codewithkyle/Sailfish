#![allow(unused)]

use std::{path::Path, fs::{self, File, OpenOptions}, io::{BufWriter, Write, Seek, SeekFrom, BufReader, Read}, sync::Mutex};
use anyhow::{Result, anyhow};
use crate::subjects::{topic::Topic, keys::generate_key, consumer::Consumer, event::Event};

pub fn create_topic_dir(topic: &str) -> Result<()> {
    let path = format!("sailfish/logs/{}", topic);
    let path = Path::new(&path);
    if !path.exists() {
        fs::create_dir_all(path)?;
    }
    create_topic_file(topic, 0)?;
    create_topic_file(topic, 1)?;
    return Ok(());
}

pub fn delete_topic_dir(topic: &str) -> Result<()> {
    let path = format!("sailfish/logs/{}", topic);
    let path = Path::new(&path);
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    return Ok(());
}

fn create_topic_file(topic: &str, file: usize) -> Result<File> {
    let path = format!("sailfish/logs/{}/{}", topic, file);
    let path = Path::new(&path);
    let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(path)?;
    return Ok(file);
}

fn get_latest_topic_file(topic: &str) -> Result<File> {
    let mut topic = Topic::hydrate(topic)?;
    let path = format!("sailfish/logs/{}/{}", topic.name, topic.curr_log_file);
    let path = Path::new(&path);
    let file = OpenOptions::new()
                    .read(true)
                    .append(true)
                    .open(path)?;


    // Greater than or equal to 1GB
    if file.metadata()?.len() >= 1000000000 {
        topic.bump()?;

        // Always have curr + next file
        let _ = create_topic_file(&topic.name, (topic.curr_log_file + 1) as usize)?;

        let file = get_latest_topic_file(&topic.name)?;
        return Ok(file);
    }

    return Ok(file);
}

fn get_topic_file(topic: &str, file_id: &u64) -> Result<File> {
    let path = format!("sailfish/logs/{}/{}", topic, file_id);
    let path = Path::new(&path);
    let file = OpenOptions::new()
                    .read(true)
                    .open(path)?;
    return Ok(file);
}

pub fn topic_exists(topic: &str) -> bool {
    let path = format!("sailfish/logs/{}", topic);
    let path = Path::new(&path);
    return path.exists();
}

fn create_configs_dir() -> Result<()> {
    let path = Path::new("sailfish/configs");
    if !path.exists() {
        fs::create_dir_all(path)?;
    }
    return Ok(());
}

pub fn add_topic_to_config(topic: &Topic) -> Result<()> {
    create_configs_dir()?;
    let path = Path::new("sailfish/configs/topics");
    let file = OpenOptions::new()
                .append(true)
                .open(path)?;

    let mut writer = BufWriter::new(&file);
    writer.seek(SeekFrom::End(0))?;

    // Write topic name to file
    let topic_bytes = topic.name.as_bytes();
    let topic_length = topic_bytes.len() as u64;
    writer.write_all(&topic_length.to_be_bytes())?; // 8 bytes
    writer.write_all(topic_bytes)?;

    // Write file info (8 bytes ea)
    writer.write_all(&topic.first_log_file.to_be_bytes())?;
    writer.write_all(&topic.curr_log_file.to_be_bytes())?;

    writer.flush()?;

    return Ok(());
}

pub fn update_topic_in_config(topic: &Topic) -> Result<()> {
    create_configs_dir()?;
    let path = Path::new("sailfish/configs/topics");
    let file = OpenOptions::new()
                .write(true)
                .open(path)?;

    let name_length = topic.name.as_bytes().len() as u64;
    let mut writer = BufWriter::new(&file);

    // Skip to offset + name len & name value
    writer.seek(SeekFrom::Start(topic.offset + 8 + name_length))?;

    // Write file info (8 bytes ea)
    writer.write_all(&topic.first_log_file.to_be_bytes())?;
    writer.write_all(&topic.curr_log_file.to_be_bytes())?;

    writer.flush()?;

    return Ok(());
}

pub fn get_topic_from_config(topic: &mut Topic) -> Result<()> {
    let path = Path::new("sailfish/configs/topics");
    let file = OpenOptions::new()
                .read(true)
                .open(path)?;

    let mut reader = BufReader::new(&file);
    reader.seek(SeekFrom::Start(0))?;

    let mut bytes_read:u64 = 0;

    loop {
        // Read & parse name length from buffer (8 bytes)
        let mut name_length_buffer = [0u8; 8];
        reader.read_exact(&mut name_length_buffer)?;
        let name_length:u64 = u64::from_be_bytes(name_length_buffer);

        // Read & parse name from buffer (??? bytes)
        let mut name_buffer:Vec<u8> = vec![0u8; name_length as usize];
        reader.read_exact(&mut name_buffer[..])?;
        let name = std::str::from_utf8(&name_buffer).unwrap_or("failed");

        if name == &topic.name{
            // Read & parse first log file
            let mut first_log_buffer = [0u8; 8];
            reader.read_exact(&mut first_log_buffer)?;
            topic.first_log_file = u64::from_be_bytes(first_log_buffer);

            // Read & parse current log file
            let mut curr_log_buffer = [0u8; 8];
            reader.read_exact(&mut curr_log_buffer)?;
            topic.curr_log_file = u64::from_be_bytes(curr_log_buffer);

            topic.offset = bytes_read;
            
            break;
        }

        // Skip the next 16 bytes (2x 8 byte file info)
        reader.seek(SeekFrom::Current(16))?;

        bytes_read += 8 + name_length + 16;
    }
    
    return Ok(());
}

pub fn delete_topic(topic: &Topic) -> Result<()> {
    let path = Path::new("sailfish/configs/topics");
    let file = OpenOptions::new()
                .read(true)
                .open(path)?;

    let mut reader = BufReader::new(&file);
    reader.seek(SeekFrom::Start(0))?;

    let mut bytes_read = 0;

    loop {
        // Read & parse name length from buffer (8 bytes)
        let mut name_length_buffer = [0u8; 8];
        reader.read_exact(&mut name_length_buffer)?;
        let name_length:u64 = u64::from_be_bytes(name_length_buffer);

        // Read & parse name from buffer (??? bytes)
        let mut name_buffer:Vec<u8> = vec![0u8; name_length as usize];
        reader.read_exact(&mut name_buffer[..])?;
        let name = std::str::from_utf8(&name_buffer).unwrap_or("failed");

        // Calc total bytes for this topic
        let topic_bytes:i64 = 16 + 8 + name_length as i64;

        if name == &topic.name{
            let mut start_buffer:Vec<u8> = vec![0; bytes_read];

            // Read everything from start until topic into buffer
            reader.seek(SeekFrom::Start(0))?;
            reader.read_exact(&mut start_buffer)?;

            // Skip dead (this) topic
            reader.seek(SeekFrom::Current(topic_bytes))?;

            let mut end_buffer:Vec<u8> = vec![];
            reader.read_to_end(&mut end_buffer)?;

            // Write buffers
            let mut writer = BufWriter::new(&file);
            writer.seek(SeekFrom::Start(0))?;
            writer.write_all(&mut start_buffer[..])?;
            writer.write_all(&mut end_buffer[..])?;

            // Truncate file to correct size
            file.set_len(start_buffer.len() as u64 + end_buffer.len() as u64)?;

            break;
        }

        // Skip the next 16 bytes (2x 8 byte file info)
        reader.seek(SeekFrom::Current(16))?;

        bytes_read += topic_bytes as usize;
    }

    return Ok(());
}

pub fn list_topics() -> Result<()> {
    let path = Path::new("sailfish/configs/topics");
    let file = OpenOptions::new()
                .read(true)
                .open(path)?;

    let mut reader = BufReader::new(&file);
    reader.seek(SeekFrom::Start(0))?;

    let total_bytes = file.metadata()?.len();
    let mut bytes_read = 0;

    loop {
        if bytes_read == total_bytes {
            break;
        }

        // Read & parse name length from buffer (8 bytes)
        let mut name_length_buffer = [0u8; 8];
        reader.read_exact(&mut name_length_buffer)?;
        let name_length:u64 = u64::from_be_bytes(name_length_buffer);

        // Read & parse name from buffer (??? bytes)
        let mut name_buffer:Vec<u8> = vec![0u8; name_length as usize];
        reader.read_exact(&mut name_buffer[..])?;
        let name = std::str::from_utf8(&name_buffer).unwrap_or("failed");
        let name = String::from(name);

        // Read & parse first log file
        let mut first_log_buffer = [0u8; 8];
        reader.read_exact(&mut first_log_buffer)?;
        let first_log_file = u64::from_be_bytes(first_log_buffer);

        // Read & parse current log file
        let mut curr_log_buffer = [0u8; 8];
        reader.read_exact(&mut curr_log_buffer)?;
        let curr_log_file = u64::from_be_bytes(curr_log_buffer);

        let topic = Topic{
            name,
            first_log_file,
            curr_log_file,
            offset: bytes_read,
        };
        println!("{}", topic);

        bytes_read += 8 + name_length + 16;
    }

    return Ok(());
}

pub fn write(topic: &str, content: &[u8]) -> Result<()> {

    let content_length = content.len() as u64;

    //let eid = generate_key();

    let capacity = 8 + content_length as usize;

    let file = get_latest_topic_file(topic)?;
    let mut writer = BufWriter::with_capacity(capacity, file);
    writer.seek(SeekFrom::End(0))?;

    // 36 bytes
    //writer.write_all(&eid.as_bytes())?;
    writer.write_all(&content_length.to_be_bytes())?;
    writer.write_all(&content)?;

    writer.flush()?;

    return Ok(());
}

pub fn read(consumer: &mut Consumer) -> Result<Event> {

    let mut file = get_topic_file(&consumer.topic, &consumer.log_file)?;

    if consumer.log_offset == file.metadata()?.len() {
        let topic = Topic::hydrate(&consumer.topic)?;
        if topic.curr_log_file != consumer.log_file {
            consumer.log_offset = 0;
            consumer.log_file += 1;
            file = get_topic_file(&consumer.topic, &consumer.log_file)?;
        } else {
            return Err(anyhow!("EOF"));
        }
    }

    let mut reader = BufReader::new(&file);
    reader.seek(SeekFrom::Start(consumer.log_offset))?;

    //let mut eid_buffer:Vec<u8> = vec![0u8; 36];
    //reader.read_exact(&mut eid_buffer)?;
    //let eid = String::from_utf8(eid_buffer)?;

    let mut content_length_buffer = [0u8; 8];
    reader.read_exact(&mut content_length_buffer)?;
    let content_length = u64::from_be_bytes(content_length_buffer);

    let mut content_buffer:Vec<u8> = vec![0u8; content_length as usize];
    reader.read_exact(&mut content_buffer)?;

    consumer.log_offset += 8 + content_length;

    let event = Event {
        eid: format!("{}-{}", consumer.log_offset, consumer.log_file),
        content: content_buffer,
    };
    
    return Ok(event);
}

pub fn delete_old_logs(curr_file: &u64, topic: &str) -> Result<()> {
    let mut current_file:u64 = 0;
    loop {
        if &current_file == curr_file {
            break;
        }
        let path = format!("sailfish/logs/{}/{}", topic, current_file);
        let path = Path::new(&path);
        if path.exists() {
            fs::remove_file(path)?;
        } else {
            break;
        }
        current_file += 1;
    }
    return Ok(());
}
