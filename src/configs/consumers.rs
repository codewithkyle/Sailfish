#![allow(unused)]

use std::{path::Path, fs::{self, File, OpenOptions}, io::{BufWriter, Write, Seek, SeekFrom, BufReader, Read}};
use anyhow::Result;
use crate::subjects::{consumer::Consumer, keys::generate_key};

fn create_configs_dir() -> Result<()> {
    let path = Path::new("sailfish/configs");
    if !path.exists() {
        fs::create_dir_all(path)?;
    }
    return Ok(());
}

pub fn create_consumer_file() -> Result<File> {
    let path = format!("sailfish/configs/consumers");
    let path = Path::new(&path);
    let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(path)?;
    return Ok(file);
}

pub fn consumers_exists() -> bool {
    let path = format!("sailfish/configs/consumers");
    let path = Path::new(&path);
    return path.exists();
}

pub fn add_consumer_to_config(consumer: &mut Consumer) -> Result<()> {
    create_configs_dir()?;
    let path = Path::new("sailfish/configs/consumers");
    let file = OpenOptions::new()
                    .append(true)
                    .open(path)?;

    let mut writer = BufWriter::new(&file);
    consumer.offset = writer.seek(SeekFrom::End(0))?;
    writer.seek(SeekFrom::Start(consumer.offset))?;

    // We don't need to track the length of the key because it is a UUIDv4
    // It will always be 36 bytes
    writer.write_all(consumer.key.as_bytes())?;

    let topic_bytes = consumer.topic.as_bytes();
    let topic_length = topic_bytes.len() as u64;
    writer.write_all(&topic_length.to_be_bytes())?;
    writer.write_all(topic_bytes)?;

    // Write 16 bytes of log file data (8 bytes ea)
    writer.write_all(&consumer.log_file.to_be_bytes())?;
    writer.write_all(&consumer.log_offset.to_be_bytes())?;

    writer.flush()?;

    return Ok(());
}

pub fn update_consumer_in_config(consumer: &Consumer) -> Result<()> {
    let path = Path::new("sailfish/configs/consumers");
    let file = OpenOptions::new()
                    .write(true)
                    .open(path)?;

    let mut writer = BufWriter::new(&file);
    writer.seek(SeekFrom::Start(consumer.offset + 36 + 8 + consumer.topic.as_bytes().len() as u64))?;

    writer.write_all(&consumer.log_file.to_be_bytes())?;
    writer.write_all(&consumer.log_offset.to_be_bytes())?;

    writer.flush()?;

    return Ok(());
}

pub fn get_consumer(offset: u64) -> Result<Consumer> {
    let path = Path::new("sailfish/configs/consumers");
    let file = OpenOptions::new()
                    .read(true)
                    .open(path)?;

    let mut reader = BufReader::new(&file);
    reader.seek(SeekFrom::Start(offset))?;

    // Read key
    let mut key_buffer = [0u8; 36];
    reader.read_exact(&mut key_buffer)?;
    let key = std::str::from_utf8(&key_buffer)?;

    // Read topic length
    let mut topic_length_buffer = [0u8; 8];
    reader.read_exact(&mut topic_length_buffer)?;
    let topic_length:u64 = u64::from_be_bytes(topic_length_buffer);

    // Read topic
    let mut topic_buffer:Vec<u8> = vec![0; topic_length as usize];
    reader.read_exact(&mut topic_buffer[..])?;
    let topic = std::str::from_utf8(&topic_buffer)?;

    // Read log file
    let mut log_file_buffer = [0u8; 8];
    reader.read_exact(&mut log_file_buffer)?;
    let log_file = u64::from_be_bytes(log_file_buffer);

    // Read log file offset
    let mut log_file_offset_buffer = [0u8; 8];
    reader.read_exact(&mut log_file_offset_buffer)?;
    let log_offset = u64::from_be_bytes(log_file_offset_buffer);

    let producer = Consumer{
        topic: topic.to_owned(),
        offset,
        key: key.to_owned(),
        log_file,
        log_offset,
    };

    return Ok(producer);
}

pub fn delete_consumer(consumer: &Consumer) -> Result<()> {
    let path = Path::new("sailfish/configs/consumers");
    let file = OpenOptions::new()
                    .write(true)
                    .open(path)?;

    let mut writer = BufWriter::new(&file);
    writer.seek(SeekFrom::Start(consumer.offset))?;

    // Overwrite key with null bytes (36)
    let null_bytes = [0u8; 36];
    writer.write_all(&null_bytes)?;

    writer.flush()?;

    return Ok(());
}

pub fn reroll_consumer_key(consumer: &Consumer) -> Result<String> {
    let path = Path::new("sailfish/configs/consumers");
    let file = OpenOptions::new()
                    .write(true)
                    .open(path)?;

    let mut writer = BufWriter::new(&file);
    writer.seek(SeekFrom::Start(consumer.offset))?;


    let new_key = generate_key();
    writer.write_all(new_key.as_bytes())?;

    return Ok(new_key);
}

pub fn list_consumers() -> Result<()> {
    let path = Path::new("sailfish/configs/consumers");
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

        // Read key
        let mut key_buffer = [0u8; 36];
        reader.read_exact(&mut key_buffer)?;
        let key = std::str::from_utf8(&key_buffer)?;

        // Read topic length
        let mut topic_length_buffer = [0u8; 8];
        reader.read_exact(&mut topic_length_buffer)?;
        let topic_length:u64 = u64::from_be_bytes(topic_length_buffer);

        // Read topic
        let mut topic_buffer:Vec<u8> = vec![0; topic_length as usize];
        reader.read_exact(&mut topic_buffer[..])?;
        let topic = std::str::from_utf8(&topic_buffer)?;

        // Read log file
        let mut log_file_buffer = [0u8; 8];
        reader.read_exact(&mut log_file_buffer)?;
        let log_file = u64::from_be_bytes(log_file_buffer);

        // Read log file offset
        let mut log_file_offset_buffer = [0u8; 8];
        reader.read_exact(&mut log_file_offset_buffer)?;
        let log_offset = u64::from_be_bytes(log_file_offset_buffer);

        if !key.escape_default().to_string().contains("\\u{0}") {
            let producer = Consumer{
                topic: topic.to_owned(),
                offset: bytes_read,
                key: key.to_owned(),
                log_file,
                log_offset,
            };
            println!("{}", producer);
        }

        bytes_read += 36 + 8 + topic_length + 16;
    }

    return Ok(());
}

pub fn get_oldest_active_log_file(topic: &str) -> Result<Option<u64>> {
    let path = Path::new("sailfish/configs/consumers");
    let file = OpenOptions::new()
                    .read(true)
                    .open(path)?;

    let mut reader = BufReader::new(&file);
    reader.seek(SeekFrom::Start(0))?;

    let total_bytes = file.metadata()?.len();
    let mut bytes_read = 0;
    let mut oldest_log_file:Option<u64> = None;

    loop {
        if bytes_read == total_bytes {
            break;
        }

        let mut key_buffer = [0u8; 36];
        reader.read_exact(&mut key_buffer)?;
        let key = std::str::from_utf8(&key_buffer)?;

        // Read topic length
        let mut topic_length_buffer = [0u8; 8];
        reader.read_exact(&mut topic_length_buffer)?;
        let topic_length:u64 = u64::from_be_bytes(topic_length_buffer);

        if !key.escape_default().to_string().contains("\\u{0}") {
            let mut topic_buffer:Vec<u8> = vec![0; topic_length as usize];
            reader.read_exact(&mut topic_buffer[..])?;
            let consumer_topic = std::str::from_utf8(&topic_buffer)?;

            if topic == consumer_topic {
                let mut log_file_buffer = [0u8; 8];
                reader.read_exact(&mut log_file_buffer)?;
                let log_file = u64::from_be_bytes(log_file_buffer);

                if log_file < oldest_log_file.unwrap_or(u64::MAX) {
                    oldest_log_file = Some(log_file);
                }
            }
        } else {
            reader.seek(SeekFrom::Current(topic_length as i64));
        }

        // Skip log file offset
        reader.seek(SeekFrom::Current(8))?;

        bytes_read += 36 + 8 + topic_length + 16;
    }

    return Ok(oldest_log_file);
}
