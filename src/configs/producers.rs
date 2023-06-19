#![allow(unused)]

use std::{path::Path, fs::{self, File, OpenOptions}, io::{BufWriter, Write, Seek, SeekFrom, BufReader, Read}};
use crate::subjects::{producer::Producer, keys::generate_key};
use anyhow::Result;

fn create_configs_dir() -> Result<()>{
    let path = Path::new("sailfish/configs");
    if !path.exists() {
        fs::create_dir_all(path)?;
    }
    return Ok(());
}

pub fn create_producers_file() -> Result<File> {
    let path = format!("sailfish/configs/producers");
    let path = Path::new(&path);
    let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(path)?;
    return Ok(file);
}

pub fn producers_exists() -> bool {
    let path = format!("sailfish/configs/producers");
    let path = Path::new(&path);
    return path.exists();
}

pub fn add_producer_to_config(producer: &mut Producer) -> Result<()> {
    create_configs_dir()?;
    let path = Path::new("sailfish/configs/producers");
    let file =  OpenOptions::new()
                    .append(true)
                    .open(path)?;

    let mut writer = BufWriter::new(&file);
    producer.offset = writer.seek(SeekFrom::End(0))?;
    writer.seek(SeekFrom::Start(producer.offset))?;

    // We don't need to track the length of the key because it is a UUIDv4
    // It will always be 36 bytes
    writer.write_all(producer.key.as_bytes())?;

    // Write topic & string length
    let topic_bytes = producer.topic.as_bytes();
    let topic_length = topic_bytes.len() as u64;
    writer.write_all(&topic_length.to_be_bytes())?;
    writer.write_all(topic_bytes)?;

    writer.flush()?;

    return Ok(());
}

pub fn get_producer(offset: u64) -> Result<Producer> {
    let path = Path::new("sailfish/configs/producers");
    let file =  OpenOptions::new()
                    .read(true)
                    .open(path)?;

    let mut reader = BufReader::new(&file);
    reader.seek(SeekFrom::Start(offset))?;

    // Read key
    let mut key_buffer = [0u8; 36];
    reader.read_exact(&mut key_buffer)?;
    let key = std::str::from_utf8(&key_buffer).unwrap();

    // Read topic length
    let mut topic_length_buffer = [0u8; 8];
    reader.read_exact(&mut topic_length_buffer)?;
    let topic_length:u64 = u64::from_be_bytes(topic_length_buffer);

    // Read topic
    let mut topic_buffer:Vec<u8> = vec![0; topic_length as usize];
    reader.read_exact(&mut topic_buffer[..])?;
    let topic = std::str::from_utf8(&topic_buffer).unwrap();

    let producer = Producer{
        topic: topic.to_owned(),
        offset,
        key: key.to_owned(),
    };

    return Ok(producer);
}

pub fn delete_producer(producer: &Producer) -> Result<()> {
    let path = Path::new("sailfish/configs/producers");
    let file =  OpenOptions::new()
                    .write(true)
                    .open(path)?;

    let mut writer = BufWriter::with_capacity(36, &file);
    writer.seek(SeekFrom::Start(producer.offset))?;

    // Overwrite key with null bytes (36)
    let null_bytes = [0u8; 36];
    writer.write_all(&null_bytes)?;

    writer.flush()?;

    return Ok(());
}

pub fn reroll_producer_key(producer: &Producer) -> Result<String> {
    let path = Path::new("sailfish/configs/producers");
    let file =  OpenOptions::new()
                    .write(true)
                    .open(path)?;

    let mut writer = BufWriter::with_capacity(36, &file);
    writer.seek(SeekFrom::Start(producer.offset))?;

    let new_key = generate_key();
    writer.write_all(new_key.as_bytes())?;

    return Ok(new_key);
}

pub fn list_producers() -> Result<()> {
    let path = Path::new("sailfish/configs/producers");
    let file =  OpenOptions::new()
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
        let key = std::str::from_utf8(&key_buffer).unwrap_or("");
        let key = String::from(key);

        // Read topic length
        let mut topic_length_buffer = [0u8; 8];
        reader.read_exact(&mut topic_length_buffer)?;
        let topic_length = u64::from_be_bytes(topic_length_buffer);

        // Read topic
        let mut topic_buffer:Vec<u8> = vec![0; topic_length as usize];
        reader.read_exact(&mut topic_buffer[..])?;
        let topic = std::str::from_utf8(&topic_buffer).unwrap();

        if !key.escape_default().to_string().contains("\\u{0}") {
            let producer = Producer{
                topic: topic.to_owned(),
                offset: bytes_read,
                key: key.to_owned(),
            };
            println!("{}", producer);
        }

        bytes_read += 36 + 8 + topic_length;
    }

    return Ok(());
}
