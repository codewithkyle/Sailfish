use std::{path::Path, fs::{self, File, OpenOptions}, io::{BufWriter, Write, Seek, SeekFrom, BufReader, Read}};

use crate::subjects::producer::{Producer};

fn create_configs_dir(){
    let path = Path::new("sailfish/configs");
    if !path.exists() {
        fs::create_dir_all(path).unwrap_or_else(|_| {
            eprintln!("Failed to create configs directory.");
            std::process::exit(1);
        });
    }
}

fn get_or_create_producers_file() -> File {
    let path = Path::new("sailfish/configs/producers");
    return OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap_or_else(|_| {
                eprintln!("Failed to open producer config file.");
                std::process::exit(1);
            });
}

pub fn add_producer_to_config(producer: &mut Producer) -> std::io::Result<()> {
    create_configs_dir();
    let file = get_or_create_producers_file();

    let mut writer = BufWriter::new(&file);
    producer.offset = writer.seek(SeekFrom::End(0))?;
    writer.seek(SeekFrom::Start(producer.offset))?;

    // We don't need to track the length of the key because it is a UUIDv4
    // It will always be 36 bytes
    writer.write_all(producer.key.as_bytes())?;

    // Write topic & string length
    let topic_bytes = producer.topic.as_bytes();
    let topic_length = topic_bytes.len() as u64;
    writer.write_all(&topic_length.to_le_bytes())?;
    writer.write_all(topic_bytes)?;

    writer.flush()?;

    return Ok(());
}

pub fn get_producer(offset: u64) -> Result<Producer, std::io::Error> {
    let file = get_or_create_producers_file();

    let mut reader = BufReader::new(&file);
    reader.seek(SeekFrom::Start(offset))?;

    // Read key
    let mut key_buffer = [0u8; 36];
    reader.read_exact(&mut key_buffer)?;
    let key = std::str::from_utf8(&key_buffer).unwrap();

    // Read topic length
    let mut topic_length_buffer = [0u8; 8];
    reader.read_exact(&mut topic_length_buffer)?;
    let topic_length:u64 = u64::from_le_bytes(topic_length_buffer);

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

pub fn delete_producer(producer: &Producer) -> std::io::Result<()> {
    let file = get_or_create_producers_file();

    let mut writer = BufWriter::new(&file);
    writer.seek(SeekFrom::Start(producer.offset))?;

    // Overwrite key with null bytes (36)
    let null_bytes = [0u8; 36];
    writer.write_all(&null_bytes)?;

    writer.flush()?;

    return Ok(());
}
