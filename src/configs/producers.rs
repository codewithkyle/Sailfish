use std::{path::Path, fs::{self, File, OpenOptions}, io::{BufWriter, Write, Seek, SeekFrom}};

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
    // It will always be 16 bytes
    writer.write_all(producer.key.as_bytes())?;

    let topic_bytes = producer.topic.as_bytes();
    let topic_length = topic_bytes.len() as u64;
    writer.write_all(&topic_length.to_le_bytes())?;
    writer.write_all(topic_bytes)?;

    // Write 16 bytes of log file data (8 bytes ea)
    writer.write_all(&producer.log_file.to_le_bytes())?;
    writer.write_all(&producer.log_offset.to_le_bytes())?;

    writer.flush()?;

    return Ok(());
}
