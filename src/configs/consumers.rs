use std::{path::Path, fs::{self, File, OpenOptions}, io::{BufWriter, Write, Seek, SeekFrom}};

use crate::subjects::consumer::Consumer;

fn create_configs_dir(){
    let path = Path::new("sailfish/configs");
    if !path.exists() {
        fs::create_dir_all(path).unwrap_or_else(|_| {
            eprintln!("Failed to create configs directory.");
            std::process::exit(1);
        });
    }
}

fn get_or_create_consumers_file() -> File {
    let path = Path::new("sailfish/configs/consumers");
    return OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap_or_else(|_| {
                eprintln!("Failed to open consumer config file.");
                std::process::exit(1);
            });
}

pub fn add_consumer_to_config(consumer: &mut Consumer) -> std::io::Result<()> {
    create_configs_dir();
    let file = get_or_create_consumers_file();

    let mut writer = BufWriter::new(&file);
    consumer.offset = writer.seek(SeekFrom::End(0))?;
    writer.seek(SeekFrom::Start(consumer.offset))?;

    // We don't need to track the length of the key because it is a UUIDv4
    // It will always be 16 bytes
    writer.write_all(consumer.key.as_bytes())?;

    let topic_bytes = consumer.topic.as_bytes();
    let topic_length = topic_bytes.len() as u64;
    writer.write_all(&topic_length.to_le_bytes())?;
    writer.write_all(topic_bytes)?;

    // Write 16 bytes of log file data (8 bytes ea)
    writer.write_all(&consumer.log_file.to_le_bytes())?;
    writer.write_all(&consumer.log_offset.to_le_bytes())?;

    writer.flush()?;

    return Ok(());
}
