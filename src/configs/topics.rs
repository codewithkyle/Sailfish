use std::{path::Path, fs::{self, File, OpenOptions}, io::{BufWriter, Write, Seek, SeekFrom, BufReader, Read}};

use crate::subjects::topic::Topic;

pub fn create_topic_dir(topic: &str) {
    let path = format!("sailfish/logs/{}", topic);
    let path = Path::new(&path);
    if !path.exists() {
        fs::create_dir_all(path).unwrap_or_else(|_| {
            eprintln!("Failed to create {} directory.", topic);
            std::process::exit(1);
        });
    }
    create_topic_file(topic, 0);
}

fn create_topic_file(topic: &str, file: usize) {
    let path = format!("sailfish/logs/{}/{}", topic, file);
    let path = Path::new(&path);
    if !path.exists() {
        File::create(path).unwrap_or_else(|_| {
            eprintln!("Failed to create {}.", topic);
            std::process::exit(1);
        });
    }
}

pub fn topic_exists(topic: &str) -> bool {
    let path = format!("sailfish/logs/{}", topic);
    let path = Path::new(&path);
    return path.exists();
}

fn create_configs_dir(){
    let path = Path::new("sailfish/configs");
    if !path.exists() {
        fs::create_dir_all(path).unwrap_or_else(|_| {
            eprintln!("Failed to create configs directory.");
            std::process::exit(1);
        });
    }
}

fn get_or_create_topics_file() -> File {
    let path = Path::new("sailfish/configs/topics");
    return OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap_or_else(|_| {
                eprintln!("Failed to open topic config file.");
                std::process::exit(1);
            });
}

pub fn add_topic_to_config(topic: &Topic) -> std::io::Result<()> {
    create_configs_dir();
    let file = get_or_create_topics_file();

    let mut writer = BufWriter::new(&file);
    writer.seek(SeekFrom::End(0))?;

    // Write topic name to file
    let topic_bytes = topic.name.as_bytes();
    let topic_length = topic_bytes.len() as u64;
    writer.write_all(&topic_length.to_le_bytes())?; // 8 bytes
    writer.write_all(topic_bytes)?;

    // Write file info (8 bytes ea)
    writer.write_all(&topic.first_log_file.to_le_bytes())?;
    writer.write_all(&topic.curr_log_file.to_le_bytes())?;

    writer.flush()?;

    return Ok(());
}

pub fn get_topic_from_config(topic: &mut Topic) -> std::io::Result<()> {
    let file = get_or_create_topics_file();

    let mut reader = BufReader::new(&file);

    loop {
        // Read & parse name length from buffer (8 bytes)
        let mut name_length_buffer = [0u8; 8];
        reader.read_exact(&mut name_length_buffer)?;

        // EOF
        if name_length_buffer.len() == 0 {
            todo!("Replace with anyhow error");
        }

        let name_length:u64 = u64::from_le_bytes(name_length_buffer);

        // Read & parse name from buffer (??? bytes)
        let mut name_buffer:Vec<u8> = vec![0u8; name_length as usize];
        reader.read_exact(&mut name_buffer[..])?;
        let name = std::str::from_utf8(&name_buffer).unwrap_or("failed");

        if name == &topic.name{
            // Read & parse first log file
            let mut first_log_buffer = [0u8; 8];
            reader.read_exact(&mut first_log_buffer)?;
            topic.first_log_file = u64::from_le_bytes(first_log_buffer);

            // Read & parse current log file
            let mut curr_log_buffer = [0u8; 8];
            reader.read_exact(&mut curr_log_buffer)?;
            topic.curr_log_file = u64::from_le_bytes(curr_log_buffer);
            
            break;
        }

        // Skip the next 16 bytes (2x 8 byte file info)
        reader.seek_relative(16)?;
    }
    
    return Ok(());
}
