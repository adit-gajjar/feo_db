use std::collections::BTreeMap;
use std::vec::Vec;
use std::fs::{File, OpenOptions, copy, remove_file};
use std::io::{Write, Error, SeekFrom};
use serde_json::Value;
use std::mem;
use std::str;
use std::fmt;
use std::io::prelude::*;
use serde_json::json;
use std::path::Path;
use std::time::SystemTime;

const MAX_SEGMENT_SIZE: u64 = 1000;
struct Config {
    main_segment_path: String,
}

struct Segment {
    segment_path: String,
    index: BTreeMap<u64, u64>,
    segment_size: u64,
    disk_time: SystemTime
}

struct DB {
    // key to byte index
    index: BTreeMap<u64, u64>,
    config: Config,
    main_segment_size: u64,
    segments: Vec<Segment>
}


fn create_index_from_segment(segment_file_path: String) -> Result<(u64, BTreeMap<u64, u64>), Error> {
    let mut index = BTreeMap::new();
    let mut segment_file = File::open(&segment_file_path)?;
    let mut buffer = [0; mem::size_of::<u64>()];
    let mut byte_index: u64 = 0;
        // attempt to read the first key
    let mut bytes_read = segment_file.read(&mut buffer[..])?;
    while bytes_read > 0 {
        if bytes_read != mem::size_of::<u64>() {
            panic!("Incorrect Segment format");
        }
        let key = u64::from_ne_bytes(buffer);
        bytes_read = segment_file.read(&mut buffer[..])?;
        if bytes_read != mem::size_of::<u64>() {
            panic!("Incorrect Segment format");
        }
        let value_size = u64::from_ne_bytes(buffer);
        index.insert(key, byte_index);
        byte_index += (2 * (mem::size_of::<u64>() as u64)) + (value_size as u64);
        // file seek to next record and read key.
        segment_file.seek(SeekFrom::Start(byte_index))?;
        bytes_read = segment_file.read(&mut buffer[..])?;
    };
    print!("The main segment size is : {}\n", byte_index);
    Ok((byte_index, index))
}

impl DB {
    fn insert(&mut self, key: u64, value: String) -> Result<(), Error> {
        // check whether their is enough space.
        // TODO: throw error if value is too large.
        let size_in_bytes = value.len();
        // store record in new segment.
        if (size_in_bytes as u64) + self.main_segment_size > MAX_SEGMENT_SIZE {
            self.new_main_segment()?;
        }

        let mut main_segment = OpenOptions::new()
        .write(true)
        .append(true)
        .open(&(self.config.main_segment_path))
        .unwrap();
        main_segment.write(&key.to_ne_bytes())?;
        main_segment.write(&size_in_bytes.to_ne_bytes())?;
        write!(main_segment, "{}",  value)?;

        // update the index
        self.index.insert(key, self.main_segment_size);
        self.main_segment_size += 16 + (size_in_bytes as u64);

        Ok(())
    }

    fn find_by_id(&self, key:&u64) -> Result<Value, Error> {
        // look up byte index in index
        let byte_index = self.index.get(key);

        if let Some(byte_index) = byte_index {
            // file seek and return value
            let mut f = File::open(&(self.config.main_segment_path))?;
            let mut buffer = [0; mem::size_of::<u64>()];

            f.seek(SeekFrom::Start(byte_index + 8))?;
            // read in JSON document size
            f.read(&mut buffer[..])?;
            let size_of_json = u64::from_ne_bytes(buffer);
            // read in JSON document
            let mut value_buffer = vec![0_u8; size_of_json as usize];
            f.read(&mut value_buffer)?;
            let stringified_json = str::from_utf8(&value_buffer);
            // return JSON value
            if let Ok(stringified_json) = stringified_json {
                let json_value : Value = serde_json::from_str(stringified_json)?;
                // for debug.
                print!("{}", json_value);
                return Ok(json_value);
            }
        }

        return Ok(json!(null));
    }

    fn new_main_segment(&mut self) -> Result<(), Error> {
        // clone current main_segment into new file.
        let new_segment_file_name = format!("segment_{}.db", self.segments.len());
        let mut new_segment_file = File::create(&new_segment_file_name)?;
        copy(&(self.config.main_segment_path), &new_segment_file_name)?;
        // create new Segment Struct
        let new_segment_struct = Segment {
            segment_path: String::from(new_segment_file_name),
            index: self.index.clone(),
            segment_size: self.main_segment_size,
            disk_time: SystemTime::now()
        };
        // add new segment struct to vec
        self.segments.push(new_segment_struct);
        // clear current index and main_segment.
        self.index.clear();
        File::create(&(self.config.main_segment_path))?;

        Ok(())
    }

    fn new() -> Result<DB, Error> {
        // if main_segment.db exists re-create the index and main_segment_size
        if Path::new("main_segment.db").exists() {
            // try read the key and the size of the item
            let (main_segment_size, index) = create_index_from_segment(String::from("main_segment.db"))?;
            return Ok(DB {
                index: index,
                config: Config {
                    main_segment_path: String::from("main_segment.db")
                },
                main_segment_size: main_segment_size,
                segments: Vec::new(),
            });
        }
        
        Ok(DB {
            index: BTreeMap::new(),
            config: Config {
                main_segment_path: String::from("main_segment.db")
            },
            main_segment_size: 0,
            segments: Vec::new(),
        })
    }

    // Size Tiered Compaction Strategy (STCS)
    // removes duplicate keys.
    fn compact_segment(&self, segment: &mut Segment) -> Result<(), Error> {
        // open of file to read the json values.
        let mut curr_segment = File::open(&(self.config.main_segment_path))?;
        let mut bytes_written: u64 = 0;
        // temp file to write to.
        let new_segment_file_name = "temp_compaction_segment.db";
        File::create(&new_segment_file_name)?;
        let mut new_segment_file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&(self.config.main_segment_path))
            .unwrap();
        let mut byte_index_updates = Vec::new();
        for (key, value) in segment.index.iter() {
            curr_segment.seek(SeekFrom::Start(*value + (mem::size_of::<u64>() as u64)))?;
            let mut buffer = [0; mem::size_of::<u64>()];
            // read the document size
            curr_segment.read(&mut buffer[..])?;
            let size_of_json = u64::from_ne_bytes(buffer);
            // read the json value
            let mut value_buffer = vec![0_u8; size_of_json as usize];
            curr_segment.read(&mut value_buffer)?;
            // write to new segment
            new_segment_file.write(&key.to_ne_bytes())?;
            new_segment_file.write(&bytes_written.to_ne_bytes())?;
            new_segment_file.write(&value_buffer)?;
            // update index
            byte_index_updates.push((*key, bytes_written));
            bytes_written += (2 * mem::size_of::<u64> as u64) + size_of_json;
        }
        // update indexes
        for (key, byte_index) in byte_index_updates.iter() {
            segment.index.insert(*key, *byte_index);
        }
        // move compacted data from temp file to main file.
        copy(new_segment_file_name, segment.segment_path.clone())?;
        remove_file(new_segment_file_name)?;

        Ok(())
    }

}

fn get_json_data(id: u64) -> Value {
    json!({
        "name": "John Doe",
        "id": id,
        "phones": [
            "+44 1234567",
            "+44 2345678"
        ]
    })
}

fn main() -> Result<(), Error> {
    let mut db = DB::new()?;

    let sample_input1 = r#"
        {
            "id": 1,
            "name": "John Doe",
            "age": 47,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;

    db.insert(123, String::from(sample_input1));
    db.find_by_id(&123);

    Ok(())
}