use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Write, Error, SeekFrom};
use serde_json::Value;
use std::mem;
use std::str;
use std::io::prelude::*;
use serde_json::json;

struct Config {
    main_segment_path: String
    segment_size: 1000 * 64
}

struct DB {
    // key to byte index
    index: BTreeMap<u64, u64>,
    config: Config,
    main_segment_size: u64
}

impl DB {
    fn insert(&mut self, key: u64, value: String) -> Result<(), Error> {
        let mut main_segment = OpenOptions::new()
              .write(true)
              .append(true)
              .open(&(self.config.main_segment_path))
              .unwrap();

        let size_in_bytes = value.len();

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

                return Ok(json_value);
            }
        }

        return Ok(json!(null));
    }
}

fn main() -> Result<(), Error> {
    let mut db = DB {
        index: BTreeMap::new(),
        config: Config {
            main_segment_path: String::from("main_segment.db")
        },
        main_segment_size: 0
    };

    Ok(())
}




