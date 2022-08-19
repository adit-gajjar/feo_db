use std::collections::BTreeMap;
use std::vec::Vec;
use std::fs::{File, OpenOptions, copy, remove_file, read_dir, metadata};
use std::io::{Write, Error, SeekFrom};
use serde_json::Value;
use std::mem;
use std::str;
use std::io::prelude::*;
use serde_json::json;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};


const MAX_SEGMENT_SIZE: u64 = 100 * 64;
const MEM_TABLE_MAX_SIZE: u64 = 1000;

struct Config {
    main_segment_path: String,
    mem_table_max_size: u64
}

struct Segment {
    segment_path: String,
    index: BTreeMap<u64, u64>,
    segment_size: u64,
    disk_time: SystemTime
}

pub struct DB {
    // key to byte index
    mem_table: BTreeMap<u64, String>,
    mem_table_size: u64,
    index: BTreeMap<u64, u64>,
    config: Config,
    main_segment_size: u64,
    segments: Vec<Segment>
}


fn create_index_from_segment(segment_file_path: &String) -> Result<(u64, BTreeMap<u64, u64>), Error> {
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

    Ok((byte_index, index))
}

impl DB {
    pub fn new() -> Result<DB, Error> {

        let mut db = DB {
            index: BTreeMap::new(),
             mem_table: BTreeMap::new(),
            config: Config {
                main_segment_path: String::from("main_segment.db"),
                mem_table_max_size: MEM_TABLE_MAX_SIZE
            },
            main_segment_size: 0,
            mem_table_size: 0,
            segments: Vec::new()
        };

        // if main_segment.db exists re-create the index and main_segment_size
        if Path::new("main_segment.db").exists() {
            // try read the key and the size of the item
            let (main_segment_size, index) = create_index_from_segment(&String::from("main_segment.db"))?;
            db.index = index;
            db.main_segment_size = main_segment_size;
        }
        // recover the rest of the segments.
        db.deserialize_segments()?;

        Ok(db)
    }

    pub fn insert(&mut self, key: u64, value: String) -> Result<(), Error> {
        let value_len = value.len() as u64;
        if value_len + self.mem_table_size > MEM_TABLE_MAX_SIZE {
            self.write_mem_table_to_segment()?;
        }
        self.mem_table.insert(key, value);
        self.mem_table_size += value_len;
        Ok(())
    }

    pub fn find_by_id(&self, key:&u64) -> Result<Value, Error> {
        // first check mem_table
        let value = self.mem_table.get(key);
        if let Some(value) = value {
            let json_value = serde_json::from_str(value)?;
            return Ok(json_value);
        }

        // look up byte index in index in main segment
        let byte_index = self.index.get(key);

        if let Some(byte_index) = byte_index {
            let json_value = self.read_document_from_segment(&self.config.main_segment_path, *byte_index)?;
            return Ok(json_value);
        }

        // search in reverse as last added segment has more recent data.
        for segment in self.segments.iter().rev() {
            let byte_index = segment.index.get(key);
            if let Some(byte_index) = byte_index {
                let json_value = self.read_document_from_segment(&segment.segment_path, *byte_index)?;
                return Ok(json_value);
            }
        }

        Ok(json!(null))
    }

    fn write_mem_table_to_segment(&mut self) -> Result<(), Error> {
        // for each key in the mem_table write it to the main segment.
        // update the index of that segment as well.
        if self.mem_table_size + self.main_segment_size > MAX_SEGMENT_SIZE {
            self.new_main_segment()?;
        }

        let mut main_segment = OpenOptions::new()
        .write(true)
        .append(true)
        .open(&(self.config.main_segment_path))
        .unwrap();
        let mut byte_index_updates = Vec::new();
        main_segment.seek(SeekFrom::Start(self.main_segment_size))?;
        // TODO: batch all these writes and write it as one chunk
        for (key, value) in self.mem_table.iter() {
            // write to new segment
            let size_in_bytes = value.len() as u64;
            main_segment.write(&key.to_ne_bytes())?;
            main_segment.write(&size_in_bytes.to_ne_bytes())?;
            write!(main_segment, "{}",  value)?;
            // update index
            byte_index_updates.push((*key, self.main_segment_size));
            self.main_segment_size += (2 * mem::size_of::<u64> as u64) + size_in_bytes;
            byte_index_updates.push((*key, self.main_segment_size));
        }

        for (key, byte_index) in byte_index_updates.iter() {
            self.index.insert(*key, *byte_index);
        }

        self.mem_table.clear();
        self.mem_table_size = 0;
        Ok(())
    }

    fn read_document_from_segment(&self, segment_path: &String, byte_index: u64) -> Result<Value, Error> {
        // file seek and return value
        let mut segment_file = File::open(segment_path)?;
        let mut buffer = [0; mem::size_of::<u64>()];

        segment_file.seek(SeekFrom::Start(byte_index + 8))?;
        // read in JSON document size
        segment_file.read(&mut buffer[..])?;
        let size_of_json = u64::from_ne_bytes(buffer);
        // read in JSON document
        let mut value_buffer = vec![0_u8; size_of_json as usize];
        segment_file.read(&mut value_buffer)?;
        let stringified_json = str::from_utf8(&value_buffer);
        // return JSON value
        if let Ok(stringified_json) = stringified_json {
            let json_value : Value = serde_json::from_str(stringified_json)?;
            // for debug.
            print!("{}", json_value);
            return Ok(json_value);
        }

        return Ok(json!(null));
    }

    fn new_main_segment(&mut self) -> Result<(), Error> {
        // clone current main_segment into new file.
        let time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let new_segment_file_name = format!("./segments/segment_{}.db", time.as_secs());
        File::create(&new_segment_file_name)?;
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

    fn deserialize_segments(&mut self) -> Result<(), Error> {
        // read the segments dir.
        // recover all the segments and then sort them by time created.
        let paths = read_dir("./segments/").unwrap();

        for path in paths {
            let path_string = path.unwrap().path().display().to_string();
            // recover the segment index.
            let (size, index) = create_index_from_segment(&path_string)?;
            let date_created = metadata(&path_string)?.created()?;
            let segment = Segment {
                segment_path: path_string.clone(),
                index: index,
                segment_size: size,
                disk_time: date_created
            };
            self.segments.push(segment);
        }

        self.segments.sort_by(|a, b| (a.disk_time.partial_cmp(&b.disk_time).unwrap()));

        Ok(())
    }

    // Size Tiered Compaction Strategy (STCS)
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

