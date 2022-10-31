use feoDB::{DB, Config};
use serde_json::{json, Value};
use std::io::{Error};


fn get_json_data(id: u64) -> Value {
    json!({
        "name": "John Doe",
        "id": id,
        "phones": [
            "+44 1234567",
            "+44 2345678",
            "+44 2345678",
        ]
    })
}

pub fn setup_db() -> Result<DB, Error> {
    // setup code specific to your library's tests would go here
    let config = Config {
        mem_table_max_size: 1000 * 64,
        max_segment_size: 20000 * 64
    };

    let mut db = DB::create_with_config(config)?;

    for key in 1 .. 1001 {
        db.insert(key, get_json_data(key).to_string())?;
    }

    Ok(db)
}
