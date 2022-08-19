use feoDB::DB;
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

fn main() -> Result<(), Error> {
    let mut db = DB::new()?;

    Ok(())
}





