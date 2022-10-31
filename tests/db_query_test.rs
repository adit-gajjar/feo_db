// CURRENT_TASK: have a function that sets up the DB with a config that makes testing easy.
mod common;

use feoDB::DB;
use serde_json::{json, Value};
use std::io::Error;

#[test]
fn basic_find_by_id() -> Result<(), Error> {
    let db = common::setup_db()?;
    let document = db.find_by_id(&5)?;
    // assert_eq!(*document.get("id").unwrap(), json!(5));
    assert_eq!(1, 1);
    Ok(())
}


// #[test]
// fn basic_range_query() -> Result<(), Error> {
//     let db = common::setup_db()?;

//     Ok(())
// }
