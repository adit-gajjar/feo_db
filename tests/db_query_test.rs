mod common;

use feoDB::DB;
use serde_json::{json, Value};
use std::io::Error;
use std::vec::Vec;

#[test]
fn basic_find_by_id() -> Result<(), Error> {
    let db = common::setup_db()?;
    let document = db.find_by_id(&5)?;
    assert_eq!(*document.get("id").unwrap(), json!(5));
    Ok(())
}

#[test]
fn basic_find_by_range_id() -> Result<(), Error> {
    let db = common::setup_db()?;
    let documents = db.find_by_id_range(&11, &100)?;
    assert_eq!(documents.len(), 90);
    Ok(())
}