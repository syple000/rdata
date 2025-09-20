use crate::error::{JsonError, Result};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::fs::File;

pub fn dump<T: Serialize>(value: &T, filepath: &str) -> Result<()> {
    let file = File::create(filepath).map_err(|e| JsonError::IOError(e))?;
    serde_json::to_writer_pretty(file, value).map_err(|e| JsonError::SerdeError(e))?;
    Ok(())
}

pub fn dumps<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string_pretty(value).map_err(|e| JsonError::SerdeError(e))
}

pub fn load<T: DeserializeOwned>(filepath: &str) -> Result<T> {
    let file = File::open(filepath).map_err(|e| JsonError::IOError(e))?;
    let data = serde_json::from_reader(file).map_err(|e| JsonError::SerdeError(e))?;
    Ok(data)
}

pub fn loads<T: DeserializeOwned>(s: &str) -> Result<T> {
    let data = serde_json::from_str(s).map_err(|e| JsonError::SerdeError(e))?;
    Ok(data)
}
