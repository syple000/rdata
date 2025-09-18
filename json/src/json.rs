use crate::error::{JsonError, Result};
use std::fs::File;

pub fn dump<T: serde::Serialize>(value: &T, filepath: &str) -> Result<()> {
    let file = File::create(filepath).map_err(|e| JsonError::FileCreationError(e.to_string()))?;
    serde_json::to_writer_pretty(file, value)
        .map_err(|e| JsonError::FileWriteError(e.to_string()))?;
    Ok(())
}

pub fn dumps<T: serde::Serialize>(value: &T) -> Result<String> {
    serde_json::to_string_pretty(value).map_err(|e| JsonError::SerializationError(e.to_string()))
}
