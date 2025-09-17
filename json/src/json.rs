use std::fs::File;

pub fn dump<T: serde::Serialize>(value: &T, filepath: &str) -> Result<(), String> {
    let file = File::create(filepath).map_err(|e| format!("Failed to create file: {}", e))?;
    serde_json::to_writer_pretty(file, value)
        .map_err(|e| format!("Failed to write JSON to file: {}", e))
}

pub fn dumps<T: serde::Serialize>(value: &T) -> Result<String, String> {
    serde_json::to_string_pretty(value)
        .map_err(|e| format!("Failed to serialize to JSON string: {}", e))
}
