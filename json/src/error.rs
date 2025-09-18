use thiserror::Error;

#[derive(Error, Debug)]
pub enum JsonError {
    #[error("Failed to create file: {0}")]
    FileCreationError(String),

    #[error("Failed to write JSON to file: {0}")]
    FileWriteError(String),

    #[error("Failed to serialize to JSON string: {0}")]
    SerializationError(String),
}

pub type Result<T> = std::result::Result<T, JsonError>;
