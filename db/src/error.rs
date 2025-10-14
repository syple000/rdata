use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Sled error: {0}")]
    SledError(#[from] sled::Error),

    #[error("Serialization/Deserialization error: {0}")]
    SerDesError(String),
}

pub type Result<T> = std::result::Result<T, Error>;
