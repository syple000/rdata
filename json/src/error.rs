use thiserror::Error;

#[derive(Error, Debug)]
pub enum JsonError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, JsonError>;
