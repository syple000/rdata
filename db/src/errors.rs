#[derive(Debug, thiserror::Error)]
pub enum DBError {
    #[error("Database connection error: {message}")]
    ConnectionError { message: String },

    #[error("Query execution error: {message}")]
    QueryError { message: String },

    #[error("Data not found: {message}")]
    NotFound { message: String },

    #[error("SQLite error: {0}")]
    SQLiteError(#[from] rusqlite::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Lock error: {message}")]
    LockError { message: String },

    #[error("Invalid parameter: {message}")]
    InvalidParameter { message: String },
}

pub type Result<T> = std::result::Result<T, DBError>;
