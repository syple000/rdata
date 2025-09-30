use thiserror::Error;

#[derive(Error, Debug)]
pub enum BinanceError {
    #[error("Parameters invalid: {message}")]
    ParametersInvalid { message: String },

    #[error("Network error: {message}")]
    NetworkError { message: String },

    #[error("Parse result error: {message}")]
    ParseResultError { message: String },

    #[error(transparent)]
    ExternalError(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Client error: {message}")]
    ClientError { message: String },
}

pub type Result<T> = std::result::Result<T, BinanceError>;
