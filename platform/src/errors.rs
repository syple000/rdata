use thiserror::Error;

#[derive(Debug, Error)]
pub enum PlatformError {
    #[error("Configuration error: {message}")]
    ConfigError { message: String },

    #[error("Market provider error: {message}")]
    MarketProviderError { message: String },

    #[error("Trade provider error: {message}")]
    TradeProviderError { message: String },
}

pub type Result<T> = std::result::Result<T, PlatformError>;
