use thiserror::Error;

#[derive(Debug, Error)]
pub enum PlatformError {
    #[error("Configuration error: {message}")]
    ConfigError { message: String },

    #[error("Market provider error: {message}")]
    MarketProviderError { message: String },

    #[error("Trade provider error: {message}")]
    TradeProviderError { message: String },

    #[error("Data manager error: {message}")]
    DataManagerError { message: String },

    #[error("Engine error: {message}")]
    EngineError { message: String },

    #[error("Factor error: {message}")]
    FactorError { message: String },

    #[error("Strategy error: {message}")]
    StrategyError { message: String },

    #[error("Platform error: {message}")]
    PlatformError { message: String },

    #[error("Execution error: {message}")]
    ExecutionError { message: String },

    #[error("Validation error: {message}")]
    ValidationError { message: String },
}

pub type Result<T> = std::result::Result<T, PlatformError>;
