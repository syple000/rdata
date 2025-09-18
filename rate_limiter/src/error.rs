use thiserror::Error;

#[derive(Error, Debug)]
pub enum RateLimiterError {
    #[error("weight must be greater than 0")]
    InvalidWeight,

    #[error("weight must be less than or equal to max_weight_limit: {0}")]
    WeightExceeded(u64),

    #[error("exceed max_weight_limit")]
    MaxWeightLimitExceeded,
}

pub type Result<T> = std::result::Result<T, RateLimiterError>;
