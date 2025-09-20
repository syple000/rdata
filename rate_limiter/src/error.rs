use thiserror::Error;

#[derive(Error, Debug)]
pub enum RateLimiterError {
    #[error("invalid weight: {message}")]
    InvalidWeight { message: String },

    #[error("reach rate limit")]
    Limited,
}

impl RateLimiterError {
    pub fn invalid_weight() -> Self {
        RateLimiterError::InvalidWeight {
            message: "weight must be greater than 0".to_string(),
        }
    }

    pub fn weight_exceeded(max: u64) -> Self {
        RateLimiterError::InvalidWeight {
            message: format!("weight exceeds max limit: {}", max),
        }
    }

    pub fn max_weight_limit_exceeded() -> Self {
        RateLimiterError::Limited
    }
}

pub type Result<T> = std::result::Result<T, RateLimiterError>;
