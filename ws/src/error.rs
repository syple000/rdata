use thiserror::Error;

#[derive(Debug, Error)]
pub enum WsError {
    #[error("connect error: {0}")]
    ConnectionError(String),

    #[error("connect timeout: {0}")]
    ConnectionTimeout(String),

    #[error("channel closed: {0}")]
    ChannelClosed(String),

    #[error("msg_id is required for call")]
    MsgIdRequired,

    #[error("call timeout")]
    CallTimeout,

    #[error("disconnected")]
    Disconnected,

    #[error("recv error: {0}")]
    RecvError(String),

    #[error("send error: {0}")]
    SendError(String),

    #[error(transparent)]
    RateLimiterError(#[from] rate_limiter::error::RateLimiterError),
}

pub type Result<T> = std::result::Result<T, WsError>;
