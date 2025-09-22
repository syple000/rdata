use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WsError {
    #[error("connection error: {message}")]
    Connection { message: String },

    #[error("transport error: {message}")]
    Transport { message: String },

    #[error("client error: {message}")]
    Client { message: String },

    #[error("config error: {message}")]
    Config { message: String },

    #[error(transparent)]
    External(#[from] ExternalError),

    #[error("handle error: {message}")]
    HandleError { message: String },
}

#[derive(Debug, Error)]
pub enum ExternalError {
    #[error("rate limiter: {0}")]
    RateLimiter(#[from] rate_limiter::error::RateLimiterError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("system time: {0}")]
    SystemTime(#[from] std::time::SystemTimeError),
}

impl WsError {
    // 初始化连接相关错误
    pub fn connection<S: Into<String>>(message: S) -> Self {
        Self::Connection {
            message: message.into(),
        }
    }

    pub fn connection_failed(url: String, source: impl std::error::Error) -> Self {
        Self::connection(format!("failed to connect to {}: {}", url, source))
    }

    pub fn connection_timeout(url: String, timeout: Duration) -> Self {
        Self::connection(format!("connection timeout after {:?} to {}", timeout, url))
    }

    /// 传输错误，含网络错误、关闭信号、内部channel错误等
    pub fn transport<S: Into<String>>(message: S) -> Self {
        Self::Transport {
            message: message.into(),
        }
    }

    pub fn send_failed(message_type: String, source: impl std::error::Error) -> Self {
        Self::transport(format!(
            "failed to send {} message: {}",
            message_type, source
        ))
    }

    pub fn receive_failed(source: impl std::error::Error) -> Self {
        Self::transport(format!("failed to receive message: {}", source))
    }

    pub fn channel_closed(channel_type: String, reason: String) -> Self {
        Self::transport(format!(
            "internal {} channel closed: {}",
            channel_type, reason
        ))
    }

    pub fn connection_closed(code: u16, reason: String) -> Self {
        Self::transport(format!(
            "connection closed by server: code={}, reason={}",
            code, reason
        ))
    }

    // 客户端错误
    pub fn client<S: Into<String>>(message: S) -> Self {
        Self::Client {
            message: message.into(),
        }
    }

    pub fn message_id_required() -> Self {
        Self::client("message ID is required for synchronous calls")
    }

    pub fn call_timeout(message_id: String, timeout: Duration) -> Self {
        Self::client(format!(
            "synchronous call timed out after {:?} waiting for message ID: {}",
            timeout, message_id
        ))
    }

    pub fn duplicated_message_id(message_id: String) -> Self {
        Self::client(format!("duplicated message ID: {}", message_id))
    }

    pub fn disconnected() -> Self {
        Self::client("client is disconnected")
    }

    /// 配置错误
    pub fn config<S: Into<String>>(message: S) -> Self {
        Self::Config {
            message: message.into(),
        }
    }

    pub fn invalid_url(url: String) -> Self {
        Self::config(format!("invalid WebSocket URL: {}", url))
    }

    pub fn invalid_timeout(field: String) -> Self {
        Self::config(format!(
            "invalid timeout configuration: {} must be > 0",
            field
        ))
    }

    pub fn invalid_heartbeat_interval() -> Self {
        Self::config("invalid heartbeat interval configuration, must be >= 0")
    }

    pub fn invalid_send_buf_size() -> Self {
        Self::config("invalid send buffer size configuration, must be > 0")
    }
}

pub type Result<T> = std::result::Result<T, WsError>;
