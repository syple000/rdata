pub mod error;
pub use error::{Result, WsError};
pub mod ws_client;
pub use ws_client::{Client, Config, RecvMsg, SendMsg};

#[cfg(test)]
mod ws_client_test;
