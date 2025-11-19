use crate::errors::Result;
use async_trait::async_trait;

#[async_trait]
pub trait Engine {
    async fn start(&mut self) -> Result<()>;
}
