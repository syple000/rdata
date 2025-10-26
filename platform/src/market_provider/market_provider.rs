use crate::{
    errors::PlatformError,
    models::{DepthData, ExchangeInfo, KlineData, KlineInterval, Ticker24hr, Trade},
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub enum MarketEvent {
    Kline(Arc<KlineData>),
    Depth(Arc<DepthData>),
    Ticker(Arc<Ticker24hr>),
    Trade(Arc<Trade>),
}

#[async_trait]
pub trait MarketProvider: Send + Sync {
    async fn init(&mut self) -> Result<(), PlatformError>;

    async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        limit: Option<u32>,
    ) -> Result<Vec<Arc<KlineData>>, PlatformError>;

    async fn get_depth(&self, symbol: &str) -> Result<Arc<DepthData>, PlatformError>;

    async fn get_ticker_24hr(&self, symbol: &str) -> Result<Arc<Ticker24hr>, PlatformError>;

    async fn get_trades(
        &self,
        symbol: &str,
        limit: Option<u32>,
    ) -> Result<Vec<Arc<Trade>>, PlatformError>;

    async fn get_exchange_info(&self) -> Result<ExchangeInfo, PlatformError>;

    fn subscribe(&self) -> broadcast::Receiver<MarketEvent>;
}
