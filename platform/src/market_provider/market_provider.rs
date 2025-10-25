use crate::models::{DepthData, ExchangeInfo, KlineData, KlineInterval, Ticker24hr, Trade};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast;

pub enum MarketEvent {
    Kline(Arc<KlineData>),
    Depth(Arc<DepthData>),
    Ticker(Arc<Ticker24hr>),
    Trade(Arc<Trade>),
}

#[async_trait]
pub trait MarketProvider: Send + Sync {
    type Error;

    async fn init(&mut self) -> Result<(), Self::Error>;

    async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        limit: Option<u32>,
    ) -> Result<Vec<Arc<KlineData>>, Self::Error>;

    async fn get_depth(&self, symbol: &str) -> Result<Arc<DepthData>, Self::Error>;

    async fn get_ticker_24hr(&self, symbol: &str) -> Result<Arc<Ticker24hr>, Self::Error>;

    async fn get_trades(
        &self,
        symbol: &str,
        limit: Option<u32>,
    ) -> Result<Vec<Arc<Trade>>, Self::Error>;

    async fn get_exchange_info(&self) -> Result<ExchangeInfo, Self::Error>;

    fn subscribe(&self) -> broadcast::Receiver<MarketEvent>;
}
