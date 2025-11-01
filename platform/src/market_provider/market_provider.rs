use crate::{
    errors::PlatformError,
    models::{
        DepthData, ExchangeInfo, GetDepthRequest, GetKlinesRequest, GetTicker24hrRequest,
        GetTradesRequest, KlineData, Ticker24hr, Trade,
    },
};
use async_trait::async_trait;
use tokio::sync::broadcast;

#[async_trait]
pub trait MarketProvider: Send + Sync {
    async fn init(&mut self) -> Result<(), PlatformError>;

    async fn get_klines(&self, req: GetKlinesRequest) -> Result<Vec<KlineData>, PlatformError>;
    async fn get_trades(&self, req: GetTradesRequest) -> Result<Vec<Trade>, PlatformError>;
    async fn get_depth(&self, req: GetDepthRequest) -> Result<DepthData, PlatformError>;
    async fn get_ticker_24hr(
        &self,
        req: GetTicker24hrRequest,
    ) -> Result<Vec<Ticker24hr>, PlatformError>;
    async fn get_exchange_info(&self) -> Result<ExchangeInfo, PlatformError>;

    fn subscribe_kline(&self) -> broadcast::Receiver<KlineData>;
    fn subscribe_trade(&self) -> broadcast::Receiver<Trade>;
    fn subscribe_depth(&self) -> broadcast::Receiver<DepthData>;
    fn subscribe_ticker(&self) -> broadcast::Receiver<Ticker24hr>;
}
