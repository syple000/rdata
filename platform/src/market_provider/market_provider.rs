use crate::{
    errors::Result,
    models::{
        DepthData, ExchangeInfo, GetDepthRequest, GetExchangeInfoRequest, GetKlinesRequest,
        GetTicker24hrRequest, GetTradesRequest, KlineData, Ticker24hr, Trade,
    },
};
use async_trait::async_trait;
use tokio::sync::broadcast;

#[async_trait]
pub trait MarketProvider: Send + Sync {
    async fn init(&mut self) -> Result<()>;

    async fn get_klines(&self, req: GetKlinesRequest) -> Result<Vec<KlineData>>;
    async fn get_trades(&self, req: GetTradesRequest) -> Result<Vec<Trade>>;
    async fn get_depth(&self, req: GetDepthRequest) -> Result<DepthData>;
    async fn get_ticker_24hr(&self, req: GetTicker24hrRequest) -> Result<Vec<Ticker24hr>>;
    async fn get_exchange_info(&self, req: GetExchangeInfoRequest) -> Result<ExchangeInfo>;

    fn subscribe_kline(&self) -> broadcast::Receiver<KlineData>;
    fn subscribe_trade(&self) -> broadcast::Receiver<Trade>;
    fn subscribe_depth(&self) -> broadcast::Receiver<DepthData>;
    fn subscribe_ticker(&self) -> broadcast::Receiver<Ticker24hr>;
}
