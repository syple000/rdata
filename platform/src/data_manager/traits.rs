use crate::{
    errors::Result,
    models::{
        Account, CancelOrderRequest, DepthData, KlineData, KlineInterval, MarketType, Order,
        PlaceOrderRequest, Ticker24hr, Trade, UserTrade,
    },
};
use async_trait::async_trait;

#[async_trait]
pub trait MarketDataManager: Send + Sync {
    async fn init(&self) -> Result<()>;

    async fn get_klines(
        &self,
        market_type: &MarketType,
        symbol: &String,
        interval: &KlineInterval,
        limit: Option<usize>,
    ) -> Result<Vec<KlineData>>;

    async fn get_trades(
        &self,
        market_type: &MarketType,
        symbol: &String,
        limit: Option<usize>,
    ) -> Result<Vec<Trade>>;

    async fn get_depth(
        &self,
        market_type: &MarketType,
        symbol: &String,
    ) -> Result<Option<DepthData>>;

    async fn get_ticker(
        &self,
        market_type: &MarketType,
        symbol: &String,
    ) -> Result<Option<Ticker24hr>>;
}

#[async_trait]
pub trait TradeDataManager: Send + Sync {
    async fn init(&self) -> Result<()>;

    async fn get_account(&self, market_type: &MarketType) -> Result<Option<Account>>;

    async fn get_open_orders(&self, market_type: &MarketType) -> Result<Vec<Order>>;

    async fn get_user_trades_by_order(
        &self,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<UserTrade>>;

    async fn get_orders(
        &self,
        market_type: &MarketType,
        symbol: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Order>>;

    async fn get_user_trades(
        &self,
        market_type: &MarketType,
        symbol: &str,
        limit: Option<usize>,
    ) -> Result<Vec<UserTrade>>;

    async fn get_order_by_client_id(
        &self,
        market_type: &MarketType,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Option<Order>>;

    async fn get_order_by_id(
        &self,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Option<Order>>;

    async fn get_last_sync_ts(&self, market_type: &MarketType) -> Result<Option<u64>>;

    async fn place_order(&self, market_type: &MarketType, req: PlaceOrderRequest) -> Result<Order>;

    async fn cancel_order(&self, market_type: &MarketType, req: CancelOrderRequest) -> Result<()>;
}
