use crate::{
    errors::Result,
    models::{
        Account, AccountUpdate, CancelOrderRequest, GetAllOrdersRequest, GetOpenOrdersRequest,
        GetOrderRequest, GetUserTradesRequest, Order, PlaceOrderRequest, UserTrade,
    },
};
use async_trait::async_trait;
use tokio::sync::broadcast;

#[async_trait]
pub trait TradeProvider: Send + Sync {
    async fn init(&mut self) -> Result<()>;

    async fn place_order(&self, req: PlaceOrderRequest) -> Result<Order>;
    async fn cancel_order(&self, req: CancelOrderRequest) -> Result<()>;
    async fn get_order(&self, req: GetOrderRequest) -> Result<Order>;
    async fn get_open_orders(&self, req: GetOpenOrdersRequest) -> Result<Vec<Order>>;
    async fn get_all_orders(&self, req: GetAllOrdersRequest) -> Result<Vec<Order>>;
    async fn get_user_trades(&self, req: GetUserTradesRequest) -> Result<Vec<UserTrade>>;
    async fn get_account(&self) -> Result<Account>;

    fn subscribe_order(&self) -> broadcast::Receiver<Order>;
    fn subscribe_user_trade(&self) -> broadcast::Receiver<UserTrade>;
    fn subscribe_account(&self) -> broadcast::Receiver<AccountUpdate>;
}
