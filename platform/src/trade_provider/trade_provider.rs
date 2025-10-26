use crate::{
    errors::Result,
    models::{Balance, Order, UserTrade},
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast;

pub enum TradeEvent {
    Order(Arc<Order>),
    Trade(Arc<UserTrade>),
    Balance(Vec<Arc<Balance>>),
}

#[async_trait]
pub trait TradeProvider: Send + Sync {
    async fn init(&mut self) -> Result<()>;

    async fn get_balances(&self) -> Result<Vec<Arc<Balance>>>;

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Arc<Order>>>;

    async fn get_trades(
        &self,
        order_id: Option<&str>,
        client_order_id: Option<&str>,
    ) -> Result<Vec<Arc<UserTrade>>>;

    async fn get_all_orders(
        &self,
        symbol: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Vec<Arc<Order>>>;

    async fn get_all_trades(
        &self,
        symbol: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Vec<Arc<UserTrade>>>;

    fn subscribe(&self) -> broadcast::Receiver<TradeEvent>;
}
