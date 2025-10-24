use crate::models::{NoExtra, OrderSide, OrderStatus, OrderType, TimeInForce};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order<Extra = NoExtra> {
    pub symbol: String,
    pub order_id: String,
    pub client_order_id: String,
    pub order_side: OrderSide,
    pub order_type: OrderType,
    pub order_status: OrderStatus,
    pub order_price: Decimal,
    pub order_quantity: Decimal,
    pub executed_qty: Decimal,
    pub cummulative_quote_qty: Decimal,
    pub time_in_force: TimeInForce,
    pub stop_price: Decimal,
    pub iceberg_qty: Decimal,
    pub create_time: u64,
    pub update_time: u64,

    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserTrade<Extra = NoExtra> {
    pub trade_id: String,
    pub order_id: String,
    pub symbol: String,
    pub order_side: OrderSide,
    pub trade_price: Decimal,
    pub trade_quantity: Decimal,
    pub commission: Decimal,
    pub commission_asset: String,
    pub is_maker: bool,
    pub timestamp: u64,

    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Balance<Extra = NoExtra> {
    pub asset: String,
    pub free: Decimal,
    pub locked: Decimal,

    #[serde(flatten)]
    pub extra: Extra,
}
