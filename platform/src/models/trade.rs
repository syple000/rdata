use crate::models::{OrderSide, OrderStatus, OrderType, TimeInForce};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Order {
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserTrade {
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Balance {
    pub asset: String,
    pub free: Decimal,
    pub locked: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Account {
    pub balances: Vec<Balance>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountUpdate {
    pub balances: Vec<Balance>,
    pub timestamp: u64,
}
