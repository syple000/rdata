use crate::models::{OrderSide, OrderStatus, OrderType, TimeInForce};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Balance {
    pub asset: String,
    pub free: Decimal,
    pub locked: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaceOrderRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub r#type: OrderType,
    pub time_in_force: Option<TimeInForce>, // LIMIT/STOP_LOSS_LIMIT/TAKE_PROFIT_LIMIT
    pub quantity: Option<Decimal>, // LIMIT/MARKET/STOP_LOSS/STOP_LOSS_LIMIT/TAKE_PROFIT/TAKE_PROFIT_LIMIT/LIMIT_MAKER
    pub price: Option<Decimal>,    // LIMIT/STOP_LOSS_LIMIT/TAKE_PROFIT_LIMIT/LIMIT_MAKER
    pub client_order_id: Option<String>,
    pub stop_price: Option<Decimal>, // STOP_LOSS/STOP_LOSS_LIMIT/TAKE_PROFIT/TAKE_PROFIT_LIMIT
    pub iceberg_qty: Option<Decimal>, // LIMIT/LIMIT_MAKER
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelOrderRequest {
    pub symbol: String,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
}
