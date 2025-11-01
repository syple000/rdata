use crate::models::{OrderSide, OrderType, TimeInForce};
use rust_decimal::Decimal;

#[derive(Clone)]
pub struct PlaceOrderRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub r#type: OrderType,
    pub time_in_force: Option<TimeInForce>, // LIMIT/STOP_LOSS_LIMIT/TAKE_PROFIT_LIMIT
    pub quantity: Option<Decimal>, // LIMIT/MARKET/STOP_LOSS/STOP_LOSS_LIMIT/TAKE_PROFIT/TAKE_PROFIT_LIMIT/LIMIT_MAKER
    pub price: Option<Decimal>,    // LIMIT/STOP_LOSS_LIMIT/TAKE_PROFIT_LIMIT/LIMIT_MAKER
    pub new_client_order_id: Option<String>,
    pub stop_price: Option<Decimal>, // STOP_LOSS/STOP_LOSS_LIMIT/TAKE_PROFIT/TAKE_PROFIT_LIMIT
    pub iceberg_qty: Option<Decimal>, // LIMIT/LIMIT_MAKER
}

#[derive(Clone)]
pub struct CancelOrderRequest {
    pub symbol: String,
    pub order_id: Option<String>,
    pub orig_client_order_id: Option<String>,
    pub new_client_order_id: Option<String>,
}

pub struct GetOrderRequest {
    pub symbol: String,
    pub order_id: Option<String>,
    pub orig_client_order_id: Option<String>,
}

pub struct GetOpenOrdersRequest {
    pub symbol: Option<String>,
}

pub struct GetAllOrdersRequest {
    pub symbol: String,
    pub from_order_id: Option<String>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub limit: Option<u32>,
}

pub struct GetUserTradesRequest {
    pub symbol: String,
    pub from_order_id: Option<String>,
    pub from_id: Option<String>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub limit: Option<u32>,
}
