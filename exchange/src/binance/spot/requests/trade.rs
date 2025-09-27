use rust_decimal::Decimal;

use crate::binance::spot::models::{OrderType, Side, TimeInForce};

pub struct PlaceOrderRequest {
    pub symbol: String,
    pub side: Side,
    pub r#type: OrderType,
    pub time_in_force: Option<TimeInForce>, // LIMIT/STOP_LOSS_LIMIT/TAKE_PROFIT_LIMIT
    pub quantity: Option<Decimal>, // LIMIT/MARKET/STOP_LOSS/STOP_LOSS_LIMIT/TAKE_PROFIT/TAKE_PROFIT_LIMIT/LIMIT_MAKER
    pub price: Option<Decimal>,    // LIMIT/STOP_LOSS_LIMIT/TAKE_PROFIT_LIMIT/LIMIT_MAKER
    pub new_client_order_id: Option<String>,
    pub stop_price: Option<Decimal>, // STOP_LOSS/STOP_LOSS_LIMIT/TAKE_PROFIT/TAKE_PROFIT_LIMIT
    pub iceberg_qty: Option<Decimal>, // LIMIT/LIMIT_MAKER
}

pub struct CancelOrderRequest {
    pub symbol: String,
    pub order_id: Option<u128>,
    pub orig_client_order_id: Option<String>,
    pub new_client_order_id: Option<String>,
}

pub struct GetAccountRequest {}

pub struct GetOrderRequest {
    pub symbol: String,
    pub order_id: Option<u128>,
    pub orig_client_order_id: Option<String>,
}

pub struct GetOpenOrdersRequest {
    pub symbol: Option<String>,
}

pub struct GetAllOrdersRequest {
    pub symbol: String,
    pub order_id: Option<u128>, // 仅返回该id以后的订单
    pub start_time: Option<u128>,
    pub end_time: Option<u128>,
    pub limit: Option<u32>, // default 500; max 1000
}

pub struct GetTradesRequest {
    pub symbol: String,
    pub from_id: Option<u128>, // 仅返回该id以后的订单
    pub start_time: Option<u128>,
    pub end_time: Option<u128>,
    pub limit: Option<u32>, // default 500; max 1000
}
