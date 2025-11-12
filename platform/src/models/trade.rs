use crate::models::{OrderSide, OrderStatus, OrderType, PlaceOrderRequest, TimeInForce};
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

impl Order {
    pub fn new_order_from_place_order_req(req: &PlaceOrderRequest) -> Self {
        Order {
            symbol: req.symbol.clone(),
            order_id: "".to_string(),
            client_order_id: req.client_order_id.clone(),
            order_side: req.side.clone(),
            order_type: req.r#type.clone(),
            order_status: OrderStatus::New,
            order_price: req.price.unwrap_or_else(|| Decimal::new(0, 0)),
            order_quantity: req.quantity.unwrap_or_else(|| Decimal::new(0, 0)),
            executed_qty: Decimal::new(0, 0),
            cummulative_quote_qty: Decimal::new(0, 0),
            time_in_force: req.time_in_force.clone().unwrap_or(TimeInForce::Gtc),
            stop_price: req.stop_price.unwrap_or_else(|| Decimal::new(0, 0)),
            iceberg_qty: req.iceberg_qty.unwrap_or_else(|| Decimal::new(0, 0)),
            create_time: 0,
            update_time: 0,
        }
    }
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
    pub is_maker: u64,
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
