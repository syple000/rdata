use super::enums::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: u64,
    pub client_order_id: String,
    pub symbol: String,
    pub order_side: Side,
    pub order_type: OrderType,
    pub order_quantity: Decimal,
    pub order_price: Decimal,
    pub executed_qty: Decimal,
    pub cummulative_quote_qty: Decimal,
    pub order_status: OrderStatus,
    pub time_in_force: TimeInForce,
    pub stop_price: Decimal,
    pub iceberg_qty: Decimal,
    pub create_time: u64,
    pub update_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub trade_id: u64,
    pub order_id: u64,
    pub symbol: String,
    pub order_side: Side,
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
pub struct Account {
    pub maker_commission_rate: Decimal,
    pub taker_commission_rate: Decimal,
    pub buyer_commission_rate: Decimal,
    pub seller_commission_rate: Decimal,
    pub balances: Vec<Balance>,
    pub can_trade: bool,
    pub update_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub symbol: String,
    pub order_id: u64,
    pub client_order_id: String,
    pub original_client_order_id: String,
    pub order_side: Side,
    pub time_in_force: TimeInForce,
    pub order_type: OrderType,
    pub execution_type: ExecutionType,
    pub order_status: OrderStatus,
    pub order_quantity: Decimal,
    pub order_price: Decimal,
    pub last_executed_qty: Decimal,
    pub last_executed_price: Decimal,
    pub cumulative_filled_qty: Decimal,
    pub cumulative_quote_qty: Decimal,
    pub commission: Decimal,
    pub commission_asset: String,
    pub transaction_time: u64,
    pub create_time: u64,
    pub trade_id: u64,
    pub is_maker: bool,
    pub stop_price: Decimal,
    pub iceberg_qty: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboundAccountPosition {
    pub balances: Vec<Balance>,
    pub transaction_time: u64,
    pub update_time: u64,
}
