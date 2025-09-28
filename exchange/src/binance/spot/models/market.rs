use super::enums::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineData {
    pub symbol: String,
    pub open_time: u64,
    pub close_time: u64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub quote_volume: Decimal,
    pub trade_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthData {
    pub symbol: String,
    pub last_update_id: u64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthUpdate {
    pub symbol: String,
    pub first_update_id: u64,
    pub last_update_id: u64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggTrade {
    pub symbol: String,
    pub agg_trade_id: u64,
    pub price: Decimal,
    pub quantity: Decimal,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub timestamp: u64,
    pub is_buyer_maker: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    pub filter_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_price: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_price: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tick_size: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_qty: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_qty: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_size: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_notional: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub apply_to_market: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_price_mins: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_num_orders: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_num_algo_orders: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Symbol {
    pub symbol: String,
    pub status: String,
    pub base_asset: String,
    pub base_asset_precision: i32,
    pub quote_asset: String,
    pub quote_asset_precision: i32,
    pub order_types: Vec<OrderType>,
    pub iceberg_allowed: bool,
    pub oco_allowed: bool,
    pub is_spot_trading_allowed: bool,
    pub is_margin_trading_allowed: bool,
    pub filters: Vec<Filter>,
    pub permissions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeInfo {
    pub timezone: String,
    pub server_time: u64,
    pub symbols: Vec<Symbol>,
}
