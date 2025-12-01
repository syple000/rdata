use crate::models::{KlineInterval, SymbolStatus};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineData {
    pub symbol: String,
    pub interval: KlineInterval,
    pub open_time: u64,
    pub close_time: u64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub quote_volume: Decimal,
    pub taker_buy_volume: Decimal,       // 部分交易所无该数据，默认0
    pub taker_buy_quote_volume: Decimal, // 部分交易所无该数据，默认0
    pub is_closed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticker24hr {
    pub symbol: String,
    pub last_price: Decimal,
    pub last_qty: Decimal,
    pub bid_price: Decimal,
    pub bid_qty: Decimal,
    pub ask_price: Decimal,
    pub ask_qty: Decimal,
    pub open_price: Decimal,
    pub high_price: Decimal,
    pub low_price: Decimal,
    pub volume: Decimal,
    pub quote_volume: Decimal,
    pub open_time: u64,
    pub close_time: u64,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthData {
    pub symbol: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub symbol: String,
    pub trade_id: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub timestamp: u64,
    pub is_buyer_maker: u64,
    pub seq_id: u64, // 验证交易所推送是否缺失，如果没有该值（默认0）
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolInfo {
    pub symbol: String,
    pub status: SymbolStatus,
    pub base_asset: String,
    pub quote_asset: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_asset_precision: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_asset_precision: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_price: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_price: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_tick_size: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_market_quantity: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_market_quantity: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_quantity_step_size: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_quantity: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_quantity: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantity_step_size: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_notional: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeInfo {
    pub symbols: Vec<SymbolInfo>,
}
