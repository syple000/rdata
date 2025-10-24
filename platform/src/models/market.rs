use crate::models::{NoExtra, SymbolStatus};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineData<Extra = NoExtra> {
    pub symbol: String,
    pub interval: String,
    pub open_time: u64,
    pub close_time: u64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub quote_volume: Decimal,

    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticker24hr<Extra = NoExtra> {
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

    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthData<Extra = NoExtra> {
    pub symbol: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: u64,

    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade<Extra = NoExtra> {
    pub symbol: String,
    pub trade_id: u64,
    pub price: Decimal,
    pub quantity: Decimal,
    pub timestamp: u64,
    pub is_buyer_maker: bool,

    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolInfo<Extra = NoExtra> {
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

    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeInfo<SymExtra = NoExtra, Extra = NoExtra> {
    pub symbols: Vec<SymbolInfo<SymExtra>>,

    #[serde(flatten)]
    pub extra: Extra,
}
