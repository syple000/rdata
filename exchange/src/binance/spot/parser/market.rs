use super::super::models::market::*;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct KlineDataRaw(
    u64,     // open time
    Decimal, // open price
    Decimal, // high price
    Decimal, // low price
    Decimal, // close price
    Decimal, // volume
    u64,     // close time
    Decimal, // quote volume
    u64,     // number of trades
    Decimal, // taker buy volume
    Decimal, // taker buy quote
    Value,   // ignore this field
);

impl From<(String, KlineDataRaw)> for KlineData {
    fn from((symbol, raw): (String, KlineDataRaw)) -> Self {
        KlineData {
            symbol,
            open_time: raw.0,
            open: raw.1,
            high: raw.2,
            low: raw.3,
            close: raw.4,
            volume: raw.5,
            close_time: raw.6,
            quote_volume: raw.7,
            trade_count: raw.8,
        }
    }
}

pub fn parse_klines(symbol: String, data: &str) -> Result<Vec<KlineData>, serde_json::Error> {
    let raw_klines: Vec<KlineDataRaw> = serde_json::from_str(data)?;
    Ok(raw_klines
        .into_iter()
        .map(|raw| (symbol.clone(), raw).into())
        .collect())
}

#[derive(Debug, Deserialize)]
pub struct DepthUpdateRaw {
    #[serde(rename = "E")]
    timestamp: u64, // 事件时间
    #[serde(rename = "U")]
    first_update_id: u64, // 首次更新ID
    #[serde(rename = "u")]
    last_update_id: u64, // 最后更新ID
    #[serde(rename = "b")]
    bids: Vec<Vec<Value>>, // 买方深度
    #[serde(rename = "a")]
    asks: Vec<Vec<Value>>, // 卖方深度
}

impl From<(String, DepthUpdateRaw)> for DepthUpdate {
    fn from((symbol, raw): (String, DepthUpdateRaw)) -> Self {
        DepthUpdate {
            symbol,
            first_update_id: raw.first_update_id,
            last_update_id: raw.last_update_id,
            bids: raw
                .bids
                .into_iter()
                .filter_map(|vec| {
                    if vec.len() >= 2 {
                        let price = vec[0].as_str()?.parse().ok()?;
                        let quantity = vec[1].as_str()?.parse().ok()?;
                        Some(PriceLevel { price, quantity })
                    } else {
                        None
                    }
                })
                .collect(),
            asks: raw
                .asks
                .into_iter()
                .filter_map(|vec| {
                    if vec.len() >= 2 {
                        let price = vec[0].as_str()?.parse().ok()?;
                        let quantity = vec[1].as_str()?.parse().ok()?;
                        Some(PriceLevel { price, quantity })
                    } else {
                        None
                    }
                })
                .collect(),
            timestamp: raw.timestamp,
        }
    }
}

pub fn parse_depth_update(symbol: String, data: &str) -> Result<DepthUpdate, serde_json::Error> {
    let raw: DepthUpdateRaw = serde_json::from_str(data)?;
    Ok((symbol, raw).into())
}

#[derive(Debug, Deserialize)]
pub struct AggTradeRaw {
    #[serde(rename = "a")]
    agg_trade_id: u64, // 归集交易ID
    #[serde(rename = "p")]
    price: Decimal, // 成交价格
    #[serde(rename = "q")]
    quantity: Decimal, // 成交数量
    #[serde(rename = "f")]
    first_trade_id: u64, // 被归集的首个交易ID
    #[serde(rename = "l")]
    last_trade_id: u64, // 被归集的末次交易ID
    #[serde(rename = "T")]
    timestamp: u64, // 成交时间
    #[serde(rename = "m")]
    is_buyer_maker: bool, // 买方是否是做市方
}

impl From<(String, AggTradeRaw)> for AggTrade {
    fn from((symbol, raw): (String, AggTradeRaw)) -> Self {
        AggTrade {
            symbol,
            agg_trade_id: raw.agg_trade_id,
            price: raw.price,
            quantity: raw.quantity,
            first_trade_id: raw.first_trade_id,
            last_trade_id: raw.last_trade_id,
            timestamp: raw.timestamp,
            is_buyer_maker: raw.is_buyer_maker,
        }
    }
}

pub fn parse_agg_trades(symbol: String, data: &str) -> Result<Vec<AggTrade>, serde_json::Error> {
    let raw_trades: Vec<AggTradeRaw> = serde_json::from_str(data)?;
    Ok(raw_trades
        .into_iter()
        .map(|raw| (symbol.clone(), raw).into())
        .collect())
}

#[derive(Debug, Deserialize)]
pub struct DepthDataRaw {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    #[serde(rename = "bids")]
    bids: Vec<Vec<Value>>,
    #[serde(rename = "asks")]
    asks: Vec<Vec<Value>>,
}

impl From<(String, DepthDataRaw)> for DepthData {
    fn from((symbol, raw): (String, DepthDataRaw)) -> Self {
        DepthData {
            symbol,
            last_update_id: raw.last_update_id,
            bids: raw
                .bids
                .into_iter()
                .filter_map(|vec| {
                    if vec.len() >= 2 {
                        let price = vec[0].as_str()?.parse().ok()?;
                        let quantity = vec[1].as_str()?.parse().ok()?;
                        Some(PriceLevel { price, quantity })
                    } else {
                        None
                    }
                })
                .collect(),
            asks: raw
                .asks
                .into_iter()
                .filter_map(|vec| {
                    if vec.len() >= 2 {
                        let price = vec[0].as_str()?.parse().ok()?;
                        let quantity = vec[1].as_str()?.parse().ok()?;
                        Some(PriceLevel { price, quantity })
                    } else {
                        None
                    }
                })
                .collect(),
        }
    }
}

pub fn parse_depth(symbol: String, data: &str) -> Result<DepthData, serde_json::Error> {
    let raw: DepthDataRaw = serde_json::from_str(data)?;
    Ok((symbol, raw).into())
}

#[derive(Debug, Deserialize)]
pub struct AggTradeStreamRaw {
    #[serde(rename = "e")]
    event_type: String, // 事件类型
    #[serde(rename = "E")]
    event_time: u64, // 事件时间
    #[serde(rename = "s")]
    symbol: String, // 交易对
    #[serde(rename = "a")]
    agg_trade_id: u64, // 归集交易ID
    #[serde(rename = "p")]
    price: Decimal, // 成交价格
    #[serde(rename = "q")]
    quantity: Decimal, // 成交数量
    #[serde(rename = "f")]
    first_trade_id: u64, // 被归集的首个交易ID
    #[serde(rename = "l")]
    last_trade_id: u64, // 被归集的末次交易ID
    #[serde(rename = "T")]
    timestamp: u64, // 成交时间
    #[serde(rename = "m")]
    is_buyer_maker: bool, // 买方是否是做市方
}

impl From<AggTradeStreamRaw> for AggTrade {
    fn from(raw: AggTradeStreamRaw) -> Self {
        AggTrade {
            symbol: raw.symbol,
            agg_trade_id: raw.agg_trade_id,
            price: raw.price,
            quantity: raw.quantity,
            first_trade_id: raw.first_trade_id,
            last_trade_id: raw.last_trade_id,
            timestamp: raw.timestamp,
            is_buyer_maker: raw.is_buyer_maker,
        }
    }
}

pub fn parse_agg_trade_stream(data: &str) -> Result<AggTrade, serde_json::Error> {
    let raw: AggTradeStreamRaw = serde_json::from_str(data)?;
    Ok(raw.into())
}

#[derive(Debug, Deserialize)]
pub struct FilterRaw {
    #[serde(rename = "filterType")]
    pub filter_type: String,
    #[serde(rename = "minPrice")]
    pub min_price: Option<String>,
    #[serde(rename = "maxPrice")]
    pub max_price: Option<String>,
    #[serde(rename = "tickSize")]
    pub tick_size: Option<String>,
    #[serde(rename = "minQty")]
    pub min_qty: Option<String>,
    #[serde(rename = "maxQty")]
    pub max_qty: Option<String>,
    #[serde(rename = "stepSize")]
    pub step_size: Option<String>,
    #[serde(rename = "minNotional")]
    pub min_notional: Option<String>,
    #[serde(rename = "applyToMarket")]
    pub apply_to_market: Option<bool>,
    #[serde(rename = "avgPriceMins")]
    pub avg_price_mins: Option<i32>,
    pub limit: Option<i32>,
    #[serde(rename = "maxNumOrders")]
    pub max_num_orders: Option<i32>,
    #[serde(rename = "maxNumAlgoOrders")]
    pub max_num_algo_orders: Option<i32>,
}

impl From<FilterRaw> for Filter {
    fn from(raw: FilterRaw) -> Self {
        Filter {
            filter_type: raw.filter_type,
            min_price: raw.min_price.and_then(|s| s.parse().ok()),
            max_price: raw.max_price.and_then(|s| s.parse().ok()),
            tick_size: raw.tick_size.and_then(|s| s.parse().ok()),
            min_qty: raw.min_qty.and_then(|s| s.parse().ok()),
            max_qty: raw.max_qty.and_then(|s| s.parse().ok()),
            step_size: raw.step_size.and_then(|s| s.parse().ok()),
            min_notional: raw.min_notional.and_then(|s| s.parse().ok()),
            apply_to_market: raw.apply_to_market,
            avg_price_mins: raw.avg_price_mins,
            limit: raw.limit,
            max_num_orders: raw.max_num_orders,
            max_num_algo_orders: raw.max_num_algo_orders,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SymbolRaw {
    pub symbol: String,
    pub status: String,
    #[serde(rename = "baseAsset")]
    pub base_asset: String,
    #[serde(rename = "baseAssetPrecision")]
    pub base_asset_precision: i32,
    #[serde(rename = "quoteAsset")]
    pub quote_asset: String,
    #[serde(rename = "quoteAssetPrecision")]
    pub quote_asset_precision: i32,
    #[serde(rename = "orderTypes")]
    pub order_types: Vec<String>,
    #[serde(rename = "icebergAllowed")]
    pub iceberg_allowed: bool,
    #[serde(rename = "ocoAllowed")]
    pub oco_allowed: bool,
    #[serde(rename = "isSpotTradingAllowed")]
    pub is_spot_trading_allowed: bool,
    #[serde(rename = "isMarginTradingAllowed")]
    pub is_margin_trading_allowed: bool,
    pub filters: Vec<FilterRaw>,
    pub permissions: Vec<String>,
}

impl From<SymbolRaw> for Symbol {
    fn from(raw: SymbolRaw) -> Self {
        Symbol {
            symbol: raw.symbol,
            status: raw.status,
            base_asset: raw.base_asset,
            base_asset_precision: raw.base_asset_precision,
            quote_asset: raw.quote_asset,
            quote_asset_precision: raw.quote_asset_precision,
            order_types: raw
                .order_types
                .into_iter()
                .filter_map(|s| {
                    // 反序列化 OrderType，例如 "MARKET" -> OrderType::Market
                    serde_json::from_value(serde_json::Value::String(s)).ok()
                })
                .collect(),
            iceberg_allowed: raw.iceberg_allowed,
            oco_allowed: raw.oco_allowed,
            is_spot_trading_allowed: raw.is_spot_trading_allowed,
            is_margin_trading_allowed: raw.is_margin_trading_allowed,
            filters: raw.filters.into_iter().map(|f| f.into()).collect(),
            permissions: raw.permissions,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ExchangeInfoRaw {
    pub timezone: String,
    #[serde(rename = "serverTime")]
    pub server_time: u64,
    pub symbols: Vec<SymbolRaw>,
}

impl From<ExchangeInfoRaw> for ExchangeInfo {
    fn from(raw: ExchangeInfoRaw) -> Self {
        ExchangeInfo {
            timezone: raw.timezone,
            server_time: raw.server_time,
            symbols: raw.symbols.into_iter().map(|s| s.into()).collect(),
        }
    }
}

pub fn parse_exchange_info(data: &str) -> Result<ExchangeInfo, serde_json::Error> {
    let raw: ExchangeInfoRaw = serde_json::from_str(data)?;
    Ok(raw.into())
}
