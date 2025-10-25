use crate::binance::spot::{models::KlineInterval, requests::GetTicker24hrRequest};

use super::super::models::market::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};
use serde_json::Value;

// 允许负数的 u64 反序列化
fn de_u64_allow_negative<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Visitor;
    use std::fmt;

    struct U64Visitor;

    impl<'de> Visitor<'de> for U64Visitor {
        type Value = u64;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a u64 or i64")
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if v < 0 {
                Ok(0)
            } else {
                Ok(v as u64)
            }
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v)
        }
    }

    deserializer.deserialize_any(U64Visitor)
}

#[derive(Debug, Deserialize)]
pub struct KlineDataRaw(
    u64,     // 0: open time
    Decimal, // 1: open price
    Decimal, // 2: high price
    Decimal, // 3: low price
    Decimal, // 4: close price
    Decimal, // 5: volume
    u64,     // 6: close time
    Decimal, // 7: quote volume
    u64,     // 8: number of trades
    Decimal, // 9: taker buy base volume
    Decimal, // 10: taker buy quote volume
    Value,   // 11: ignore this field
);

impl From<(String, KlineInterval, KlineDataRaw)> for KlineData {
    fn from((symbol, interval, raw): (String, KlineInterval, KlineDataRaw)) -> Self {
        KlineData {
            symbol,
            interval,
            open_time: raw.0,
            close_time: raw.6,
            open: raw.1,
            high: raw.2,
            low: raw.3,
            close: raw.4,
            volume: raw.5,
            quote_volume: raw.7,
            trade_count: raw.8,
            taker_buy_volume: raw.9,
            taker_buy_quote_volume: raw.10,
            first_trade_id: 0, // Not available in REST API response
            last_trade_id: 0,  // Not available in REST API response
            is_closed: false,  // Not available in REST API response
        }
    }
}

pub fn parse_klines(
    symbol: String,
    interval: KlineInterval,
    data: &str,
) -> Result<Vec<KlineData>, serde_json::Error> {
    let raw_klines: Vec<KlineDataRaw> = serde_json::from_str(data)?;
    Ok(raw_klines
        .into_iter()
        .map(|raw| (symbol.clone(), interval.clone(), raw).into())
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
            timestamp: 0,
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
pub struct KlineStreamDataRaw {
    #[serde(rename = "t")]
    open_time: u64, // K线开始时间
    #[serde(rename = "T")]
    close_time: u64, // K线结束时间
    #[serde(rename = "s")]
    symbol: String, // 交易对
    #[serde(rename = "i")]
    interval: KlineInterval, // K线间隔
    #[serde(rename = "f", deserialize_with = "de_u64_allow_negative")]
    first_trade_id: u64, // 这根K线期间第一笔成交ID
    #[serde(rename = "L", deserialize_with = "de_u64_allow_negative")]
    last_trade_id: u64, // 这根K线期间末一笔成交ID
    #[serde(rename = "o")]
    open: Decimal, // 开盘价
    #[serde(rename = "c")]
    close: Decimal, // 收盘价
    #[serde(rename = "h")]
    high: Decimal, // 最高价
    #[serde(rename = "l")]
    low: Decimal, // 最低价
    #[serde(rename = "v")]
    volume: Decimal, // 成交量
    #[serde(rename = "n")]
    trade_count: u64, // 成交笔数
    #[serde(rename = "x")]
    is_closed: bool, // 这根K线是否完结
    #[serde(rename = "q")]
    quote_volume: Decimal, // 成交额
    #[serde(rename = "V")]
    taker_buy_volume: Decimal, // 主动买入成交量
    #[serde(rename = "Q")]
    taker_buy_quote_volume: Decimal, // 主动买入成交额
    #[serde(rename = "B")]
    ignore: String, // 忽略此参数
}

#[derive(Debug, Deserialize)]
pub struct KlineDataStreamRaw {
    #[serde(rename = "e")]
    event_type: String, // 事件类型
    #[serde(rename = "E")]
    event_time: u64, // 事件时间
    #[serde(rename = "s")]
    symbol: String, // 交易对
    #[serde(rename = "k")]
    kline: KlineStreamDataRaw, // K线数据
}

impl From<KlineDataStreamRaw> for KlineData {
    fn from(raw: KlineDataStreamRaw) -> Self {
        KlineData {
            symbol: raw.kline.symbol,
            interval: raw.kline.interval,
            open_time: raw.kline.open_time,
            close_time: raw.kline.close_time,
            open: raw.kline.open,
            high: raw.kline.high,
            low: raw.kline.low,
            close: raw.kline.close,
            volume: raw.kline.volume,
            quote_volume: raw.kline.quote_volume,
            trade_count: raw.kline.trade_count,
            taker_buy_volume: raw.kline.taker_buy_volume,
            taker_buy_quote_volume: raw.kline.taker_buy_quote_volume,
            first_trade_id: raw.kline.first_trade_id,
            last_trade_id: raw.kline.last_trade_id,
            is_closed: raw.kline.is_closed,
        }
    }
}

pub fn parse_kline_stream(data: &str) -> Result<KlineData, serde_json::Error> {
    let raw: KlineDataStreamRaw = serde_json::from_str(data)?;
    Ok(raw.into())
}

#[derive(Debug, Deserialize)]
pub struct TickerStreamRaw {
    #[serde(rename = "e")]
    event_type: String, // 事件类型
    #[serde(rename = "E")]
    event_time: u64, // 事件时间
    #[serde(rename = "s")]
    symbol: String, // 交易对
    #[serde(rename = "p")]
    price_change: Decimal, // 24小时价格变化
    #[serde(rename = "P")]
    price_change_percent: Decimal, // 24小时价格变化（百分比）
    #[serde(rename = "w")]
    weighted_avg_price: Decimal, // 平均价格
    #[serde(rename = "x")]
    prev_close_price: Decimal, // 整整24小时之前，向前数的最后一次成交价格
    #[serde(rename = "c")]
    last_price: Decimal, // 最新成交价格
    #[serde(rename = "Q")]
    last_qty: Decimal, // 最新成交交易的成交量
    #[serde(rename = "b")]
    bid_price: Decimal, // 目前最高买单价
    #[serde(rename = "B")]
    bid_qty: Decimal, // 目前最高买单价的挂单量
    #[serde(rename = "a")]
    ask_price: Decimal, // 目前最低卖单价
    #[serde(rename = "A")]
    ask_qty: Decimal, // 目前最低卖单价的挂单量
    #[serde(rename = "o")]
    open_price: Decimal, // 整整24小时前，向后数的第一次成交价格
    #[serde(rename = "h")]
    high_price: Decimal, // 24小时内最高成交价
    #[serde(rename = "l")]
    low_price: Decimal, // 24小时内最低成交价
    #[serde(rename = "v")]
    volume: Decimal, // 24小时内成交量
    #[serde(rename = "q")]
    quote_volume: Decimal, // 24小时内成交额
    #[serde(rename = "O")]
    open_time: u64, // 统计开始时间
    #[serde(rename = "C")]
    close_time: u64, // 统计结束时间
    #[serde(rename = "F")]
    first_id: u64, // 24小时内第一笔成交交易ID
    #[serde(rename = "L")]
    last_id: u64, // 24小时内最后一笔成交交易ID
    #[serde(rename = "n")]
    count: u64, // 24小时内成交数
}

impl From<TickerStreamRaw> for Ticker24hr {
    fn from(raw: TickerStreamRaw) -> Self {
        Ticker24hr {
            symbol: raw.symbol,
            price_change: raw.price_change,
            price_change_percent: raw.price_change_percent,
            weighted_avg_price: raw.weighted_avg_price,
            prev_close_price: raw.prev_close_price,
            last_price: raw.last_price,
            last_qty: raw.last_qty,
            bid_price: raw.bid_price,
            bid_qty: raw.bid_qty,
            ask_price: raw.ask_price,
            ask_qty: raw.ask_qty,
            open_price: raw.open_price,
            high_price: raw.high_price,
            low_price: raw.low_price,
            volume: raw.volume,
            quote_volume: raw.quote_volume,
            open_time: raw.open_time,
            close_time: raw.close_time,
            first_id: raw.first_id,
            last_id: raw.last_id,
            count: raw.count,
        }
    }
}

pub fn parse_ticker_stream(data: &str) -> Result<Ticker24hr, serde_json::Error> {
    let raw: TickerStreamRaw = serde_json::from_str(data)?;
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

#[derive(Debug, Deserialize)]
pub struct Ticker24hrRaw {
    pub symbol: String,
    #[serde(rename = "priceChange")]
    pub price_change: String,
    #[serde(rename = "priceChangePercent")]
    pub price_change_percent: String,
    #[serde(rename = "weightedAvgPrice")]
    pub weighted_avg_price: String,
    #[serde(rename = "prevClosePrice")]
    pub prev_close_price: String,
    #[serde(rename = "lastPrice")]
    pub last_price: String,
    #[serde(rename = "lastQty")]
    pub last_qty: String,
    #[serde(rename = "bidPrice")]
    pub bid_price: String,
    #[serde(rename = "bidQty")]
    pub bid_qty: String,
    #[serde(rename = "askPrice")]
    pub ask_price: String,
    #[serde(rename = "askQty")]
    pub ask_qty: String,
    #[serde(rename = "openPrice")]
    pub open_price: String,
    #[serde(rename = "highPrice")]
    pub high_price: String,
    #[serde(rename = "lowPrice")]
    pub low_price: String,
    pub volume: String,
    #[serde(rename = "quoteVolume")]
    pub quote_volume: String,
    #[serde(rename = "openTime")]
    pub open_time: u64,
    #[serde(rename = "closeTime")]
    pub close_time: u64,
    #[serde(rename = "firstId")]
    pub first_id: u64,
    #[serde(rename = "lastId")]
    pub last_id: u64,
    pub count: u64,
}

impl From<Ticker24hrRaw> for Ticker24hr {
    fn from(raw: Ticker24hrRaw) -> Self {
        Ticker24hr {
            symbol: raw.symbol,
            price_change: raw.price_change.parse().unwrap_or_default(),
            price_change_percent: raw.price_change_percent.parse().unwrap_or_default(),
            weighted_avg_price: raw.weighted_avg_price.parse().unwrap_or_default(),
            prev_close_price: raw.prev_close_price.parse().unwrap_or_default(),
            last_price: raw.last_price.parse().unwrap_or_default(),
            last_qty: raw.last_qty.parse().unwrap_or_default(),
            bid_price: raw.bid_price.parse().unwrap_or_default(),
            bid_qty: raw.bid_qty.parse().unwrap_or_default(),
            ask_price: raw.ask_price.parse().unwrap_or_default(),
            ask_qty: raw.ask_qty.parse().unwrap_or_default(),
            open_price: raw.open_price.parse().unwrap_or_default(),
            high_price: raw.high_price.parse().unwrap_or_default(),
            low_price: raw.low_price.parse().unwrap_or_default(),
            volume: raw.volume.parse().unwrap_or_default(),
            quote_volume: raw.quote_volume.parse().unwrap_or_default(),
            open_time: raw.open_time,
            close_time: raw.close_time,
            first_id: raw.first_id,
            last_id: raw.last_id,
            count: raw.count,
        }
    }
}

pub fn parse_ticker_24hr(
    req: &GetTicker24hrRequest,
    data: &str,
) -> Result<Vec<Ticker24hr>, serde_json::Error> {
    if req.symbol.is_some() {
        let raw = serde_json::from_str::<Ticker24hrRaw>(data)?;
        Ok(vec![raw.into()])
    } else {
        let raw_tickers: Vec<Ticker24hrRaw> = serde_json::from_str(data)?;
        Ok(raw_tickers.into_iter().map(|raw| raw.into()).collect())
    }
}
