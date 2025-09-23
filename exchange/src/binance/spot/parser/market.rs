use super::super::models::market::*;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct KlineDataRaw(
    u128,    // open time
    Decimal, // open price
    Decimal, // high price
    Decimal, // low price
    Decimal, // close price
    Decimal, // volume
    u128,    // close time
    Decimal, // quote volume
    u128,    // number of trades
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
    timestamp: u128, // 事件时间
    #[serde(rename = "U")]
    first_update_id: u128, // 首次更新ID
    #[serde(rename = "u")]
    last_update_id: u128, // 最后更新ID
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
