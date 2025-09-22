use super::super::models::market::*;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct KlineDataRaw(
    pub u128,    // open time
    pub Decimal, // open price
    pub Decimal, // high price
    pub Decimal, // low price
    pub Decimal, // close price
    pub Decimal, // volume
    pub u128,    // close time
    pub Decimal, // quote volume
    pub u128,    // number of trades
    pub Decimal, // taker buy volume
    pub Decimal, // taker buy quote
    pub Value,   // ignore this field
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
    E: u128,                           // 事件时间
    U: u128,                           // 首次更新ID
    u: u128,                           // 最后更新ID
    b: Vec<(Decimal, Decimal, Value)>, // 买方深度
    a: Vec<(Decimal, Decimal, Value)>, // 卖方深度
}

impl From<(String, DepthUpdateRaw)> for DepthUpdate {
    fn from((symbol, raw): (String, DepthUpdateRaw)) -> Self {
        DepthUpdate {
            symbol,
            first_update_id: raw.U,
            last_update_id: raw.u,
            bids: raw
                .b
                .into_iter()
                .map(|(price, quantity, _)| PriceLevel { price, quantity })
                .collect(),
            asks: raw
                .a
                .into_iter()
                .map(|(price, quantity, _)| PriceLevel { price, quantity })
                .collect(),
            timestamp: raw.E,
        }
    }
}

pub fn parse_depth_update(symbol: String, data: &str) -> Result<DepthUpdate, serde_json::Error> {
    let raw: DepthUpdateRaw = serde_json::from_str(data)?;
    Ok((symbol, raw).into())
}
