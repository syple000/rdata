use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MarketType {
    #[serde(rename = "binance_spot")]
    BinanceSpot,
}

impl MarketType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MarketType::BinanceSpot => "binance_spot",
        }
    }
}

pub enum FetchStrategy {
    CacheOnly,
    CacheOrApi,
    ApiOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SymbolStatus {
    Trading,
    Halted,
    Break,
    EndOfDay,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderType {
    Limit,
    Market,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
    LimitMaker,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    New,
    PendingNew,
    PartiallyFilled,
    Filled,
    Canceled,
    PendingCancel,
    Rejected,
    Expired,
    ExpiredInMatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum KlineInterval {
    #[serde(rename = "1s")]
    OneSecond,
    #[serde(rename = "1m")]
    OneMinute,
    #[serde(rename = "3m")]
    ThreeMinutes,
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "15m")]
    FifteenMinutes,
    #[serde(rename = "30m")]
    ThirtyMinutes,
    #[serde(rename = "1h")]
    OneHour,
    #[serde(rename = "2h")]
    TwoHours,
    #[serde(rename = "4h")]
    FourHours,
    #[serde(rename = "6h")]
    SixHours,
    #[serde(rename = "8h")]
    EightHours,
    #[serde(rename = "12h")]
    TwelveHours,
    #[serde(rename = "1d")]
    OneDay,
    #[serde(rename = "3d")]
    ThreeDays,
    #[serde(rename = "1w")]
    OneWeek,
    #[serde(rename = "1M")]
    OneMonth,
}

impl KlineInterval {
    pub fn to_millis(&self) -> u64 {
        match self {
            KlineInterval::OneSecond => 1000,
            KlineInterval::OneMinute => 60 * 1000,
            KlineInterval::ThreeMinutes => 3 * 60 * 1000,
            KlineInterval::FiveMinutes => 5 * 60 * 1000,
            KlineInterval::FifteenMinutes => 15 * 60 * 1000,
            KlineInterval::ThirtyMinutes => 30 * 60 * 1000,
            KlineInterval::OneHour => 60 * 60 * 1000,
            KlineInterval::TwoHours => 2 * 60 * 60 * 1000,
            KlineInterval::FourHours => 4 * 60 * 60 * 1000,
            KlineInterval::SixHours => 6 * 60 * 60 * 1000,
            KlineInterval::EightHours => 8 * 60 * 60 * 1000,
            KlineInterval::TwelveHours => 12 * 60 * 60 * 1000,
            KlineInterval::OneDay => 24 * 60 * 60 * 1000,
            KlineInterval::ThreeDays => 3 * 24 * 60 * 60 * 1000,
            KlineInterval::OneWeek => 7 * 24 * 60 * 60 * 1000,
            KlineInterval::OneMonth => 30 * 24 * 60 * 60 * 1000,
        }
    }
}
