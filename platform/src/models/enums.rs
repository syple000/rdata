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

impl SymbolStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SymbolStatus::Trading => "TRADING",
            SymbolStatus::Halted => "HALTED",
            SymbolStatus::Break => "BREAK",
            SymbolStatus::EndOfDay => "END_OF_DAY",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
    }
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

impl OrderType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderType::Limit => "LIMIT",
            OrderType::Market => "MARKET",
            OrderType::StopLoss => "STOP_LOSS",
            OrderType::StopLossLimit => "STOP_LOSS_LIMIT",
            OrderType::TakeProfit => "TAKE_PROFIT",
            OrderType::TakeProfitLimit => "TAKE_PROFIT_LIMIT",
            OrderType::LimitMaker => "LIMIT_MAKER",
        }
    }
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

impl OrderStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderStatus::New => "NEW",
            OrderStatus::PendingNew => "PENDING_NEW",
            OrderStatus::PartiallyFilled => "PARTIALLY_FILLED",
            OrderStatus::Filled => "FILLED",
            OrderStatus::Canceled => "CANCELED",
            OrderStatus::PendingCancel => "PENDING_CANCEL",
            OrderStatus::Rejected => "REJECTED",
            OrderStatus::Expired => "EXPIRED",
            OrderStatus::ExpiredInMatch => "EXPIRED_IN_MATCH",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
}

impl TimeInForce {
    pub fn as_str(&self) -> &'static str {
        match self {
            TimeInForce::Gtc => "GTC",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Fok => "FOK",
        }
    }
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

    pub fn as_str(&self) -> &'static str {
        match self {
            KlineInterval::OneSecond => "1s",
            KlineInterval::OneMinute => "1m",
            KlineInterval::ThreeMinutes => "3m",
            KlineInterval::FiveMinutes => "5m",
            KlineInterval::FifteenMinutes => "15m",
            KlineInterval::ThirtyMinutes => "30m",
            KlineInterval::OneHour => "1h",
            KlineInterval::TwoHours => "2h",
            KlineInterval::FourHours => "4h",
            KlineInterval::SixHours => "6h",
            KlineInterval::EightHours => "8h",
            KlineInterval::TwelveHours => "12h",
            KlineInterval::OneDay => "1d",
            KlineInterval::ThreeDays => "3d",
            KlineInterval::OneWeek => "1w",
            KlineInterval::OneMonth => "1M",
        }
    }
}
