use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Side {
    #[serde(rename = "BUY")]
    Buy,
    #[serde(rename = "SELL")]
    Sell,
}

impl Side {
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "BUY" => Some(Side::Buy),
            "SELL" => Some(Side::Sell),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderStatus {
    #[serde(rename = "NEW")]
    New,
    #[serde(rename = "PENDING_NEW")]
    PendingNew,
    #[serde(rename = "PARTIALLY_FILLED")]
    PartiallyFilled,
    #[serde(rename = "FILLED")]
    Filled,
    #[serde(rename = "CANCELED")]
    Canceled,
    #[serde(rename = "PENDING_CANCEL")]
    PendingCancel,
    #[serde(rename = "REJECTED")]
    Rejected,
    #[serde(rename = "EXPIRED")]
    Expired,
    #[serde(rename = "EXPIRED_IN_MATCH")]
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

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "NEW" => Some(OrderStatus::New),
            "PENDING_NEW" => Some(OrderStatus::PendingNew),
            "PARTIALLY_FILLED" => Some(OrderStatus::PartiallyFilled),
            "FILLED" => Some(OrderStatus::Filled),
            "CANCELED" => Some(OrderStatus::Canceled),
            "PENDING_CANCEL" => Some(OrderStatus::PendingCancel),
            "REJECTED" => Some(OrderStatus::Rejected),
            "EXPIRED" => Some(OrderStatus::Expired),
            "EXPIRED_IN_MATCH" => Some(OrderStatus::ExpiredInMatch),
            _ => None,
        }
    }

    pub fn is_final(&self) -> bool {
        matches!(
            self,
            OrderStatus::Filled
                | OrderStatus::Canceled
                | OrderStatus::Rejected
                | OrderStatus::Expired
                | OrderStatus::ExpiredInMatch
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeInForce {
    #[serde(rename = "GTC")]
    Gtc,
    #[serde(rename = "IOC")]
    Ioc,
    #[serde(rename = "FOK")]
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

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "GTC" => Some(TimeInForce::Gtc),
            "IOC" => Some(TimeInForce::Ioc),
            "FOK" => Some(TimeInForce::Fok),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderType {
    #[serde(rename = "MARKET")]
    Market,
    #[serde(rename = "LIMIT")]
    Limit,
    #[serde(rename = "TAKE_PROFIT")]
    TakeProfit,
    #[serde(rename = "STOP_LOSS")]
    StopLoss,
    #[serde(rename = "STOP_LOSS_LIMIT")]
    StopLossLimit,
    #[serde(rename = "TAKE_PROFIT_LIMIT")]
    TakeProfitLimit,
    #[serde(rename = "LIMIT_MAKER")]
    LimitMaker,
}

impl OrderType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderType::Market => "MARKET",
            OrderType::Limit => "LIMIT",
            OrderType::TakeProfit => "TAKE_PROFIT",
            OrderType::StopLoss => "STOP_LOSS",
            OrderType::StopLossLimit => "STOP_LOSS_LIMIT",
            OrderType::TakeProfitLimit => "TAKE_PROFIT_LIMIT",
            OrderType::LimitMaker => "LIMIT_MAKER",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "MARKET" => Some(OrderType::Market),
            "LIMIT" => Some(OrderType::Limit),
            "TAKE_PROFIT" => Some(OrderType::TakeProfit),
            "STOP_LOSS" => Some(OrderType::StopLoss),
            "STOP_LOSS_LIMIT" => Some(OrderType::StopLossLimit),
            "TAKE_PROFIT_LIMIT" => Some(OrderType::TakeProfitLimit),
            "LIMIT_MAKER" => Some(OrderType::LimitMaker),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionType {
    #[serde(rename = "NEW")]
    New,
    #[serde(rename = "CANCELED")]
    Canceled,
    #[serde(rename = "REPLACED")]
    Replaced,
    #[serde(rename = "REJECTED")]
    Rejected,
    #[serde(rename = "TRADE")]
    Trade,
    #[serde(rename = "EXPIRED")]
    Expired,
    #[serde(rename = "TRADE_PREVENTION")]
    TradePrevention,
}

impl ExecutionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionType::New => "NEW",
            ExecutionType::Canceled => "CANCELED",
            ExecutionType::Replaced => "REPLACED",
            ExecutionType::Rejected => "REJECTED",
            ExecutionType::Trade => "TRADE",
            ExecutionType::Expired => "EXPIRED",
            ExecutionType::TradePrevention => "TRADE_PREVENTION",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "NEW" => Some(ExecutionType::New),
            "CANCELED" => Some(ExecutionType::Canceled),
            "REPLACED" => Some(ExecutionType::Replaced),
            "REJECTED" => Some(ExecutionType::Rejected),
            "TRADE" => Some(ExecutionType::Trade),
            "EXPIRED" => Some(ExecutionType::Expired),
            "TRADE_PREVENTION" => Some(ExecutionType::TradePrevention),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

    pub fn from_str(s: &str) -> Option<KlineInterval> {
        match s {
            "1s" => Some(KlineInterval::OneSecond),
            "1m" => Some(KlineInterval::OneMinute),
            "3m" => Some(KlineInterval::ThreeMinutes),
            "5m" => Some(KlineInterval::FiveMinutes),
            "15m" => Some(KlineInterval::FifteenMinutes),
            "30m" => Some(KlineInterval::ThirtyMinutes),
            "1h" => Some(KlineInterval::OneHour),
            "2h" => Some(KlineInterval::TwoHours),
            "4h" => Some(KlineInterval::FourHours),
            "6h" => Some(KlineInterval::SixHours),
            "8h" => Some(KlineInterval::EightHours),
            "12h" => Some(KlineInterval::TwelveHours),
            "1d" => Some(KlineInterval::OneDay),
            "3d" => Some(KlineInterval::ThreeDays),
            "1w" => Some(KlineInterval::OneWeek),
            "1M" => Some(KlineInterval::OneMonth),
            _ => None,
        }
    }

    pub fn to_millis(&self) -> u64 {
        match self {
            KlineInterval::OneSecond => 1_000,
            KlineInterval::OneMinute => 60_000,
            KlineInterval::ThreeMinutes => 3 * 60_000,
            KlineInterval::FiveMinutes => 5 * 60_000,
            KlineInterval::FifteenMinutes => 15 * 60_000,
            KlineInterval::ThirtyMinutes => 30 * 60_000,
            KlineInterval::OneHour => 60 * 60_000,
            KlineInterval::TwoHours => 2 * 60 * 60_000,
            KlineInterval::FourHours => 4 * 60 * 60_000,
            KlineInterval::SixHours => 6 * 60 * 60_000,
            KlineInterval::EightHours => 8 * 60 * 60_000,
            KlineInterval::TwelveHours => 12 * 60 * 60_000,
            KlineInterval::OneDay => 24 * 60 * 60_000,
            KlineInterval::ThreeDays => 3 * 24 * 60 * 60_000,
            KlineInterval::OneWeek => 7 * 24 * 60 * 60_000,
            KlineInterval::OneMonth => 30 * 24 * 60 * 60_000, // Approximation
        }
    }
}
