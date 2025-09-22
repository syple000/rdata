pub enum KlineInterval {
    OneSecond,
    OneMinute,
    ThreeMinutes,
    FiveMinutes,
    FifteenMinutes,
    ThirtyMinutes,
    OneHour,
    TwoHours,
    FourHours,
    SixHours,
    EightHours,
    TwelveHours,
    OneDay,
    ThreeDays,
    OneWeek,
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

    pub fn to_millis(&self) -> u128 {
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

pub struct GetKlinesRequest {
    pub symbol: String,
    pub interval: KlineInterval,

    pub start_time: Option<u128>,
    pub end_time: Option<u128>,

    pub limit: Option<u32>,
}
