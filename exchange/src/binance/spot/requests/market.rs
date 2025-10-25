use crate::binance::spot::models::KlineInterval;

pub struct GetKlinesRequest {
    pub symbol: String,
    pub interval: KlineInterval,

    pub start_time: Option<u64>,
    pub end_time: Option<u64>,

    pub limit: Option<u32>,
}

pub struct GetAggTradesRequest {
    pub symbol: String,
    pub from_id: Option<u64>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub limit: Option<u32>,
}

pub struct GetDepthRequest {
    pub symbol: String,
    pub limit: Option<u32>,
}

pub struct GetExchangeInfoRequest {
    pub symbol: Option<String>,
    pub symbols: Option<Vec<String>>,
}

pub struct GetTicker24hrRequest {
    pub symbol: Option<String>,
    pub symbols: Option<Vec<String>>,
}
