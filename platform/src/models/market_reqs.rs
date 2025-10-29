use crate::models::{FetchStrategy, KlineInterval};

pub struct GetKlinesRequest {
    pub fetch_strategy: FetchStrategy,
    pub symbol: String,
    pub interval: KlineInterval,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub limit: Option<u32>,
}

pub struct GetTradesRequest {
    pub fetch_strategy: FetchStrategy,
    pub symbol: String,
    pub from_id: Option<String>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub limit: Option<u32>,
}

pub struct GetDepthRequest {
    pub fetch_strategy: FetchStrategy,
    pub symbol: String,
    pub limit: Option<u32>,
}

pub struct GetExchangeInfoRequest {
    pub fetch_strategy: FetchStrategy,
    pub symbol: Option<String>,
    pub symbols: Option<Vec<String>>,
}

pub struct GetTicker24hrRequest {
    pub fetch_strategy: FetchStrategy,
    pub symbol: Option<String>,
    pub symbols: Option<Vec<String>>,
}
