use crate::models::KlineInterval;

#[derive(Debug, Clone)]
pub struct GetKlinesRequest {
    pub symbol: String,
    pub interval: KlineInterval,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct GetTradesRequest {
    pub symbol: String,
    pub from_id: Option<String>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct GetDepthRequest {
    pub symbol: String,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct GetExchangeInfoRequest {
    pub symbol: Option<String>,
    pub symbols: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct GetTicker24hrRequest {
    pub symbol: Option<String>,
    pub symbols: Option<Vec<String>>,
}
