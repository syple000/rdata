use super::super::errors::*;
use super::super::utils::*;
use super::parser::*;
use super::requests::market::*;
use super::responses::market::*;
use log::error;
use rate_limiter::RateLimiter;
use std::sync::Arc;
use std::time::Duration;

pub struct MarketApi {
    client: Option<reqwest::Client>,
    base_url: String,
    proxy_url: Option<String>,
    rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    timeout_milli_secs: u64,
}

impl MarketApi {
    pub fn new(
        base_url: String,
        proxy_url: Option<String>,
        rate_limiters: Option<Arc<Vec<RateLimiter>>>,
        timeout_milli_secs: u64,
    ) -> Self {
        MarketApi {
            client: None,
            base_url,
            proxy_url,
            rate_limiters: rate_limiters,
            timeout_milli_secs,
        }
    }

    pub fn init(&mut self) -> Result<()> {
        let client_builder = reqwest::Client::builder();

        let client = if let Some(proxy_url) = &self.proxy_url {
            client_builder
                .proxy(reqwest::Proxy::all(proxy_url).map_err(|e| {
                    crate::binance::errors::BinanceError::ParametersInvalid {
                        message: format!("proxy url invalid: {}, error: {}", proxy_url, e),
                    }
                })?)
                .build()
                .map_err(
                    |e| crate::binance::errors::BinanceError::ParametersInvalid {
                        message: format!(
                            "build client with proxy url: {} failed: {}",
                            proxy_url, e
                        ),
                    },
                )?
        } else {
            client_builder.build().map_err(|e| {
                crate::binance::errors::BinanceError::ParametersInvalid {
                    message: format!("build client failed: {}", e),
                }
            })?
        };

        self.client = Some(client);
        Ok(())
    }

    pub async fn get_klines(&self, req: GetKlinesRequest) -> Result<GetKlinesResponse> {
        let mut params = vec![
            ("symbol", req.symbol.clone()),
            ("interval", req.interval.as_str().to_string()),
            ("limit", req.limit.unwrap_or(500).to_string()),
        ];
        if let Some(start_time) = req.start_time {
            params.push(("startTime", start_time.to_string()));
        }
        if let Some(end_time) = req.end_time {
            params.push(("endTime", end_time.to_string()));
        }

        let text = self
            .send_request(reqwest::Method::GET, "/api/v3/klines", params, 2)
            .await?;

        let mut klines =
            parse_klines(req.symbol.clone(), req.interval.clone(), &text).map_err(|e| {
                error!("Parse result: {:?} error: {:?}", text, e);
                BinanceError::ParseResultError {
                    message: e.to_string(),
                }
            })?;
        klines.sort_by(|a, b| a.open_time.cmp(&b.open_time));

        Ok(klines)
    }

    pub async fn get_agg_trades(&self, req: GetAggTradesRequest) -> Result<GetAggTradesResponse> {
        let mut params = vec![
            ("symbol", req.symbol.clone()),
            ("limit", req.limit.unwrap_or(500).to_string()),
        ];
        if let Some(from_id) = req.from_id {
            params.push(("fromId", from_id.to_string()));
        }
        if let Some(start_time) = req.start_time {
            params.push(("startTime", start_time.to_string()));
        }
        if let Some(end_time) = req.end_time {
            params.push(("endTime", end_time.to_string()));
        }

        let text = self
            .send_request(reqwest::Method::GET, "/api/v3/aggTrades", params, 4)
            .await?;

        let mut trades = parse_agg_trades(req.symbol.clone(), &text).map_err(|e| {
            error!("Parse result: {:?} error: {:?}", text, e);
            BinanceError::ParseResultError {
                message: e.to_string(),
            }
        })?;

        trades.sort_by(|a, b| a.agg_trade_id.cmp(&b.agg_trade_id));

        Ok(trades)
    }

    pub async fn get_depth(&self, req: GetDepthRequest) -> Result<GetDepthResponse> {
        let mut params = vec![("symbol", req.symbol.clone())];

        if let Some(limit) = req.limit {
            params.push(("limit", limit.to_string()));
        }

        // 根据limit计算权重
        let weight = match req.limit.unwrap_or(100) {
            1..=100 => 5,
            101..=500 => 25,
            501..=1000 => 50,
            1001..=5000 => 250,
            _ => 250,
        };

        let text = self
            .send_request(reqwest::Method::GET, "/api/v3/depth", params, weight)
            .await?;

        parse_depth(req.symbol.clone(), &text).map_err(|e| {
            error!("Parse result: {:?} error: {:?}", text, e);
            BinanceError::ParseResultError {
                message: e.to_string(),
            }
        })
    }

    pub async fn get_exchange_info(
        &self,
        req: GetExchangeInfoRequest,
    ) -> Result<GetExchangeInfoResponse> {
        let mut params = Vec::new();

        if let Some(symbol) = req.symbol {
            params.push(("symbol", symbol));
        } else if let Some(symbols) = req.symbols {
            let symbols_json = serde_json::to_string(&symbols).map_err(|e| {
                error!("Serialize symbols error: {:?}", e);
                BinanceError::ParseResultError {
                    message: e.to_string(),
                }
            })?;
            params.push(("symbols", symbols_json));
        }

        let text = self
            .send_request(reqwest::Method::GET, "/api/v3/exchangeInfo", params, 20)
            .await?;

        parse_exchange_info(&text).map_err(|e| {
            error!("Parse result: {:?} error: {:?}", text, e);
            BinanceError::ParseResultError {
                message: e.to_string(),
            }
        })
    }

    pub async fn get_ticker_24hr(
        &self,
        req: GetTicker24hrRequest,
    ) -> Result<GetTicker24hrResponse> {
        // 根据请求参数计算权重
        let weight = if req.symbol.is_some() {
            2
        } else if let Some(ref symbols) = req.symbols {
            let count = symbols.len();
            match count {
                1..=20 => 2,
                21..=100 => 40,
                _ => 80,
            }
        } else {
            80 // 不提供symbol参数
        };

        let mut params = Vec::new();

        if let Some(symbol) = &req.symbol {
            params.push(("symbol", symbol.to_string()));
        } else if let Some(symbols) = &req.symbols {
            let symbols_json = serde_json::to_string(symbols).map_err(|e| {
                error!("Serialize symbols error: {:?}", e);
                BinanceError::ParseResultError {
                    message: e.to_string(),
                }
            })?;
            params.push(("symbols", symbols_json));
        }

        let text = self
            .send_request(reqwest::Method::GET, "/api/v3/ticker/24hr", params, weight)
            .await?;

        parse_ticker_24hr(&req, &text).map_err(|e| {
            error!("Parse result: {:?} error: {:?}", text, e);
            BinanceError::ParseResultError {
                message: e.to_string(),
            }
        })
    }

    async fn send_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        mut params: Vec<(&str, String)>,
        weight: u64,
    ) -> Result<String> {
        if let None = &self.client {
            return Err(BinanceError::ParametersInvalid {
                message: "client is not initialized, please call init() first".to_string(),
            });
        }
        let client = self.client.as_ref().unwrap();

        sort_params(&mut params);

        if let Some(rate_limiters) = &self.rate_limiters {
            for rl in rate_limiters.iter() {
                _ = rl.wait(weight).await;
            }
        }

        let resp = match method {
            reqwest::Method::GET => {
                client
                    .get(format!("{}{}", self.base_url, endpoint))
                    .query(&params)
                    .timeout(Duration::from_millis(self.timeout_milli_secs))
                    .send()
                    .await
            }
            reqwest::Method::POST => {
                client
                    .post(format!("{}{}", self.base_url, endpoint))
                    .query(&params)
                    .timeout(Duration::from_millis(self.timeout_milli_secs))
                    .send()
                    .await
            }
            reqwest::Method::DELETE => {
                client
                    .delete(format!("{}{}", self.base_url, endpoint))
                    .query(&params)
                    .timeout(Duration::from_millis(self.timeout_milli_secs))
                    .send()
                    .await
            }
            _ => {
                return Err(BinanceError::ParametersInvalid {
                    message: format!("unsupported http method: {}", method),
                })
            }
        };

        let resp = resp.map_err(|e| {
            error!("Network error: {:?}", e);
            BinanceError::NetworkError {
                message: e.to_string(),
            }
        })?;
        if resp.status() != reqwest::StatusCode::OK {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            error!("Response error: status: {}, text: {}", status, text);
            return Err(BinanceError::ParseResultError {
                message: format!("status: {}, text: {}", status, text),
            });
        }
        let text = resp.text().await.map_err(|e| {
            error!("Network error: {:?}", e);
            BinanceError::NetworkError {
                message: e.to_string(),
            }
        })?;

        Ok(text)
    }
}
