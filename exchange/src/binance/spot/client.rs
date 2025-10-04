use std::sync::Arc;

use rate_limiter::RateLimiter;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::binance::{
    errors::Result,
    spot::{market_api, market_stream, trade_api, trade_stream},
};

pub struct SpotClientConfig {
    api_base_url: String,
    wss_base_url: String,
    wss_api_base_url: String,

    proxy_url: Option<String>,

    api_key: String,
    api_secret: String,

    rate_limiters: Option<Arc<Vec<RateLimiter>>>,

    target_symbols: Option<Vec<String>>,
}

pub struct SpotClient {
    config: SpotClientConfig,

    market_api: Option<market_api::MarketApi>,
    trade_api: Option<trade_api::TradeApi>,
    market_stream: Option<Arc<Mutex<market_stream::MarketStream>>>,
    trade_stream: Option<Arc<Mutex<trade_stream::TradeStream>>>,

    market_stream_shutdown_token: Option<CancellationToken>,
    trade_stream_shutdown_token: Option<CancellationToken>,
}

impl SpotClient {
    pub fn new(config: SpotClientConfig) -> Self {
        Self {
            config: config,
            market_api: None,
            trade_api: None,
            market_stream: None,
            trade_stream: None,
            market_stream_shutdown_token: None,
            trade_stream_shutdown_token: None,
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        let market_api = market_api::MarketApi::new(
            self.config.api_base_url.clone(),
            self.config.rate_limiters.clone(),
        );
        let trade_api = trade_api::TradeApi::new(
            self.config.api_base_url.clone(),
            self.config.rate_limiters.clone(),
            self.config.api_key.clone(),
            self.config.api_secret.clone(),
        );
        let mut market_stream = market_stream::MarketStream::new(
            self.config.wss_base_url.clone(),
            self.config.proxy_url.clone(),
            self.config.rate_limiters.clone(),
        );
        let mut trade_stream = trade_stream::TradeStream::new(
            self.config.wss_api_base_url.clone(),
            self.config.proxy_url.clone(),
            self.config.rate_limiters.clone(),
            self.config.api_key.clone(),
            self.config.api_secret.clone(),
        );

        let market_stream_shutdown_token = market_stream.init().await?;
        let trade_stream_shutdown_token = trade_stream.init().await?;

        self.market_api = Some(market_api);
        self.trade_api = Some(trade_api);
        self.market_stream = Some(Arc::new(Mutex::new(market_stream)));
        self.trade_stream = Some(Arc::new(Mutex::new(trade_stream)));
        self.market_stream_shutdown_token = Some(market_stream_shutdown_token);
        self.trade_stream_shutdown_token = Some(trade_stream_shutdown_token);
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        if let Some(token) = &self.market_stream_shutdown_token {
            token.cancel();
        }
        if let Some(token) = &self.trade_stream_shutdown_token {
            token.cancel();
        }
        if let Some(market_stream) = self.market_stream.as_ref() {
            let market_stream = market_stream.lock().await;
            market_stream.close().await?;
        }
        if let Some(trade_stream) = self.trade_stream.as_ref() {
            let trade_stream = trade_stream.lock().await;
            trade_stream.close().await?;
        }
        Ok(())
    }
}
