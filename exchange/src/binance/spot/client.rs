use crate::binance::{
    errors::Result,
    spot::{
        depth::Depth,
        kline::Kline,
        market_api::MarketApi,
        market_stream::MarketStream,
        models::{AggTrade, DepthUpdate},
        trade::Trade,
        trade_api::TradeApi,
        trade_stream::TradeStream,
    },
};
use arc_swap::ArcSwap;
use rate_limiter::RateLimiter;
use std::{collections::HashMap, pin::Pin, sync::Arc};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;

pub struct SpotClientConfig {
    api_base_url: String,
    wss_base_url: String,
    wss_api_base_url: String,

    proxy_url: Option<String>,

    api_key: String,
    api_secret: String,

    rate_limiters: Option<Arc<Vec<RateLimiter>>>,

    symbols: Option<Arc<Vec<String>>>,
    kline_intervals: Option<Arc<Vec<u64>>>,

    max_reconnect_attempts: u32,
    reconnect_interval_millis: u64,
}

pub struct SpotClient {
    config: Arc<SpotClientConfig>,

    market_api: Arc<MarketApi>,
    trade_api: Arc<TradeApi>,
    market_stream: ArcSwap<MarketStream>,
    trade_stream: ArcSwap<TradeStream>,

    shutdown_token: CancellationToken,

    depths: Arc<
        HashMap<
            String,
            (
                UnboundedSender<DepthUpdate>,
                UnboundedReceiver<DepthUpdate>,
                Depth,
            ),
        >,
    >,
    trades: Arc<
        HashMap<
            String,
            (
                UnboundedSender<AggTrade>,
                UnboundedReceiver<AggTrade>,
                Trade,
            ),
        >,
    >,
    klines: Arc<HashMap<(String, u64), Kline>>,
}

type Fut = Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>;
impl SpotClient {
    pub fn new(config: SpotClientConfig) -> Result<Self> {
        let market_api = MarketApi::new(
            config.api_base_url.clone(),
            config.proxy_url.clone(),
            config.rate_limiters.clone(),
        );
        market_api.init().map_err(|e| {
            crate::binance::errors::BinanceError::ClientError{
                message: format!("Failed to initialize MarketApi: {}", e)
            }
        })?;
        let trade_api = TradeApi::new(
            config.api_base_url.clone(),
            config.proxy_url.clone(),
            config.rate_limiters.clone(),
            config.api_key.clone(),
            config.api_secret.clone(),
        );
        let market_stream = ArcSwap::from_pointee(MarketStream::new(
            config.wss_base_url.clone(),
            config.proxy_url.clone(),
            config.rate_limiters.clone(),
        ));
        let trade_stream = ArcSwap::from_pointee(TradeStream::new(
            config.wss_api_base_url.clone(),
            config.proxy_url.clone(),
            config.rate_limiters.clone(),
            config.api_key.clone(),
            config.api_secret.clone(),
        ));
        let mut depths = HashMap::new();
        let mut trades = HashMap::new();
        let mut klines = HashMap::new();
        for symbol in config.symbols.as_ref().unwrap_or(&vec![]).iter() {
            let (depth_sender, depth_receiver) = tokio::sync::mpsc::unbounded_channel();
            depths.insert(
                symbol.to_string(),
                (depth_sender, depth_receiver, Depth::new(symbol.to_string())),
            );
            let (trade_sender, trade_receiver) = tokio::sync::mpsc::unbounded_channel();
            trades.insert(
                symbol.to_string(),
                (
                    trade_sender,
                    trade_receiver,
                    Trade::new(symbol.to_string(), 2048, 2, 4096),
                ),
            );
            for interval in config.kline_intervals.as_ref().unwrap_or(&vec![]).iter() {
                klines.insert(
                    (symbol.to_string(), *interval),
                    Kline::new(symbol.to_string(), *interval, 32, 2, 1024),
                );
            }
        }
        Self {
            config: Arc::new(config),
            market_api: Arc::new(market_api),
            trade_api: Arc::new(trade_api),
            market_stream,
            trade_stream,
            shutdown_token: CancellationToken::new(),
            depths: Arc::new(depths),
            trades: Arc::new(trades),
            klines: Arc::new(klines),
        }
    }

    fn get_market_stream(
        base_url: String,
        proxy_url: String,
        rate_limiters: Arc<Vec<RateLimiter>>,
        symbols: 
    ) -> MarketStream {
        unimplemented!()
    }

    pub async fn init(&self) -> Result<()> {
        // 执行：stream订阅、回调函数注册等
        unimplemented!()
    }
}
