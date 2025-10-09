use crate::binance::{
    errors::Result,
    spot::{
        depth::Depth,
        kline::Kline,
        market_api::MarketApi,
        market_stream::MarketStream,
        models::{AggTrade, DepthUpdate, ExecutionReport, OutboundAccountPosition},
        trade::Trade,
        trade_api::TradeApi,
        trade_stream::TradeStream,
    },
};
use arc_swap::ArcSwap;
use log::error;
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

    depth_sender: UnboundedSender<DepthUpdate>,
    trade_sender: UnboundedSender<AggTrade>,
    execution_report_sender: UnboundedSender<ExecutionReport>,
    outbound_account_position_sender: UnboundedSender<OutboundAccountPosition>,

    depths: Arc<HashMap<String, Depth>>,
    trades: Arc<HashMap<String, Trade>>,
    klines: Arc<HashMap<(String, u64), Kline>>,
}

type Fut = Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>;
impl SpotClient {
    pub async fn new(config: SpotClientConfig) -> Result<Self> {
        let mut market_api = MarketApi::new(
            config.api_base_url.clone(),
            config.proxy_url.clone(),
            config.rate_limiters.clone(),
        );
        market_api
            .init()
            .map_err(|e| crate::binance::errors::BinanceError::ClientError {
                message: format!("Failed to initialize MarketApi: {}", e),
            })?;

        let mut trade_api = TradeApi::new(
            config.api_base_url.clone(),
            config.proxy_url.clone(),
            config.rate_limiters.clone(),
            config.api_key.clone(),
            config.api_secret.clone(),
        );
        trade_api
            .init()
            .map_err(|e| crate::binance::errors::BinanceError::ClientError {
                message: format!("Failed to initialize TradeApi: {}", e),
            })?;

        let (depth_sender, depth_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (trade_sender, trade_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (execution_report_sender, execution_report_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (outbound_account_position_sender, outbound_account_position_receiver) =
            tokio::sync::mpsc::unbounded_channel();

        let market_stream = ArcSwap::from_pointee(
            Self::get_market_stream(
                config.wss_base_url.clone(),
                config.proxy_url.clone(),
                config.rate_limiters.clone(),
                config.symbols.clone(),
                depth_sender.clone(),
                trade_sender.clone(),
            )
            .await
            .map_err(|e| crate::binance::errors::BinanceError::ClientError {
                message: format!("Failed to initialize MarketStream: {}", e),
            })?,
        );

        let trade_stream = ArcSwap::from_pointee(
            Self::get_trade_stream(
                config.wss_api_base_url.clone(),
                config.proxy_url.clone(),
                config.rate_limiters.clone(),
                config.api_key.clone(),
                config.api_secret.clone(),
                execution_report_sender.clone(),
                outbound_account_position_sender.clone(),
            )
            .await
            .map_err(|e| crate::binance::errors::BinanceError::ClientError {
                message: format!("Failed to initialize TradeStream: {}", e),
            })?,
        );

        let mut depths = HashMap::new();
        let mut trades = HashMap::new();
        let mut klines = HashMap::new();
        for symbol in config.symbols.clone().unwrap_or(Arc::new(vec![])).iter() {
            depths.insert(symbol.to_string(), Depth::new(symbol.to_string()));
            trades.insert(
                symbol.to_string(),
                Trade::new(symbol.to_string(), 1024, 2, 8192),
            );
            for interval in config
                .kline_intervals
                .clone()
                .unwrap_or(Arc::new(vec![]))
                .iter()
            {
                klines.insert(
                    (symbol.to_string(), *interval),
                    Kline::new(symbol.to_string(), *interval, 32, 2, 1024),
                );
            }
        }

        // depth处理协程
        // trade处理协程
        // execution_report处理协程
        // outbound_account_position处理协程
        // 重连协程

        Ok(Self {
            config: Arc::new(config),
            market_api: Arc::new(market_api),
            trade_api: Arc::new(trade_api),
            market_stream,
            trade_stream,
            shutdown_token: CancellationToken::new(),
            depth_sender,
            trade_sender,
            execution_report_sender,
            outbound_account_position_sender,
            depths: Arc::new(depths),
            trades: Arc::new(trades),
            klines: Arc::new(klines),
        })
    }

    async fn get_market_stream(
        base_url: String,
        proxy_url: Option<String>,
        rate_limiters: Option<Arc<Vec<RateLimiter>>>,
        symbols: Option<Arc<Vec<String>>>,
        depth_sender: UnboundedSender<DepthUpdate>,
        trade_sender: UnboundedSender<AggTrade>,
    ) -> Result<MarketStream> {
        let mut market_stream = MarketStream::new(base_url, proxy_url, rate_limiters);
        for symbol in symbols.unwrap_or(Arc::new(vec![])).iter() {
            market_stream.subscribe_depth_update(symbol);
            market_stream.subscribe_agg_trade(symbol);
        }
        market_stream.register_depth_update_callback(move |depth_update: DepthUpdate| {
            let sender = depth_sender.clone();
            Box::pin(async move {
                if let Err(e) = sender.send(depth_update) {
                    error!("Failed to send depth update: {}", e);
                }
                Ok(())
            })
        });
        market_stream.register_agg_trade_callback(move |agg_trade: AggTrade| {
            let sender = trade_sender.clone();
            Box::pin(async move {
                if let Err(e) = sender.send(agg_trade) {
                    error!("Failed to send agg trade: {}", e);
                }
                Ok(())
            })
        });

        market_stream.init().await?;
        return Ok(market_stream);
    }

    async fn get_trade_stream(
        base_url: String,
        proxy_url: Option<String>,
        rate_limiters: Option<Arc<Vec<RateLimiter>>>,
        api_key: String,
        api_secret: String,
        execution_report_sender: UnboundedSender<ExecutionReport>,
        outbound_account_position_sender: UnboundedSender<OutboundAccountPosition>,
    ) -> Result<TradeStream> {
        let mut trade_stream =
            TradeStream::new(base_url, proxy_url, rate_limiters, api_key, api_secret);
        trade_stream.register_execution_report_callback(
            move |execution_report: ExecutionReport| {
                let sender = execution_report_sender.clone();
                Box::pin(async move {
                    if let Err(e) = sender.send(execution_report) {
                        error!("Failed to send execution report: {}", e);
                    }
                    Ok(())
                })
            },
        );
        trade_stream.register_outbound_account_position_callback(
            move |outbound_account_position: OutboundAccountPosition| {
                let sender = outbound_account_position_sender.clone();
                Box::pin(async move {
                    if let Err(e) = sender.send(outbound_account_position) {
                        error!("Failed to send outbound account position: {}", e);
                    }
                    Ok(())
                })
            },
        );
        trade_stream.init().await?;
        return Ok(trade_stream);
    }
}
