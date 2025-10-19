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
        trading::Trading,
    },
};
use arc_swap::ArcSwap;
use log::error;
use rate_limiter::RateLimiter;
use std::{collections::HashMap, sync::Arc};
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

    reconnect_interval_millis: u64,

    db: sled::Db,
}

pub struct SpotClient {
    config: Arc<SpotClientConfig>,

    market_api: Arc<MarketApi>,
    trade_api: Arc<TradeApi>,
    market_stream: ArcSwap<MarketStream>,
    trade_stream: ArcSwap<TradeStream>,

    depths: Arc<HashMap<String, Arc<Depth>>>,
    klines: Arc<HashMap<(String, u64), Arc<Kline>>>,
    trades: Arc<HashMap<String, Arc<Trade>>>,
    trading: Arc<Trading>,

    shutdown_token: CancellationToken,
}

impl SpotClient {
    pub async fn new(config: Arc<SpotClientConfig>) -> Result<Arc<Self>> {
        let market_api = Self::new_market_api_client(config.clone())?;
        let trade_api = Self::new_trade_api_client(config.clone())?;

        let (depth_sender, _depth_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (trade_sender, _trade_receiver) = tokio::sync::mpsc::unbounded_channel();
        let market_stream =
            Self::new_market_stream_client(config.clone(), depth_sender, trade_sender).await?;

        let (execution_report_sender, _execution_report_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (outbound_account_position_sender, _outbound_account_position_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let trade_stream = Self::new_trade_stream_client(
            config.clone(),
            execution_report_sender,
            outbound_account_position_sender,
        )
        .await?;

        let mut depths = HashMap::new();
        let mut klines = HashMap::new();
        let mut trades = HashMap::new();
        if let Some(symbols) = config.symbols.as_ref() {
            for symbol in symbols.iter() {
                let depth = Arc::new(Depth::new(symbol, &config.db, None)?);
                depths.insert(symbol.clone(), depth);
                let trade = Arc::new(Trade::new(symbol, 2048, 1024, 2048, &config.db, None)?);
                trades.insert(symbol.clone(), trade);
                if let Some(intervals) = config.kline_intervals.as_ref() {
                    for interval in intervals.iter() {
                        let kline = Arc::new(Kline::new(
                            symbol, *interval, 2048, 1024, 2048, &config.db, None,
                        )?);
                        klines.insert((symbol.clone(), *interval), kline);
                    }
                }
            }
        }

        let trading = Trading::new(&config.db, None, None, None, None)?;

        let shutdown_token = CancellationToken::new();

        let client = Arc::new(SpotClient {
            config,
            market_api: Arc::new(market_api),
            trade_api: Arc::new(trade_api),
            market_stream: ArcSwap::from_pointee(market_stream),
            trade_stream: ArcSwap::from_pointee(trade_stream),
            depths: Arc::new(depths),
            klines: Arc::new(klines),
            trades: Arc::new(trades),
            trading: Arc::new(trading),
            shutdown_token,
        });

        // 注册各协程

        Ok(client)
    }

    fn monitor_market_stream_alive_loop(client: Arc<Self>) {
        // TODO
    }

    fn monitor_trade_stream_alive_loop(client: Arc<Self>) {
        // TODO
    }

    fn handle_depth_update_loop(client: Arc<Self>, receiver: UnboundedReceiver<DepthUpdate>) {
        // TODO
    }

    fn handle_agg_trade_loop(client: Arc<Self>, receiver: UnboundedReceiver<AggTrade>) {
        // TODO
    }

    fn handle_execution_report_loop(
        client: Arc<Self>,
        receiver: UnboundedReceiver<ExecutionReport>,
    ) {
        // TODO
    }

    fn handle_outbound_account_position_loop(
        client: Arc<Self>,
        receiver: UnboundedReceiver<OutboundAccountPosition>,
    ) {
        // TODO
    }

    fn new_market_api_client(config: Arc<SpotClientConfig>) -> Result<MarketApi> {
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
        Ok(market_api)
    }

    fn new_trade_api_client(config: Arc<SpotClientConfig>) -> Result<TradeApi> {
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
        Ok(trade_api)
    }

    async fn new_market_stream_client(
        config: Arc<SpotClientConfig>,
        depth_sender: UnboundedSender<DepthUpdate>,
        trade_sender: UnboundedSender<AggTrade>,
    ) -> Result<MarketStream> {
        let mut market_stream = MarketStream::new(
            config.wss_base_url.clone(),
            config.proxy_url.clone(),
            config.rate_limiters.clone(),
        );
        for symbol in config.symbols.as_ref().unwrap_or(&Arc::new(vec![])).iter() {
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
        Ok(market_stream)
    }

    async fn new_trade_stream_client(
        config: Arc<SpotClientConfig>,
        execution_report_sender: UnboundedSender<ExecutionReport>,
        outbound_account_position_sender: UnboundedSender<OutboundAccountPosition>,
    ) -> Result<TradeStream> {
        let mut trade_stream = TradeStream::new(
            config.wss_api_base_url.clone(),
            config.proxy_url.clone(),
            config.rate_limiters.clone(),
            config.api_key.clone(),
            config.api_secret.clone(),
        );
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
        Ok(trade_stream)
    }
}
