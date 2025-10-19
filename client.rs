use crate::binance::{
    errors::Result,
    spot::{
        depth::Depth,
        kline::Kline,
        market_api::MarketApi,
        market_stream::MarketStream,
        models::{
            self, Account, AggTrade, DepthUpdate, ExecutionReport, Order, OrderStatus,
            OutboundAccountPosition,
        },
        requests::{GetAccountRequest, GetAggTradesRequest, GetDepthRequest},
        trade::Trade,
        trade_api::TradeApi,
        trade_stream::TradeStream,
    },
};
use arc_swap::ArcSwap;
use log::error;
use rate_limiter::RateLimiter;
use rust_decimal::Decimal;
use scopeguard::defer;
use std::{collections::HashMap, pin::Pin, sync::Arc};
use time::get_current_milli_timestamp;
use tokio::{
    select,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        RwLock,
    },
};
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

    account: Arc<RwLock<Account>>,
    account_close_orders: Arc<RwLock<HashMap<u64, Order>>>,
    account_open_orders: Arc<RwLock<HashMap<u64, Order>>>,
    account_trades: Arc<RwLock<HashMap<u64, Vec<models::Trade>>>>,

    join_handles: Vec<tokio::task::JoinHandle<Result<()>>>,
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
        let market_api = Arc::new(market_api);

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
        let trade_api = Arc::new(trade_api);

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
        let depths = Arc::new(depths);
        let trades = Arc::new(trades);
        let klines = Arc::new(klines);

        let shutdown_token = CancellationToken::new();

        // depth处理协程
        let shutdown_token1 = shutdown_token.clone();
        let depths1 = depths.clone();
        let market_api1 = market_api.clone();
        let depth_join_handle = tokio::spawn(async move {
            let mut depth_receiver = depth_receiver;
            let market_api = market_api1.clone();
            defer!(
                shutdown_token1.cancel();
            );
            loop {
                select! {
                    _ = shutdown_token1.cancelled() => {
                        return Ok(());
                    }
                    depth_update = depth_receiver.recv() => {
                        if let None = depth_update {
                            return Ok(());
                        }
                        let depth_update = depth_update.unwrap();
                        if let Some(depth) = depths1.get(&depth_update.symbol) {
                            if let Err(_) = depth.update_by_depth_update(&depth_update).await {
                                let all_depth = market_api
                                    .get_depth(GetDepthRequest {
                                        symbol: depth_update.symbol.clone(),
                                        limit: Some(5000),
                                    })
                                    .await;
                                match all_depth {
                                    Ok(all_depth) => {
                                        depth.update_by_depth(&all_depth).await;
                                    }
                                    Err(e) => {
                                        error!("Failed to fetch depth for {}: {}", depth_update.symbol, e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        // trade处理协程
        let shutdown_token2 = shutdown_token.clone();
        let trades1 = trades.clone();
        let trade_join_handle = tokio::spawn(async move {
            let mut trade_receiver = trade_receiver;
            defer!(
                shutdown_token2.cancel();
            );
            loop {
                select! {
                    _ = shutdown_token2.cancelled() => {
                        return Ok(());
                    }
                    agg_trade = trade_receiver.recv() => {
                        if let None = agg_trade {
                            return Ok(());
                        }
                        let agg_trade = agg_trade.unwrap();
                        if let Some(trade) = trades1.get(&agg_trade.symbol) {
                            if let Ok(is_new) = trade.update(&agg_trade).await {
                                if is_new {
                                    if let Some(klines_for_symbol) =
                                        config.kline_intervals.clone().map(|intervals| {
                                            intervals
                                                .iter()
                                                .filter_map(|interval| {
                                                    klines.get(&(agg_trade.symbol.clone(), *interval))
                                                })
                                                .collect::<Vec<&Kline>>()
                                        })
                                    {
                                        for kline in klines_for_symbol {
                                            kline.update_by_trade(&agg_trade).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        // 定期获取trade的协程
        let shutdown_token3 = shutdown_token.clone();
        let trades2 = trades.clone();
        let market_api2 = market_api.clone();
        let trade_fetch_join_handle = tokio::spawn(async move {
            let latest_trade_id = None;
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
            defer!(
                shutdown_token3.cancel();
            );
            loop {
                select! {
                    _ = shutdown_token3.cancelled() => {
                        return Ok(());
                    }
                    _ = interval.tick() => {
                        for (symbol, trade) in trades2.iter() {
                            let now = get_current_milli_timestamp();
                            let trades_result = market_api2.get_agg_trades(GetAggTradesRequest {
                                symbol: symbol.clone(),
                                from_id: latest_trade_id.map(|id| id + 1),
                                start_time: Some(now - 60000),
                                end_time: Some(now),
                                limit: Some(1000),
                            }).await;
                            match trades_result {
                                Ok(trades_data) => {
                                    for agg_trade in trades_data {
                                        if let Ok(is_new) = trade.update(&agg_trade).await {
                                            if is_new {
                                                if let Some(klines_for_symbol) =
                                                    config.kline_intervals.clone().map(|intervals| {
                                                        intervals
                                                            .iter()
                                                            .filter_map(|interval| {
                                                                klines.get(&(agg_trade.symbol.clone(), *interval))
                                                            })
                                                            .collect::<Vec<&Kline>>()
                                                    })
                                                {
                                                    for kline in klines_for_symbol {
                                                        kline.update_by_trade(&agg_trade).await;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    latest_trade_id = trades_data.last().map(|t| t.agg_trade_id);
                                }
                                Err(e) => {
                                    error!("Failed to fetch agg trades for {}: {}", symbol, e);
                                }
                            }
                        }
                    }
                }
            }
        });
        // outbound_account_position处理协程
        let shutdown_token4 = shutdown_token.clone();
        let account = trade_api.get_account(GetAccountRequest {}).await?;
        let account = Arc::new(RwLock::new(account));
        let account1 = account.clone();
        let outbound_account_position_join_handle = tokio::spawn(async move {
            let mut outbound_account_position_receiver = outbound_account_position_receiver;
            defer!(
                shutdown_token4.cancel();
            );
            loop {
                select! {
                    _ = shutdown_token4.cancelled() => {
                        return Ok(());
                    }
                    outbound_account_position = outbound_account_position_receiver.recv() => {
                        if let None = outbound_account_position {
                            return Ok(());
                        }
                        let outbound_account_position = outbound_account_position.unwrap();
                        let mut account = account1.write().await;
                        for balance in account.balances.iter_mut() {
                            if let Some(new_balance) = outbound_account_position.balances.iter().find(|b| b.asset == balance.asset) {
                                balance.free = new_balance.free;
                                balance.locked = new_balance.locked;
                            }
                        }
                        account.update_time = outbound_account_position.update_time;
                    }
                }
            }
        });
        // 定期获取account的协程
        let shutdown_token5 = shutdown_token.clone();
        let trade_api1 = trade_api.clone();
        let account2 = account.clone();
        let account_fetch_join_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
            defer!(
                shutdown_token5.cancel();
            );
            loop {
                select! {
                    _ = shutdown_token5.cancelled() => {
                        return Ok(());
                    }
                    _ = interval.tick() => {
                        match trade_api1.get_account(GetAccountRequest {}).await {
                            Ok(new_account) => {
                                let mut account = account2.write().await;
                                *account = new_account;
                            }
                            Err(e) => {
                                error!("Failed to fetch account info: {}", e);
                            }
                        }
                    }
                }
            }
        });
        // execution_report处理协程
        // 定期获取order/trade的协程
        // 重连协程

        Ok(Self {
            config: Arc::new(config),
            market_api: market_api,
            trade_api: trade_api,
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
