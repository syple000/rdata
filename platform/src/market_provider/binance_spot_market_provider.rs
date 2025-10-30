use crate::{
    config::Config,
    errors::{PlatformError, Result},
    market_provider::MarketProvider,
    models::{DepthData, ExchangeInfo, KlineData, KlineInterval, PriceLevel, Ticker24hr, Trade},
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use exchange::binance::spot::{
    market_api::MarketApi,
    market_stream::MarketStream,
    models::{self, AggTrade},
    requests::{GetAggTradesRequest, GetDepthRequest, GetExchangeInfoRequest, GetKlinesRequest},
};
use log::error;
use rate_limiter::RateLimiter;
use rust_decimal::Decimal;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use time::LatencyGuard;
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;
use ws::WsError;

struct DepthState {
    symbol: String,
    last_update_id: u64,
    bids: BTreeMap<Decimal, PriceLevel>,
    asks: BTreeMap<Decimal, PriceLevel>,
    timestamp: u64,
}

impl DepthState {
    pub fn from_depth(depth: &models::DepthData) -> Self {
        let _lg = LatencyGuard::new("BinanceSpotDepthState::from_depth");

        let _lg_1 = LatencyGuard::new("BinanceSpotDepthState::from_depth::asks_bids");
        let bids = depth
            .bids
            .iter()
            .map(|bid| {
                (
                    bid.price.clone(),
                    PriceLevel {
                        price: bid.price.clone(),
                        quantity: bid.quantity.clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let asks = depth
            .asks
            .iter()
            .map(|ask| {
                (
                    ask.price.clone(),
                    PriceLevel {
                        price: ask.price.clone(),
                        quantity: ask.quantity.clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        drop(_lg_1);

        let last_update_id = depth.last_update_id;
        let timestamp = depth.timestamp;

        Self {
            symbol: depth.symbol.clone(),
            last_update_id,
            bids,
            asks,
            timestamp,
        }
    }

    pub fn update_depth(&mut self, update: &models::DepthUpdate) -> Result<bool> {
        let _lg = LatencyGuard::new("BinanceSpotDepthState::update");
        if self.symbol != update.symbol {
            return Err(PlatformError::MarketProviderError {
                message: format!(
                    "Depth update symbol mismatch. Expected: {}, got: {}",
                    self.symbol, update.symbol
                ),
            });
        }

        if update.last_update_id <= self.last_update_id {
            return Ok(false);
        }

        if update.first_update_id <= self.last_update_id + 1 {
            let _lg = LatencyGuard::new("BinanceSpotDepthState::update::apply_update");
            for bid in update.bids.iter() {
                if bid.quantity.is_zero() {
                    self.bids.remove(&bid.price);
                } else {
                    self.bids.insert(bid.price.clone(), bid.clone().into());
                }
            }
            for ask in update.asks.iter() {
                if ask.quantity.is_zero() {
                    self.asks.remove(&ask.price);
                } else {
                    self.asks.insert(ask.price.clone(), ask.clone().into());
                }
            }
            self.last_update_id = update.last_update_id;
            self.timestamp = update.timestamp;
            drop(_lg);

            return Ok(true);
        }

        Err(PlatformError::MarketProviderError {
            message: format!(
                "Depth update out of order. Last update id: {}, update first id: {}",
                self.last_update_id, update.first_update_id
            ),
        })
    }

    pub fn depth(&self) -> DepthData {
        DepthData {
            symbol: self.symbol.clone(),
            bids: self.bids.values().rev().cloned().collect(),
            asks: self.asks.values().cloned().collect(),
            timestamp: self.timestamp,
        }
    }
}

pub struct BinanceSpotMarketProvider {
    config: Arc<Config>,
    api_rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    stream_rate_limiters: Option<Arc<Vec<RateLimiter>>>,

    market_api: Option<Arc<MarketApi>>,
    market_stream: Option<Arc<ArcSwap<MarketStream>>>, // stream当前订阅后初始化，不支持后续订阅或取消订阅

    kline_sender: broadcast::Sender<KlineData>,
    kline_receiver: broadcast::Receiver<KlineData>,
    trade_sender: broadcast::Sender<Trade>,
    trade_receiver: broadcast::Receiver<Trade>,
    depth_sender: broadcast::Sender<DepthData>,
    depth_receiver: broadcast::Receiver<DepthData>,
    ticker_sender: broadcast::Sender<Ticker24hr>,
    ticker_receiver: broadcast::Receiver<Ticker24hr>,
}

impl BinanceSpotMarketProvider {
    pub fn new(config: Arc<Config>) -> Result<Self> {
        let api_rate_limits: Option<Vec<(u64, u64)>> =
            config.get("binance.spot.api_rate_limits").ok();
        let api_rate_limiters = api_rate_limits.map(|limits| {
            Arc::new(
                limits
                    .into_iter()
                    .map(|(duration, limit)| {
                        RateLimiter::new(Duration::from_millis(duration), limit)
                    })
                    .collect::<Vec<_>>(),
            )
        });

        let stream_rate_limits: Option<Vec<(u64, u64)>> =
            config.get("binance.spot.stream_rate_limits").ok();
        let stream_rate_limiters = stream_rate_limits.map(|limits| {
            Arc::new(
                limits
                    .into_iter()
                    .map(|(duration, limit)| {
                        RateLimiter::new(Duration::from_millis(duration), limit)
                    })
                    .collect::<Vec<_>>(),
            )
        });

        let kline_chan_cap = config
            .get("binance.spot.kline_event_channel_capacity")
            .unwrap_or(5000);
        let trade_chan_cap = config
            .get("binance.spot.trade_event_channel_capacity")
            .unwrap_or(5000);
        let depth_chan_cap = config
            .get("binance.spot.depth_event_channel_capacity")
            .unwrap_or(5000);
        let ticker_chan_cap = config
            .get("binance.spot.ticker_event_channel_capacity")
            .unwrap_or(5000);

        let (kline_sender, kline_receiver) = broadcast::channel(kline_chan_cap);
        let (trade_sender, trade_receiver) = broadcast::channel(trade_chan_cap);
        let (depth_sender, depth_receiver) = broadcast::channel(depth_chan_cap);
        let (ticker_sender, ticker_receiver) = broadcast::channel(ticker_chan_cap);

        Ok(Self {
            config,
            api_rate_limiters,
            stream_rate_limiters,
            market_api: None,
            market_stream: None,
            kline_sender,
            kline_receiver,
            trade_sender,
            trade_receiver,
            depth_sender,
            depth_receiver,
            ticker_sender,
            ticker_receiver,
        })
    }
}

fn create_market_api(
    config: Arc<Config>,
    rate_limiters: Option<Arc<Vec<RateLimiter>>>,
) -> Result<MarketApi> {
    let base_url: String =
        config
            .get("binance.spot.api_base_url")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get api_base_url: {}", e),
            })?;

    let proxy_url: Option<String> = config.get("proxy.url").ok();

    let mut market_api = MarketApi::new(base_url, proxy_url, rate_limiters);
    market_api
        .init()
        .map_err(|e| PlatformError::MarketProviderError {
            message: format!("Failed to init market_api: {}", e),
        })?;

    Ok(market_api)
}

async fn create_market_stream(
    market_api: Arc<MarketApi>,
    config: Arc<Config>,
    rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    kline_sender: broadcast::Sender<KlineData>,
    trade_sender: broadcast::Sender<Trade>,
    depth_sender: broadcast::Sender<DepthData>,
    ticker_sender: broadcast::Sender<Ticker24hr>,
) -> Result<MarketStream> {
    let stream_base_url: String =
        config
            .get("binance.spot.stream_base_url")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get stream_base_url: {}", e),
            })?;
    let proxy_url: Option<String> = config.get("proxy.url").ok();

    let mut market_stream = MarketStream::new(stream_base_url, proxy_url, rate_limiters);

    let subscribed_symbols = config
        .get::<Vec<String>>("binance.spot.subscribed_symbols")
        .map_err(|e| PlatformError::ConfigError {
            message: format!("Failed to get subscribed_symbols: {}", e),
        })?;
    let subscribed_kline_intervals = config
        .get::<Vec<KlineInterval>>("binance.spot.subscribed_kline_intervals")
        .map_err(|e| PlatformError::ConfigError {
            message: format!("Failed to get subscribed_kline_intervals: {}", e),
        })?;
    for symbol in subscribed_symbols.iter() {
        market_stream.subscribe_agg_trade(symbol);
        market_stream.subscribe_depth_update(symbol);
        market_stream.subscribe_ticker(symbol);
        for interval in subscribed_kline_intervals.iter() {
            market_stream.subscribe_kline(symbol, &interval.clone().into());
        }
    }

    market_stream.register_agg_trade_callback(move |trade| {
        let trade_sender_clone = trade_sender.clone();
        Box::pin(async move {
            let _ = trade_sender_clone
                .send(trade.into())
                .map_err(|e| WsError::HandleError {
                    message: format!("Failed to send trade event: {}", e),
                })?;
            Ok(())
        })
    });
    market_stream.register_kline_callback(move |kline| {
        let kline_sender_clone = kline_sender.clone();
        Box::pin(async move {
            let _ = kline_sender_clone
                .send(kline.into())
                .map_err(|e| WsError::HandleError {
                    message: format!("Failed to send kline event: {}", e),
                })?;
            Ok(())
        })
    });
    market_stream.register_ticker_callback(move |ticker| {
        let ticker_sender_clone = ticker_sender.clone();
        Box::pin(async move {
            let _ = ticker_sender_clone
                .send(ticker.into())
                .map_err(|e| WsError::HandleError {
                    message: format!("Failed to send ticker event: {}", e),
                })?;
            Ok(())
        })
    });
    // websocket都区分symbol发送到独立的channel
    // 每一个独立的channel单独运行在一个协程中处理并发送到depth chan
    let depth_cache_chan_cap = config
        .get("binance.spot.depth_cache_channel_capacity")
        .unwrap_or(5000);
    let depth_updates: Arc<
        HashMap<
            String,
            (
                Arc<RwLock<Option<DepthState>>>,
                broadcast::Sender<models::DepthUpdate>,
                broadcast::Receiver<models::DepthUpdate>,
            ),
        >,
    > = Arc::new(
        subscribed_symbols
            .iter()
            .map(|s| {
                let (sender, receiver) =
                    broadcast::channel::<models::DepthUpdate>(depth_cache_chan_cap);
                (
                    s.to_string(),
                    (Arc::new(RwLock::new(None)), sender, receiver),
                )
            })
            .collect::<HashMap<_, _>>(),
    );
    market_stream.register_depth_update_callback(move |update| {
        let depth_updates_clone = depth_updates.clone();
        Box::pin(async move {
            if !depth_updates_clone.contains_key(&update.symbol) {
                return Err(WsError::HandleError {
                    message: format!("Failed to find depth update symbol: {}", &update.symbol),
                });
            }
            let _ = depth_updates_clone[&update.symbol]
                .1
                .send(update.into())
                .map_err(|e| WsError::HandleError {
                    message: format!("Failed to send depth update event: {}", e),
                })?;
            Ok(())
        })
    });

    let init_latency_guard = time::LatencyGuard::new("BinanceSpotMarketStream::init");
    let shutdown_token =
        market_stream
            .init()
            .await
            .map_err(|e| PlatformError::MarketProviderError {
                message: format!("Failed to init market_stream: {}", e),
            })?;
    drop(init_latency_guard);

    for (symbol, (state_lock, _, receiver)) in depth_updates.iter() {
        let market_api = market_api.clone();
        let depth_sender = depth_sender.clone();
        let shutdown_token = shutdown_token.clone();
        let state_lock = state_lock.clone();
        let mut receiver = receiver.resubscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_token_clone.cancelled() => {
                        break;
                    }
                    recv_result = receiver.recv() => {
                        let update = match recv_result {
                            Ok(update) => update,
                            Err(e) => {
                                error!("Depth update receive error for symbol {}: {}", symbol, e);
                                continue;
                            }
                        };
                        let mut state_guard = state_lock.write().await;
                    }
                }
            }
        });
    }

    Ok(market_stream)
}

#[async_trait]
impl MarketProvider for BinanceSpotMarketProvider {
    async fn init(&mut self) -> Result<()> {
        let _lg = LatencyGuard::new("BinanceSpotMarketProvider::init");
        let market_api = Arc::new(create_market_api(
            self.config.clone(),
            self.api_rate_limiters.clone(),
        )?);

        let market_stream = create_market_stream(
            market_api.clone(),
            self.config.clone(),
            self.symbols.clone(),
            self.kline_intervals.clone(),
            self.stream_rate_limiters.clone(),
            self.sender.clone(),
            self.klines.clone(),
            self.trades.clone(),
            self.depth.clone(),
            self.ticker_24hr.clone(),
        )
        .await?;

        self.market_api = Some(market_api);
        self.market_stream = Some(Arc::new(ArcSwap::from_pointee(market_stream)));

        let get_exchange_info = async |market_api: Arc<MarketApi>| -> Result<ExchangeInfo> {
            let exchange_info = market_api
                .get_exchange_info(GetExchangeInfoRequest {
                    symbol: None,
                    symbols: None,
                })
                .await
                .map_err(|e| PlatformError::MarketProviderError {
                    message: format!("Failed to get exchange info: {}", e),
                })?
                .into();
            Ok(exchange_info)
        };
        let get_exchange_info_latency_guard =
            time::LatencyGuard::new("BinanceSpotMarketProvider::get_exchange_info");
        let exchange_info = get_exchange_info(self.market_api.as_ref().unwrap().clone()).await?;
        self.exchange_info = Some(Arc::new(ArcSwap::from_pointee(exchange_info)));
        drop(get_exchange_info_latency_guard);

        // 定期刷新exchange_info
        let market_api = self.market_api.as_ref().unwrap().clone();
        let exchange_info = self.exchange_info.as_ref().unwrap().clone();
        let shutdown_token = self.shutdown_token.clone();
        let refresh_interval: u64 = self
            .config
            .get("binance.spot.exchange_info_refresh_interval_ms")
            .unwrap_or(300000);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(refresh_interval));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        match get_exchange_info(market_api.clone()).await {
                            Ok(new_exchange_info) => {
                                exchange_info.store(Arc::new(new_exchange_info));
                            }
                            Err(e) => {
                                error!("Failed to refresh exchange info: {}", e);
                            }
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        break;
                    }
                }
            }
        });

        // 监听stream状态，自动重连
        let market_stream = self.market_stream.as_ref().unwrap().clone();
        let market_api = self.market_api.as_ref().unwrap().clone();
        let config = self.config.clone();
        let symbols = self.symbols.clone();
        let kline_intervals = self.kline_intervals.clone();
        let stream_rate_limiters = self.stream_rate_limiters.clone();
        let sender = self.sender.clone();
        let klines = self.klines.clone();
        let trades = self.trades.clone();
        let depth = self.depth.clone();
        let ticker_24hr = self.ticker_24hr.clone();
        let shutdown_token = self.shutdown_token.clone();
        let retry_interval: u64 = self
            .config
            .get("binance.spot.stream_retry_interval_ms")
            .unwrap_or(10000);
        tokio::spawn(async move {
            let mut pre_retry_ts = 0;
            loop {
                let market_stream_ = market_stream.load_full();
                let dead_token = market_stream_.get_ws_shutdown_token().unwrap();
                tokio::select! {
                    _ = dead_token.cancelled() => {
                        if time::get_current_milli_timestamp() - pre_retry_ts < retry_interval {
                            tokio::time::sleep(Duration::from_millis(retry_interval - time::get_current_milli_timestamp() + pre_retry_ts)).await;
                        }
                        pre_retry_ts = time::get_current_milli_timestamp();

                        match create_market_stream(
                            market_api.clone(),
                            config.clone(),
                            symbols.clone(),
                            kline_intervals.clone(),
                            stream_rate_limiters.clone(),
                            sender.clone(),
                            klines.clone(),
                            trades.clone(),
                            depth.clone(),
                            ticker_24hr.clone(),
                        ).await {
                            Ok(new_market_stream) => {
                                market_stream.store(Arc::new(new_market_stream));
                            }
                            Err(e) => {
                                error!("Failed to recreate market stream: {}", e);
                            }
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        limit: Option<u32>,
    ) -> Result<Vec<Arc<KlineData>>> {
        let klines_entry = self.klines.get(&(symbol.to_string(), interval.clone()));
        if let Some(klines_entry) = klines_entry {
            let klines = klines_entry.read().await;
            let mut result: Vec<Arc<KlineData>> = klines.values().cloned().collect();

            if let Some(limit) = limit {
                let start = result.len().saturating_sub(limit as usize);
                result = result[start..].to_vec();
            }

            Ok(result)
        } else {
            Ok(Vec::new())
        }
    }

    async fn get_depth(&self, symbol: &str) -> Result<Arc<DepthData>> {
        let depth_entry = self.depth.get(symbol);
        if let Some(depth_entry) = depth_entry {
            let depth_state = depth_entry.read().await;
            if let Some(state) = depth_state.as_ref() {
                Ok(state.depth.clone())
            } else {
                Err(PlatformError::MarketProviderError {
                    message: format!("Depth for symbol {} not available yet", symbol),
                })
            }
        } else {
            Err(PlatformError::MarketProviderError {
                message: format!("Depth for symbol {} not found", symbol),
            })
        }
    }

    async fn get_ticker_24hr(&self, symbol: &str) -> Result<Arc<Ticker24hr>> {
        let ticker_entry = self.ticker_24hr.get(symbol);
        if let Some(ticker_entry) = ticker_entry {
            let ticker = ticker_entry.read().await;
            if let Some(ticker) = ticker.as_ref() {
                Ok(ticker.clone())
            } else {
                Err(PlatformError::MarketProviderError {
                    message: format!("Ticker 24hr for symbol {} not available yet", symbol),
                })
            }
        } else {
            Err(PlatformError::MarketProviderError {
                message: format!("Ticker 24hr for symbol {} not found", symbol),
            })
        }
    }

    async fn get_trades(&self, symbol: &str, limit: Option<u32>) -> Result<Vec<Arc<Trade>>> {
        let trades_entry = self.trades.get(symbol);
        if let Some(trades_entry) = trades_entry {
            let trades = trades_entry.read().await;
            let mut result: Vec<Arc<Trade>> = trades.values().cloned().collect();

            if let Some(limit) = limit {
                let start = result.len().saturating_sub(limit as usize);
                result = result[start..].to_vec();
            }

            Ok(result)
        } else {
            Ok(Vec::new())
        }
    }

    async fn get_exchange_info(&self) -> Result<ExchangeInfo> {
        if let Some(exchange_info) = &self.exchange_info {
            Ok(exchange_info.load().as_ref().clone())
        } else {
            Err(PlatformError::MarketProviderError {
                message: "Exchange info not initialized".to_string(),
            })
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<MarketEvent> {
        self.sender.subscribe()
    }
}

impl Drop for BinanceSpotMarketProvider {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
    }
}
