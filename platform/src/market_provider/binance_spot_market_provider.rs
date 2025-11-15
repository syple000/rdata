use crate::{
    config::Config,
    errors::{PlatformError, Result},
    market_provider::MarketProvider,
    models::{
        DepthData, ExchangeInfo, GetDepthRequest, GetExchangeInfoRequest, GetKlinesRequest,
        GetTicker24hrRequest, GetTradesRequest, KlineData, KlineInterval, PriceLevel, Ticker24hr,
        Trade,
    },
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use exchange::binance::spot::{
    market_api::MarketApi,
    market_stream::MarketStream,
    models::{self},
    requests::{self},
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
        let _lg = LatencyGuard::new("BinanceSpotDepthState::depth");
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

    shutdown_token: CancellationToken,
}

impl BinanceSpotMarketProvider {
    pub fn new(config: Arc<Config>) -> Result<Self> {
        let api_rate_limits: Option<Vec<(u64, u64)>> =
            config.get("binance_spot.api_rate_limits").ok();
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
            config.get("binance_spot.stream_rate_limits").ok();
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
            .get("binance_spot.kline_event_channel_capacity")
            .unwrap_or(5000);
        let trade_chan_cap = config
            .get("binance_spot.trade_event_channel_capacity")
            .unwrap_or(5000);
        let depth_chan_cap = config
            .get("binance_spot.depth_event_channel_capacity")
            .unwrap_or(5000);
        let ticker_chan_cap = config
            .get("binance_spot.ticker_event_channel_capacity")
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
            shutdown_token: CancellationToken::new(),
        })
    }
}

fn create_market_api(
    config: Arc<Config>,
    rate_limiters: Option<Arc<Vec<RateLimiter>>>,
) -> Result<MarketApi> {
    let base_url: String =
        config
            .get("binance_spot.api_base_url")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get api_base_url: {}", e),
            })?;
    let proxy_url: Option<String> = config.get("proxy.url").ok();
    let timeout_milli_secs: u64 =
        config
            .get("binance_spot.api_timeout_milli_secs")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get api_timeout_milli_secs: {}", e),
            })?;

    let mut market_api = MarketApi::new(base_url, proxy_url, rate_limiters, timeout_milli_secs);
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
            .get("binance_spot.stream_base_url")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get stream_base_url: {}", e),
            })?;
    let proxy_url: Option<String> = config.get("proxy.url").ok();

    let mut market_stream = MarketStream::new(stream_base_url, proxy_url, rate_limiters);

    let subscribed_symbols = config
        .get::<Vec<String>>("binance_spot.subscribed_symbols")
        .map_err(|e| PlatformError::ConfigError {
            message: format!("Failed to get subscribed_symbols: {}", e),
        })?;
    let subscribed_kline_intervals = config
        .get::<Vec<KlineInterval>>("binance_spot.subscribed_kline_intervals")
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
        let trade_sender = trade_sender.clone();
        Box::pin(async move {
            let _ = trade_sender
                .send(trade.into())
                .map_err(|e| WsError::HandleError {
                    message: format!("Failed to send trade event: {}", e),
                })?;
            Ok(())
        })
    });
    market_stream.register_kline_callback(move |kline| {
        let kline_sender = kline_sender.clone();
        Box::pin(async move {
            let _ = kline_sender
                .send(kline.into())
                .map_err(|e| WsError::HandleError {
                    message: format!("Failed to send kline event: {}", e),
                })?;
            Ok(())
        })
    });
    market_stream.register_ticker_callback(move |ticker| {
        let ticker_sender = ticker_sender.clone();
        Box::pin(async move {
            let _ = ticker_sender
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
        .get("binance_spot.depth_cache_channel_capacity")
        .unwrap_or(5000);
    let depth_updates = Arc::new(
        subscribed_symbols
            .iter()
            .map(|s| {
                let (sender, receiver) =
                    broadcast::channel::<models::DepthUpdate>(depth_cache_chan_cap);
                (
                    s.to_string(),
                    (Arc::new(RwLock::new(None::<DepthState>)), sender, receiver),
                )
            })
            .collect::<HashMap<_, _>>(),
    );
    let depth_updates_clone = depth_updates.clone();
    market_stream.register_depth_update_callback(move |update| {
        let depth_updates = depth_updates_clone.clone();
        Box::pin(async move {
            if !depth_updates.contains_key(&update.symbol) {
                return Err(WsError::HandleError {
                    message: format!("Failed to find depth update symbol: {}", &update.symbol),
                });
            }
            let _ = depth_updates[&update.symbol]
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
        let symbol = symbol.to_string();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_token.cancelled() => {
                        break;
                    }
                    recv_result = receiver.recv() => {
                        let _lg = LatencyGuard::new("BinanceSpotMarketProvider::depth_update_handler");
                        let update = match recv_result {
                            Ok(update) => update,
                            Err(e) => {
                                error!("Depth update receive error for symbol {}: {}", symbol, e);
                                continue;
                            }
                        };
                        let _lg_1 = LatencyGuard::new("BinanceSpotMarketProvider::depth_update_handler::process_update");
                        let mut state_guard = state_lock.write().await;
                        if state_guard.is_some() {
                            if let Some(updated) = state_guard.as_mut().unwrap().update_depth(&update).ok() {
                                if updated {
                                    let depth_data = state_guard.as_ref().unwrap().depth();
                                    let _ = depth_sender.send(depth_data).map_err(|e| {
                                        error!("send depth data error for symbol {}: {}", symbol, e);
                                    });
                                }
                                continue;
                            } else {
                                error!("Failed to apply depth update for symbol {}", symbol);
                            }
                        }
                        drop(state_guard);
                        drop(_lg_1);

                        let _lg_2 = LatencyGuard::new("BinanceSpotMarketProvider::depth_update_handler::fetch_initial_and_update_depth");
                        let depth_data = market_api
                            .get_depth(requests::GetDepthRequest {
                                symbol: symbol.clone(),
                                limit: Some(5000),
                            })
                            .await;

                        let depth_state = match depth_data {
                            Ok(depth) => {
                                let mut depth_state = DepthState::from_depth(&depth);
                                match depth_state.update_depth(&update) {
                                    Ok(_) => depth_state,
                                    Err(e) => {
                                        error!("Failed to apply depth update after fetching initial depth for symbol {}: {}", symbol, e);
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to fetch initial depth for symbol {}: {}", symbol, e);
                                continue;
                            }
                        };
                        let depth = depth_state.depth();

                        let mut state_guard = state_lock.write().await;
                        *state_guard = Some(depth_state);
                        drop(state_guard);
                        drop(_lg_2);

                        let _ = depth_sender.send(depth).map_err(|e| {
                            error!("send depth data error for symbol {}: {}", symbol, e);
                        });
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
        let market_api = Arc::new(create_market_api(
            self.config.clone(),
            self.api_rate_limiters.clone(),
        )?);

        let market_stream = create_market_stream(
            market_api.clone(),
            self.config.clone(),
            self.stream_rate_limiters.clone(),
            self.kline_sender.clone(),
            self.trade_sender.clone(),
            self.depth_sender.clone(),
            self.ticker_sender.clone(),
        )
        .await?;

        self.market_api = Some(market_api);
        self.market_stream = Some(Arc::new(ArcSwap::new(Arc::new(market_stream))));

        // stream断连自动重连
        let shutdown_token = self.shutdown_token.clone();
        let market_stream = self.market_stream.as_ref().unwrap().clone();
        let market_api = self.market_api.as_ref().unwrap().clone();
        let config = self.config.clone();
        let stream_rate_limiters = self.stream_rate_limiters.clone();
        let kline_sender = self.kline_sender.clone();
        let trade_sender = self.trade_sender.clone();
        let depth_sender = self.depth_sender.clone();
        let ticker_sender = self.ticker_sender.clone();
        tokio::spawn(async move {
            let retry_interval = config
                .get::<u64>("binance_spot.stream_reconnect_interval_milli_secs")
                .unwrap_or(3000);
            let mut latest_retry_ts = 0u64;
            loop {
                let stream_shutdown_token =
                    market_stream.load_full().get_ws_shutdown_token().unwrap();
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        break;
                    },
                    _ = stream_shutdown_token.cancelled() => {
                        let now = time::get_current_milli_timestamp();
                        if now - latest_retry_ts < retry_interval {
                            tokio::time::sleep(Duration::from_millis(retry_interval - (now - latest_retry_ts))).await;
                        }
                        latest_retry_ts = now;
                        let new_stream = create_market_stream(
                            market_api.clone(),
                            config.clone(),
                            stream_rate_limiters.clone(),
                            kline_sender.clone(),
                            trade_sender.clone(),
                            depth_sender.clone(),
                            ticker_sender.clone(),
                        ).await;
                        match new_stream {
                            Ok(stream) => {
                                market_stream.store(Arc::new(stream));
                            },
                            Err(e) => {
                                error!("Failed to recreate market stream: {}", e);
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn get_klines(&self, req: GetKlinesRequest) -> Result<Vec<KlineData>> {
        let api = self
            .market_api
            .as_ref()
            .ok_or(PlatformError::MarketProviderError {
                message: "Market API not initialized".to_string(),
            })?;

        let klines =
            api.get_klines(req.into())
                .await
                .map_err(|e| PlatformError::MarketProviderError {
                    message: format!("Failed to get klines: {}", e),
                })?;

        Ok(klines.into_iter().map(|k| k.into()).collect())
    }

    async fn get_trades(&self, req: GetTradesRequest) -> Result<Vec<Trade>> {
        let api = self
            .market_api
            .as_ref()
            .ok_or(PlatformError::MarketProviderError {
                message: "Market API not initialized".to_string(),
            })?;

        let trades = api.get_agg_trades(req.into()).await.map_err(|e| {
            PlatformError::MarketProviderError {
                message: format!("Failed to get trades: {}", e),
            }
        })?;

        Ok(trades.into_iter().map(|t| t.into()).collect())
    }

    async fn get_depth(&self, req: GetDepthRequest) -> Result<DepthData> {
        let api = self
            .market_api
            .as_ref()
            .ok_or(PlatformError::MarketProviderError {
                message: "Market API not initialized".to_string(),
            })?;

        let depth =
            api.get_depth(req.into())
                .await
                .map_err(|e| PlatformError::MarketProviderError {
                    message: format!("Failed to get depth: {}", e),
                })?;

        Ok(depth.into())
    }

    async fn get_ticker_24hr(&self, req: GetTicker24hrRequest) -> Result<Vec<Ticker24hr>> {
        let api = self
            .market_api
            .as_ref()
            .ok_or(PlatformError::MarketProviderError {
                message: "Market API not initialized".to_string(),
            })?;

        let ticker = api.get_ticker_24hr(req.into()).await.map_err(|e| {
            PlatformError::MarketProviderError {
                message: format!("Failed to get ticker: {}", e),
            }
        })?;

        Ok(ticker.into_iter().map(|t| t.into()).collect())
    }

    async fn get_exchange_info(&self, req: GetExchangeInfoRequest) -> Result<ExchangeInfo> {
        let api = self
            .market_api
            .as_ref()
            .ok_or(PlatformError::MarketProviderError {
                message: "Market API not initialized".to_string(),
            })?;

        let info = api.get_exchange_info(req.into()).await.map_err(|e| {
            PlatformError::MarketProviderError {
                message: format!("Failed to get exchange info: {}", e),
            }
        })?;

        Ok(info.into())
    }

    fn subscribe_kline(&self) -> broadcast::Receiver<KlineData> {
        self.kline_receiver.resubscribe()
    }

    fn subscribe_trade(&self) -> broadcast::Receiver<Trade> {
        self.trade_receiver.resubscribe()
    }

    fn subscribe_depth(&self) -> broadcast::Receiver<DepthData> {
        self.depth_receiver.resubscribe()
    }

    fn subscribe_ticker(&self) -> broadcast::Receiver<Ticker24hr> {
        self.ticker_receiver.resubscribe()
    }
}

impl Drop for BinanceSpotMarketProvider {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
    }
}
