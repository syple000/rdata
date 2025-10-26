use crate::{
    config::Config,
    errors::{PlatformError, Result},
    market_provider::{MarketEvent, MarketProvider},
    models::{DepthData, ExchangeInfo, KlineData, KlineInterval, PriceLevel, Ticker24hr, Trade},
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use dashmap::DashMap;
use exchange::binance::spot::{
    market_api::MarketApi,
    market_stream::MarketStream,
    models,
    requests::{GetAggTradesRequest, GetDepthRequest, GetKlinesRequest},
};
use rate_limiter::RateLimiter;
use rust_decimal::Decimal;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{broadcast, RwLock};
use ws::WsError;

struct BinanceSpotDepthState {
    symbol: String,
    last_update_id: u64,
    bids: HashMap<Decimal, PriceLevel>,
    asks: HashMap<Decimal, PriceLevel>,
    timestamp: u64,

    depth: Arc<DepthData>, // snapshot
}

impl BinanceSpotDepthState {
    pub fn from_depth(depth: &models::DepthData) -> Self {
        let mut bids = HashMap::new();
        let mut asks = HashMap::new();
        for bid in depth.bids.iter() {
            bids.insert(bid.price.clone(), bid.clone().into());
        }
        for ask in depth.asks.iter() {
            asks.insert(ask.price.clone(), ask.clone().into());
        }
        Self {
            symbol: depth.symbol.clone(),
            last_update_id: depth.last_update_id,
            bids,
            asks,
            timestamp: depth.timestamp,
            depth: Arc::new((*depth).clone().into()),
        }
    }

    pub fn update_depth(&mut self, update: &models::DepthUpdate) -> Result<Option<Arc<DepthData>>> {
        if self.symbol != update.symbol {
            return Err(PlatformError::MarketProviderError {
                message: format!(
                    "Depth update symbol mismatch. Expected: {}, got: {}",
                    self.symbol, update.symbol
                ),
            });
        }

        if update.last_update_id <= self.last_update_id {
            return Ok(None);
        }

        if update.first_update_id <= self.last_update_id + 1 {
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

            self.depth = Arc::new(DepthData {
                symbol: self.symbol.clone(),
                bids: {
                    let mut bids: Vec<PriceLevel> = self.bids.values().cloned().collect();
                    bids.sort_by(|a, b| b.price.cmp(&a.price));
                    bids
                },
                asks: {
                    let mut asks: Vec<PriceLevel> = self.asks.values().cloned().collect();
                    asks.sort_by(|a, b| a.price.cmp(&b.price));
                    asks
                },
                timestamp: self.timestamp,
            });

            return Ok(Some(self.depth.clone()));
        }

        Err(PlatformError::MarketProviderError {
            message: format!(
                "Depth update out of order. Last update id: {}, update first id: {}",
                self.last_update_id, update.first_update_id
            ),
        })
    }
}

pub struct BinanceSpotMarketProvider {
    config: Arc<Config>,
    api_rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    stream_rate_limiters: Option<Arc<Vec<RateLimiter>>>,

    market_api: Option<Arc<MarketApi>>,
    market_stream: Option<Arc<ArcSwap<MarketStream>>>,

    klines: Arc<DashMap<(String, KlineInterval), RwLock<BTreeMap<u64, Arc<KlineData>>>>>,
    trades: Arc<DashMap<String, RwLock<BTreeMap<u64, Arc<Trade>>>>>,
    depth: Arc<DashMap<String, RwLock<Option<BinanceSpotDepthState>>>>,
    ticker_24hr: Arc<DashMap<String, RwLock<Option<Arc<Ticker24hr>>>>>,
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

        Ok(Self {
            config: config.clone(),
            api_rate_limiters,
            stream_rate_limiters,
            market_api: None,
            market_stream: None,
            klines: Arc::new(DashMap::new()),
            trades: Arc::new(DashMap::new()),
            depth: Arc::new(DashMap::new()),
            ticker_24hr: Arc::new(DashMap::new()),
        })
    }
}

async fn create_market_api(
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
    sender: broadcast::Sender<MarketEvent>,
    klines: Arc<DashMap<(String, KlineInterval), RwLock<BTreeMap<u64, Arc<KlineData>>>>>,
    trades: Arc<DashMap<String, RwLock<BTreeMap<u64, Arc<Trade>>>>>,
    depth: Arc<DashMap<String, RwLock<Option<BinanceSpotDepthState>>>>,
    ticker_24hr: Arc<DashMap<String, RwLock<Option<Arc<Ticker24hr>>>>>,
) -> Result<MarketStream> {
    // 订阅的symbols
    let symbols: Vec<String> =
        config
            .get("binance.spot.symbols")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get symbols: {}", e),
            })?;
    // 订阅的kline intervals
    let kline_intervals: Vec<KlineInterval> =
        config
            .get("binance.spot.kline_intervals")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get kline_intervals: {}", e),
            })?;
    // 连接url
    let stream_base_url: String =
        config
            .get("binance.spot.stream_base_url")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get stream_base_url: {}", e),
            })?;
    let proxy_url: Option<String> = config.get("proxy.url").ok();

    let mut market_stream = MarketStream::new(stream_base_url, proxy_url, rate_limiters);

    // 订阅
    let max_trade_cnt: usize = config.get("binance.spot.max_trade_cnt").unwrap_or(1000) as usize;
    let max_kline_cnt: usize = config.get("binance.spot.max_kline_cnt").unwrap_or(1000) as usize;
    for symbol in symbols.iter() {
        market_stream.subscribe_agg_trade(symbol);
        market_stream.subscribe_depth_update(symbol);
        market_stream.subscribe_ticker(symbol);
        for interval in kline_intervals.iter() {
            market_stream.subscribe_kline(symbol, &interval.clone().into());
        }
    }

    // 注册回调
    let trades_clone = trades.clone();
    let sender_clone = sender.clone();
    market_stream.register_agg_trade_callback(move |trade| {
        let trades_clone = trades_clone.clone();
        let sender_clone = sender_clone.clone();
        let trade_id = trade.agg_trade_id;
        let trade: Arc<Trade> = Arc::new(trade.into());
        Box::pin(async move {
            let symbol_trades = trades_clone
                .entry(trade.symbol.clone())
                .or_insert_with(|| RwLock::new(BTreeMap::new()));

            let mut symbol_trades = symbol_trades.write().await;
            symbol_trades.insert(trade_id, trade.clone());
            if symbol_trades.len() > max_trade_cnt {
                symbol_trades.pop_first();
            }
            drop(symbol_trades);

            sender_clone
                .send(MarketEvent::Trade(trade.clone()))
                .map_err(|e| WsError::HandleError {
                    message: format!("Failed to send trade event: {}", e),
                })?;
            Ok(())
        })
    });

    let klines_clone = klines.clone();
    let sender_clone = sender.clone();
    market_stream.register_kline_callback(move |kline| {
        let klines_clone = klines_clone.clone();
        let sender_clone = sender_clone.clone();
        let kline_open_time = kline.open_time;
        let kline: Arc<KlineData> = Arc::new(kline.into());
        Box::pin(async move {
            let symbol_klines = klines_clone
                .entry((kline.symbol.clone(), kline.interval.clone()))
                .or_insert_with(|| RwLock::new(BTreeMap::new()));

            let mut symbol_klines = symbol_klines.write().await;
            symbol_klines.insert(kline_open_time, kline.clone());
            if symbol_klines.len() > max_kline_cnt {
                symbol_klines.pop_first();
            }
            drop(symbol_klines);

            sender_clone
                .send(MarketEvent::Kline(kline.clone()))
                .map_err(|e| WsError::HandleError {
                    message: format!("Failed to send kline event: {}", e),
                })?;
            Ok(())
        })
    });

    let ticker_24hr_clone = ticker_24hr.clone();
    let sender_clone = sender.clone();
    market_stream.register_ticker_callback(move |ticker| {
        let ticker_24hr_clone = ticker_24hr_clone.clone();
        let sender_clone = sender_clone.clone();
        let ticker: Arc<Ticker24hr> = Arc::new(ticker.into());
        Box::pin(async move {
            let current_ticker = ticker_24hr_clone
                .entry(ticker.symbol.clone())
                .or_insert_with(|| RwLock::new(None));
            let mut current_ticker = current_ticker.write().await;
            if current_ticker.is_none()
                || current_ticker.as_ref().unwrap().close_time < ticker.close_time
            {
                *current_ticker = Some(ticker.clone());
                drop(current_ticker);

                sender_clone
                    .send(MarketEvent::Ticker(ticker.clone()))
                    .map_err(|e| WsError::HandleError {
                        message: format!("Failed to send ticker event: {}", e),
                    })?;
            }
            Ok(())
        })
    });

    let depth_clone = depth.clone();
    let sender_clone = sender.clone();
    let market_api_clone = market_api.clone();
    market_stream.register_depth_update_callback(move |depth_update| {
        let depth_clone = depth_clone.clone();
        let sender_clone = sender_clone.clone();
        let market_api_clone = market_api_clone.clone();
        Box::pin(async move {
            let symbol_depth_ref = depth_clone
                .entry(depth_update.symbol.clone())
                .or_insert_with(|| RwLock::new(None));
            let mut symbol_depth = symbol_depth_ref.write().await;

            if symbol_depth.is_some() {
                match symbol_depth.as_mut().unwrap().update_depth(&depth_update) {
                    Ok(Some(depth)) => {
                        drop(symbol_depth);

                        sender_clone
                            .send(MarketEvent::Depth(depth.clone()))
                            .map_err(|e| WsError::HandleError {
                                message: format!("Failed to send depth event: {}", e),
                            })?;
                        return Ok(());
                    }
                    Ok(None) => {
                        return Ok(());
                    }
                    Err(_) => {} // 重新获取快照并更新
                }
            }

            drop(symbol_depth);
            let mut state = BinanceSpotDepthState::from_depth(
                &market_api_clone
                    .get_depth(GetDepthRequest {
                        symbol: depth_update.symbol.to_string(),
                        limit: Some(5000),
                    })
                    .await
                    .map_err(|e| WsError::HandleError {
                        message: format!(
                            "Failed to get initial depth for {}: {}",
                            &depth_update.symbol, e
                        ),
                    })?,
            );
            state
                .update_depth(&depth_update)
                .map_err(|e| WsError::HandleError {
                    message: format!("Failed to update depth after fetch snapshot: {}", e),
                })?;

            let mut symbol_depth = symbol_depth_ref.write().await;
            let depth = state.depth.clone();
            *symbol_depth = Some(state);
            drop(symbol_depth);

            sender_clone
                .send(MarketEvent::Depth(depth.clone()))
                .map_err(|e| WsError::HandleError {
                    message: format!("Failed to send depth event: {}", e),
                })?;
            Ok(())
        })
    });

    market_stream
        .init()
        .await
        .map_err(|e| PlatformError::MarketProviderError {
            message: format!("Failed to init market_stream: {}", e),
        })?;

    // 在stream启动后，通过api获取trades/klines初始数据，并覆盖一次
    for symbol in symbols.iter() {
        let api_trades = market_api
            .get_agg_trades(GetAggTradesRequest {
                symbol: symbol.to_string(),
                from_id: None,
                start_time: None,
                end_time: None,
                limit: Some(max_trade_cnt as u32),
            })
            .await
            .map_err(|e| PlatformError::MarketProviderError {
                message: format!("Failed to get agg trades for {}: {}", symbol, e),
            })?
            .iter()
            .map(|trade| {
                let trade_id = trade.agg_trade_id;
                let trade: Arc<Trade> = Arc::new(trade.clone().into());
                (trade_id, trade)
            })
            .collect::<BTreeMap<_, _>>();
        trades
            .entry(symbol.to_string())
            .insert(RwLock::new(api_trades));

        for interval in kline_intervals.iter() {
            let api_klines = market_api
                .get_klines(GetKlinesRequest {
                    symbol: symbol.to_string(),
                    interval: interval.clone().into(),
                    start_time: None,
                    end_time: None,
                    limit: Some(max_kline_cnt as u32),
                })
                .await
                .map_err(|e| PlatformError::MarketProviderError {
                    message: format!(
                        "Failed to get klines for {} interval {:?}: {}",
                        symbol, interval, e
                    ),
                })?
                .iter()
                .map(|kline| {
                    let kline_open_time = kline.open_time;
                    let kline: Arc<KlineData> = Arc::new(kline.clone().into());
                    (kline_open_time, kline)
                })
                .collect::<BTreeMap<_, _>>();
            klines
                .entry((symbol.to_string(), interval.clone()))
                .insert(RwLock::new(api_klines));
        }
    }

    Ok(market_stream)
}

#[async_trait]
impl MarketProvider for BinanceSpotMarketProvider {
    async fn init(&mut self) -> Result<()> {
        todo!()
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        limit: Option<u32>,
    ) -> Result<Vec<Arc<KlineData>>> {
        todo!()
    }

    async fn get_depth(&self, symbol: &str) -> Result<Arc<DepthData>> {
        todo!()
    }

    async fn get_ticker_24hr(&self, symbol: &str) -> Result<Arc<Ticker24hr>> {
        todo!()
    }

    async fn get_trades(&self, symbol: &str, limit: Option<u32>) -> Result<Vec<Arc<Trade>>> {
        todo!()
    }

    async fn get_exchange_info(&self) -> Result<ExchangeInfo> {
        todo!()
    }

    fn subscribe(&self) -> broadcast::Receiver<MarketEvent> {
        todo!()
    }
}
