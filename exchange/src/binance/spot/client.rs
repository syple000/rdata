use crate::binance::{
    errors::Result,
    spot::{
        market_api::MarketApi,
        market_stream,
        models::{AggTrade, DepthData, DepthUpdate, KlineData, PriceLevel},
        requests::GetKlinesRequest,
        responses::GetKlinesResponse,
        trade_api::TradeApi,
        trade_stream,
    },
};
use rate_limiter::RateLimiter;
use rust_decimal::Decimal;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    pin::Pin,
    sync::Arc,
};
use tokio::{select, sync::Mutex, sync::RwLock};
use tokio_util::sync::CancellationToken;

pub struct SpotClientConfig {
    api_base_url: String,
    wss_base_url: String,
    wss_api_base_url: String,

    proxy_url: Option<String>,

    api_key: String,
    api_secret: String,

    rate_limiters: Option<Arc<Vec<RateLimiter>>>,

    target_symbols: Option<Vec<String>>,
    target_kline_intervals: Option<Vec<u64>>,

    max_reconnect_attempts: u32,
    reconnect_interval_millis: u64,
}

struct DepthWrapperStat {
    last_update_id: u64,
    bids: HashMap<Decimal, PriceLevel>,
    asks: HashMap<Decimal, PriceLevel>,
    timestamp: u64,
}

struct DepthWrapper {
    symbol: String,
    stat: RwLock<DepthWrapperStat>,
}

impl DepthWrapper {
    fn new(symbol: String) -> Self {
        Self {
            symbol: symbol,
            stat: RwLock::new(DepthWrapperStat {
                last_update_id: 0,
                bids: HashMap::new(),
                asks: HashMap::new(),
                timestamp: 0,
            }),
        }
    }

    async fn get_depth(&self) -> DepthData {
        let mut bids: Vec<PriceLevel>;
        let mut asks: Vec<PriceLevel>;
        let last_update_id: u64;
        {
            let stat = self.stat.read().await;
            bids = stat.bids.values().cloned().collect();
            asks = stat.asks.values().cloned().collect();
            last_update_id = stat.last_update_id;
        }

        bids.sort_by(|a, b| b.price.cmp(&a.price));
        asks.sort_by(|a, b| a.price.cmp(&b.price));
        DepthData {
            symbol: self.symbol.clone(),
            last_update_id,
            bids,
            asks,
        }
    }

    async fn update_by_depth(&self, depth: DepthData) -> Result<()> {
        if self.symbol != depth.symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Depth symbol mismatch".to_string(),
            });
        }
        let mut bids = HashMap::new();
        let mut asks = HashMap::new();
        for bid in depth.bids {
            bids.insert(bid.price.clone(), bid);
        }
        for ask in depth.asks {
            asks.insert(ask.price.clone(), ask);
        }
        let stat = &mut *self.stat.write().await;
        stat.bids = bids;
        stat.asks = asks;
        stat.last_update_id = depth.last_update_id;
        stat.timestamp = 0;
        Ok(())
    }

    async fn update_by_depth_update(&self, update: DepthUpdate) -> Result<()> {
        if self.symbol != update.symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Depth update symbol mismatch".to_string(),
            });
        }
        let stat = &mut *self.stat.write().await;

        if update.last_update_id <= stat.last_update_id {
            return Ok(());
        }

        if update.first_update_id <= stat.last_update_id + 1 {
            for bid in update.bids {
                if bid.quantity.is_zero() {
                    stat.bids.remove(&bid.price);
                } else {
                    stat.bids.insert(bid.price.clone(), bid);
                }
            }
            for ask in update.asks {
                if ask.quantity.is_zero() {
                    stat.asks.remove(&ask.price);
                } else {
                    stat.asks.insert(ask.price.clone(), ask);
                }
            }
            stat.last_update_id = update.last_update_id;
            stat.timestamp = update.timestamp;
            return Ok(());
        }

        Err(crate::binance::errors::BinanceError::ClientError {
            message: format!(
                "Depth update out of order. Last update id: {}, update first id: {}",
                stat.last_update_id, update.first_update_id
            ),
        })
    }
}

#[derive(Clone)]
struct Kline {
    kline: KlineData,
    allow_update_by_trade: bool,
    first_trade_id: Option<u64>,
    last_trade_id: Option<u64>,
}

struct KlineWrapper {
    symbol: String,
    interval: u64,
    max_cnt: usize,
    klines: RwLock<VecDeque<Kline>>,
}

impl KlineWrapper {
    fn new(symbol: String, interval: u64, max_cnt: usize) -> Self {
        Self {
            symbol,
            interval,
            max_cnt,
            klines: RwLock::new(VecDeque::new()),
        }
    }

    async fn get_klines(&self, cnt: usize) -> Vec<KlineData> {
        let klines = &*self.klines.read().await;
        let start = klines.len().saturating_sub(cnt);
        klines.iter().skip(start).map(|k| k.kline.clone()).collect()
    }

    fn _push_back(&self, klines: &mut VecDeque<Kline>, kline: Kline) {
        if klines.len() >= self.max_cnt {
            klines.pop_front();
        }
        klines.push_back(kline);
    }

    async fn update_by_kline(&self, kline: KlineData) -> Result<()> {
        if kline.close_time - kline.open_time != self.interval {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Kline interval mismatch".to_string(),
            });
        }
        if kline.open_time % self.interval != 0 {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Kline open time not aligned with interval".to_string(),
            });
        }
        if kline.symbol != self.symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Kline symbol mismatch".to_string(),
            });
        }

        let klines = &mut *self.klines.write().await;

        if let Some(front) = klines.front() {
            if kline.open_time < front.kline.open_time {
                return Ok(());
            }
            let index = ((kline.open_time - front.kline.open_time) / self.interval) as usize;
            while klines.len() <= index {
                let mut kline = klines.back().unwrap().clone();
                kline.kline.open_time += self.interval;
                kline.kline.close_time += self.interval;
                kline.kline.volume = Decimal::new(0, 0);
                kline.kline.quote_volume = Decimal::new(0, 0);
                kline.kline.trade_count = 0;
                kline.allow_update_by_trade = true;
                kline.first_trade_id = None;
                kline.last_trade_id = None;
                self._push_back(klines, kline);
            }
            klines[index] = Kline {
                kline: kline,
                allow_update_by_trade: false,
                first_trade_id: None,
                last_trade_id: None,
            };
        } else {
            klines.push_back(Kline {
                kline: kline,
                allow_update_by_trade: false,
                first_trade_id: None,
                last_trade_id: None,
            });
        }
        Ok(())
    }

    async fn update_by_trade(&self, trade: AggTrade) -> Result<()> {
        if trade.symbol != self.symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Trade symbol mismatch".to_string(),
            });
        }

        let klines = &mut *self.klines.write().await;

        if let Some(front) = klines.front() {
            if trade.timestamp < front.kline.open_time {
                return Ok(());
            }
            let index = ((trade.timestamp - front.kline.open_time) / self.interval) as usize;
            while klines.len() <= index {
                let mut kline = klines.back().unwrap().clone();
                kline.kline.open_time += self.interval;
                kline.kline.close_time += self.interval;
                kline.kline.volume = Decimal::new(0, 0);
                kline.kline.quote_volume = Decimal::new(0, 0);
                kline.kline.trade_count = 0;
                kline.allow_update_by_trade = true;
                kline.first_trade_id = None;
                kline.last_trade_id = None;
                self._push_back(klines, kline);
            }
            let kline = &mut klines[index];
            if !kline.allow_update_by_trade {
                return Ok(());
            }
            if kline.first_trade_id.is_none() && kline.last_trade_id.is_none() {
                kline.first_trade_id = Some(trade.first_trade_id);
                kline.last_trade_id = Some(trade.last_trade_id);
                kline.kline.symbol = trade.symbol;
                kline.kline.open = trade.price.clone();
                kline.kline.high = trade.price.clone();
                kline.kline.low = trade.price.clone();
                kline.kline.close = trade.price.clone();
                kline.kline.volume = trade.quantity.clone();
                kline.kline.quote_volume = trade.price * trade.quantity;
                kline.kline.trade_count = trade.last_trade_id - trade.first_trade_id + 1;
            } else {
                if kline.first_trade_id.unwrap() > trade.first_trade_id {
                    kline.kline.open = trade.price.clone();
                }
                if kline.last_trade_id.unwrap() < trade.last_trade_id {
                    kline.kline.close = trade.price.clone();
                }
                if kline.kline.high < trade.price {
                    kline.kline.high = trade.price.clone();
                }
                if kline.kline.low > trade.price {
                    kline.kline.low = trade.price.clone();
                }
                kline.kline.volume += trade.quantity.clone();
                kline.kline.quote_volume += trade.price * trade.quantity;
                kline.kline.trade_count += trade.last_trade_id - trade.first_trade_id + 1;
            }
        } else {
            let open_time = trade.timestamp - (trade.timestamp % self.interval);
            let close_time = open_time + self.interval;
            self._push_back(
                klines,
                Kline {
                    kline: KlineData {
                        symbol: trade.symbol,
                        open_time,
                        close_time,
                        open: trade.price.clone(),
                        high: trade.price.clone(),
                        low: trade.price.clone(),
                        close: trade.price.clone(),
                        volume: trade.quantity.clone(),
                        quote_volume: trade.price * trade.quantity,
                        trade_count: trade.last_trade_id - trade.first_trade_id + 1,
                    },
                    allow_update_by_trade: true,
                    first_trade_id: Some(trade.first_trade_id),
                    last_trade_id: Some(trade.last_trade_id),
                },
            );
        }
        Ok(())
    }
}

struct TradeDedupStat {
    last_checked_trade_id: u64,
    trade_ids: HashSet<u64>,
}
struct TradeDedup {
    symbol: String,
    stat: RwLock<TradeDedupStat>,
}

impl TradeDedup {
    fn new(symbol: String) -> Self {
        Self {
            symbol: symbol,
            stat: RwLock::new(TradeDedupStat {
                last_checked_trade_id: 0,
                trade_ids: HashSet::new(),
            }),
        }
    }

    async fn set_last_checked_trade_id(&self, symbol: String, trade_id: u64) -> Result<()> {
        if self.symbol != symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Trade dedup symbol mismatch".to_string(),
            });
        }
        let stat = &mut *self.stat.write().await;
        let last_checked_trade_id = stat.last_checked_trade_id;
        if trade_id <= last_checked_trade_id {
            return Ok(());
        }
        // 这里假定了id基本严格递增，如果稀疏，性能不佳
        for id in last_checked_trade_id..=trade_id {
            stat.trade_ids.remove(&id);
        }
        stat.last_checked_trade_id = trade_id;
        return Ok(());
    }

    async fn update_trade_id(&self, symbol: String, trade_id: u64) -> Result<bool> {
        if self.symbol != symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Trade dedup symbol mismatch".to_string(),
            });
        }
        let stat = &mut *self.stat.write().await;
        if trade_id <= stat.last_checked_trade_id {
            return Ok(false);
        }
        if stat.trade_ids.contains(&trade_id) {
            return Ok(false);
        }
        stat.trade_ids.insert(trade_id);
        Ok(true)
    }
}

pub struct SpotClient {
    config: Arc<SpotClientConfig>,

    market_api: Arc<MarketApi>,
    trade_api: Arc<TradeApi>,
    market_stream: Arc<Mutex<market_stream::MarketStream>>,
    trade_stream: Arc<Mutex<trade_stream::TradeStream>>,

    shutdown_token: CancellationToken,

    depth_wrappers: Arc<HashMap<String, DepthWrapper>>,
    kline_wrappers: Arc<HashMap<(String, u64), KlineWrapper>>,
    trade_dedups: Arc<HashMap<String, TradeDedup>>,
}

type Fut = Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>;
impl SpotClient {
    pub fn new(config: SpotClientConfig) -> Self {
        let market_api = MarketApi::new(config.api_base_url.clone(), config.rate_limiters.clone());
        let trade_api = TradeApi::new(
            config.api_base_url.clone(),
            config.rate_limiters.clone(),
            config.api_key.clone(),
            config.api_secret.clone(),
        );
        let market_stream = market_stream::MarketStream::new(
            config.wss_base_url.clone(),
            config.proxy_url.clone(),
            config.rate_limiters.clone(),
        );
        let trade_stream = trade_stream::TradeStream::new(
            config.wss_api_base_url.clone(),
            config.proxy_url.clone(),
            config.rate_limiters.clone(),
            config.api_key.clone(),
            config.api_secret.clone(),
        );

        let mut depth_wrappers = HashMap::new();
        let mut kline_wrappers = HashMap::new();
        let mut trade_dedups = HashMap::new();
        if let Some(symbols) = &config.target_symbols {
            for symbol in symbols.iter() {
                depth_wrappers.insert(symbol.clone(), DepthWrapper::new(symbol.clone()));
                trade_dedups.insert(symbol.clone(), TradeDedup::new(symbol.clone()));
                if let Some(intervals) = &config.target_kline_intervals {
                    for interval in intervals.iter() {
                        kline_wrappers.insert(
                            (symbol.clone(), *interval),
                            KlineWrapper::new(symbol.clone(), *interval, 1024),
                        );
                    }
                }
            }
        }

        Self {
            config: Arc::new(config),
            market_api: Arc::new(market_api),
            trade_api: Arc::new(trade_api),
            market_stream: Arc::new(Mutex::new(market_stream)),
            trade_stream: Arc::new(Mutex::new(trade_stream)),
            shutdown_token: CancellationToken::new(),
            depth_wrappers: Arc::new(depth_wrappers),
            kline_wrappers: Arc::new(kline_wrappers),
            trade_dedups: Arc::new(trade_dedups),
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        self.market_api = Some(Arc::new(market_api));
        self.trade_api = Some(Arc::new(trade_api));

        if let Some(symbols) = &self.config.target_symbols {
            let mut set = HashSet::new();
            for symbol in symbols {
                set.insert(symbol.to_uppercase());
            }
            self.target_symbols = Some(set);
        }

        Ok(())
    }

    pub async fn get_shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    async fn new_market_stream<F1, F2>(
        config: Arc<SpotClientConfig>,
        depth_update_cb: F1,
        agg_trade_cb: F2,
    ) -> Result<market_stream::MarketStream>
    where
        F1: Fn(DepthUpdate) -> Fut + Send + Sync + 'static,
        F2: Fn(AggTrade) -> Fut + Send + Sync + 'static,
    {
        let mut market_stream = market_stream::MarketStream::new(
            config.wss_base_url.clone(),
            config.proxy_url.clone(),
            config.rate_limiters.clone(),
        );
        market_stream.init().await?;
        for symbol in config.target_symbols.as_ref().unwrap_or(&vec![]).iter() {
            market_stream.subscribe_depth_update(symbol);
            market_stream.subscribe_agg_trade(symbol);
        }
        market_stream.register_depth_update_callback(depth_update_cb);
        market_stream.register_agg_trade_callback(agg_trade_cb);
        Ok(market_stream)
    }

    async fn depth_update_cb(update: DepthUpdate, market_api: Arc<MarketApi>) -> Fut {
        unimplemented!()
    }
    async fn agg_trade_cb(trade: AggTrade) -> Fut {
        unimplemented!()
    }

    async fn reconnect_market_stream(
        config: Arc<SpotClientConfig>,
        shutdown_token: CancellationToken,
        market_stream: Arc<Mutex<market_stream::MarketStream>>,
    ) {
        loop {
            let market_stream_token = market_stream.lock().await.get_shutdown_token().await;
            if market_stream_token.is_none() {
                break;
            }
            let market_stream_token = market_stream_token.unwrap();

            select! {
                _ = shutdown_token.cancelled() => {
                    break;
                },
                _ = market_stream_token.cancelled() => {
                    let mut attempts = 0;
                    loop {
                        attempts += 1;
                        if attempts > config.max_reconnect_attempts {
                            shutdown_token.cancel();
                            break;
                        }
                        let market_stream = market_stream.lock().await;
                        let mut new_market_stream = market_stream::MarketStream::new(
                            config.wss_base_url.clone(),
                            config.proxy_url.clone(),
                            config.rate_limiters.clone(),
                        );
                        new_market_stream.init().await.ok();
                        tokio::time::sleep(std::time::Duration::from_millis(config.reconnect_interval_millis)).await;
                    }

                },
            }
        }
    }

    pub async fn close(&self) -> Result<()> {
        if let Some(token) = &self.shutdown_token {
            token.cancel();
        }
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

    // 对外暴露行情接口（k线、深度、成交）
    pub async fn get_klines(&self, req: GetKlinesRequest) -> Result<GetKlinesResponse> {
        if let Some(target_symbols) = &self.target_symbols {
            if !target_symbols.contains(&req.symbol.to_uppercase()) {
                return Err(crate::binance::errors::BinanceError::ClientError {
                    message: "Symbol not in target list".to_string(),
                });
            }
        }

        unimplemented!()
    }
    // 对外暴露交易接口（下单、撤单、查询订单、查询所有订单、查询开订单、查询成交、查询账户）
}
