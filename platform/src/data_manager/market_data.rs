use super::MarketDataManager;
use crate::{
    config::Config,
    errors::{PlatformError, Result},
    market_provider::MarketProvider,
    models::{DepthData, KlineData, KlineInterval, MarketType, Ticker24hr, Trade},
};
use async_trait::async_trait;
use log::info;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

struct Cache<T: Clone> {
    capacity: usize,
    data: BTreeMap<u64, T>,
}

impl<T: Clone> Cache<T> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            data: BTreeMap::new(),
        }
    }

    fn get_capacity(&self) -> usize {
        self.capacity
    }

    fn add(&mut self, key: u64, value: T) -> Option<T> {
        if self.data.contains_key(&key) {
            self.data.insert(key, value);
            return None;
        }

        if self.data.len() >= self.capacity
            && self.data.first_key_value().map(|(k, _)| k.clone()).unwrap() > key
        {
            return None;
        }

        let mut ret = None;
        if self.data.len() >= self.capacity {
            ret = self.data.pop_first().map(|(_, value)| value);
        }

        self.data.insert(key, value);
        return ret;
    }

    fn get(&self, limit: Option<usize>) -> Vec<T> {
        self.data
            .values()
            .skip(self.data.len() - limit.unwrap_or(self.data.len()))
            .cloned()
            .collect()
    }
}

pub struct MarketData {
    market_types: Arc<Vec<MarketType>>,
    market_providers: Arc<HashMap<MarketType, Arc<dyn MarketProvider>>>,
    klines: Arc<HashMap<(MarketType, String, KlineInterval), Arc<RwLock<Cache<KlineData>>>>>,
    trades: Arc<HashMap<(MarketType, String), Arc<RwLock<Cache<Trade>>>>>,
    depths: Arc<HashMap<(MarketType, String), Arc<RwLock<Option<DepthData>>>>>,
    tickers: Arc<HashMap<(MarketType, String), Arc<RwLock<Option<Ticker24hr>>>>>,
    shutdown_token: CancellationToken,
}

impl MarketData {
    pub fn new(
        config: Arc<Config>,
        market_providers: Arc<HashMap<MarketType, Arc<dyn MarketProvider>>>,
    ) -> Result<Self> {
        let market_types: Arc<Vec<MarketType>> =
            Arc::new(
                config
                    .get("markets")
                    .map_err(|e| PlatformError::DataManagerError {
                        message: format!("data_manager markets not found: {}", e),
                    })?,
            );

        let mut klines = HashMap::new();
        let mut trades = HashMap::new();
        let mut depths = HashMap::new();
        let mut tickers = HashMap::new();
        for market_type in market_types.iter() {
            let cache_capacity: usize = config
                .get(&format!("{}.cache_capacity", market_type.as_str()))
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!(
                        "data_manager {} cache_capacity not found: {}",
                        market_type.as_str(),
                        e
                    ),
                })?;

            let symbols: Vec<String> = config
                .get(&format!("{}.subscribed_symbols", market_type.as_str()))
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!(
                        "data_manager {} symbols not found: {}",
                        market_type.as_str(),
                        e
                    ),
                })?;

            let kline_intervals: Vec<KlineInterval> = config
                .get(&format!(
                    "{}.subscribed_kline_intervals",
                    market_type.as_str()
                ))
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!(
                        "data_manager {} kline_intervals not found: {}",
                        market_type.as_str(),
                        e
                    ),
                })?;

            for symbol in symbols {
                for interval in &kline_intervals {
                    klines.insert(
                        (market_type.clone(), symbol.clone(), interval.clone()),
                        Arc::new(RwLock::new(Cache::<KlineData>::new(cache_capacity))),
                    );
                }
                trades.insert(
                    (market_type.clone(), symbol.clone()),
                    Arc::new(RwLock::new(Cache::<Trade>::new(cache_capacity))),
                );
                depths.insert(
                    (market_type.clone(), symbol.clone()),
                    Arc::new(RwLock::new(Option::<DepthData>::None)),
                );
                tickers.insert(
                    (market_type.clone(), symbol.clone()),
                    Arc::new(RwLock::new(Option::<Ticker24hr>::None)),
                );
            }
        }

        Ok(Self {
            market_types,
            market_providers,
            klines: Arc::new(klines),
            trades: Arc::new(trades),
            depths: Arc::new(depths),
            tickers: Arc::new(tickers),
            shutdown_token: CancellationToken::new(),
        })
    }

    async fn init(&self) -> Result<()> {
        for market_type in self.market_types.iter() {
            let market_provider =
                self.market_providers
                    .get(market_type)
                    .ok_or(PlatformError::DataManagerError {
                        message: format!(
                            "Market provider not found for market type: {:?}",
                            market_type
                        ),
                    })?;

            // 订阅kline更新
            let shutdown_token = self.shutdown_token.clone();
            let klines = self.klines.clone();
            let market_type_clone = market_type.clone();
            let mut kline_sub = market_provider.subscribe_kline();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => {
                            break;
                        }
                        kline = kline_sub.recv() => {
                            match kline {
                                Ok(kline) => {
                                    let symbol = kline.symbol.clone();
                                    let kline_interval = kline.interval.clone();
                                    if Self::add_kline_inner(klines.clone(), &market_type_clone, kline).await.is_err() {
                                        log::error!("Failed to add kline data for market type: {:?}, symbol: {}, kline_interval: {:?}", market_type_clone, symbol, kline_interval);
                                    }
                                }
                                Err(_e) => {
                                    log::error!("Kline subscription error for market type: {:?}", market_type_clone);
                                }
                            }
                        }
                    }
                }
            });

            // 订阅trade更新
            let shutdown_token = self.shutdown_token.clone();
            let trades = self.trades.clone();
            let market_type_clone = market_type.clone();
            let mut trade_sub = market_provider.subscribe_trade();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => {
                            break;
                        }
                        trade = trade_sub.recv() => {
                            match trade {
                                Ok(trade) => {
                                    let symbol = trade.symbol.clone();
                                    if Self::add_trade_inner(trades.clone(), &market_type_clone, trade).await.is_err() {
                                        log::error!("Failed to add trade data for market type: {:?}, symbol: {}", market_type_clone, symbol);
                                    }
                                }
                                Err(_e) => {
                                    log::error!("Trade subscription error for market type: {:?}", market_type_clone);
                                }
                            }
                        }
                    }
                }
            });

            // 订阅depth更新
            let shutdown_token = self.shutdown_token.clone();
            let depths = self.depths.clone();
            let market_type_clone = market_type.clone();
            let mut depth_sub = market_provider.subscribe_depth();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => {
                            break;
                        }
                        depth = depth_sub.recv() => {
                            match depth {
                                Ok(depth) => {
                                    let symbol = depth.symbol.clone();
                                    if Self::add_depth_inner(depths.clone(), &market_type_clone, depth).await.is_err() {
                                        log::error!("Failed to add depth data for market type: {:?}, symbol: {:?}", market_type_clone, symbol);
                                    }
                                }
                                Err(_e) => {
                                    log::error!("Depth subscription error for market type: {:?}", market_type_clone);
                                }
                            }
                        }
                    }
                }
            });

            // 订阅ticker更新
            let shutdown_token = self.shutdown_token.clone();
            let tickers = self.tickers.clone();
            let market_type_clone = market_type.clone();
            let mut ticker_sub = market_provider.subscribe_ticker();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => {
                            break;
                        }
                        ticker = ticker_sub.recv() => {
                            match ticker {
                                Ok(ticker) => {
                                    let symbol = ticker.symbol.clone();
                                    if Self::add_ticker_inner(tickers.clone(), &market_type_clone, ticker).await.is_err() {
                                        log::error!("Failed to add ticker data for market type: {:?}, symbol: {}", market_type_clone, symbol);
                                    }
                                }
                                Err(_e) => {
                                    log::error!("Ticker subscription error for market type: {:?}", market_type_clone);
                                }
                            }
                        }
                    }
                }
            });
        }

        // API获取kline/trade/depth/ticker数据初始化
        self.init_data_from_api(self.market_providers.clone())
            .await?;

        Ok(())
    }

    async fn init_data_from_api(
        &self,
        market_providers: Arc<HashMap<MarketType, Arc<dyn MarketProvider>>>,
    ) -> Result<()> {
        for market_type in self.market_types.iter() {
            let market_provider = market_providers.get(market_type).unwrap();

            // 获取当前market_type的所有symbol
            let symbols: Vec<String> = self
                .klines
                .keys()
                .filter(|(mt, _, _)| mt == market_type)
                .map(|(_, symbol, _)| symbol.clone())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();

            for symbol in symbols {
                // 初始化kline数据
                let intervals: Vec<KlineInterval> = self
                    .klines
                    .keys()
                    .filter(|(mt, s, _)| mt == market_type && s == &symbol)
                    .map(|(_, _, interval)| interval.clone())
                    .collect();

                for interval in intervals {
                    info!(
                        "Initializing kline data for {:?} {} {:?}",
                        market_type, symbol, interval
                    );
                    if let Some(cache) =
                        self.klines
                            .get(&(market_type.clone(), symbol.clone(), interval.clone()))
                    {
                        let capacity = cache.read().await.get_capacity();

                        let klines: Vec<KlineData> = market_provider
                            .get_klines(crate::models::GetKlinesRequest {
                                symbol: symbol.clone(),
                                interval: interval.clone(),
                                start_time: None,
                                end_time: None,
                                limit: Some(capacity as u32),
                            })
                            .await
                            .map_err(|e| PlatformError::DataManagerError {
                                message: format!("init data from api, fetch klines err: {}", e),
                            })?;
                        for kline in klines {
                            if Self::add_kline_inner(self.klines.clone(), market_type, kline)
                                .await
                                .is_err()
                            {
                                log::error!(
                                    "Failed to add kline data for market type: {:?}, symbol: {}, kline_interval: {:?}",
                                    market_type,
                                    symbol,
                                    interval
                                );
                            }
                        }
                    }
                }

                // 初始化trade数据
                info!("Initializing trade data for {:?} {}", market_type, symbol);
                if let Some(cache) = self.trades.get(&(market_type.clone(), symbol.clone())) {
                    let capacity = cache.read().await.get_capacity();

                    let trades: Vec<Trade> = market_provider
                        .get_trades(crate::models::GetTradesRequest {
                            symbol: symbol.clone(),
                            from_id: None,
                            start_time: None,
                            end_time: None,
                            limit: Some(capacity as u32),
                        })
                        .await
                        .map_err(|e| PlatformError::DataManagerError {
                            message: format!("init data from api, fetch trades err: {}", e),
                        })?;

                    for trade in trades {
                        if Self::add_trade_inner(self.trades.clone(), market_type, trade)
                            .await
                            .is_err()
                        {
                            log::error!(
                                "Failed to add trade data for market type: {:?}, symbol: {}",
                                market_type,
                                symbol
                            );
                        }
                    }
                }

                // 初始化depth数据（获取最新的）
                info!("Initializing depth data for {:?} {}", market_type, symbol);
                let depth = market_provider
                    .get_depth(crate::models::GetDepthRequest {
                        symbol: symbol.clone(),
                        limit: None,
                    })
                    .await
                    .map_err(|e| PlatformError::DataManagerError {
                        message: format!("init data from api, fetch depth err: {}", e),
                    })?;
                if Self::add_depth_inner(self.depths.clone(), market_type, depth)
                    .await
                    .is_err()
                {
                    log::error!(
                        "Failed to add depth data for market type: {:?}, symbol: {}",
                        market_type,
                        symbol
                    );
                }

                // 初始化ticker数据（获取最新的）
                info!("Initializing ticker data for {:?} {}", market_type, symbol);
                let tickers: Vec<Ticker24hr> = market_provider
                    .get_ticker_24hr(crate::models::GetTicker24hrRequest {
                        symbol: Some(symbol.clone()),
                        symbols: None,
                    })
                    .await
                    .map_err(|e| PlatformError::DataManagerError {
                        message: format!("init data from api, fetch ticker err: {}", e),
                    })?;
                if tickers.is_empty() {
                    return Err(PlatformError::DataManagerError {
                        message: format!(
                            "init data from api, fetch ticker got empty for {:?} {}",
                            market_type, symbol
                        ),
                    });
                }
                let ticker = tickers.first().unwrap().clone();
                if Self::add_ticker_inner(self.tickers.clone(), market_type, ticker)
                    .await
                    .is_err()
                {
                    log::error!(
                        "Failed to add ticker for market type: {:?}, symbol: {}",
                        market_type,
                        symbol
                    );
                }
            }
        }

        Ok(())
    }

    async fn add_kline_inner(
        klines: Arc<HashMap<(MarketType, String, KlineInterval), Arc<RwLock<Cache<KlineData>>>>>,
        market_type: &MarketType,
        kline: KlineData,
    ) -> Result<Option<KlineData>> {
        if let Some(cache) = klines.get(&(
            market_type.clone(),
            kline.symbol.clone(),
            kline.interval.clone(),
        )) {
            let mut cache = cache.write().await;
            return Ok(cache.add(kline.open_time, kline));
        } else {
            Err(PlatformError::DataManagerError {
                message: format!("Kline cache not found for: {:?}", kline),
            })
        }
    }

    async fn add_trade_inner(
        trades: Arc<HashMap<(MarketType, String), Arc<RwLock<Cache<Trade>>>>>,
        market_type: &MarketType,
        trade: Trade,
    ) -> Result<Option<Trade>> {
        if let Some(cache) = trades.get(&(market_type.clone(), trade.symbol.clone())) {
            let mut cache = cache.write().await;
            return Ok(cache.add(trade.seq_id, trade));
        } else {
            Err(PlatformError::DataManagerError {
                message: format!("Trade cache not found for: {:?}", trade),
            })
        }
    }

    async fn add_depth_inner(
        depths: Arc<HashMap<(MarketType, String), Arc<RwLock<Option<DepthData>>>>>,
        market_type: &MarketType,
        depth: DepthData,
    ) -> Result<()> {
        if let Some(cache) = depths.get(&(market_type.clone(), depth.symbol.clone())) {
            let mut cache = cache.write().await;
            // 只允许更新最新的数据
            if let Some(existing) = cache.as_ref() {
                if depth.timestamp <= existing.timestamp {
                    return Ok(());
                }
            }
            *cache = Some(depth);
            return Ok(());
        } else {
            Err(PlatformError::DataManagerError {
                message: format!("Depth cache not found for: {:?}", depth),
            })
        }
    }

    async fn add_ticker_inner(
        tickers: Arc<HashMap<(MarketType, String), Arc<RwLock<Option<Ticker24hr>>>>>,
        market_type: &MarketType,
        ticker: Ticker24hr,
    ) -> Result<()> {
        if let Some(cache) = tickers.get(&(market_type.clone(), ticker.symbol.clone())) {
            let mut cache = cache.write().await;
            // 只允许更新最新的数据
            if let Some(existing) = cache.as_ref() {
                if ticker.close_time <= existing.close_time {
                    return Ok(());
                }
            }
            *cache = Some(ticker);
            return Ok(());
        } else {
            Err(PlatformError::DataManagerError {
                message: format!("Ticker cache not found for: {:?}", ticker),
            })
        }
    }
}

#[async_trait]
impl MarketDataManager for MarketData {
    async fn init(&self) -> Result<()> {
        self.init().await
    }

    async fn get_klines(
        &self,
        market_type: &MarketType,
        symbol: &String,
        interval: &KlineInterval,
        limit: Option<usize>,
    ) -> Result<Vec<KlineData>> {
        if let Some(cache) =
            self.klines
                .get(&(market_type.clone(), symbol.clone(), interval.clone()))
        {
            let cache = cache.read().await;
            return Ok(cache.get(limit));
        }
        Err(PlatformError::DataManagerError {
            message: format!(
                "Kline cache not found for: {:?}",
                (market_type, symbol, interval)
            ),
        })
    }

    async fn get_trades(
        &self,
        market_type: &MarketType,
        symbol: &String,
        limit: Option<usize>,
    ) -> Result<Vec<Trade>> {
        if let Some(cache) = self.trades.get(&(market_type.clone(), symbol.clone())) {
            let cache = cache.read().await;
            return Ok(cache.get(limit));
        }
        Err(PlatformError::DataManagerError {
            message: format!("Trade cache not found for: {:?}", (market_type, symbol)),
        })
    }

    async fn get_depth(
        &self,
        market_type: &MarketType,
        symbol: &String,
    ) -> Result<Option<DepthData>> {
        if let Some(cache) = self.depths.get(&(market_type.clone(), symbol.clone())) {
            let cache = cache.read().await;
            return Ok(cache.clone());
        }
        Err(PlatformError::DataManagerError {
            message: format!("Depth cache not found for: {:?}", (market_type, symbol)),
        })
    }

    async fn get_ticker(
        &self,
        market_type: &MarketType,
        symbol: &String,
    ) -> Result<Option<Ticker24hr>> {
        if let Some(cache) = self.tickers.get(&(market_type.clone(), symbol.clone())) {
            let cache = cache.read().await;
            return Ok(cache.clone());
        }
        Err(PlatformError::DataManagerError {
            message: format!("Ticker cache not found for: {:?}", (market_type, symbol)),
        })
    }
}

impl Drop for MarketData {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
    }
}
