use crate::{
    data_manager::{
        config::DataManagerConfig,
        data_cache::{DepthCache, KlineCache, TickerCache, TradeCache},
    },
    errors::Result,
    market_provider::MarketProvider,
    models::*,
};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;

/// 市场数据管理器
/// 负责管理 kline、trade、ticker、depth 数据的缓存和同步
pub struct MarketDataManager {
    provider: Arc<dyn MarketProvider>,
    config: DataManagerConfig,

    // 数据缓存
    kline_cache: KlineCache,
    trade_cache: TradeCache,
    ticker_cache: TickerCache,
    depth_cache: DepthCache,

    // 广播通道（用于推送数据）
    kline_tx: broadcast::Sender<KlineData>,
    trade_tx: broadcast::Sender<Trade>,
    ticker_tx: broadcast::Sender<Ticker24hr>,
    depth_tx: broadcast::Sender<DepthData>,

    // 最后更新时间追踪
    last_update: Arc<RwLock<LastUpdateTracker>>,

    // 后台任务句柄
    tasks: Arc<RwLock<Vec<JoinHandle<()>>>>,
}

#[derive(Debug)]
struct LastUpdateTracker {
    /// symbol -> interval -> last_update_time
    kline_updates: std::collections::HashMap<String, std::collections::HashMap<KlineInterval, u64>>,
    /// symbol -> last_update_time
    trade_updates: std::collections::HashMap<String, u64>,
}

impl LastUpdateTracker {
    fn new() -> Self {
        Self {
            kline_updates: std::collections::HashMap::new(),
            trade_updates: std::collections::HashMap::new(),
        }
    }

    fn update_kline(&mut self, symbol: String, interval: KlineInterval) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.kline_updates
            .entry(symbol)
            .or_insert_with(std::collections::HashMap::new)
            .insert(interval, now);
    }

    fn update_trade(&mut self, symbol: String) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.trade_updates.insert(symbol, now);
    }

    fn get_kline_last_update(&self, symbol: &str, interval: &KlineInterval) -> Option<u64> {
        self.kline_updates
            .get(symbol)
            .and_then(|m| m.get(interval))
            .copied()
    }

    fn get_trade_last_update(&self, symbol: &str) -> Option<u64> {
        self.trade_updates.get(symbol).copied()
    }
}

impl MarketDataManager {
    pub fn new(provider: Arc<dyn MarketProvider>, config: DataManagerConfig) -> Self {
        let (kline_tx, _) = broadcast::channel(1000);
        let (trade_tx, _) = broadcast::channel(1000);
        let (ticker_tx, _) = broadcast::channel(100);
        let (depth_tx, _) = broadcast::channel(100);

        Self {
            provider,
            kline_cache: KlineCache::new(config.kline_cache_config.max_count),
            trade_cache: TradeCache::new(config.trade_cache_config.max_count),
            ticker_cache: TickerCache::new(),
            depth_cache: DepthCache::new(),
            kline_tx,
            trade_tx,
            ticker_tx,
            depth_tx,
            last_update: Arc::new(RwLock::new(LastUpdateTracker::new())),
            tasks: Arc::new(RwLock::new(Vec::new())),
            config,
        }
    }

    /// 初始化数据管理器
    /// 从 API 加载初始数据并启动推送监听任务
    pub async fn init(
        &mut self,
        symbols: Vec<String>,
        intervals: Vec<KlineInterval>,
    ) -> Result<()> {
        // 初始化 kline 数据
        for symbol in &symbols {
            for interval in &intervals {
                let req = GetKlinesRequest {
                    symbol: symbol.clone(),
                    interval: interval.clone(),
                    limit: Some(self.config.kline_cache_config.max_count as u32),
                    start_time: None,
                    end_time: None,
                };

                match self.provider.get_klines(req).await {
                    Ok(klines) => {
                        self.kline_cache.add_batch(klines).await;
                        self.last_update
                            .write()
                            .await
                            .update_kline(symbol.clone(), interval.clone());
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to initialize kline data for {}-{:?}: {:?}",
                            symbol,
                            interval,
                            e
                        );
                    }
                }
            }
        }

        // 初始化 trade 数据
        for symbol in &symbols {
            let req = GetTradesRequest {
                symbol: symbol.clone(),
                from_id: None,
                start_time: None,
                end_time: None,
                limit: Some(self.config.trade_cache_config.max_count as u32),
            };

            match self.provider.get_trades(req).await {
                Ok(trades) => {
                    self.trade_cache.add_batch(trades).await;
                    self.last_update.write().await.update_trade(symbol.clone());
                }
                Err(e) => {
                    log::warn!("Failed to initialize trade data for {}: {:?}", symbol, e);
                }
            }
        }

        // 启动推送监听任务
        self.start_stream_listeners().await;

        // 启动数据同步检查任务
        self.start_sync_checker(symbols, intervals).await;

        Ok(())
    }

    /// 启动推送监听任务
    async fn start_stream_listeners(&self) {
        let mut tasks = self.tasks.write().await;

        // Kline 推送监听
        let kline_cache = self.kline_cache.clone();
        let kline_tx = self.kline_tx.clone();
        let last_update = self.last_update.clone();
        let mut kline_rx = self.provider.subscribe_kline();
        tasks.push(tokio::spawn(async move {
            while let Ok(kline) = kline_rx.recv().await {
                kline_cache.add(kline.clone()).await;
                last_update
                    .write()
                    .await
                    .update_kline(kline.symbol.clone(), kline.interval.clone());
                let _ = kline_tx.send(kline);
            }
        }));

        // Trade 推送监听
        let trade_cache = self.trade_cache.clone();
        let trade_tx = self.trade_tx.clone();
        let last_update = self.last_update.clone();
        let mut trade_rx = self.provider.subscribe_trade();
        tasks.push(tokio::spawn(async move {
            while let Ok(trade) = trade_rx.recv().await {
                trade_cache.add(trade.clone()).await;
                last_update.write().await.update_trade(trade.symbol.clone());
                let _ = trade_tx.send(trade);
            }
        }));

        // Ticker 推送监听
        let ticker_cache = self.ticker_cache.clone();
        let ticker_tx = self.ticker_tx.clone();
        let mut ticker_rx = self.provider.subscribe_ticker();
        tasks.push(tokio::spawn(async move {
            while let Ok(ticker) = ticker_rx.recv().await {
                ticker_cache.update(ticker.clone()).await;
                let _ = ticker_tx.send(ticker);
            }
        }));

        // Depth 推送监听
        let depth_cache = self.depth_cache.clone();
        let depth_tx = self.depth_tx.clone();
        let mut depth_rx = self.provider.subscribe_depth();
        tasks.push(tokio::spawn(async move {
            while let Ok(depth) = depth_rx.recv().await {
                depth_cache.update(depth.clone()).await;
                let _ = depth_tx.send(depth);
            }
        }));
    }

    /// 启动数据同步检查任务
    async fn start_sync_checker(&self, symbols: Vec<String>, intervals: Vec<KlineInterval>) {
        let provider = self.provider.clone();
        let kline_cache = self.kline_cache.clone();
        let trade_cache = self.trade_cache.clone();
        let last_update = self.last_update.clone();
        let config = self.config.clone();

        let mut tasks = self.tasks.write().await;
        tasks.push(tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(Duration::from_secs(10));

            loop {
                interval_timer.tick().await;

                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                // 检查 Kline 数据
                for symbol in &symbols {
                    for interval in &intervals {
                        let last_update_time = last_update
                            .read()
                            .await
                            .get_kline_last_update(symbol, interval);

                        if let Some(last_time) = last_update_time {
                            let gap = now - last_time;
                            if gap > config.sync_config.market_gap_threshold.as_millis() as u64 {
                                // 数据间断，触发同步
                                log::info!(
                                    "Kline data gap detected for {}-{:?}, syncing...",
                                    symbol,
                                    interval
                                );
                                let req = GetKlinesRequest {
                                    symbol: symbol.clone(),
                                    interval: interval.clone(),
                                    limit: Some(config.kline_cache_config.max_count as u32),
                                    start_time: None,
                                    end_time: None,
                                };
                                if let Ok(klines) = provider.get_klines(req).await {
                                    kline_cache.add_batch(klines).await;
                                    last_update
                                        .write()
                                        .await
                                        .update_kline(symbol.clone(), interval.clone());
                                }
                            }
                        }

                        // 检查 Kline 是否跳跃
                        if let Some(last_open_time) =
                            kline_cache.get_last_open_time(symbol, interval).await
                        {
                            let expected_next = last_open_time + Self::interval_to_ms(interval);
                            if now
                                > expected_next
                                    + config.sync_config.kline_jump_threshold.as_millis() as u64
                            {
                                log::info!(
                                    "Kline jump detected for {}-{:?}, syncing...",
                                    symbol,
                                    interval
                                );
                                let req = GetKlinesRequest {
                                    symbol: symbol.clone(),
                                    interval: interval.clone(),
                                    limit: Some(config.kline_cache_config.max_count as u32),
                                    start_time: None,
                                    end_time: None,
                                };
                                if let Ok(klines) = provider.get_klines(req).await {
                                    kline_cache.add_batch(klines).await;
                                }
                            }
                        }
                    }
                }

                // 检查 Trade 数据
                for symbol in &symbols {
                    let last_update_time = last_update.read().await.get_trade_last_update(symbol);

                    if let Some(last_time) = last_update_time {
                        let gap = now - last_time;
                        if gap > config.sync_config.market_gap_threshold.as_millis() as u64 {
                            log::info!("Trade data gap detected for {}, syncing...", symbol);
                            let req = GetTradesRequest {
                                symbol: symbol.clone(),
                                from_id: None,
                                start_time: None,
                                end_time: None,
                                limit: Some(config.trade_cache_config.max_count as u32),
                            };
                            if let Ok(trades) = provider.get_trades(req).await {
                                trade_cache.add_batch(trades).await;
                                last_update.write().await.update_trade(symbol.clone());
                            }
                        }
                    }
                }
            }
        }));
    }

    /// 将 KlineInterval 转换为毫秒
    fn interval_to_ms(interval: &KlineInterval) -> u64 {
        match interval {
            KlineInterval::OneSecond => 1_000,
            KlineInterval::OneMinute => 60_000,
            KlineInterval::ThreeMinutes => 180_000,
            KlineInterval::FiveMinutes => 300_000,
            KlineInterval::FifteenMinutes => 900_000,
            KlineInterval::ThirtyMinutes => 1_800_000,
            KlineInterval::OneHour => 3_600_000,
            KlineInterval::TwoHours => 7_200_000,
            KlineInterval::FourHours => 14_400_000,
            KlineInterval::SixHours => 21_600_000,
            KlineInterval::EightHours => 28_800_000,
            KlineInterval::TwelveHours => 43_200_000,
            KlineInterval::OneDay => 86_400_000,
            KlineInterval::ThreeDays => 259_200_000,
            KlineInterval::OneWeek => 604_800_000,
            KlineInterval::OneMonth => 2_592_000_000, // 约 30 天
        }
    }

    /// 停止所有后台任务
    pub async fn shutdown(&self) {
        let mut tasks = self.tasks.write().await;
        for task in tasks.drain(..) {
            task.abort();
        }
    }
}

// 实现 MarketProvider trait，优先从缓存获取数据
#[async_trait]
impl MarketProvider for MarketDataManager {
    async fn init(&mut self) -> Result<()> {
        // 由外部调用 MarketDataManager::init
        Ok(())
    }

    async fn get_klines(&self, req: GetKlinesRequest) -> Result<Vec<KlineData>> {
        let limit = req.limit.unwrap_or(500) as usize;
        let cached = self
            .kline_cache
            .get(&req.symbol, &req.interval, limit)
            .await;

        // 如果缓存足够，直接返回
        if cached.len() >= limit {
            return Ok(cached);
        }

        // 否则从 API 获取并更新缓存
        let data = self.provider.get_klines(req).await?;
        self.kline_cache.add_batch(data.clone()).await;
        Ok(data)
    }

    async fn get_trades(&self, req: GetTradesRequest) -> Result<Vec<Trade>> {
        let limit = req.limit.unwrap_or(500) as usize;
        let cached = self.trade_cache.get(&req.symbol, limit).await;

        // 如果缓存足够，直接返回
        if cached.len() >= limit {
            return Ok(cached);
        }

        // 否则从 API 获取并更新缓存
        let data = self.provider.get_trades(req).await?;
        self.trade_cache.add_batch(data.clone()).await;
        Ok(data)
    }

    async fn get_depth(&self, req: GetDepthRequest) -> Result<DepthData> {
        // 优先从缓存获取
        if let Some(cached) = self.depth_cache.get(&req.symbol).await {
            return Ok(cached);
        }

        // 否则从 API 获取并更新缓存
        let data = self.provider.get_depth(req).await?;
        self.depth_cache.update(data.clone()).await;
        Ok(data)
    }

    async fn get_ticker_24hr(&self, req: GetTicker24hrRequest) -> Result<Vec<Ticker24hr>> {
        if let Some(symbol) = &req.symbol {
            // 单个 symbol，优先从缓存获取
            if let Some(cached) = self.ticker_cache.get(symbol).await {
                return Ok(vec![cached]);
            }
        }

        // 从 API 获取并更新缓存
        let data = self.provider.get_ticker_24hr(req).await?;
        for ticker in &data {
            self.ticker_cache.update(ticker.clone()).await;
        }
        Ok(data)
    }

    async fn get_exchange_info(&self) -> Result<ExchangeInfo> {
        self.provider.get_exchange_info().await
    }

    fn subscribe_kline(&self) -> broadcast::Receiver<KlineData> {
        self.kline_tx.subscribe()
    }

    fn subscribe_trade(&self) -> broadcast::Receiver<Trade> {
        self.trade_tx.subscribe()
    }

    fn subscribe_depth(&self) -> broadcast::Receiver<DepthData> {
        self.depth_tx.subscribe()
    }

    fn subscribe_ticker(&self) -> broadcast::Receiver<Ticker24hr> {
        self.ticker_tx.subscribe()
    }
}
