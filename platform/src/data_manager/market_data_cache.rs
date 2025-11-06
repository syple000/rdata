use crate::{
    errors::{PlatformError, Result},
    models::{DepthData, KlineData, KlineInterval, Ticker24hr, Trade},
};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct SymbolKlineCache {
    symbol: String,
    interval: KlineInterval,
    capacity: usize,
    klines: Vec<Option<KlineData>>,
    latest_open_time: u64,
    size: u64,
}

impl SymbolKlineCache {
    fn new(symbol: String, interval: KlineInterval, capacity: usize) -> Self {
        Self {
            symbol: symbol,
            interval: interval,
            capacity: capacity,
            klines: vec![None; capacity],
            latest_open_time: 0,
            size: 0,
        }
    }

    fn add_kline(&mut self, kline: KlineData) -> Result<Vec<KlineData>> {
        if self.symbol != kline.symbol {
            return Err(PlatformError::DataManagerError {
                message: format!(
                    "Symbol mismatch: cache symbol {}, kline symbol {}",
                    self.symbol, kline.symbol
                ),
            });
        }
        if kline.open_time % self.interval.to_millis() != 0 {
            return Err(PlatformError::DataManagerError {
                message: format!(
                    "Kline open time {} is not aligned with interval {:?}",
                    kline.open_time, self.interval
                ),
            });
        }
        if kline.open_time
            <= self
                .latest_open_time
                .saturating_sub(self.interval.to_millis() * (self.capacity as u64))
        {
            return Ok(Vec::new());
        }

        let mut removed_klines = Vec::with_capacity(
            (kline.open_time.saturating_sub(self.latest_open_time) / self.interval.to_millis())
                .min(self.size) as usize,
        );
        if kline.open_time > self.latest_open_time {
            let mut old_first_open_time = self
                .latest_open_time
                .saturating_sub(self.interval.to_millis() * (self.capacity as u64 - 1));
            let new_first_open_time = kline
                .open_time
                .saturating_sub(self.interval.to_millis() * (self.capacity as u64 - 1));

            while old_first_open_time < new_first_open_time && self.size > 0 {
                let index = (old_first_open_time / self.interval.to_millis()
                    % (self.capacity as u64)) as usize;
                if let Some(removed_kline) = &self.klines[index]
                    && removed_kline.open_time == old_first_open_time
                {
                    removed_klines.push(removed_kline.clone());
                    self.size -= 1;
                }
                old_first_open_time += self.interval.to_millis();
            }

            self.latest_open_time = kline.open_time;
        }

        let index =
            ((kline.open_time / self.interval.to_millis()) % (self.capacity as u64)) as usize;
        if let Some(exist_kline) = &self.klines[index]
            && exist_kline.open_time == kline.open_time
        {
            // do nothing, just replace
        } else {
            self.size += 1;
        }
        self.klines[index] = Some(kline);

        Ok(removed_klines)
    }

    fn get_klines(&self, limit: Option<usize>) -> Result<Vec<Option<KlineData>>> {
        if self.size == 0 {
            return Ok(Vec::new());
        }
        let mut limit = limit.unwrap_or(self.capacity);
        if limit > self.capacity {
            limit = self.capacity;
        }
        let first_open_time = self
            .latest_open_time
            .saturating_sub(self.interval.to_millis() * (limit as u64 - 1));

        let mut klines = Vec::with_capacity(limit);
        for i in 0..limit as u64 {
            let open_time = first_open_time + i * self.interval.to_millis();
            let index = (open_time / self.interval.to_millis() % (self.capacity as u64)) as usize;
            if let Some(kline) = &self.klines[index]
                && kline.open_time == open_time
            {
                klines.push(Some(kline.clone()));
            } else {
                klines.push(None);
            }
        }
        Ok(klines)
    }
}

pub struct KlineCache {
    kline_caches: DashMap<(String, KlineInterval), Arc<RwLock<SymbolKlineCache>>>,
    capacity: usize,
}

impl KlineCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            kline_caches: DashMap::new(),
            capacity,
        }
    }

    pub async fn add_kline(&self, kline: KlineData) -> Result<Vec<KlineData>> {
        let key = (kline.symbol.clone(), kline.interval.clone());
        let symbol_cache = self
            .kline_caches
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(RwLock::new(SymbolKlineCache::new(
                    kline.symbol.clone(),
                    kline.interval.clone(),
                    self.capacity,
                )))
            })
            .clone();
        let mut cache = symbol_cache.write().await;
        cache.add_kline(kline)
    }

    pub async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        limit: Option<usize>,
    ) -> Result<Vec<Option<KlineData>>> {
        let key = (symbol.to_string(), interval);
        if let Some(symbol_cache) = self.kline_caches.get(&key) {
            let cache = symbol_cache.value().read().await;
            cache.get_klines(limit)
        } else {
            Ok(Vec::new())
        }
    }
}

pub struct SymbolTradeCache {
    symbol: String,
    capacity: usize,
    trades: Vec<Option<Trade>>,
    latest_seq_id: u64,
    size: u64,
}

impl SymbolTradeCache {
    fn new(symbol: String, capacity: usize) -> Self {
        Self {
            symbol: symbol,
            capacity: capacity,
            trades: vec![None; capacity],
            latest_seq_id: 0,
            size: 0,
        }
    }

    fn add_trade(&mut self, trade: Trade) -> Result<Vec<Trade>> {
        if self.symbol != trade.symbol {
            return Err(PlatformError::DataManagerError {
                message: format!(
                    "Symbol mismatch: cache symbol {}, trade symbol {}",
                    self.symbol, trade.symbol
                ),
            });
        }

        if trade.seq_id <= self.latest_seq_id.saturating_sub(self.capacity as u64) {
            return Ok(Vec::new());
        }

        let mut removed_trades = Vec::with_capacity(
            (trade.seq_id.saturating_sub(self.latest_seq_id)).min(self.size) as usize,
        );
        if trade.seq_id > self.latest_seq_id {
            let mut old_first_seq_id = self.latest_seq_id.saturating_sub(self.capacity as u64 - 1);
            let new_first_seq_id = trade.seq_id.saturating_sub(self.capacity as u64 - 1);
            while old_first_seq_id < new_first_seq_id && self.size > 0 {
                let index = (old_first_seq_id % (self.capacity as u64)) as usize;
                if let Some(remove_trade) = &self.trades[index]
                    && remove_trade.seq_id == old_first_seq_id
                {
                    removed_trades.push(remove_trade.clone());
                    self.size -= 1;
                }
                old_first_seq_id += 1;
            }

            self.latest_seq_id = trade.seq_id;
        }

        let index = (trade.seq_id % (self.capacity as u64)) as usize;
        if let Some(exist_trade) = &self.trades[index]
            && exist_trade.seq_id == trade.seq_id
        {
            // do nothing, just replace
        } else {
            self.size += 1;
        }
        self.trades[index] = Some(trade);

        Ok(removed_trades)
    }

    fn get_trades(&self, limit: Option<usize>) -> Result<Vec<Option<Trade>>> {
        if self.size == 0 {
            return Ok(Vec::new());
        }
        let mut limit = limit.unwrap_or(self.capacity);
        if limit > self.capacity {
            limit = self.capacity;
        }
        let first_seq_id = self.latest_seq_id.saturating_sub(limit as u64 - 1);

        let mut trades = Vec::with_capacity(limit);
        for i in 0..limit as u64 {
            let index = ((first_seq_id + i) % (self.capacity as u64)) as usize;
            if let Some(trade) = &self.trades[index]
                && trade.seq_id == first_seq_id + i
            {
                trades.push(Some(trade.clone()));
            } else {
                trades.push(None);
            }
        }
        Ok(trades)
    }
}

pub struct TradeCache {
    trade_caches: DashMap<String, Arc<RwLock<SymbolTradeCache>>>,
    capacity: usize,
}

impl TradeCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            trade_caches: DashMap::new(),
            capacity,
        }
    }

    pub async fn add_trade(&self, trade: Trade) -> Result<Vec<Trade>> {
        let key = trade.symbol.clone();
        let symbol_cache = self
            .trade_caches
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(RwLock::new(SymbolTradeCache::new(
                    trade.symbol.clone(),
                    self.capacity,
                )))
            })
            .clone();
        let mut cache = symbol_cache.write().await;
        cache.add_trade(trade)
    }

    pub async fn get_trades(
        &self,
        symbol: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Option<Trade>>> {
        let key = symbol.to_string();
        if let Some(symbol_cache) = self.trade_caches.get(&key) {
            let cache = symbol_cache.value().read().await;
            cache.get_trades(limit)
        } else {
            Ok(Vec::new())
        }
    }
}

pub struct Ticker24hrCache {
    tickers: DashMap<String, Arc<RwLock<Ticker24hr>>>,
}

impl Ticker24hrCache {
    pub fn new() -> Self {
        Self {
            tickers: DashMap::new(),
        }
    }

    pub async fn update_ticker(&self, update: Ticker24hr) {
        let ticker = self
            .tickers
            .entry(update.symbol.clone())
            .or_insert_with(|| Arc::new(RwLock::new(update.clone())))
            .clone();

        let mut ticker = ticker.write().await;
        if ticker.close_time < update.close_time {
            *ticker = update;
        }
    }

    pub async fn get_ticker(&self, symbol: &str) -> Option<Ticker24hr> {
        if let Some(ticker) = self.tickers.get(symbol) {
            let ticker = ticker.value().read().await;
            Some(ticker.clone())
        } else {
            None
        }
    }
}

pub struct DepthCache {
    depths: DashMap<String, Arc<RwLock<DepthData>>>,
}

impl DepthCache {
    pub fn new() -> Self {
        Self {
            depths: DashMap::new(),
        }
    }

    pub async fn update_depth(&self, update: DepthData) {
        let depth = self
            .depths
            .entry(update.symbol.clone())
            .or_insert_with(|| Arc::new(RwLock::new(update.clone())))
            .clone();

        let mut depth = depth.write().await;
        if depth.timestamp < update.timestamp {
            *depth = update;
        }
    }

    pub async fn get_depth(&self, symbol: &str) -> Option<DepthData> {
        if let Some(depth) = self.depths.get(symbol) {
            let depth = depth.value().read().await;
            Some(depth.clone())
        } else {
            None
        }
    }
}

pub struct MarketDataCache {
    kline_cache: KlineCache,
    trade_cache: TradeCache,
    ticker_cache: Ticker24hrCache,
    depth_cache: DepthCache,
}

impl MarketDataCache {
    pub fn new(kline_capacity: usize, trade_capacity: usize) -> Self {
        Self {
            kline_cache: KlineCache::new(kline_capacity),
            trade_cache: TradeCache::new(trade_capacity),
            ticker_cache: Ticker24hrCache::new(),
            depth_cache: DepthCache::new(),
        }
    }

    pub async fn add_kline(&self, kline: KlineData) -> Result<Vec<KlineData>> {
        self.kline_cache.add_kline(kline).await
    }

    pub async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        limit: Option<usize>,
    ) -> Result<Vec<Option<KlineData>>> {
        self.kline_cache.get_klines(symbol, interval, limit).await
    }

    pub async fn add_trade(&self, trade: Trade) -> Result<Vec<Trade>> {
        self.trade_cache.add_trade(trade).await
    }

    pub async fn get_trades(
        &self,
        symbol: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Option<Trade>>> {
        self.trade_cache.get_trades(symbol, limit).await
    }

    pub async fn update_ticker(&self, update: Ticker24hr) {
        self.ticker_cache.update_ticker(update).await;
    }

    pub async fn get_ticker(&self, symbol: &str) -> Option<Ticker24hr> {
        self.ticker_cache.get_ticker(symbol).await
    }

    pub async fn update_depth(&self, update: DepthData) {
        self.depth_cache.update_depth(update).await;
    }

    pub async fn get_depth(&self, symbol: &str) -> Option<DepthData> {
        self.depth_cache.get_depth(symbol).await
    }
}
