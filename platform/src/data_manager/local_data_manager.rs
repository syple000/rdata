use crate::{
    config::PlatformConfig,
    data_manager::{db::*, MarketDataManager},
    errors::{PlatformError, Result},
    models::{DepthData, KlineData, KlineInterval, MarketType, SymbolInfo, Ticker24hr, Trade},
};
use async_trait::async_trait;
use db::sqlite::SQLiteDB;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::RwLock;

pub struct Clock {
    cur_ts: AtomicU64, // 毫秒时间戳
}

impl Clock {
    pub fn new(cur_ts: u64) -> Self {
        Self {
            cur_ts: AtomicU64::new(cur_ts),
        }
    }

    pub fn set_cur_ts(&self, cur_ts: u64) {
        self.cur_ts.store(cur_ts, Ordering::Release);
    }

    pub fn cur_ts(&self) -> u64 {
        self.cur_ts.load(Ordering::Acquire)
    }
}

pub struct LocalMarketDataManager {
    clock: Arc<Clock>,

    db: Arc<SQLiteDB>,
    max_cache_size: usize,

    klines: Arc<HashMap<(MarketType, String, KlineInterval), Arc<RwLock<VecDeque<KlineData>>>>>,
    trades: Arc<HashMap<(MarketType, String), Arc<RwLock<VecDeque<Trade>>>>>,
}

impl LocalMarketDataManager {
    pub fn new(
        config: Arc<PlatformConfig>,
        clock: Arc<Clock>,
        db: Arc<SQLiteDB>,
        max_cache_size: usize,
    ) -> Self {
        let mut klines = HashMap::new();
        let mut trades = HashMap::new();
        for market_type in config.markets.iter() {
            let market_config = config.configs.get(market_type).unwrap();
            for symbol in market_config.subscribed_symbols.iter() {
                for interval in market_config.subscribed_kline_intervals.iter() {
                    klines.insert(
                        (market_type.clone(), symbol.clone(), interval.clone()),
                        Arc::new(RwLock::new(VecDeque::with_capacity(max_cache_size))),
                    );
                }
                trades.insert(
                    (market_type.clone(), symbol.clone()),
                    Arc::new(RwLock::new(VecDeque::with_capacity(max_cache_size))),
                );
            }
        }

        Self {
            clock,
            db,
            max_cache_size,
            klines: Arc::new(klines),
            trades: Arc::new(trades),
        }
    }

    async fn load_klines(
        &self,
        market_type: &MarketType,
        symbol: &String,
        interval: &KlineInterval,
    ) -> Result<()> {
        let cur_ts = self.clock.cur_ts();

        let cache = match self
            .klines
            .get(&(market_type.clone(), symbol.clone(), interval.clone()))
        {
            None => {
                return Err(PlatformError::PlatformError {
                    message: format!(
                        "kline cache not found for {:?}, {}, {:?}",
                        market_type, symbol, interval
                    ),
                });
            }
            Some(cache) => cache,
        };

        let mut klines = cache.write().await;

        loop {
            if !klines.is_empty() && klines.back().unwrap().close_time > cur_ts {
                return Ok(());
            }
            let start_time = if !klines.is_empty() {
                Some(klines.back().unwrap().close_time + 1)
            } else {
                None
            };
            let end_time = if start_time.is_none() {
                Some(cur_ts)
            } else {
                None
            };
            let db_klines = get_klines(
                self.db.clone(),
                market_type,
                symbol,
                interval,
                start_time,
                end_time,
                None,
            )
            .map_err(|e| PlatformError::PlatformError {
                message: format!("get klines db err: {}", e),
            })?;

            if db_klines.is_empty() {
                return Ok(());
            }

            for kline in db_klines.iter() {
                if klines.len() > self.max_cache_size {
                    klines.pop_front();
                }
                klines.push_back(kline.clone());
            }
        }
    }

    async fn load_trades(&self, market_type: &MarketType, symbol: &String) -> Result<()> {
        todo!()
    }
}

#[async_trait]
impl MarketDataManager for LocalMarketDataManager {
    async fn init(&self) -> Result<()> {
        for (market_type, symbol, interval) in self.klines.keys() {
            self.load_klines(market_type, symbol, interval).await?;
        }

        for (market_type, symbol) in self.trades.keys() {
            self.load_trades(market_type, symbol).await?;
        }

        Ok(())
    }

    async fn get_klines(
        &self,
        market_type: &MarketType,
        symbol: &String,
        interval: &KlineInterval,
        limit: Option<usize>,
    ) -> Result<Vec<KlineData>> {
        todo!()
    }

    async fn get_trades(
        &self,
        market_type: &MarketType,
        symbol: &String,
        limit: Option<usize>,
    ) -> Result<Vec<Trade>> {
        todo!()
    }

    async fn get_depth(
        &self,
        market_type: &MarketType,
        symbol: &String,
    ) -> Result<Option<DepthData>> {
        todo!()
    }

    async fn get_ticker(
        &self,
        market_type: &MarketType,
        symbol: &String,
    ) -> Result<Option<Ticker24hr>> {
        todo!()
    }

    async fn get_symbol_info(
        &self,
        market_type: &MarketType,
        symbol: &String,
    ) -> Result<Option<SymbolInfo>> {
        todo!()
    }

    async fn get_symbol(
        &self,
        market_type: &MarketType,
        base_asset: &String,
        quote_asset: &String,
    ) -> Result<Option<String>> {
        todo!()
    }
}

pub struct LocalTradeDataManager {}
