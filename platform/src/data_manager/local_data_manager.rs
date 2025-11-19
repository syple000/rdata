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

use crate::{
    config::PlatformConfig,
    data_manager::MarketDataManager,
    errors::Result,
    models::{DepthData, KlineData, KlineInterval, MarketType, SymbolInfo, Ticker24hr, Trade},
};

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
    config: Arc<PlatformConfig>,

    clock: Arc<Clock>,

    db: Arc<SQLiteDB>,
    load_batch_size: usize,
    max_cache_size: usize,

    klines: Arc<HashMap<(MarketType, String, KlineInterval), Arc<RwLock<VecDeque<KlineData>>>>>,
    trades: Arc<HashMap<(MarketType, String), Arc<RwLock<VecDeque<Trade>>>>>,
}

impl LocalMarketDataManager {
    pub fn new(
        config: Arc<PlatformConfig>,
        clock: Arc<Clock>,
        db: Arc<SQLiteDB>,
        load_batch_size: usize,
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
            config,
            clock,
            db,
            load_batch_size,
            max_cache_size,
            klines: Arc::new(klines),
            trades: Arc::new(trades),
        }
    }
}

#[async_trait]
impl MarketDataManager for LocalMarketDataManager {
    async fn init(&self) -> Result<()> {
        todo!()
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
