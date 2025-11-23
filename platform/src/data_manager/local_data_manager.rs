use crate::{
    config::PlatformConfig,
    data_manager::{db::*, MarketDataManager, TradeDataManager},
    errors::{PlatformError, Result},
    models::{
        Account, CancelOrderRequest, DepthData, KlineData, KlineInterval, MarketType, Order,
        PlaceOrderRequest, SymbolInfo, Ticker24hr, Trade, UserTrade,
    },
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

    cache_capacities: Arc<HashMap<MarketType, usize>>,
    klines: Arc<HashMap<(MarketType, String, KlineInterval), Arc<RwLock<VecDeque<KlineData>>>>>,
    trades: Arc<HashMap<(MarketType, String), Arc<RwLock<VecDeque<Trade>>>>>,
}

impl LocalMarketDataManager {
    pub fn new(
        config: Arc<PlatformConfig>,
        clock: Arc<Clock>,
        db: Arc<SQLiteDB>,
        max_cache_size: usize,
    ) -> Result<Self> {
        let mut cache_capacities = HashMap::new();
        let mut klines = HashMap::new();
        let mut trades = HashMap::new();
        for market_type in config.markets.iter() {
            let market_config = config.configs.get(market_type).unwrap();
            cache_capacities.insert(market_type.clone(), market_config.cache_capacity);
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

        Ok(Self {
            clock,
            db,
            max_cache_size,
            cache_capacities: Arc::new(cache_capacities),
            klines: Arc::new(klines),
            trades: Arc::new(trades),
        })
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
        let cur_ts = self.clock.cur_ts();

        let cache = match self.trades.get(&(market_type.clone(), symbol.clone())) {
            None => {
                return Err(PlatformError::PlatformError {
                    message: format!("trade cache not found for {:?}, {}", market_type, symbol),
                });
            }
            Some(cache) => cache,
        };

        let mut trades = cache.write().await;

        loop {
            if !trades.is_empty() && trades.back().unwrap().timestamp > cur_ts {
                return Ok(());
            }
            let from_seq_id = if !trades.is_empty() {
                Some(trades.back().unwrap().seq_id + 1)
            } else {
                None
            };
            let end_time = if from_seq_id.is_none() {
                Some(cur_ts)
            } else {
                None
            };
            let db_trades = get_trades(
                self.db.clone(),
                market_type,
                symbol,
                None,
                end_time,
                from_seq_id,
                None,
            )
            .map_err(|e| PlatformError::PlatformError {
                message: format!("get trades db err: {}", e),
            })?;

            if db_trades.is_empty() {
                return Ok(());
            }

            for trade in db_trades.iter() {
                if trades.len() > self.max_cache_size {
                    trades.pop_front();
                }
                trades.push_back(trade.clone());
            }
        }
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
        // 获取数据
        self.load_klines(market_type, symbol, interval).await?;

        let cache_capacity = match self.cache_capacities.get(market_type) {
            None => {
                return Err(PlatformError::PlatformError {
                    message: format!("cache capacity not found for {:?}", market_type),
                });
            }
            Some(capacity) => *capacity,
        };

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

        let klines = cache.read().await;

        let mut result: Vec<KlineData> = klines
            .iter()
            .filter(|kline| kline.close_time <= cur_ts)
            .rev()
            .take(limit.unwrap_or(cache_capacity))
            .cloned()
            .collect::<Vec<_>>();
        result.reverse();

        Ok(result)
    }

    async fn get_trades(
        &self,
        market_type: &MarketType,
        symbol: &String,
        limit: Option<usize>,
    ) -> Result<Vec<Trade>> {
        // 获取数据
        self.load_trades(market_type, symbol).await?;

        let cache_capacity = match self.cache_capacities.get(market_type) {
            None => {
                return Err(PlatformError::PlatformError {
                    message: format!("cache capacity not found for {:?}", market_type),
                });
            }
            Some(capacity) => *capacity,
        };

        let cur_ts = self.clock.cur_ts();

        let cache = match self.trades.get(&(market_type.clone(), symbol.clone())) {
            None => {
                return Err(PlatformError::PlatformError {
                    message: format!("trade cache not found for {:?}, {}", market_type, symbol),
                });
            }
            Some(cache) => cache,
        };

        let trades = cache.read().await;
        let mut result: Vec<Trade> = trades
            .iter()
            .filter(|trade| trade.timestamp <= cur_ts)
            .rev()
            .take(limit.unwrap_or(cache_capacity))
            .cloned()
            .collect::<Vec<_>>();
        result.reverse();

        Ok(result)
    }

    #[allow(unused_variables)]
    async fn get_depth(
        &self,
        market_type: &MarketType,
        symbol: &String,
    ) -> Result<Option<DepthData>> {
        unimplemented!("local data manager get depth not implemented");
    }

    #[allow(unused_variables)]
    async fn get_ticker(
        &self,
        market_type: &MarketType,
        symbol: &String,
    ) -> Result<Option<Ticker24hr>> {
        unimplemented!("local data manager get ticker not implemented");
    }

    #[allow(unused_variables)]
    async fn get_symbol_info(
        &self,
        market_type: &MarketType,
        symbol: &String,
    ) -> Result<Option<SymbolInfo>> {
        unimplemented!("local data manager get symbol info not implemented");
    }

    #[allow(unused_variables)]
    async fn get_symbol(
        &self,
        market_type: &MarketType,
        base_asset: &String,
        quote_asset: &String,
    ) -> Result<Option<String>> {
        unimplemented!("local data manager get symbol not implemented");
    }
}

pub struct LocalTradeDataManager {
    accounts: Arc<HashMap<MarketType, Arc<RwLock<Account>>>>,
    open_orders: Arc<HashMap<MarketType, Arc<RwLock<HashMap<String, Order>>>>>, // client_id
    closed_orders: Arc<HashMap<MarketType, Arc<RwLock<HashMap<String, Order>>>>>, // client_id
    user_trades: Arc<HashMap<MarketType, Arc<RwLock<HashMap<String, Vec<UserTrade>>>>>>, // order_id
}

impl LocalTradeDataManager {
    pub fn new(
        config: Arc<PlatformConfig>,
        init_accounts: HashMap<MarketType, Account>,
    ) -> Result<Self> {
        let mut accounts = HashMap::new();
        let mut open_orders = HashMap::new();
        let mut closed_orders = HashMap::new();
        let mut user_trades = HashMap::new();
        for market_type in config.markets.iter() {
            if !init_accounts.contains_key(market_type) {
                return Err(PlatformError::PlatformError {
                    message: format!("market type: {:?} not found in init accounts", market_type),
                });
            }
            accounts.insert(
                market_type.clone(),
                Arc::new(RwLock::new(init_accounts.get(market_type).unwrap().clone()))
                    as Arc<RwLock<Account>>,
            );
            open_orders.insert(
                market_type.clone(),
                Arc::new(RwLock::new(HashMap::new())) as Arc<RwLock<HashMap<String, Order>>>,
            );
            closed_orders.insert(
                market_type.clone(),
                Arc::new(RwLock::new(HashMap::new())) as Arc<RwLock<HashMap<String, Order>>>,
            );
            user_trades.insert(
                market_type.clone(),
                Arc::new(RwLock::new(HashMap::new()))
                    as Arc<RwLock<HashMap<String, Vec<UserTrade>>>>,
            );
        }

        Ok(Self {
            accounts: Arc::new(accounts),
            open_orders: Arc::new(open_orders),
            closed_orders: Arc::new(closed_orders),
            user_trades: Arc::new(user_trades),
        })
    }

    // 测试环境调整clock时，需要check一次订单是否有匹配的成交产生
    pub fn matching_order() -> Result<()> {
        todo!()
    }
}

#[async_trait]
impl TradeDataManager for LocalTradeDataManager {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn get_account(&self, market_type: &MarketType) -> Result<Option<Account>> {
        match self.accounts.get(market_type) {
            None => Err(PlatformError::PlatformError {
                message: format!("market type: {:?} account not found", market_type),
            }),
            Some(account_lock) => {
                let account = account_lock.read().await;
                Ok(Some(account.clone()))
            }
        }
    }

    async fn get_open_orders(&self, market_type: &MarketType) -> Result<Vec<Order>> {
        match self.open_orders.get(market_type) {
            None => Err(PlatformError::PlatformError {
                message: format!("market type: {:?} open orders not found", market_type),
            }),
            Some(orders_lock) => {
                let orders = orders_lock.read().await;
                Ok(orders.values().cloned().collect())
            }
        }
    }

    async fn get_user_trades_by_order(
        &self,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<UserTrade>> {
        let user_trades = match self.user_trades.get(market_type) {
            None => {
                return Err(PlatformError::PlatformError {
                    message: format!("market type: {:?} user trades not found", market_type),
                })
            }
            Some(user_trades_lock) => user_trades_lock.read().await,
        };
        if user_trades.contains_key(order_id) {
            Ok(user_trades
                .get(order_id)
                .unwrap()
                .iter()
                .filter(|e| e.symbol == symbol)
                .map(|e| e.clone())
                .collect())
        } else {
            Ok(vec![])
        }
    }

    async fn get_orders(
        &self,
        market_type: &MarketType,
        symbol: &str,
        start_time: Option<u64>,
        end_time: Option<u64>,
        limit: Option<usize>,
    ) -> Result<Vec<Order>> {
        let limit = limit.unwrap_or(1000);
        let open_orders = match self.open_orders.get(market_type) {
            None => {
                return Err(PlatformError::PlatformError {
                    message: format!("market type: {:?} open orders not found", market_type),
                })
            }
            Some(orders_lock) => orders_lock.read().await,
        };
        let closed_orders = match self.closed_orders.get(market_type) {
            None => {
                return Err(PlatformError::PlatformError {
                    message: format!("market type: {:?} closed orders not found", market_type),
                })
            }
            Some(orders_lock) => orders_lock.read().await,
        };
        let mut orders = vec![];
        orders.extend_from_slice(
            &open_orders
                .iter()
                .filter(|e| {
                    if start_time.is_some() && e.1.update_time < start_time.unwrap() {
                        return false;
                    }
                    if end_time.is_some() && e.1.update_time > end_time.unwrap() {
                        return false;
                    }
                    if symbol != e.1.symbol {
                        return false;
                    }
                    return true;
                })
                .map(|e| e.1.clone())
                .collect::<Vec<_>>(),
        );
        orders.extend_from_slice(
            &closed_orders
                .iter()
                .filter(|e| {
                    if start_time.is_some() && e.1.update_time < start_time.unwrap() {
                        return false;
                    }
                    if end_time.is_some() && e.1.update_time > end_time.unwrap() {
                        return false;
                    }
                    if symbol != e.1.symbol {
                        return false;
                    }
                    return true;
                })
                .map(|e| e.1.clone())
                .collect::<Vec<_>>(),
        );
        orders.sort_by(|a, b| a.update_time.cmp(&b.update_time));
        if orders.len() > limit {
            orders = orders[orders.len() - limit..].to_vec();
        }
        Ok(orders)
    }

    async fn get_user_trades(
        &self,
        market_type: &MarketType,
        symbol: &str,
        start_time: Option<u64>,
        end_time: Option<u64>,
        limit: Option<usize>,
    ) -> Result<Vec<crate::models::UserTrade>> {
        let limit = limit.unwrap_or(1000);
        let user_trades = match self.user_trades.get(market_type) {
            None => {
                return Err(PlatformError::PlatformError {
                    message: format!("market type: {:?} user trades not found", market_type),
                })
            }
            Some(user_trades_lock) => user_trades_lock.read().await,
        };
        let mut trades = user_trades
            .iter()
            .flat_map(|e| {
                e.1.iter()
                    .filter(|ut| {
                        if start_time.is_some() && ut.timestamp < start_time.unwrap() {
                            return false;
                        }
                        if end_time.is_some() && ut.timestamp > end_time.unwrap() {
                            return false;
                        }
                        if symbol != ut.symbol {
                            return false;
                        }
                        return true;
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        trades.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        if trades.len() > limit {
            trades = trades[trades.len() - limit..].to_vec();
        }
        Ok(trades)
    }

    #[allow(unused_variables)]
    async fn get_order_by_client_id(
        &self,
        market_type: &MarketType,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Option<Order>> {
        unimplemented!("local trade data manager get_order_by_client_id not implemented")
    }

    #[allow(unused_variables)]
    async fn get_order_by_id(
        &self,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Option<Order>> {
        unimplemented!("local trade data manager get_order_by_id not implemented")
    }

    #[allow(unused_variables)]
    async fn get_last_sync_ts(&self, market_type: &MarketType) -> Result<Option<u64>> {
        unimplemented!("local trade data manager get_last_sync_ts not implemented")
    }

    #[allow(unused_variables)]
    async fn place_order(&self, market_type: &MarketType, req: PlaceOrderRequest) -> Result<Order> {
        unimplemented!("local trade data manager place_order not implemented")
    }

    #[allow(unused_variables)]
    async fn cancel_order(&self, market_type: &MarketType, req: CancelOrderRequest) -> Result<()> {
        unimplemented!("local trade data manager cancel_order not implemented")
    }
}
