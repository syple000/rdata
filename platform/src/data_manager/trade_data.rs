use super::TradeDataManager;
use crate::{
    config::PlatformConfig,
    data_manager::db::*,
    errors::{PlatformError, Result},
    models::{
        Account, AccountUpdate, CancelOrderRequest, GetAllOrdersRequest, GetOpenOrdersRequest,
        GetUserTradesRequest, MarketType, Order, OrderStatus, PlaceOrderRequest, UserTrade,
    },
    trade_provider::TradeProvider,
};
use async_trait::async_trait;
use db::sqlite::SQLiteDB;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

struct OpenOrderTradeStat {
    orders: HashMap<String, Order>, // client_id -> order
}

pub struct TradeData {
    market_types: Arc<Vec<MarketType>>,
    refresh_intervals: Arc<HashMap<MarketType, Duration>>,
    shutdown_token: CancellationToken,
    trade_providers: Arc<HashMap<MarketType, Arc<dyn TradeProvider>>>,

    // 账户缓存
    accounts: Arc<HashMap<MarketType, Arc<RwLock<Option<Account>>>>>,
    // 在途订单缓存
    open_order_stats: Arc<HashMap<MarketType, Arc<RwLock<OpenOrderTradeStat>>>>,

    db: Arc<SQLiteDB>,
}

impl TradeData {
    pub fn new(
        config: Arc<PlatformConfig>,
        trade_providers: Arc<HashMap<MarketType, Arc<dyn TradeProvider>>>,
    ) -> Result<Self> {
        let market_types: Arc<Vec<MarketType>> = Arc::new(config.markets.clone());
        let db_path: String = config.db_path.clone();

        let mut accounts = HashMap::new();
        let mut stats = HashMap::new();
        let mut refresh_intervals = HashMap::new();
        for market_type in market_types.iter() {
            accounts.insert(market_type.clone(), Arc::new(RwLock::new(None)));
            stats.insert(
                market_type.clone(),
                Arc::new(RwLock::new(OpenOrderTradeStat {
                    orders: HashMap::new(),
                })),
            );
            refresh_intervals.insert(
                market_type.clone(),
                Duration::from_secs(
                    config.configs[&MarketType::BinanceSpot].trade_refresh_interval_secs,
                ),
            );
        }

        let db =
            Arc::new(
                SQLiteDB::new(&db_path).map_err(|e| PlatformError::DataManagerError {
                    message: format!("connect db failed: {}", e),
                })?,
            );

        Ok(Self {
            market_types,
            trade_providers,
            refresh_intervals: Arc::new(refresh_intervals),
            shutdown_token: CancellationToken::new(),
            accounts: Arc::new(accounts),
            open_order_stats: Arc::new(stats),
            db,
        })
    }

    async fn _fetch_api_data(
        market_type: &MarketType,
        trade_provider: Arc<dyn TradeProvider>,
    ) -> Result<(Account, Vec<Order>)> {
        let account = match trade_provider.get_account().await {
            Ok(acc) => acc,
            Err(e) => {
                return Err(PlatformError::DataManagerError {
                    message: format!(
                        "get account from trade provider failed for market_type {:?}: {}",
                        market_type, e
                    ),
                });
            }
        };

        let orders = match trade_provider
            .get_open_orders(GetOpenOrdersRequest { symbol: None })
            .await
        {
            Ok(ords) => ords,
            Err(e) => {
                return Err(PlatformError::DataManagerError {
                    message: format!(
                        "get open orders from trade provider failed for market_type {:?}: {}",
                        market_type, e
                    ),
                });
            }
        };

        Ok((account, orders))
    }

    async fn _fetch_all_orders(
        trade_provider: Arc<dyn TradeProvider>,
        symbol: String,
        start_time: u64,
        end_time: u64,
    ) -> Result<Vec<Order>> {
        let mut orders = vec![];
        let mut cur_start_time = start_time;
        loop {
            let mut batch: Vec<Order> = trade_provider
                .get_all_orders(GetAllOrdersRequest {
                    symbol: symbol.clone(),
                    from_id: None,
                    start_time: Some(cur_start_time),
                    end_time: Some(end_time),
                    limit: Some(1000),
                })
                .await
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!(
                        "get all orders from trade provider failed for symbol {}: {}",
                        symbol, e
                    ),
                })?;

            let batch_size = batch.len();
            orders.extend(batch.drain(..));
            if batch_size < 1000 {
                break;
            }
            cur_start_time = orders.last().unwrap().update_time;
        }
        Ok(orders)
    }

    async fn _fetch_all_trades(
        trade_provider: Arc<dyn TradeProvider>,
        symbol: String,
        start_time: u64,
        end_time: u64,
    ) -> Result<Vec<UserTrade>> {
        let mut trades = vec![];
        let mut cur_start_time = start_time;
        loop {
            let mut batch: Vec<UserTrade> = trade_provider
                .get_user_trades(GetUserTradesRequest {
                    symbol: symbol.clone(),
                    order_id: None,
                    from_id: None,
                    start_time: Some(cur_start_time),
                    end_time: Some(end_time),
                    limit: Some(1000),
                })
                .await
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!(
                        "get user trades from trade provider failed for symbol {}: {}",
                        symbol, e
                    ),
                })?;
            let batch_size = batch.len();
            trades.extend(batch.drain(..));
            if batch_size < 1000 {
                break;
            }
            cur_start_time = trades.last().unwrap().timestamp;
        }
        Ok(trades)
    }

    pub async fn init(&self) -> Result<()> {
        // 初始化数据库
        create_api_sync_ts_table(self.db.clone())?;
        create_account_balance_table(self.db.clone())?;
        create_orders_table(self.db.clone())?;
        create_user_trades_table(self.db.clone())?;

        for market_type in self.market_types.iter() {
            let trade_provider =
                self.trade_providers
                    .get(market_type)
                    .ok_or(PlatformError::DataManagerError {
                        message: format!(
                            "Trade provider not found for market type: {:?}",
                            market_type
                        ),
                    })?;

            let (api_account, api_orders) =
                Self::_fetch_api_data(market_type, trade_provider.clone()).await?;
            Self::update_account_inner(
                self.accounts.clone(),
                self.db.clone(),
                market_type,
                api_account,
            )
            .await?;
            for order in api_orders {
                Self::update_order_inner(
                    self.open_order_stats.clone(),
                    self.db.clone(),
                    market_type,
                    order,
                )
                .await?;
            }

            // 订阅/定期更新
            let shutdown_token = self.shutdown_token.clone();
            let db = self.db.clone();
            let open_order_stats = self.open_order_stats.clone();
            let market_type_clone = market_type.clone();
            let mut order_sub = trade_provider.subscribe_order();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => {
                            break;
                        },
                        order = order_sub.recv() => {
                            match order {
                                Ok(order) => {
                                    if Self::update_order_inner(
                                        open_order_stats.clone(),
                                        db.clone(),
                                        &market_type_clone,
                                        order,
                                    ).await.is_err() {
                                        log::error!("update order failed for market_type {:?}", market_type_clone);
                                    }
                                },
                                Err(e) => {
                                    log::error!("order subscription error for market_type {:?}: {}", market_type_clone, e);
                                }
                            }
                        }
                    }
                }
            });

            let shutdown_token = self.shutdown_token.clone();
            let db = self.db.clone();
            let open_order_stats = self.open_order_stats.clone();
            let market_type_clone = market_type.clone();
            let mut trade_sub = trade_provider.subscribe_user_trade();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => {
                            break;
                        },
                        trade = trade_sub.recv() => {
                            match trade {
                                Ok(trade) => {
                                    if Self::update_user_trade_inner(
                                        open_order_stats.clone(),
                                        db.clone(),
                                        &market_type_clone,
                                        trade,
                                    ).await.is_err() {
                                        log::error!("update user trade failed for market_type {:?}", market_type_clone);
                                    }
                                },
                                Err(e) => {
                                    log::error!("user trade subscription error for market_type {:?}: {}", market_type_clone, e);
                                }
                            }
                        }
                    }
                }
            });

            let shutdown_token = self.shutdown_token.clone();
            let db = self.db.clone();
            let accounts = self.accounts.clone();
            let market_type_clone = market_type.clone();
            let mut account_update_sub = trade_provider.subscribe_account_update();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => {
                            break;
                        },
                        account_update = account_update_sub.recv() => {
                            match account_update {
                                Ok(account_update) => {
                                    if Self::update_account_update_inner(
                                        accounts.clone(),
                                        db.clone(),
                                        &market_type_clone,
                                        account_update,
                                    ).await.is_err() {
                                        log::error!("update account update failed for market_type {:?}", market_type_clone);
                                    }
                                },
                                Err(e) => {
                                    log::error!("account update subscription error for market_type {:?}: {}", market_type_clone, e);
                                }
                            }
                        }
                    }
                }
            });

            let shutdown_token = self.shutdown_token.clone();
            let accounts = self.accounts.clone();
            let open_order_stats = self.open_order_stats.clone();
            let market_type_clone = market_type.clone();
            let refresh_interval = self
                .refresh_intervals
                .get(&market_type_clone)
                .unwrap()
                .clone();
            let trade_provider_clone = trade_provider.clone();
            let db = self.db.clone();
            tokio::spawn(async move {
                let mut interval_tick = tokio::time::interval(refresh_interval);
                loop {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => {
                            break;
                        },
                        _ = interval_tick.tick() => {
                            let (account , api_orders) =
                                match Self::_fetch_api_data(&market_type_clone, trade_provider_clone.clone()).await {
                                    Ok(data) => data,
                                    Err(e) => {
                                        log::error!("periodic fetch api data failed for market_type {:?}: {}", market_type_clone, e);
                                        continue;
                                    }
                                };
                            if Self::update_account_inner(
                                accounts.clone(),
                                db.clone(),
                                &market_type_clone,
                                account,
                            ).await.is_err() {
                                log::error!("update account failed for market_type {:?}", market_type_clone);
                                continue;
                            }
                            let mut update_open_order_err = false;
                            for order in api_orders {
                                if Self::update_order_inner(
                                    open_order_stats.clone(),
                                    db.clone(),
                                    &market_type_clone,
                                    order,
                                ).await.is_err() {
                                    update_open_order_err = true;
                                    log::error!("update order failed for market_type {:?}", market_type_clone);
                                }
                            }
                            if update_open_order_err {
                                continue;
                            }

                            // 从上一次更新的时间位置，获取全量的order/trade并更新
                            let symbols = match get_all_symbol(db.clone(), &market_type_clone) {
                                Ok(symbols) => symbols,
                                Err(e) => {
                                    log::error!("get all symbols failed for market_type {:?}: {}", market_type_clone, e);
                                    continue;
                                }
                            };

                            let current_ts = time::get_current_milli_timestamp() - 5 * 1000;
                            let mut last_sync_ts = match get_last_sync_ts(db.clone(), &market_type_clone) {
                                Ok(ts) => ts.unwrap_or(current_ts - 24 * 60 * 60 * 1000 + 1), // 从未同步过，获取最近一天
                                Err(e) => {
                                    log::error!("get last sync ts failed for market_type {:?}: {}", market_type_clone, e);
                                    continue;
                                }
                            };

                            if current_ts - last_sync_ts >= 24 * 60 * 60 * 1000 {
                                log::warn!("large time gap detected for market_type {:?}, last_sync_ts {}, current_ts {}. Limiting to 1 day interval", market_type_clone, last_sync_ts, current_ts);
                                last_sync_ts = current_ts - 24 * 60 * 60 * 1000 + 1;
                            }

                            let mut orders = Vec::new();
                            let mut trades = Vec::new();
                            let mut get_order_trade_succ = true;
                            for symbol in symbols.iter() {
                                orders.extend(
                                    match Self::_fetch_all_orders(
                                        trade_provider_clone.clone(),
                                        symbol.clone(),
                                        last_sync_ts,
                                        current_ts,
                                    ).await {
                                        Ok(ords) => ords,
                                        Err(e) => {
                                            log::error!("fetch all orders failed for market_type {:?}, symbol {}: {}", market_type_clone, symbol, e);
                                            get_order_trade_succ = false;
                                            break;
                                        }
                                    }
                                );
                                trades.extend(
                                    match Self::_fetch_all_trades(
                                        trade_provider_clone.clone(),
                                        symbol.clone(),
                                        last_sync_ts,
                                        current_ts,
                                    ).await {
                                        Ok(trds) => trds,
                                        Err(e) => {
                                            log::error!("fetch all trades failed for market_type {:?}, symbol {}: {}", market_type_clone, symbol, e);
                                            get_order_trade_succ = false;
                                            break;
                                        }
                                    }
                                );
                            }
                            if !get_order_trade_succ {
                                continue;
                            }
                            let mut update_order_trade_err = false;
                            for order in orders {
                                if Self::update_order_inner(
                                    open_order_stats.clone(),
                                    db.clone(),
                                    &market_type_clone,
                                    order,
                                ).await.is_err() {
                                    update_order_trade_err = true;
                                    log::error!("update order failed for market_type {:?}", market_type_clone);
                                }
                            }
                            for trade in trades {
                                if Self::update_user_trade_inner(
                                    open_order_stats.clone(),
                                    db.clone(),
                                    &market_type_clone,
                                    trade,
                                ).await.is_err() {
                                    update_order_trade_err = true;
                                    log::error!("update user trade failed for market_type {:?}", market_type_clone);
                                }
                            }
                            if update_order_trade_err {
                                continue;
                            }

                            // 更新last_sync_ts
                            if update_last_sync_ts(
                                db.clone(),
                                &market_type_clone,
                                current_ts,
                            ).is_err() {
                                log::error!("update last sync ts failed for market_type {:?}", market_type_clone);
                            };
                        }
                    }
                }
            });
        }
        Ok(())
    }

    async fn update_order_inner(
        open_order_stats: Arc<HashMap<MarketType, Arc<RwLock<OpenOrderTradeStat>>>>,
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        order: Order,
    ) -> Result<()> {
        update_order(db.clone(), market_type, &order)?;

        let stat_lock =
            open_order_stats
                .get(market_type)
                .ok_or(PlatformError::DataManagerError {
                    message: format!(
                        "open_order_stats lock not found for market_type {:?}",
                        market_type
                    ),
                })?;
        let mut stat_guard = stat_lock.write().await;

        if !stat_guard.orders.contains_key(&order.client_order_id)
            || stat_guard.orders[&order.client_order_id].update_time <= order.update_time
        {
            stat_guard
                .orders
                .insert(order.client_order_id.clone(), order.clone());
        }

        if order.order_status != OrderStatus::New
            && order.order_status != OrderStatus::PendingNew
            && order.order_status != OrderStatus::PartiallyFilled
        {
            stat_guard.orders.remove(&order.client_order_id);
        }

        Ok(())
    }

    // 当前缓存状态不考虑trade，仅做透传
    async fn update_user_trade_inner(
        _open_order_stats: Arc<HashMap<MarketType, Arc<RwLock<OpenOrderTradeStat>>>>,
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        trade: UserTrade,
    ) -> Result<()> {
        update_user_trade(db.clone(), market_type, &trade)
    }

    async fn update_account_inner(
        accounts: Arc<HashMap<MarketType, Arc<RwLock<Option<Account>>>>>,
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        account: Account,
    ) -> Result<()> {
        update_account(db.clone(), market_type, &account)?;

        let account_lock = accounts
            .get(market_type)
            .ok_or(PlatformError::DataManagerError {
                message: format!("account lock not found for market_type {:?}", market_type),
            })?;
        let mut account_guard = account_lock.write().await;

        if account_guard.is_none() || account_guard.as_ref().unwrap().timestamp <= account.timestamp
        {
            *account_guard = Some(account.clone());
        }

        Ok(())
    }

    async fn update_account_update_inner(
        accounts: Arc<HashMap<MarketType, Arc<RwLock<Option<Account>>>>>,
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        account_update: AccountUpdate,
    ) -> Result<()> {
        update_account_update(db.clone(), market_type, &account_update)?;

        let account_lock = accounts
            .get(market_type)
            .ok_or(PlatformError::DataManagerError {
                message: format!("account lock not found for market_type {:?}", market_type),
            })?;
        let mut account_guard = account_lock.write().await;

        if let Some(account) = &mut *account_guard {
            if account.timestamp > account_update.timestamp {
                Ok(())
            } else {
                for updated_balance in &account_update.balances {
                    if let Some(balance) = account
                        .balances
                        .iter_mut()
                        .find(|b| b.asset == updated_balance.asset)
                    {
                        balance.free = updated_balance.free;
                        balance.locked = updated_balance.locked;
                    } else {
                        account.balances.push(updated_balance.clone());
                    }
                }
                account.timestamp = account_update.timestamp;

                Ok(())
            }
        } else {
            Err(PlatformError::DataManagerError {
                message: format!("account not initialized for market_type {:?}", market_type),
            })
        }
    }

    // 暴露db获取接口，仅供测试使用
    pub fn get_account_from_db(&self, market_type: &MarketType) -> Result<Option<Account>> {
        get_account(self.db.clone(), market_type)
    }

    pub fn get_open_orders_from_db(&self, market_type: &MarketType) -> Result<Vec<Order>> {
        get_open_orders(self.db.clone(), market_type)
    }
}

#[async_trait]
impl TradeDataManager for TradeData {
    async fn init(&self) -> Result<()> {
        self.init().await
    }

    async fn get_account(&self, market_type: &MarketType) -> Result<Option<Account>> {
        let account_lock =
            self.accounts
                .get(market_type)
                .ok_or(PlatformError::DataManagerError {
                    message: format!("account lock not found for market_type {:?}", market_type),
                })?;

        let account_guard = account_lock.read().await;
        Ok(account_guard.clone())
    }

    async fn get_open_orders(&self, market_type: &MarketType) -> Result<Vec<Order>> {
        let stat_lock =
            self.open_order_stats
                .get(market_type)
                .ok_or(PlatformError::DataManagerError {
                    message: format!(
                        "open_order_stats lock not found for market_type {:?}",
                        market_type
                    ),
                })?;

        let stat_guard = stat_lock.read().await;
        let orders: Vec<Order> = stat_guard.orders.values().cloned().collect();
        Ok(orders)
    }

    async fn get_user_trades_by_order(
        &self,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<UserTrade>> {
        get_user_trades_by_order(self.db.clone(), market_type, symbol, order_id)
    }

    async fn get_orders(
        &self,
        market_type: &MarketType,
        symbol: &str,
        start_time: Option<u64>,
        end_time: Option<u64>,
        limit: Option<usize>,
    ) -> Result<Vec<Order>> {
        get_orders(
            self.db.clone(),
            market_type,
            symbol,
            start_time,
            end_time,
            limit,
        )
    }

    async fn get_user_trades(
        &self,
        market_type: &MarketType,
        symbol: &str,
        start_time: Option<u64>,
        end_time: Option<u64>,
        limit: Option<usize>,
    ) -> Result<Vec<UserTrade>> {
        get_user_trades(
            self.db.clone(),
            market_type,
            symbol,
            start_time,
            end_time,
            limit,
        )
    }

    async fn get_order_by_client_id(
        &self,
        market_type: &MarketType,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Option<Order>> {
        get_order_by_client_id(self.db.clone(), market_type, symbol, client_order_id)
    }

    async fn get_order_by_id(
        &self,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Option<Order>> {
        get_order_by_id(self.db.clone(), market_type, symbol, order_id)
    }

    async fn get_last_sync_ts(&self, market_type: &MarketType) -> Result<Option<u64>> {
        get_last_sync_ts(self.db.clone(), market_type)
    }

    async fn place_order(&self, market_type: &MarketType, req: PlaceOrderRequest) -> Result<Order> {
        Self::update_order_inner(
            self.open_order_stats.clone(),
            self.db.clone(),
            market_type,
            Order::new_order_from_place_order_req(&req),
        )
        .await?;

        let trade_provider =
            self.trade_providers
                .get(market_type)
                .ok_or(PlatformError::DataManagerError {
                    message: format!(
                        "Trade provider not found for market type: {:?}",
                        market_type
                    ),
                })?;
        trade_provider.place_order(req).await
    }

    async fn cancel_order(&self, market_type: &MarketType, req: CancelOrderRequest) -> Result<()> {
        let trade_provider =
            self.trade_providers
                .get(market_type)
                .ok_or(PlatformError::DataManagerError {
                    message: format!(
                        "Trade provider not found for market type: {:?}",
                        market_type
                    ),
                })?;
        trade_provider.cancel_order(req).await
    }
}

impl Drop for TradeData {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
    }
}
