use crate::{
    config::Config,
    errors::{PlatformError, Result},
    models::{
        Account, AccountUpdate, Balance, GetOpenOrdersRequest, GetUserTradesRequest, MarketType,
        Order, OrderStatus, UserTrade,
    },
    trade_provider::TradeProvider,
};
use db::{common::Row, sqlite::SQLiteDB};
use rust_decimal::Decimal;
use std::{collections::HashMap, hash::Hash, str::FromStr, sync::Arc};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

struct OpenOrderTradeStat {
    orders: HashMap<String, Order>,
    trades: HashMap<String, UserTrade>,
    order_trade_ids: HashMap<String, Vec<String>>,
}

pub struct TradeData {
    market_types: Arc<Vec<MarketType>>,
    shutdown_token: CancellationToken,

    // 账户缓存
    accounts: Arc<HashMap<MarketType, Arc<RwLock<Option<Account>>>>>,
    // 在途订单缓存
    open_order_stats: Arc<HashMap<MarketType, Arc<RwLock<OpenOrderTradeStat>>>>,

    db: Arc<SQLiteDB>,
}

impl TradeData {
    pub fn new(config: Config) -> Result<Self> {
        let market_types: Arc<Vec<MarketType>> =
            Arc::new(config.get("data_manager.market_types").map_err(|e| {
                PlatformError::DataManagerError {
                    message: format!("data_manager market_types not found: {}", e),
                }
            })?);
        let db_path: String =
            config
                .get("data_manager.db_path")
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("data_manager db_path not found: {}", e),
                })?;

        let mut accounts = HashMap::new();
        let mut stats = HashMap::new();
        for market_type in market_types.iter() {
            accounts.insert(market_type.clone(), Arc::new(RwLock::new(None)));
            stats.insert(
                market_type.clone(),
                Arc::new(RwLock::new(OpenOrderTradeStat {
                    orders: HashMap::new(),
                    trades: HashMap::new(),
                    order_trade_ids: HashMap::new(),
                })),
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
            shutdown_token: CancellationToken::new(),
            accounts: Arc::new(accounts),
            open_order_stats: Arc::new(stats),
            db,
        })
    }

    fn _create_account_balance_table(db: Arc<SQLiteDB>) -> Result<()> {
        let query = r#"
            CREATE TABLE IF NOT EXISTS account_balance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_type TEXT NOT NULL,
                asset TEXT NOT NULL,
                free TEXT NOT NULL,
                locked TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(market_type, asset)
            )
        "#;
        db.execute_update(query, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create account_balance table failed: {}", e),
            })?;

        Ok(())
    }

    fn _create_orders_table(db: Arc<SQLiteDB>) -> Result<()> {
        let query = r#"
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_type TEXT NOT NULL,
                symbol TEXT NOT NULL,
                order_id TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                order_side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                order_status TEXT NOT NULL,
                order_price TEXT NOT NULL,
                order_quantity TEXT NOT NULL,
                executed_qty TEXT NOT NULL,
                cummulative_quote_qty TEXT NOT NULL,
                time_in_force TEXT NOT NULL,
                stop_price TEXT NOT NULL,
                iceberg_qty TEXT NOT NULL,
                create_time INTEGER NOT NULL,
                update_time INTEGER NOT NULL,
                UNIQUE(market_type, symbol, order_id)
            )
        "#;
        db.execute_update(query, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create orders table failed: {}", e),
            })?;

        // 创建索引：按 market_type、symbol 和 update_time 查询（最近更新）
        let index = r#"
            CREATE INDEX IF NOT EXISTS idx_orders_market_type_symbol_update_time 
            ON orders(market_type, symbol, update_time DESC)
        "#;
        db.execute_update(index, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create orders index2 failed: {}", e),
            })?;

        // 创建索引：按 market_type、order_status 和 update_time 查询
        let index = r#"
            CREATE INDEX IF NOT EXISTS idx_orders_market_type_status_update_time 
            ON orders(market_type, order_status, update_time DESC)
        "#;
        db.execute_update(index, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create orders index3 failed: {}", e),
            })?;

        Ok(())
    }

    fn _create_user_trades_table(db: Arc<SQLiteDB>) -> Result<()> {
        let query = r#"
            CREATE TABLE IF NOT EXISTS user_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_type TEXT NOT NULL,
                trade_id TEXT NOT NULL,
                order_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                order_side TEXT NOT NULL,
                trade_price TEXT NOT NULL,
                trade_quantity TEXT NOT NULL,
                commission TEXT NOT NULL,
                commission_asset TEXT NOT NULL,
                is_maker INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                UNIQUE(market_type, symbol, trade_id)
            )
        "#;
        db.execute_update(query, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create user_trades table failed: {}", e),
            })?;

        // 创建索引：按 market_type、symbol 和 order_id 查询（关联查询）
        let index = r#"
            CREATE INDEX IF NOT EXISTS idx_user_trades_market_type_order_id 
            ON user_trades(market_type, symbol, order_id)
        "#;
        db.execute_update(index, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create user_trades index2 failed: {}", e),
            })?;

        // 创建索引：按 market_type、symbol 和 timestamp 查询（最近交易）
        let index = r#"
            CREATE INDEX IF NOT EXISTS idx_user_trades_market_type_symbol_timestamp 
            ON user_trades(market_type, symbol, timestamp DESC)
        "#;
        db.execute_update(index, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create user_trades index3 failed: {}", e),
            })?;

        Ok(())
    }

    fn _update_account_balance(
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        balances: &Vec<Balance>,
        timestamp: u64,
    ) -> Result<()> {
        let placeholders = balances
            .iter()
            .map(|_| "(?, ?, ?, ?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            r#"
            INSERT INTO account_balance (market_type, asset, free, locked, updated_at)
            VALUES {}
            ON CONFLICT(market_type, asset) DO UPDATE SET
                free = excluded.free,
                locked = excluded.locked,
                updated_at = excluded.updated_at
        "#,
            placeholders
        );
        let mut params: Vec<String> = Vec::new();
        for balance in balances {
            params.push(market_type.as_str().to_string());
            params.push(balance.asset.clone());
            params.push(balance.free.to_string());
            params.push(balance.locked.to_string());
            params.push(timestamp.to_string());
        }
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

        db.execute_update(&query, &params_refs)
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("update account balance err: {}", e),
            })?;
        Ok(())
    }

    fn _update_account_update(
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        account_update: &AccountUpdate,
    ) -> Result<()> {
        Self::_update_account_balance(
            db,
            market_type,
            &account_update.balances,
            account_update.timestamp,
        )
    }

    fn _update_account(
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        account: &Account,
    ) -> Result<()> {
        Self::_update_account_balance(db, market_type, &account.balances, account.timestamp)
    }

    fn _update_order(db: Arc<SQLiteDB>, market_type: &MarketType, order: &Order) -> Result<()> {
        let query = r#"
            INSERT INTO orders (
                market_type, symbol, order_id, client_order_id, order_side, 
                order_type, order_status, order_price, order_quantity, 
                executed_qty, cummulative_quote_qty, time_in_force, 
                stop_price, iceberg_qty, create_time, update_time
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
            ON CONFLICT(market_type, symbol, order_id) DO UPDATE SET
                client_order_id = excluded.client_order_id,
                order_side = excluded.order_side,
                order_type = excluded.order_type,
                order_status = excluded.order_status,
                order_price = excluded.order_price,
                order_quantity = excluded.order_quantity,
                executed_qty = excluded.executed_qty,
                cummulative_quote_qty = excluded.cummulative_quote_qty,
                time_in_force = excluded.time_in_force,
                stop_price = excluded.stop_price,
                iceberg_qty = excluded.iceberg_qty,
                update_time = excluded.update_time
        "#;

        let params: Vec<String> = vec![
            market_type.as_str().to_string(),
            order.symbol.clone(),
            order.order_id.clone(),
            order.client_order_id.clone(),
            serde_json::to_string(&order.order_side).unwrap(),
            serde_json::to_string(&order.order_type).unwrap(),
            serde_json::to_string(&order.order_status).unwrap(),
            order.order_price.to_string(),
            order.order_quantity.to_string(),
            order.executed_qty.to_string(),
            order.cummulative_quote_qty.to_string(),
            serde_json::to_string(&order.time_in_force).unwrap(),
            order.stop_price.to_string(),
            order.iceberg_qty.to_string(),
            order.create_time.to_string(),
            order.update_time.to_string(),
        ];
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

        db.execute_update(query, &params_refs)
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("update order err: {}", e),
            })?;
        Ok(())
    }

    fn _update_user_trade(
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        trade: &UserTrade,
    ) -> Result<()> {
        let query = r#"
            INSERT INTO user_trades (
                market_type, trade_id, order_id, symbol, order_side,
                trade_price, trade_quantity, commission, commission_asset,
                is_maker, timestamp
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(market_type, symbol, trade_id) DO UPDATE SET
                order_id = excluded.order_id,
                order_side = excluded.order_side,
                trade_price = excluded.trade_price,
                trade_quantity = excluded.trade_quantity,
                commission = excluded.commission,
                commission_asset = excluded.commission_asset,
                is_maker = excluded.is_maker,
                timestamp = excluded.timestamp
        "#;

        let params: Vec<String> = vec![
            market_type.as_str().to_string(),
            trade.trade_id.clone(),
            trade.order_id.clone(),
            trade.symbol.clone(),
            serde_json::to_string(&trade.order_side).unwrap(),
            trade.trade_price.to_string(),
            trade.trade_quantity.to_string(),
            trade.commission.to_string(),
            trade.commission_asset.clone(),
            (trade.is_maker as i64).to_string(),
            trade.timestamp.to_string(),
        ];
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

        db.execute_update(query, &params_refs)
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("update user trade err: {}", e),
            })?;
        Ok(())
    }

    fn _get_account(db: Arc<SQLiteDB>, market_type: &MarketType) -> Result<Option<Account>> {
        let get_row_column_string = |row: &Row, col: &str| -> Result<String> {
            row.get_string(col).ok_or(PlatformError::DataManagerError {
                message: format!("column {} not found", col),
            })
        };
        let get_row_column_i64 = |row: &Row, col: &str| -> Result<i64> {
            row.get_i64(col).ok_or(PlatformError::DataManagerError {
                message: format!("column {} not found", col),
            })
        };
        let get_row_column_decimal = |row: &Row, col: &str| -> Result<Decimal> {
            let s = row.get_string(col).ok_or(PlatformError::DataManagerError {
                message: format!("column {} not found", col),
            })?;
            Decimal::from_str(&s).map_err(|e| PlatformError::DataManagerError {
                message: format!("parse decimal error for column {}: {}", col, e),
            })
        };

        // 1. 获取账户balance
        let query = r#"
            SELECT asset, free, locked, updated_at
            FROM account_balance
            WHERE market_type = ?1
        "#;
        let market_type_str = market_type.as_str().to_string();
        let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str];

        let result =
            db.execute_query(query, &params)
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("get account balance err: {}", e),
                })?;

        if result.is_empty() {
            return Ok(None);
        }

        let mut account = Account {
            balances: vec![],
            timestamp: 0,
        };
        for row in result.rows {
            let timestamp = get_row_column_i64(&row, "updated_at")? as u64;

            if timestamp > account.timestamp {
                account.timestamp = timestamp;
            }
            let asset = get_row_column_string(&row, "asset")?;
            let free = get_row_column_decimal(&row, "free")?;
            let locked = get_row_column_decimal(&row, "locked")?;
            account.balances.push(Balance {
                asset,
                free,
                locked,
            });
        }

        Ok(Some(account))
    }

    fn _get_orders(
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        symbol: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Order>> {
        let limit = limit.unwrap_or(1000);
        let query = format!(
            r#"
            SELECT symbol, order_id, client_order_id, order_side, order_type,
                   order_status, order_price, order_quantity, executed_qty,
                   cummulative_quote_qty, time_in_force, stop_price, iceberg_qty,
                   create_time, update_time
            FROM orders
            WHERE market_type = ?1 AND symbol = ?2
            ORDER BY update_time DESC
            LIMIT {}
        "#,
            limit
        );
        let market_type_str = market_type.as_str().to_string();
        let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str, &symbol];

        let result =
            db.execute_query(&query, &params)
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("get orders err: {}", e),
                })?;

        result
            .into_struct()
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("get_orders into err: {}", e),
            })
    }

    fn _get_order_by_id(
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<Order>> {
        let query = r#"
            SELECT symbol, order_id, client_order_id, order_side, order_type,
                   order_status, order_price, order_quantity, executed_qty,
                   cummulative_quote_qty, time_in_force, stop_price, iceberg_qty,
                   create_time, update_time
            FROM orders
            WHERE market_type = ?1 AND symbol = ?2 AND order_id = ?3
        "#;
        let market_type_str = market_type.as_str().to_string();
        let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str, &symbol, &order_id];

        let result =
            db.execute_query(&query, &params)
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("get orders by ids err: {}", e),
                })?;

        result
            .into_struct()
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("get_order_by_ids into err: {}", e),
            })
    }

    fn _get_open_orders(db: Arc<SQLiteDB>, market_type: &MarketType) -> Result<Vec<Order>> {
        let query = r#"
            SELECT symbol, order_id, client_order_id, order_side, order_type,
                   order_status, order_price, order_quantity, executed_qty,
                   cummulative_quote_qty, time_in_force, stop_price, iceberg_qty,
                   create_time, update_time
            FROM orders
            WHERE market_type = ?1 AND order_status IN ('NEW', 'PENDING_NEW', 'PARTIALLY_FILLED')
            ORDER BY update_time DESC
        "#;
        let market_type_str = market_type.as_str().to_string();
        let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str];

        let result =
            db.execute_query(query, &params)
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("get open orders err: {}", e),
                })?;

        result
            .into_struct()
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("get_open_orders into err: {}", e),
            })
    }

    fn _get_user_trades(
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        symbol: &str,
        limit: Option<usize>,
    ) -> Result<Vec<UserTrade>> {
        let limit = limit.unwrap_or(1000);
        let query = format!(
            r#"
            SELECT trade_id, order_id, symbol, order_side, trade_price,
                   trade_quantity, commission, commission_asset, is_maker, timestamp
            FROM user_trades
            WHERE market_type = ?1 AND symbol = ?2
            ORDER BY timestamp DESC
            LIMIT {}
        "#,
            limit
        );
        let market_type_str = market_type.as_str().to_string();
        let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str, &symbol];

        let result =
            db.execute_query(&query, &params)
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("get user trades err: {}", e),
                })?;

        result
            .into_struct()
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("get_user_trades into err: {}", e),
            })
    }

    fn _get_user_trades_by_order_id(
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<UserTrade>> {
        let query = r#"
            SELECT trade_id, order_id, symbol, order_side, trade_price,
                   trade_quantity, commission, commission_asset, is_maker, timestamp
            FROM user_trades
            WHERE market_type = ?1 AND symbol = ?2 AND order_id = ?3
            ORDER BY timestamp DESC
        "#;
        let market_type_str = market_type.as_str().to_string();
        let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str, &symbol, &order_id];

        let result =
            db.execute_query(&query, &params)
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("get user trades by order_id err: {}", e),
                })?;

        result
            .into_struct()
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("get_user_trades_by_order_ids into err: {}", e),
            })
    }

    fn _account_equal(account1: &Option<Account>, account2: &Option<Account>) -> bool {
        if account1.is_none() && account2.is_none() {
            return true;
        }
        if account1.is_none() || account2.is_none() {
            return false;
        }
        let account1 = account1.as_ref().unwrap();
        let account2 = account2.as_ref().unwrap();
        if account1.balances.len() != account2.balances.len() {
            return false;
        }
        let mut balance_map: HashMap<String, &Balance> = HashMap::new();
        for balance in &account1.balances {
            balance_map.insert(balance.asset.clone(), balance);
        }
        for balance in &account2.balances {
            match balance_map.get(&balance.asset) {
                Some(b1) => {
                    if b1.free != balance.free || b1.locked != balance.locked {
                        return false;
                    }
                }
                None => {
                    return false;
                }
            }
        }
        true
    }

    fn _orders_equal(orders1: &Vec<Order>, orders2: &Vec<Order>) -> bool {
        if orders1.len() != orders2.len() {
            return false;
        }
        let mut order_map: HashMap<String, &Order> = HashMap::new();
        for order in orders1 {
            order_map.insert(order.order_id.clone(), order);
        }
        for order in orders2 {
            match order_map.get(&order.order_id) {
                Some(o1) => {
                    if o1 != &order {
                        return false;
                    }
                }
                None => {
                    return false;
                }
            }
        }
        true
    }

    fn _trades_equal(trades1: &Vec<UserTrade>, trades2: &Vec<UserTrade>) -> bool {
        if trades1.len() != trades2.len() {
            return false;
        }
        let mut trade_map: HashMap<String, &UserTrade> = HashMap::new();
        for trade in trades1 {
            trade_map.insert(trade.trade_id.clone(), trade);
        }
        for trade in trades2 {
            match trade_map.get(&trade.trade_id) {
                Some(t1) => {
                    if t1 != &trade {
                        return false;
                    }
                }
                None => {
                    return false;
                }
            }
        }
        true
    }

    pub async fn init(
        &self,
        trade_providers: HashMap<MarketType, Arc<dyn TradeProvider>>,
    ) -> Result<()> {
        for market_type in self.market_types.iter() {
            let trade_provider =
                trade_providers
                    .get(market_type)
                    .ok_or(PlatformError::DataManagerError {
                        message: format!(
                            "Trade provider not found for market type: {:?}",
                            market_type
                        ),
                    })?;
            let account_lock =
                self.accounts
                    .get(market_type)
                    .ok_or(PlatformError::DataManagerError {
                        message: format!(
                            "account lock not found for market_type {:?}",
                            market_type
                        ),
                    })?;
            let stat_lock =
                self.open_order_stats
                    .get(market_type)
                    .ok_or(PlatformError::DataManagerError {
                        message: format!(
                            "open_order_stats lock not found for market_type {:?}",
                            market_type
                        ),
                    })?;
            // 初始化数据库
            Self::_create_account_balance_table(self.db.clone())?;
            Self::_create_orders_table(self.db.clone())?;
            Self::_create_user_trades_table(self.db.clone())?;

            // 校验数据库和api数据一致性（更新缓存数据）
            // 1. account
            let db_account = Self::_get_account(self.db.clone(), market_type)?;
            let api_account = match trade_provider.get_account().await {
                Ok(acc) => Some(acc),
                Err(e) => {
                    return Err(PlatformError::DataManagerError {
                        message: format!(
                            "get account from trade provider failed for market_type {:?}: {}",
                            market_type, e
                        ),
                    });
                }
            };
            if !Self::_account_equal(&db_account, &api_account) {
                log::error!(
                    "account data mismatch for market_type {:?}: db_account={:?}, api_account={:?}",
                    market_type,
                    db_account,
                    api_account
                );
            }
            {
                let mut account_guard = account_lock.write().await;
                *account_guard = api_account;
            }

            // 2. open orders
            let db_orders = Self::_get_open_orders(self.db.clone(), market_type)?;
            let api_orders = match trade_provider
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
            if !Self::_orders_equal(&db_orders, &api_orders) {
                log::error!(
                    "open orders data mismatch for market_type {:?}: db_orders={:?}, api_orders={:?}",
                    market_type,
                    db_orders,
                    api_orders
                );
            }

            let order_ids: Vec<(String, String)> = api_orders
                .iter()
                .map(|o| (o.order_id.clone(), o.symbol.clone()))
                .collect();

            // 3. user trades
            let mut trades: Vec<UserTrade> = vec![];
            for (order_id, symbol) in order_ids {
                let db_trades = Self::_get_user_trades_by_order_id(
                    self.db.clone(),
                    market_type,
                    &symbol,
                    &order_id,
                )?;
                let mut api_trades = match trade_provider
                    .get_user_trades(GetUserTradesRequest {
                        symbol: symbol.clone(),
                        order_id: Some(order_id.clone()),
                        from_id: None,
                        start_time: None,
                        end_time: None,
                        limit: None,
                    })
                    .await
                {
                    Ok(trds) => trds,
                    Err(e) => {
                        return Err(PlatformError::DataManagerError {
                            message: format!(
                                "get user trades by order_id from trade provider failed for market_type {:?}, order_id {}: {}",
                                market_type, order_id, e
                            ),
                        });
                    }
                };
                if !Self::_trades_equal(&db_trades, &api_trades) {
                    log::error!(
                        "user trades data mismatch for market_type {:?}, order_id {}: db_trades={:?}, api_trades={:?}",
                        market_type,
                        order_id,
                        db_trades,
                        api_trades
                    );
                }
                trades.extend(api_trades.drain(..));
            }

            {
                let mut stat = stat_lock.write().await;
                api_orders.into_iter().for_each(|o| {
                    stat.orders.insert(o.order_id.clone(), o);
                });
                trades.into_iter().for_each(|t| {
                    stat.trades.insert(t.trade_id.clone(), t.clone());
                    stat.order_trade_ids
                        .entry(t.order_id.clone())
                        .or_insert(vec![])
                        .push(t.trade_id.clone());
                });
            }

            // 订阅并定期清理结束订单和更新
        }
        Ok(())
    }

    pub async fn update_order(&self, market_type: &MarketType, order: Order) -> Result<()> {
        let stat_lock =
            self.open_order_stats
                .get(market_type)
                .ok_or(PlatformError::DataManagerError {
                    message: format!(
                        "open_order_stats lock not found for market_type {:?}",
                        market_type
                    ),
                })?;

        let mut stat_guard = stat_lock.write().await;
        let entry = stat_guard
            .orders
            .entry(order.order_id.clone())
            .or_insert(order.clone());
        if entry.update_time < order.update_time {
            *entry = order.clone();
        } else {
            return Ok(());
        }

        Self::_update_order(self.db.clone(), market_type, &order)?;

        Ok(())
    }

    pub async fn update_user_trade(
        &self,
        market_type: &MarketType,
        trade: UserTrade,
    ) -> Result<()> {
        let stat_lock =
            self.open_order_stats
                .get(market_type)
                .ok_or(PlatformError::DataManagerError {
                    message: format!(
                        "open_order_stats lock not found for market_type {:?}",
                        market_type
                    ),
                })?;

        let mut stat_guard = stat_lock.write().await;
        if stat_guard.trades.contains_key(&trade.trade_id) {
            return Ok(());
        }
        stat_guard
            .trades
            .insert(trade.trade_id.clone(), trade.clone());
        stat_guard
            .order_trade_ids
            .entry(trade.order_id.clone())
            .or_insert(vec![])
            .push(trade.trade_id.clone());

        Self::_update_user_trade(self.db.clone(), market_type, &trade)?;
        Ok(())
    }

    pub async fn update_account(&self, market_type: &MarketType, account: Account) -> Result<()> {
        let account_lock =
            self.accounts
                .get(market_type)
                .ok_or(PlatformError::DataManagerError {
                    message: format!("account lock not found for market_type {:?}", market_type),
                })?;

        let mut account_guard = account_lock.write().await;
        if let Some(account_guard) = account_guard.as_ref() {
            if account_guard.timestamp > account.timestamp {
                return Ok(());
            }
        }
        *account_guard = Some(account.clone());

        Self::_update_account(self.db.clone(), market_type, &account)?;
        Ok(())
    }

    pub async fn update_account_update(
        &self,
        market_type: &MarketType,
        account_update: AccountUpdate,
    ) -> Result<()> {
        let account_lock =
            self.accounts
                .get(market_type)
                .ok_or(PlatformError::DataManagerError {
                    message: format!("account lock not found for market_type {:?}", market_type),
                })?;

        let mut account_guard = account_lock.write().await;
        if let Some(account) = &mut *account_guard {
            if account.timestamp > account_update.timestamp {
                return Ok(());
            }

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
            Self::_update_account_update(self.db.clone(), market_type, &account_update)?;
            Ok(())
        } else {
            return Err(PlatformError::DataManagerError {
                message: format!("account not initialized for market_type {:?}", market_type),
            });
        }
    }
}
