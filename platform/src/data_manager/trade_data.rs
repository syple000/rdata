use super::TradeDataManager;
use crate::{
    config::Config,
    errors::{PlatformError, Result},
    models::{
        Account, AccountUpdate, Balance, CancelOrderRequest, GetAllOrdersRequest,
        GetOpenOrdersRequest, GetUserTradesRequest, MarketType, Order, OrderStatus,
        PlaceOrderRequest, UserTrade,
    },
    trade_provider::TradeProvider,
};
use async_trait::async_trait;
use db::{common::Row, sqlite::SQLiteDB};
use rust_decimal::Decimal;
use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

struct OpenOrderTradeStat {
    orders: HashMap<String, Order>, // client_id -> order
}

pub struct TradeData {
    market_types: Arc<Vec<MarketType>>,
    refresh_interval: Duration,
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
        config: Config,
        trade_providers: Arc<HashMap<MarketType, Arc<dyn TradeProvider>>>,
    ) -> Result<Self> {
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
        let refresh_interval_secs: u64 = config
            .get("data_manager.refresh_interval_secs")
            .unwrap_or(300); // 默认300秒

        let mut accounts = HashMap::new();
        let mut stats = HashMap::new();
        for market_type in market_types.iter() {
            accounts.insert(market_type.clone(), Arc::new(RwLock::new(None)));
            stats.insert(
                market_type.clone(),
                Arc::new(RwLock::new(OpenOrderTradeStat {
                    orders: HashMap::new(),
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
            trade_providers,
            refresh_interval: Duration::from_secs(refresh_interval_secs),
            shutdown_token: CancellationToken::new(),
            accounts: Arc::new(accounts),
            open_order_stats: Arc::new(stats),
            db,
        })
    }

    fn _create_api_sync_ts_table(db: Arc<SQLiteDB>) -> Result<()> {
        let query = r#"
            CREATE TABLE IF NOT EXISTS api_sync_ts (
                market_type TEXT NOT NULL PRIMARY KEY,
                last_sync_ts INTEGER NOT NULL
            )
        "#;
        db.execute_update(query, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create api_sync_ts table failed: {}", e),
            })?;
        Ok(())
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
                UNIQUE(market_type, symbol, client_order_id)
            )
        "#;
        db.execute_update(query, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create orders table failed: {}", e),
            })?;

        let index = r#"
            CREATE INDEX IF NOT EXISTS idx_orders_market_type_symbol_update_time 
            ON orders(market_type, symbol, update_time DESC)
        "#;
        db.execute_update(index, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!(
                    "create orders (market_type, symbol, update_time) index failed: {}",
                    e
                ),
            })?;

        let index = r#"
            CREATE INDEX IF NOT EXISTS idx_orders_market_type_status_update_time 
            ON orders(market_type, order_status, update_time DESC)
        "#;
        db.execute_update(index, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!(
                    "create orders (market_type, order_status, update_time) index failed: {}",
                    e
                ),
            })?;

        let index = r#"
            CREATE INDEX IF NOT EXISTS idx_orders_market_type_order_id 
            ON orders(market_type, symbol, order_id)
        "#;
        db.execute_update(index, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!(
                    "create orders (market_type, symbol, order_id) index failed: {}",
                    e
                ),
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

        let index = r#"
            CREATE INDEX IF NOT EXISTS idx_user_trades_market_type_order_id 
            ON user_trades(market_type, symbol, order_id)
        "#;
        db.execute_update(index, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!(
                    "create user_trades (market_type, symbol, order_id) index failed: {}",
                    e
                ),
            })?;

        let index = r#"
            CREATE INDEX IF NOT EXISTS idx_user_trades_market_type_symbol_timestamp 
            ON user_trades(market_type, symbol, timestamp DESC)
        "#;
        db.execute_update(index, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!(
                    "create user_trades (market_type, symbol, timestamp) index failed: {}",
                    e
                ),
            })?;

        Ok(())
    }

    fn _get_last_sync_ts(db: Arc<SQLiteDB>, market_type: &MarketType) -> Result<Option<u64>> {
        let query = r#"
            SELECT last_sync_ts
            FROM api_sync_ts
            WHERE market_type = ?1
        "#;
        let market_type_str = market_type.as_str().to_string();
        let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str];

        let result =
            db.execute_query(query, &params)
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("get last sync ts err: {}", e),
                })?;

        if result.is_empty() {
            return Ok(None);
        }

        let row = &result.rows[0];
        let last_sync_ts = row
            .get_i64("last_sync_ts")
            .ok_or(PlatformError::DataManagerError {
                message: "column last_sync_ts not found".to_string(),
            })? as u64;

        Ok(Some(last_sync_ts))
    }

    fn _update_last_sync_ts(
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        last_sync_ts: u64,
    ) -> Result<()> {
        let query = r#"
            INSERT INTO api_sync_ts (market_type, last_sync_ts)
            VALUES (?1, ?2)
            ON CONFLICT(market_type) DO UPDATE SET
                last_sync_ts = excluded.last_sync_ts
            WHERE excluded.last_sync_ts >= api_sync_ts.last_sync_ts
        "#;
        let market_type_str = market_type.as_str().to_string();
        let last_sync_ts_i64 = last_sync_ts as i64;
        let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str, &last_sync_ts_i64];

        db.execute_update(query, &params)
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("update last sync ts err: {}", e),
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
            WHERE excluded.updated_at >= account_balance.updated_at
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
            ON CONFLICT(market_type, symbol, client_order_id) DO UPDATE SET
                order_id = excluded.order_id,
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
                create_time = excluded.create_time,
                update_time = excluded.update_time
            WHERE excluded.update_time >= orders.update_time
        "#;

        let params: Vec<String> = vec![
            market_type.as_str().to_string(),
            order.symbol.clone(),
            order.order_id.clone(),
            order.client_order_id.clone(),
            order.order_side.as_str().to_string(),
            order.order_type.as_str().to_string(),
            order.order_status.as_str().to_string(),
            order.order_price.to_string(),
            order.order_quantity.to_string(),
            order.executed_qty.to_string(),
            order.cummulative_quote_qty.to_string(),
            order.time_in_force.as_str().to_string(),
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
            trade.order_side.as_str().to_string(),
            trade.trade_price.to_string(),
            trade.trade_quantity.to_string(),
            trade.commission.to_string(),
            trade.commission_asset.clone(),
            trade.is_maker.to_string(),
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

    fn _get_order_by_client_id(
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Option<Order>> {
        let query = r#"
            SELECT symbol, order_id, client_order_id, order_side, order_type,
                   order_status, order_price, order_quantity, executed_qty,
                   cummulative_quote_qty, time_in_force, stop_price, iceberg_qty,
                   create_time, update_time
            FROM orders
            WHERE market_type = ?1 AND symbol = ?2 AND client_order_id = ?3
        "#;
        let market_type_str = market_type.as_str().to_string();
        let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str, &symbol, &client_order_id];

        let result =
            db.execute_query(&query, &params)
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("get orders by client id err: {}", e),
                })?;

        let orders: Vec<Order> =
            result
                .into_struct()
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("get_order_by_client_id into err: {}", e),
                })?;
        if orders.is_empty() {
            return Ok(None);
        }
        Ok(Some(orders[0].clone()))
    }

    fn _get_order_by_id(
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Option<Order>> {
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
                    message: format!("get orders by id err: {}", e),
                })?;

        let orders: Vec<Order> =
            result
                .into_struct()
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("get_order_by_id into err: {}", e),
                })?;
        if orders.is_empty() {
            return Ok(None);
        }
        Ok(Some(orders[0].clone()))
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

    fn _get_user_trades_by_order(
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

    fn _get_all_symbol(db: Arc<SQLiteDB>, market_type: &MarketType) -> Result<Vec<String>> {
        let query = r#"
            SELECT DISTINCT symbol
            FROM orders
            WHERE market_type = ?1
        "#;
        let market_type_str = market_type.as_str().to_string();
        let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str];

        let result =
            db.execute_query(&query, &params)
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("get all symbols err: {}", e),
                })?;

        let mut symbols = vec![];
        for row in result.rows {
            let symbol = row
                .get_string("symbol")
                .ok_or(PlatformError::DataManagerError {
                    message: "column symbol not found".to_string(),
                })?;
            symbols.push(symbol);
        }
        Ok(symbols)
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
        Self::_create_api_sync_ts_table(self.db.clone())?;
        Self::_create_account_balance_table(self.db.clone())?;
        Self::_create_orders_table(self.db.clone())?;
        Self::_create_user_trades_table(self.db.clone())?;

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
            let refresh_interval = self.refresh_interval.clone();
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
                            let symbols = match Self::_get_all_symbol(db.clone(), &market_type_clone) {
                                Ok(symbols) => symbols,
                                Err(e) => {
                                    log::error!("get all symbols failed for market_type {:?}: {}", market_type_clone, e);
                                    continue;
                                }
                            };

                            let current_ts = time::get_current_milli_timestamp() - 5 * 1000;
                            let mut last_sync_ts = match Self::_get_last_sync_ts(db.clone(), &market_type_clone) {
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
                            if Self::_update_last_sync_ts(
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
        Self::_update_order(db.clone(), market_type, &order)?;

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
        Self::_update_user_trade(db.clone(), market_type, &trade)
    }

    async fn update_account_inner(
        accounts: Arc<HashMap<MarketType, Arc<RwLock<Option<Account>>>>>,
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        account: Account,
    ) -> Result<()> {
        Self::_update_account(db.clone(), market_type, &account)?;

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
        Self::_update_account_update(db.clone(), market_type, &account_update)?;

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
        Self::_get_account(self.db.clone(), market_type)
    }

    pub fn get_open_orders_from_db(&self, market_type: &MarketType) -> Result<Vec<Order>> {
        Self::_get_open_orders(self.db.clone(), market_type)
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
        Self::_get_user_trades_by_order(self.db.clone(), market_type, symbol, order_id)
    }

    async fn get_orders(
        &self,
        market_type: &MarketType,
        symbol: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Order>> {
        Self::_get_orders(self.db.clone(), market_type, symbol, limit)
    }

    async fn get_user_trades(
        &self,
        market_type: &MarketType,
        symbol: &str,
        limit: Option<usize>,
    ) -> Result<Vec<UserTrade>> {
        Self::_get_user_trades(self.db.clone(), market_type, symbol, limit)
    }

    async fn get_order_by_client_id(
        &self,
        market_type: &MarketType,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Option<Order>> {
        Self::_get_order_by_client_id(self.db.clone(), market_type, symbol, client_order_id)
    }

    async fn get_order_by_id(
        &self,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Option<Order>> {
        Self::_get_order_by_id(self.db.clone(), market_type, symbol, order_id)
    }

    async fn get_last_sync_ts(&self, market_type: &MarketType) -> Result<Option<u64>> {
        Self::_get_last_sync_ts(self.db.clone(), market_type)
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
