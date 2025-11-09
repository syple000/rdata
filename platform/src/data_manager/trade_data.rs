use crate::{
    config::Config,
    errors::{PlatformError, Result},
    models::{
        Account, AccountUpdate, Balance, CancelOrderRequest, GetAllOrdersRequest,
        GetOpenOrdersRequest, GetOrderRequest, GetUserTradesRequest, MarketType, Order,
        OrderStatus, PlaceOrderRequest, UserTrade,
    },
    trade_provider::TradeProvider,
};
use db::{common::Row, sqlite::SQLiteDB};
use rust_decimal::Decimal;
use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

struct OpenOrderTradeStat {
    orders: HashMap<String, Order>,
    trades: HashMap<String, UserTrade>,
    order_trade_ids: HashMap<String, Vec<String>>,
}

pub struct TradeData {
    market_types: Arc<Vec<MarketType>>,
    symbols: Arc<HashMap<MarketType, Vec<String>>>,
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
        let mut symbols = HashMap::new();
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
            let ss: Vec<String> = config
                .get(&format!("data_manager.{}.symbols", market_type.as_str()))
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!(
                        "data_manager {} symbols not found: {}",
                        market_type.as_str(),
                        e
                    ),
                })?;
            symbols.insert(market_type.clone(), ss);
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
            symbols: Arc::new(symbols),
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
                message: format!(
                    "create orders (market_type, symbol, update_time) index failed: {}",
                    e
                ),
            })?;

        // 创建索引：按 market_type、order_status 和 update_time 查询
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
                message: format!(
                    "create user_trades (market_type, symbol, order_id) index failed: {}",
                    e
                ),
            })?;

        // 创建索引：按 market_type、symbol 和 timestamp 查询（最近交易）
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

    async fn _fetch_api_data(
        market_type: &MarketType,
        trade_provider: Arc<dyn TradeProvider>,
    ) -> Result<(Account, Vec<Order>, Vec<UserTrade>)> {
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
        let order_ids: Vec<(String, String)> = orders
            .iter()
            .map(|o| (o.order_id.clone(), o.symbol.clone()))
            .collect();

        let mut trades: Vec<UserTrade> = vec![];
        for (order_id, symbol) in order_ids {
            let mut order_trades = match trade_provider
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
            trades.extend(order_trades.drain(..));
        }

        Ok((account, orders, trades))
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
            let mut batch = trade_provider
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

            // 初始化数据库
            Self::_create_api_sync_ts_table(self.db.clone())?;
            Self::_create_account_balance_table(self.db.clone())?;
            Self::_create_orders_table(self.db.clone())?;
            Self::_create_user_trades_table(self.db.clone())?;

            // 初始化缓存
            let (api_account, api_orders, api_trades) =
                Self::_fetch_api_data(market_type, trade_provider.clone()).await?;
            self.update_account(market_type, api_account).await?;
            for order in api_orders {
                self.update_order(market_type, order).await?;
            }
            for trade in api_trades {
                self.update_user_trade(market_type, trade).await?;
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
                                    let _ = Self::update_order_inner(
                                        open_order_stats.clone(),
                                        db.clone(),
                                        &market_type_clone,
                                        order,
                                    ).await;
                                },
                                Err(_) => {
                                    // pass 不应该出现错误
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
                                    let _ = Self::update_user_trade_inner(
                                        open_order_stats.clone(),
                                        db.clone(),
                                        &market_type_clone,
                                        trade,
                                    ).await;
                                },
                                Err(_) => {
                                    // pass 不应该出现错误
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
                                    let _ = Self::update_account_update_inner(
                                        accounts.clone(),
                                        db.clone(),
                                        &market_type_clone,
                                        account_update,
                                    ).await;
                                },
                                Err(_) => {
                                    // pass 不应该出现错误
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
            let symbols = self
                .symbols
                .get(&market_type_clone)
                .unwrap_or(&vec![])
                .clone();
            tokio::spawn(async move {
                let mut interval_tick = tokio::time::interval(refresh_interval);
                loop {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => {
                            break;
                        },
                        _ = interval_tick.tick() => {
                            log::info!("periodic refresh for market_type {:?}", market_type_clone);

                            // 1. 获取当前缓存所有在途订单及其成交进行更新
                            let open_orders = {
                                if let Some(stat_lock) = open_order_stats.get(&market_type_clone) {
                                    let stat_guard = stat_lock.read().await;
                                    stat_guard.orders.values().cloned().collect::<Vec<Order>>()
                                } else {
                                    vec![]
                                }
                            };
                            for order in open_orders {
                                match trade_provider_clone
                                    .get_order(GetOrderRequest {
                                        symbol: order.symbol.clone(),
                                        order_id: Some(order.order_id.clone()),
                                        orig_client_order_id: None,
                                    })
                                    .await {
                                    Ok(order) => {
                                        let _ = Self::update_order_inner(
                                            open_order_stats.clone(),
                                            db.clone(),
                                            &market_type_clone,
                                            order,
                                        ).await;
                                    },
                                    Err(_) => {
                                        log::error!("periodic fetch order failed for market_type {:?}, order_id {}", market_type_clone, order.order_id);
                                    }
                                }
                                match trade_provider_clone
                                    .get_user_trades(GetUserTradesRequest {
                                        symbol: order.symbol.clone(),
                                        order_id: Some(order.order_id.clone()),
                                        from_id: None,
                                        start_time: None,
                                        end_time: None,
                                        limit: None,
                                    })
                                    .await {
                                    Ok(trades) => {
                                        for trade in trades {
                                            let _ = Self::update_user_trade_inner(
                                                open_order_stats.clone(),
                                                db.clone(),
                                                &market_type_clone,
                                                trade,
                                            ).await;
                                        }
                                    },
                                    Err(_) => {
                                        log::error!("periodic fetch user trades failed for market_type {:?}, order_id {}", market_type_clone, order.order_id);
                                    }
                                }
                            }

                            // 2. 获取在途订单、成交、账户
                            let (account , api_orders, api_trades) =
                                match Self::_fetch_api_data(&market_type_clone, trade_provider_clone.clone()).await {
                                    Ok(data) => data,
                                    Err(e) => {
                                        log::error!("periodic fetch api data failed for market_type {:?}: {}", market_type_clone, e);
                                        continue;
                                    }
                                };
                            let _ = Self::update_account_inner(
                                accounts.clone(),
                                db.clone(),
                                &market_type_clone,
                                account,
                            ).await;
                            for order in api_orders {
                                let _ = Self::update_order_inner(
                                    open_order_stats.clone(),
                                    db.clone(),
                                    &market_type_clone,
                                    order,
                                ).await;
                            }
                            for trade in api_trades {
                                let _ = Self::update_user_trade_inner(
                                    open_order_stats.clone(),
                                    db.clone(),
                                    &market_type_clone,
                                    trade,
                                ).await;
                            }

                            // 3. 从上一次更新的时间位置，获取全量的order/trade并更新。注意必须逐步成功，失败就跳过
                            let current_ts = time::get_current_milli_timestamp() - 60 * 1000;
                            let mut last_sync_ts = match Self::_get_last_sync_ts(db.clone(), &market_type_clone) {
                                Ok(ts) => ts.unwrap_or(current_ts - 24 * 60 * 60 * 1000 + 1), // 从未同步过，获取最近一天的订单
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
                            for order in orders {
                                let _ = Self::_update_order(
                                    db.clone(),
                                    &market_type_clone,
                                    &order,
                                );
                            }
                            for trade in trades {
                                let _ = Self::_update_user_trade(
                                    db.clone(),
                                    &market_type_clone,
                                    &trade,
                                );
                            }
                            // 更新last_sync_ts
                            let _ = Self::_update_last_sync_ts(
                                db.clone(),
                                &market_type_clone,
                                current_ts,
                            );
                        }
                    }
                }
            });
        }
        Ok(())
    }

    pub async fn update_order(&self, market_type: &MarketType, order: Order) -> Result<()> {
        Self::update_order_inner(
            self.open_order_stats.clone(),
            self.db.clone(),
            market_type,
            order,
        )
        .await
    }

    async fn update_order_inner(
        open_order_stats: Arc<HashMap<MarketType, Arc<RwLock<OpenOrderTradeStat>>>>,
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        order: Order,
    ) -> Result<()> {
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

        // 如果订单状态发生了更新，更新缓存
        if stat_guard.orders.contains_key(&order.order_id)
            && stat_guard.orders[&order.order_id].update_time > order.update_time
        {
            log::info!(
                "skip updating order {:?} as existing one is newer",
                order.order_id
            );
            return Ok(());
        }

        stat_guard
            .orders
            .insert(order.order_id.clone(), order.clone());

        // 确认订单状态，如果订单已完成或取消，则从缓存中移除
        if order.order_status != OrderStatus::New
            && order.order_status != OrderStatus::PendingNew
            && order.order_status != OrderStatus::PartiallyFilled
        {
            stat_guard.orders.remove(&order.order_id);
            let trade_ids = stat_guard.order_trade_ids.remove(&order.order_id);
            if let Some(trade_ids) = trade_ids {
                for trade_id in trade_ids {
                    stat_guard.trades.remove(&trade_id);
                }
            }
        }

        // 更新数据库
        Self::_update_order(db.clone(), market_type, &order)?;

        Ok(())
    }

    pub async fn update_user_trade(
        &self,
        market_type: &MarketType,
        trade: UserTrade,
    ) -> Result<()> {
        Self::update_user_trade_inner(
            self.open_order_stats.clone(),
            self.db.clone(),
            market_type,
            trade,
        )
        .await
    }

    async fn update_user_trade_inner(
        open_order_stats: Arc<HashMap<MarketType, Arc<RwLock<OpenOrderTradeStat>>>>,
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        trade: UserTrade,
    ) -> Result<()> {
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

        // 如果交易已存在，跳过
        if stat_guard.trades.contains_key(&trade.trade_id) {
            log::info!(
                "skip updating trade {:?} as it already exists",
                trade.trade_id
            );
            return Ok(());
        }

        if stat_guard.orders.get(&trade.order_id).is_some() {
            stat_guard
                .trades
                .insert(trade.trade_id.clone(), trade.clone());
            stat_guard
                .order_trade_ids
                .entry(trade.order_id.clone())
                .or_insert(vec![])
                .push(trade.trade_id.clone());
        }
        Self::_update_user_trade(db.clone(), market_type, &trade)?;

        Ok(())
    }

    pub async fn update_account(&self, market_type: &MarketType, account: Account) -> Result<()> {
        Self::update_account_inner(self.accounts.clone(), self.db.clone(), market_type, account)
            .await
    }

    async fn update_account_inner(
        accounts: Arc<HashMap<MarketType, Arc<RwLock<Option<Account>>>>>,
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        account: Account,
    ) -> Result<()> {
        let account_lock = accounts
            .get(market_type)
            .ok_or(PlatformError::DataManagerError {
                message: format!("account lock not found for market_type {:?}", market_type),
            })?;

        let mut account_guard = account_lock.write().await;

        if let Some(account_guard) = account_guard.as_ref() {
            if account_guard.timestamp > account.timestamp {
                log::info!(
                    "skip updating account for market_type {:?} as existing one is newer",
                    market_type
                );
                return Ok(());
            }
        }
        *account_guard = Some(account.clone());

        // 更新数据库
        Self::_update_account(db.clone(), market_type, &account)?;
        Ok(())
    }

    pub async fn update_account_update(
        &self,
        market_type: &MarketType,
        account_update: AccountUpdate,
    ) -> Result<()> {
        Self::update_account_update_inner(
            self.accounts.clone(),
            self.db.clone(),
            market_type,
            account_update,
        )
        .await
    }

    async fn update_account_update_inner(
        accounts: Arc<HashMap<MarketType, Arc<RwLock<Option<Account>>>>>,
        db: Arc<SQLiteDB>,
        market_type: &MarketType,
        account_update: AccountUpdate,
    ) -> Result<()> {
        let account_lock = accounts
            .get(market_type)
            .ok_or(PlatformError::DataManagerError {
                message: format!("account lock not found for market_type {:?}", market_type),
            })?;

        let mut account_guard = account_lock.write().await;

        if let Some(account) = &mut *account_guard {
            if account.timestamp > account_update.timestamp {
                log::info!(
                    "skip updating account update for market_type {:?} as existing one is newer",
                    market_type
                );
                return Ok(());
            }

            // 更新缓存
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

            // 更新数据库
            Self::_update_account_update(db.clone(), market_type, &account_update)?;
            Ok(())
        } else {
            return Err(PlatformError::DataManagerError {
                message: format!("account not initialized for market_type {:?}", market_type),
            });
        }
    }

    pub async fn get_account(&self, market_type: &MarketType) -> Result<Option<Account>> {
        let account_lock =
            self.accounts
                .get(market_type)
                .ok_or(PlatformError::DataManagerError {
                    message: format!("account lock not found for market_type {:?}", market_type),
                })?;

        let account_guard = account_lock.read().await;
        Ok(account_guard.clone())
    }

    pub async fn get_open_orders(&self, market_type: &MarketType) -> Result<Vec<Order>> {
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

    pub async fn get_user_trades(&self, market_type: &MarketType) -> Result<Vec<UserTrade>> {
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
        let trades: Vec<UserTrade> = stat_guard.trades.values().cloned().collect();
        Ok(trades)
    }

    pub async fn get_user_trades_by_order(
        &self,
        market_type: &MarketType,
        order_id: &str,
    ) -> Result<Vec<UserTrade>> {
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

        let trade_ids =
            stat_guard
                .order_trade_ids
                .get(order_id)
                .ok_or(PlatformError::DataManagerError {
                    message: format!("no trades found for order_id {}", order_id),
                })?;

        let mut trades = Vec::new();
        for trade_id in trade_ids {
            if let Some(trade) = stat_guard.trades.get(trade_id) {
                trades.push(trade.clone());
            }
        }

        Ok(trades)
    }

    pub async fn place_order(
        &self,
        market_type: &MarketType,
        req: PlaceOrderRequest,
    ) -> Result<Order> {
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

    pub async fn cancel_order(
        &self,
        market_type: &MarketType,
        req: CancelOrderRequest,
    ) -> Result<()> {
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

    pub async fn get_account_from_db(&self, market_type: &MarketType) -> Result<Option<Account>> {
        Self::_get_account(self.db.clone(), market_type)
    }

    pub async fn get_orders_from_db(
        &self,
        market_type: &MarketType,
        symbol: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Order>> {
        Self::_get_orders(self.db.clone(), market_type, symbol, limit)
    }

    pub async fn get_user_trades_from_db(
        &self,
        market_type: &MarketType,
        symbol: &str,
        limit: Option<usize>,
    ) -> Result<Vec<UserTrade>> {
        Self::_get_user_trades(self.db.clone(), market_type, symbol, limit)
    }

    pub async fn get_last_sync_ts_from_db(&self, market_type: &MarketType) -> Result<Option<u64>> {
        Self::_get_last_sync_ts(self.db.clone(), market_type)
    }

    pub async fn get_order_by_id_from_db(
        &self,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<Order>> {
        Self::_get_order_by_id(self.db.clone(), market_type, symbol, order_id)
    }

    pub async fn get_open_orders_from_db(&self, market_type: &MarketType) -> Result<Vec<Order>> {
        Self::_get_open_orders(self.db.clone(), market_type)
    }

    pub async fn get_user_trades_by_order_id_from_db(
        &self,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<UserTrade>> {
        Self::_get_user_trades_by_order_id(self.db.clone(), market_type, symbol, order_id)
    }
}

impl Drop for TradeData {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
    }
}
