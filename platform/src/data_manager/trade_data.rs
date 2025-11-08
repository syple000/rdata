use crate::{
    config::Config,
    errors::{PlatformError, Result},
    models::{Account, Balance, MarketType, Order, UserTrade},
    trade_provider::TradeProvider,
};
use db::{common::Row, sqlite::SQLiteDB};
use rust_decimal::Decimal;
use std::{collections::HashMap, str::FromStr, sync::Arc};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

pub struct TradeData {
    market_types: Vec<MarketType>,
    shutdown_token: CancellationToken,

    // 账户缓存
    accounts: Arc<HashMap<MarketType, Arc<RwLock<Option<Account>>>>>,
    // 在途订单缓存
    orders: Arc<HashMap<MarketType, Arc<RwLock<HashMap<String, Order>>>>>,
    trades: Arc<HashMap<MarketType, Arc<RwLock<HashMap<String, UserTrade>>>>>,
    order_trades_map: Arc<HashMap<MarketType, Arc<RwLock<HashMap<String, Vec<String>>>>>>,

    db: Arc<SQLiteDB>,
}

impl TradeData {
    pub fn new(config: Config) -> Result<Self> {
        let market_types: Vec<MarketType> =
            config.get("data_manager.market_types").map_err(|e| {
                PlatformError::DataManagerError {
                    message: format!("data_manager market_types not found: {}", e),
                }
            })?;
        let db_path: String =
            config
                .get("data_manager.db_path")
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("data_manager db_path not found: {}", e),
                })?;

        let mut accounts = HashMap::new();
        let mut orders = HashMap::new();
        let mut trades = HashMap::new();
        let mut order_trades_map = HashMap::new();
        for market_type in &market_types {
            accounts.insert(market_type.clone(), Arc::new(RwLock::new(None)));
            orders.insert(market_type.clone(), Arc::new(RwLock::new(HashMap::new())));
            trades.insert(market_type.clone(), Arc::new(RwLock::new(HashMap::new())));
            order_trades_map.insert(market_type.clone(), Arc::new(RwLock::new(HashMap::new())));
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
            orders: Arc::new(orders),
            trades: Arc::new(trades),
            order_trades_map: Arc::new(order_trades_map),
            db,
        })
    }

    // db里account/trade/order表初始化（必须有market_type），定义索引，索引需要高效支持trade_provider中定义的查询接口语义(关联查询、最近更新查询等)。设计增删改查方法

    fn _create_account_balance_table(&self) -> Result<()> {
        let query = r#"
            CREATE TABLE IF NOT EXISTS account_balance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_type TEXT NOT NULL,
                account_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                free TEXT NOT NULL,
                locked TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(market_type, account_id, asset)
            )
        "#;
        self.db
            .execute_update(query, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create account_balance table failed: {}", e),
            })?;

        Ok(())
    }

    fn _create_orders_table(&self) -> Result<()> {
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
                UNIQUE(market_type, order_id)
            )
        "#;
        self.db
            .execute_update(query, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create orders table failed: {}", e),
            })?;

        // 创建索引：按 market_type、symbol 和 update_time 查询（最近更新）
        let index = r#"
            CREATE INDEX IF NOT EXISTS idx_orders_market_type_symbol_update_time 
            ON orders(market_type, symbol, update_time DESC)
        "#;
        self.db
            .execute_update(index, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create orders index2 failed: {}", e),
            })?;

        // 创建索引：按 market_type、order_status 和 update_time 查询
        let index = r#"
            CREATE INDEX IF NOT EXISTS idx_orders_market_type_status_update_time 
            ON orders(market_type, order_status, update_time DESC)
        "#;
        self.db
            .execute_update(index, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create orders index3 failed: {}", e),
            })?;

        Ok(())
    }

    fn _create_user_trades_table(&self) -> Result<()> {
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
                UNIQUE(market_type, trade_id)
            )
        "#;
        self.db
            .execute_update(query, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create user_trades table failed: {}", e),
            })?;

        // 创建索引：按 market_type、order_id 查询（关联查询）
        let index = r#"
            CREATE INDEX IF NOT EXISTS idx_user_trades_market_type_order_id 
            ON user_trades(market_type, order_id)
        "#;
        self.db
            .execute_update(index, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create user_trades index2 failed: {}", e),
            })?;

        // 创建索引：按 market_type、symbol 和 timestamp 查询（最近交易）
        let index = r#"
            CREATE INDEX IF NOT EXISTS idx_user_trades_market_type_symbol_timestamp 
            ON user_trades(market_type, symbol, timestamp DESC)
        "#;
        self.db
            .execute_update(index, &[])
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("create user_trades index3 failed: {}", e),
            })?;

        Ok(())
    }

    fn _update_account_balance(&self, market_type: &MarketType, account: &Account) -> Result<()> {
        let placeholders = account
            .balances
            .iter()
            .map(|_| "(?, ?, ?, ?, ?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            r#"
            INSERT INTO account_balance (market_type, account_id, asset, free, locked, updated_at)
            VALUES {}
            ON CONFLICT(market_type, account_id, asset) DO UPDATE SET
                free = excluded.free,
                locked = excluded.locked,
                updated_at = excluded.updated_at
        "#,
            placeholders
        );
        let mut params: Vec<String> = Vec::new();
        for balance in &account.balances {
            params.push(market_type.as_str().to_string());
            params.push(account.account_id.to_string());
            params.push(balance.asset.clone());
            params.push(balance.free.to_string());
            params.push(balance.locked.to_string());
            params.push(account.timestamp.to_string());
        }
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

        self.db.execute_update(&query, &params_refs).map_err(|e| {
            PlatformError::DataManagerError {
                message: format!("update account balance err: {}", e),
            }
        })?;
        Ok(())
    }

    fn _update_order(&self, market_type: &MarketType, order: &Order) -> Result<()> {
        let query = r#"
            INSERT INTO orders (
                market_type, symbol, order_id, client_order_id, order_side, 
                order_type, order_status, order_price, order_quantity, 
                executed_qty, cummulative_quote_qty, time_in_force, 
                stop_price, iceberg_qty, create_time, update_time
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
            ON CONFLICT(market_type, order_id) DO UPDATE SET
                symbol = excluded.symbol,
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

        self.db.execute_update(query, &params_refs).map_err(|e| {
            PlatformError::DataManagerError {
                message: format!("update order err: {}", e),
            }
        })?;
        Ok(())
    }

    fn _update_user_trade(&self, market_type: &MarketType, trade: &UserTrade) -> Result<()> {
        let query = r#"
            INSERT INTO user_trades (
                market_type, trade_id, order_id, symbol, order_side,
                trade_price, trade_quantity, commission, commission_asset,
                is_maker, timestamp
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(market_type, trade_id) DO UPDATE SET
                order_id = excluded.order_id,
                symbol = excluded.symbol,
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

        self.db.execute_update(query, &params_refs).map_err(|e| {
            PlatformError::DataManagerError {
                message: format!("update user trade err: {}", e),
            }
        })?;
        Ok(())
    }

    fn _get_account(&self, market_type: &MarketType) -> Result<Vec<Account>> {
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
            SELECT account_id, asset, free, locked, updated_at
            FROM account_balance
            WHERE market_type = ?1
        "#;
        let market_type_str = market_type.as_str().to_string();
        let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str];

        let result =
            self.db
                .execute_query(query, &params)
                .map_err(|e| PlatformError::DataManagerError {
                    message: format!("get account balance err: {}", e),
                })?;

        if result.is_empty() {
            return Ok(vec![]);
        }

        let mut accounts = HashMap::<String, Account>::new();
        for row in result.rows {
            let account_id = get_row_column_string(&row, "account_id")?;
            let timestamp = get_row_column_i64(&row, "updated_at")? as u64;

            let account = accounts.entry(account_id.clone()).or_insert(Account {
                account_id: account_id.clone(),
                balances: vec![],
                timestamp,
            });

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

        Ok(accounts.into_values().collect())
    }

    fn _get_orders(&self, market_type: &MarketType, limit: Option<usize>) -> Result<Vec<Order>> {
        let limit = limit.unwrap_or(1000);
        let query = format!(
            r#"
            SELECT symbol, order_id, client_order_id, order_side, order_type,
                   order_status, order_price, order_quantity, executed_qty,
                   cummulative_quote_qty, time_in_force, stop_price, iceberg_qty,
                   create_time, update_time
            FROM orders
            WHERE market_type = ?1
            ORDER BY update_time DESC
            LIMIT {}
        "#,
            limit
        );
        let market_type_str = market_type.as_str().to_string();
        let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str];

        let result = self.db.execute_query(&query, &params).map_err(|e| {
            PlatformError::DataManagerError {
                message: format!("get orders err: {}", e),
            }
        })?;

        result
            .into_struct()
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("get_orders into err: {}", e),
            })
    }

    fn _get_order_by_ids(
        &self,
        market_type: &MarketType,
        order_ids: Vec<String>,
    ) -> Result<Vec<Order>> {
        if order_ids.is_empty() {
            return Ok(vec![]);
        }

        let placeholders = order_ids
            .iter()
            .map(|_| "?".to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            r#"
            SELECT symbol, order_id, client_order_id, order_side, order_type,
                   order_status, order_price, order_quantity, executed_qty,
                   cummulative_quote_qty, time_in_force, stop_price, iceberg_qty,
                   create_time, update_time
            FROM orders
            WHERE market_type = ?1 AND order_id IN ({})
        "#,
            placeholders
        );
        let market_type_str = market_type.as_str().to_string();
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str];
        for order_id in &order_ids {
            params.push(order_id);
        }

        let result = self.db.execute_query(&query, &params).map_err(|e| {
            PlatformError::DataManagerError {
                message: format!("get orders by ids err: {}", e),
            }
        })?;

        result
            .into_struct()
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("get_order_by_ids into err: {}", e),
            })
    }

    fn _get_open_orders(&self, market_type: &MarketType) -> Result<Vec<Order>> {
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
            self.db
                .execute_query(query, &params)
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
        &self,
        market_type: &MarketType,
        limit: Option<usize>,
    ) -> Result<Vec<UserTrade>> {
        let limit = limit.unwrap_or(1000);
        let query = format!(
            r#"
            SELECT trade_id, order_id, symbol, order_side, trade_price,
                   trade_quantity, commission, commission_asset, is_maker, timestamp
            FROM user_trades
            WHERE market_type = ?1
            ORDER BY timestamp DESC
            LIMIT {}
        "#,
            limit
        );
        let market_type_str = market_type.as_str().to_string();
        let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str];

        let result = self.db.execute_query(&query, &params).map_err(|e| {
            PlatformError::DataManagerError {
                message: format!("get user trades err: {}", e),
            }
        })?;

        result
            .into_struct()
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("get_user_trades into err: {}", e),
            })
    }

    fn _get_user_trades_by_order_ids(
        &self,
        market_type: &MarketType,
        order_ids: Vec<String>,
    ) -> Result<Vec<UserTrade>> {
        let placeholders = order_ids
            .iter()
            .map(|_| "?".to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            r#"
            SELECT trade_id, order_id, symbol, order_side, trade_price,
                   trade_quantity, commission, commission_asset, is_maker, timestamp
            FROM user_trades
            WHERE market_type = ?1 AND order_id IN ({})
            ORDER BY timestamp DESC
        "#,
            placeholders
        );
        let market_type_str = market_type.as_str().to_string();
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str];
        for order_id in &order_ids {
            params.push(order_id);
        }

        let result = self.db.execute_query(&query, &params).map_err(|e| {
            PlatformError::DataManagerError {
                message: format!("get user trades by order_id err: {}", e),
            }
        })?;

        result
            .into_struct()
            .map_err(|e| PlatformError::DataManagerError {
                message: format!("get_user_trades_by_order_ids into err: {}", e),
            })
    }

    pub async fn init(
        &self,
        trade_providers: HashMap<MarketType, Arc<dyn TradeProvider>>,
    ) -> Result<()> {
        Ok(())
    }
}
