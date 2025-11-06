use crate::models::{Account, Order, UserTrade};
use std::collections::HashMap;

/// sqls
pub const CREATE_ACCOUNT_BALANCE_TABLE_SQL: &str = "
CREATE TABLE IF NOT EXISTS account_balance (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id      TEXT NOT NULL,
    asset           TEXT NOT NULL,
    free            REAL NOT NULL,
    locked          REAL NOT NULL,
    updated_at      BIGINT NOT NULL,
    UNIQUE(account_id, asset)
);
";
pub const CREATE_ACCOUNT_BALANCE_TABLE_INDEX_SQL: &str = "
CREATE INDEX IF NOT EXISTS idx_account_balance_account_id ON account_balance (account_id);
";

pub const CREATE_ORDERS_TABLE_SQL: &str = "
CREATE TABLE IF NOT EXISTS orders (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id            TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    client_order_id     TEXT NOT NULL,
    order_side          TEXT NOT NULL,
    order_type          TEXT NOT NULL,
    order_status        TEXT NOT NULL,
    order_price         REAL NOT NULL,
    order_quantity      REAL NOT NULL,
    executed_qty        REAL NOT NULL,
    cummulative_quote_qty REAL NOT NULL,
    time_in_force       TEXT NOT NULL,
    stop_price          REAL NOT NULL,
    iceberg_qty         REAL NOT NULL,
    create_time         BIGINT NOT NULL,
    update_time         BIGINT NOT NULL,
    UNIQUE(order_id)
);
";

/// 结构体定义
pub struct AccountCache {
    account: Account,
}

pub struct OrderUserTradeCache {
    orders: HashMap<String, Order>,
    trades: HashMap<String, UserTrade>,

    symbol_orders: HashMap<String, Vec<String>>,
    symbol_trades: HashMap<String, Vec<String>>,
    order_trades: HashMap<String, Vec<String>>,
}

pub struct TradeDataCache {}
