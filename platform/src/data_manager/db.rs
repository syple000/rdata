use crate::{
    errors::{PlatformError, Result},
    models::{
        Account, AccountUpdate, Balance, KlineData, KlineInterval, MarketType, Order, SymbolInfo,
        Trade, UserTrade,
    },
};
use db::{common::Row, sqlite::SQLiteDB};
use rusqlite::ToSql;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;

pub fn create_symbol_info_table(db: Arc<SQLiteDB>) -> Result<()> {
    let sql = r#"
    CREATE TABLE IF NOT EXISTS symbol_info (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        market_type TEXT NOT NULL,
        symbol TEXT NOT NULL,
        status TEXT NOT NULL,
        base_asset TEXT NOT NULL,
        quote_asset TEXT NOT NULL,
        base_asset_precision INTEGER,
        quote_asset_precision INTEGER,
        min_price TEXT,
        max_price TEXT,
        price_tick_size TEXT,
        min_market_quantity TEXT,
        max_market_quantity TEXT,
        market_quantity_step_size TEXT,
        min_quantity TEXT,
        max_quantity TEXT,
        quantity_step_size TEXT,
        min_notional TEXT,
        UNIQUE(market_type, symbol)
    );
    "#;
    db.execute_update(sql, &[])
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to create symbol_info table: {}", e),
        })?;
    Ok(())
}

pub fn update_symbol_info(
    db: Arc<SQLiteDB>,
    market_type: &MarketType,
    symbol_infos: &[SymbolInfo],
) -> Result<()> {
    if symbol_infos.is_empty() {
        return Ok(());
    }

    let placeholder = symbol_infos
        .iter()
        .map(|_| "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
    INSERT INTO symbol_info (
        market_type, symbol, status, base_asset, quote_asset,
        base_asset_precision, quote_asset_precision,
        min_price, max_price, price_tick_size,
        min_market_quantity, max_market_quantity, market_quantity_step_size,
        min_quantity, max_quantity, quantity_step_size, min_notional
    )
    VALUES {} 
    ON CONFLICT(market_type, symbol) DO UPDATE SET
        status=excluded.status,
        base_asset=excluded.base_asset,
        quote_asset=excluded.quote_asset,
        base_asset_precision=excluded.base_asset_precision,
        quote_asset_precision=excluded.quote_asset_precision,
        min_price=excluded.min_price,
        max_price=excluded.max_price,
        price_tick_size=excluded.price_tick_size,
        min_market_quantity=excluded.min_market_quantity,
        max_market_quantity=excluded.max_market_quantity,
        market_quantity_step_size=excluded.market_quantity_step_size,
        min_quantity=excluded.min_quantity,
        max_quantity=excluded.max_quantity,
        quantity_step_size=excluded.quantity_step_size,
        min_notional=excluded.min_notional;
    "#,
        placeholder
    );

    let values = symbol_infos
        .iter()
        .flat_map(|info| {
            vec![
                market_type.as_str().to_string(),
                info.symbol.clone(),
                info.status.as_str().to_string(),
                info.base_asset.clone(),
                info.quote_asset.clone(),
                info.base_asset_precision
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                info.quote_asset_precision
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                info.min_price.map(|v| v.to_string()).unwrap_or_default(),
                info.max_price.map(|v| v.to_string()).unwrap_or_default(),
                info.price_tick_size
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                info.min_market_quantity
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                info.max_market_quantity
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                info.market_quantity_step_size
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                info.min_quantity.map(|v| v.to_string()).unwrap_or_default(),
                info.max_quantity.map(|v| v.to_string()).unwrap_or_default(),
                info.quantity_step_size
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                info.min_notional.map(|v| v.to_string()).unwrap_or_default(),
            ]
        })
        .collect::<Vec<_>>();
    let params = values.iter().map(|v| v as &dyn ToSql).collect::<Vec<_>>();
    db.execute_update(&sql, &params)
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to update symbol_info data: {}", e),
        })?;
    Ok(())
}

pub fn get_symbol_info(
    db: Arc<SQLiteDB>,
    market_type: &MarketType,
    symbol: &str,
) -> Result<Option<SymbolInfo>> {
    let sql = r#"
    SELECT symbol, status, base_asset, quote_asset,
           base_asset_precision, quote_asset_precision,
           min_price, max_price, price_tick_size,
           min_market_quantity, max_market_quantity, market_quantity_step_size,
           min_quantity, max_quantity, quantity_step_size, min_notional
    FROM symbol_info
    WHERE market_type = ? AND symbol = ?;
    "#;
    let values: Vec<String> = vec![market_type.as_str().to_string(), symbol.to_string()];
    let params: Vec<&dyn ToSql> = values.iter().map(|v| v as &dyn ToSql).collect();
    let result = db
        .execute_query(&sql, &params)
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to get symbol_info: {}", e),
        })?;

    if result.is_empty() {
        return Ok(None);
    }

    let symbol_infos: Vec<SymbolInfo> =
        result
            .into_struct::<SymbolInfo>()
            .map_err(|e| PlatformError::PlatformError {
                message: format!("Fail to into symbol_info: {}", e),
            })?;

    Ok(symbol_infos.into_iter().next())
}

pub fn get_all_symbol_info(db: Arc<SQLiteDB>, market_type: &MarketType) -> Result<Vec<SymbolInfo>> {
    let sql = r#"
    SELECT symbol, status, base_asset, quote_asset,
           base_asset_precision, quote_asset_precision,
           min_price, max_price, price_tick_size,
           min_market_quantity, max_market_quantity, market_quantity_step_size,
           min_quantity, max_quantity, quantity_step_size, min_notional
    FROM symbol_info
    WHERE market_type = ?
    ORDER BY symbol;
    "#;
    let values: Vec<String> = vec![market_type.as_str().to_string()];
    let params: Vec<&dyn ToSql> = values.iter().map(|v| v as &dyn ToSql).collect();
    let result = db
        .execute_query(&sql, &params)
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to get all symbol_info: {}", e),
        })?;

    result
        .into_struct::<SymbolInfo>()
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to into symbol_info list: {}", e),
        })
}

pub fn create_kline_table(db: Arc<SQLiteDB>) -> Result<()> {
    let sql = r#"
    CREATE TABLE IF NOT EXISTS kline (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        market_type TEXT NOT NULL,
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        open_time INTEGER NOT NULL,
        close_time INTEGER NOT NULL,
        open TEXT NOT NULL,
        high TEXT NOT NULL,
        low TEXT NOT NULL,
        close TEXT NOT NULL,
        volume TEXT NOT NULL,
        quote_volume TEXT NOT NULL,
        taker_buy_volume TEXT NOT NULL,
        taker_buy_quote_volume TEXT NOT NULL,
        is_closed INTEGER NOT NULL,
        UNIQUE(market_type, symbol, interval, open_time)
    );
    "#;
    db.execute_update(sql, &[])
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to create kline table: {}", e),
        })?;
    Ok(())
}

pub fn create_trade_table(db: Arc<SQLiteDB>) -> Result<()> {
    let sql = r#"
    CREATE TABLE IF NOT EXISTS trade (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        market_type TEXT NOT NULL,
        symbol TEXT NOT NULL,
        trade_id TEXT NOT NULL,
        price TEXT NOT NULL,
        quantity TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        is_buyer_maker INTEGER NOT NULL,
        seq_id INTEGER NOT NULL,
        UNIQUE(market_type, symbol, seq_id)
    );
    "#;
    db.execute_update(sql, &[])
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to create trade table: {}", e),
        })?;
    let index = r#"
    CREATE INDEX IF NOT EXISTS idx_trade_symbol_timestamp_seq_id
    ON trade (market_type, symbol, timestamp, seq_id);
    "#;
    db.execute_update(index, &[])
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to create trade index: {}", e),
        })?;
    Ok(())
}

pub fn update_kline_data(
    db: Arc<SQLiteDB>,
    market_type: &MarketType,
    klines: &[KlineData],
) -> Result<()> {
    let placeholder = klines
        .iter()
        .map(|_| "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
    INSERT INTO kline (market_type, symbol, interval, open_time, close_time, open, high, low, close, volume, quote_volume, taker_buy_volume, taker_buy_quote_volume, is_closed)
    VALUES {} 
    ON CONFLICT(market_type, symbol, interval, open_time) DO UPDATE SET
        close_time=excluded.close_time,
        open=excluded.open,
        high=excluded.high,
        low=excluded.low,
        close=excluded.close,
        volume=excluded.volume,
        quote_volume=excluded.quote_volume,
        taker_buy_volume=excluded.taker_buy_volume,
        taker_buy_quote_volume=excluded.taker_buy_quote_volume,
        is_closed=excluded.is_closed;
    "#,
        placeholder
    );
    let values = klines
        .iter()
        .flat_map(|kline| {
            vec![
                market_type.as_str().to_string(),
                kline.symbol.clone(),
                kline.interval.as_str().to_string(),
                kline.open_time.to_string(),
                kline.close_time.to_string(),
                kline.open.to_string(),
                kline.high.to_string(),
                kline.low.to_string(),
                kline.close.to_string(),
                kline.volume.to_string(),
                kline.quote_volume.to_string(),
                kline.taker_buy_volume.to_string(),
                kline.taker_buy_quote_volume.to_string(),
                kline.is_closed.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    let params = values.iter().map(|v| v as &dyn ToSql).collect::<Vec<_>>();
    db.execute_update(&sql, &params)
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to update kline data: {}", e),
        })?;
    Ok(())
}

pub fn update_trade_data(
    db: Arc<SQLiteDB>,
    market_type: &MarketType,
    trades: &[Trade],
) -> Result<()> {
    let placeholder = trades
        .iter()
        .map(|_| "(?, ?, ?, ?, ?, ?, ?, ?)")
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
    INSERT INTO trade (market_type, symbol, trade_id, price, quantity, timestamp, is_buyer_maker, seq_id)
    VALUES {} 
    ON CONFLICT(market_type, symbol, seq_id) DO NOTHING;
    "#,
        placeholder
    );
    let values = trades
        .iter()
        .flat_map(|trade| {
            vec![
                market_type.as_str().to_string(),
                trade.symbol.clone(),
                trade.trade_id.clone(),
                trade.price.to_string(),
                trade.quantity.to_string(),
                trade.timestamp.to_string(),
                trade.is_buyer_maker.to_string(),
                trade.seq_id.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    let params = values.iter().map(|v| v as &dyn ToSql).collect::<Vec<_>>();
    db.execute_update(&sql, &params)
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to update trade data: {}", e),
        })?;
    Ok(())
}

pub fn get_klines(
    db: Arc<SQLiteDB>,
    market_type: &MarketType,
    symbol: &str,
    interval: &KlineInterval,
    start_time: Option<u64>,
    end_time: Option<u64>,
    limit: Option<u64>,
) -> Result<Vec<KlineData>> {
    let start_time = start_time.unwrap_or(0);
    let end_time = end_time.unwrap_or(i64::MAX as u64);
    let limit = limit.unwrap_or(1000);
    let order_direction = if start_time > 0 { "ASC" } else { "DESC" };
    let sql = format!(
        r#"
    SELECT symbol, interval, open_time, close_time, open, high, low, close, volume, quote_volume, taker_buy_volume, taker_buy_quote_volume, is_closed
    FROM kline
    WHERE market_type = ? AND symbol = ? AND interval = ? AND open_time >= ? AND open_time <= ?
    ORDER BY open_time {}
    LIMIT {}; 
    "#,
        order_direction, limit
    );
    let values: Vec<String> = vec![
        market_type.as_str().to_string(),
        symbol.to_string(),
        interval.as_str().to_string(),
        start_time.to_string(),
        end_time.to_string(),
    ];
    let params: Vec<&dyn ToSql> = values.iter().map(|v| v as &dyn ToSql).collect();
    let result = db
        .execute_query(&sql, &params)
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to get klines: {}", e),
        })?;
    result
        .into_struct::<KlineData>()
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to into klines: {}", e),
        })
        .map(|mut v| {
            v.sort_by(|a, b| a.open_time.cmp(&b.open_time));
            v
        })
}

pub fn get_trades(
    db: Arc<SQLiteDB>,
    market_type: &MarketType,
    symbol: &str,
    start_time: Option<u64>,
    end_time: Option<u64>,
    from_seq_id: Option<u64>,
    limit: Option<u64>,
) -> Result<Vec<Trade>> {
    let limit = limit.unwrap_or(1000);
    let sql = if from_seq_id.is_some() {
        let from_seq_id = from_seq_id.unwrap();
        format!(
            r#"
        SELECT symbol, trade_id, price, quantity, timestamp, is_buyer_maker, seq_id
        FROM trade
        WHERE market_type = ? AND symbol = ? AND seq_id >= {}
        ORDER BY seq_id ASC
        LIMIT {};
        "#,
            from_seq_id, limit
        )
    } else {
        let start_time = start_time.unwrap_or(0);
        let end_time = end_time.unwrap_or(i64::MAX as u64);
        if start_time > 0 {
            format!(
                r#"
            SELECT symbol, trade_id, price, quantity, timestamp, is_buyer_maker, seq_id
            FROM trade
            WHERE market_type = ? AND symbol = ? AND timestamp >= {} AND timestamp <= {}
            ORDER BY timestamp ASC
            LIMIT {};
            "#,
                start_time, end_time, limit
            )
        } else {
            format!(
                r#"
            SELECT symbol, trade_id, price, quantity, timestamp, is_buyer_maker, seq_id
            FROM trade
            WHERE market_type = ? AND symbol = ? AND timestamp >= {} AND timestamp <= {}
            ORDER BY timestamp DESC
            LIMIT {};
            "#,
                start_time, end_time, limit
            )
        }
    };

    let values: Vec<String> = vec![market_type.as_str().to_string(), symbol.to_string()];
    let params: Vec<&dyn ToSql> = values.iter().map(|v| v as &dyn ToSql).collect();
    let result = db
        .execute_query(&sql, &params)
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to get trades: {}", e),
        })?;
    result
        .into_struct::<Trade>()
        .map_err(|e| PlatformError::PlatformError {
            message: format!("Fail to into trades: {}", e),
        })
        .map(|mut v| {
            v.sort_by(|a, b| a.seq_id.cmp(&b.seq_id));
            v
        })
}

pub fn create_api_sync_ts_table(db: Arc<SQLiteDB>) -> Result<()> {
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

pub fn create_account_balance_table(db: Arc<SQLiteDB>) -> Result<()> {
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

pub fn create_orders_table(db: Arc<SQLiteDB>) -> Result<()> {
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

pub fn create_user_trades_table(db: Arc<SQLiteDB>) -> Result<()> {
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

pub fn get_last_sync_ts(db: Arc<SQLiteDB>, market_type: &MarketType) -> Result<Option<u64>> {
    let query = r#"
        SELECT last_sync_ts
        FROM api_sync_ts
        WHERE market_type = ?1
    "#;
    let market_type_str = market_type.as_str().to_string();
    let params: Vec<&dyn rusqlite::ToSql> = vec![&market_type_str];

    let result = db
        .execute_query(query, &params)
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

pub fn update_last_sync_ts(
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

pub fn update_account_balance(
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

pub fn update_account_update(
    db: Arc<SQLiteDB>,
    market_type: &MarketType,
    account_update: &AccountUpdate,
) -> Result<()> {
    update_account_balance(
        db,
        market_type,
        &account_update.balances,
        account_update.timestamp,
    )
}

pub fn update_account(
    db: Arc<SQLiteDB>,
    market_type: &MarketType,
    account: &Account,
) -> Result<()> {
    update_account_balance(db, market_type, &account.balances, account.timestamp)
}

pub fn update_order(db: Arc<SQLiteDB>, market_type: &MarketType, order: &Order) -> Result<()> {
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

pub fn update_user_trade(
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

pub fn get_account(db: Arc<SQLiteDB>, market_type: &MarketType) -> Result<Option<Account>> {
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

    let result = db
        .execute_query(query, &params)
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

pub fn get_orders(
    db: Arc<SQLiteDB>,
    market_type: &MarketType,
    symbol: &str,
    start_time: Option<u64>,
    end_time: Option<u64>,
    limit: Option<usize>,
) -> Result<Vec<Order>> {
    let limit = limit.unwrap_or(1000);
    let start_time = start_time.unwrap_or(0);
    let end_time = end_time.unwrap_or(i64::MAX as u64);
    let order_direction = if start_time > 0 { "ASC" } else { "DESC" };
    let query = format!(
        r#"
        SELECT symbol, order_id, client_order_id, order_side, order_type,
               order_status, order_price, order_quantity, executed_qty,
               cummulative_quote_qty, time_in_force, stop_price, iceberg_qty,
               create_time, update_time
        FROM orders
        WHERE market_type = ?1 AND symbol = ?2 and update_time >= {} AND update_time <= {}
        ORDER BY update_time {}
        LIMIT {}
    "#,
        start_time, end_time, order_direction, limit
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
        .map(|mut v| {
            v.sort_by(|a: &Order, b: &Order| a.update_time.cmp(&b.update_time));
            v
        })
}

pub fn get_order_by_client_id(
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

    let orders: Vec<Order> = result
        .into_struct()
        .map_err(|e| PlatformError::DataManagerError {
            message: format!("get_order_by_client_id into err: {}", e),
        })?;
    if orders.is_empty() {
        return Ok(None);
    }
    Ok(Some(orders[0].clone()))
}

pub fn get_order_by_id(
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

    let orders: Vec<Order> = result
        .into_struct()
        .map_err(|e| PlatformError::DataManagerError {
            message: format!("get_order_by_id into err: {}", e),
        })?;
    if orders.is_empty() {
        return Ok(None);
    }
    Ok(Some(orders[0].clone()))
}

pub fn get_open_orders(db: Arc<SQLiteDB>, market_type: &MarketType) -> Result<Vec<Order>> {
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

    let result = db
        .execute_query(query, &params)
        .map_err(|e| PlatformError::DataManagerError {
            message: format!("get open orders err: {}", e),
        })?;

    result
        .into_struct()
        .map_err(|e| PlatformError::DataManagerError {
            message: format!("get_open_orders into err: {}", e),
        })
        .map(|mut v| {
            v.sort_by(|a: &Order, b: &Order| a.update_time.cmp(&b.update_time));
            v
        })
}

pub fn get_user_trades(
    db: Arc<SQLiteDB>,
    market_type: &MarketType,
    symbol: &str,
    start_time: Option<u64>,
    end_time: Option<u64>,
    limit: Option<usize>,
) -> Result<Vec<UserTrade>> {
    let limit = limit.unwrap_or(1000);
    let start_time = start_time.unwrap_or(0);
    let end_time = end_time.unwrap_or(i64::MAX as u64);
    let order_direction = if start_time > 0 { "ASC" } else { "DESC" };
    let query = format!(
        r#"
        SELECT trade_id, order_id, symbol, order_side, trade_price,
               trade_quantity, commission, commission_asset, is_maker, timestamp
        FROM user_trades
        WHERE market_type = ?1 AND symbol = ?2 and timestamp >= {} AND timestamp <= {}
        ORDER BY timestamp {}
        LIMIT {}
    "#,
        start_time, end_time, order_direction, limit
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
        .map(|mut v| {
            v.sort_by(|a: &UserTrade, b: &UserTrade| a.timestamp.cmp(&b.timestamp));
            v
        })
}

pub fn get_user_trades_by_order(
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
        .map(|mut v| {
            v.sort_by(|a: &UserTrade, b: &UserTrade| a.timestamp.cmp(&b.timestamp));
            v
        })
}

pub fn get_all_symbol(db: Arc<SQLiteDB>, market_type: &MarketType) -> Result<Vec<String>> {
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
