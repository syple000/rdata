use std::{collections::HashMap, sync::Arc};

use db::sqlite::SQLiteDB;
use rusqlite::ToSql;

use crate::{
    errors::{PlatformError, Result},
    market_provider::MarketProvider,
    models::{GetKlinesRequest, KlineData, KlineInterval, MarketType, Trade},
};

fn _create_kline_table(db: Arc<SQLiteDB>) -> Result<()> {
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

fn _create_trade_table(db: Arc<SQLiteDB>) -> Result<()> {
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

fn _update_kline_data(
    db: Arc<SQLiteDB>,
    market_type: &MarketType,
    klines: &[KlineData],
) -> Result<()> {
    let placeholder = klines
        .iter()
        .map(|_| "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
    INSERT INTO kline (market_type, symbol, interval, open_time, close_time, open, high, low, close, volume, quote_volume, is_closed)
    VALUES {} 
    ON CONFLICT(market_type, symbol, interval, open_time) DO UPDATE SET
        close_time=excluded.close_time,
        open=excluded.open,
        high=excluded.high,
        low=excluded.low,
        close=excluded.close,
        volume=excluded.volume,
        quote_volume=excluded.quote_volume,
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

fn _update_trade_data(db: Arc<SQLiteDB>, market_type: &MarketType, trades: &[Trade]) -> Result<()> {
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

fn _get_klines(
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
    let sql = format!(
        r#"
    SELECT symbol, interval, open_time, close_time, open, high, low, close, volume, quote_volume, is_closed
    FROM kline
    WHERE market_type = ? AND symbol = ? AND interval = ? AND open_time >= ? AND open_time <= ?
    ORDER BY open_time DESC
    LIMIT {}; 
    "#,
        limit
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
}

fn _get_trades(
    db: Arc<SQLiteDB>,
    market_type: &MarketType,
    symbol: &str,
    start_time: Option<u64>,
    end_time: Option<u64>,
    from_seq_id: Option<u64>,
    limit: Option<u64>,
) -> Result<Vec<Trade>> {
    let start_time = start_time.unwrap_or(0);
    let end_time = end_time.unwrap_or(i64::MAX as u64);
    let from_seq_id = from_seq_id.unwrap_or(0);
    let limit = limit.unwrap_or(1000);
    let sql = format!(
        r#"
    SELECT symbol, trade_id, price, quantity, timestamp, is_buyer_maker, seq_id
    FROM trade
    WHERE market_type = ? AND symbol = ? AND timestamp >= ? AND timestamp <= ? AND seq_id >= ?
    ORDER BY seq_id DESC
    LIMIT {};
    "#,
        limit
    );
    let values: Vec<String> = vec![
        market_type.as_str().to_string(),
        symbol.to_string(),
        start_time.to_string(),
        end_time.to_string(),
        from_seq_id.to_string(),
    ];
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
}

async fn fetch_1m_klines(
    market_type: MarketType,
    provider: Arc<dyn MarketProvider>,
    symbols: Vec<String>,
    db: Arc<SQLiteDB>,
) {
    let mut join_handle_vec = vec![];
    for symbol in symbols.iter() {
        let from_open_time = _get_klines(
            db.clone(),
            &market_type,
            symbol,
            &KlineInterval::OneMinute,
            None,
            None,
            Some(1),
        )
        .ok()
        .and_then(|klines| klines.first().map(|kline| kline.open_time + 1))
        .unwrap_or(time::get_current_milli_timestamp() - 5 * 365 * 24 * 60 * 60 * 1000);
        let now = time::get_current_milli_timestamp();

        let provider_clone = provider.clone();
        let symbol_clone = symbol.clone();
        let db_clone = db.clone();
        let market_type_clone = market_type.clone();
        let handle = tokio::spawn(async move {
            let mut from_open_time = from_open_time;
            while from_open_time < now {
                match provider_clone
                    .get_klines(GetKlinesRequest {
                        symbol: symbol_clone.clone(),
                        interval: KlineInterval::OneMinute,
                        start_time: Some(from_open_time),
                        end_time: None,
                        limit: Some(1000),
                    })
                    .await
                {
                    Ok(klines) => {
                        if klines.is_empty() {
                            break;
                        }
                        log::info!(
                            "Fetched {} 1m klines for {} {} starting from {}",
                            klines.len(),
                            market_type_clone.as_str(),
                            symbol_clone,
                            from_open_time
                        );
                        let last_open_time = klines.last().unwrap().open_time;
                        let res = _update_kline_data(db_clone.clone(), &market_type_clone, &klines);
                        if res.is_err() {
                            log::error!(
                                "Failed to update kline data for {} {}: {:?}",
                                market_type_clone.as_str(),
                                symbol_clone,
                                res.err()
                            );
                            break;
                        }
                        from_open_time = last_open_time + 1;
                    }
                    Err(_) => {
                        log::warn!(
                            "Failed to fetch kline data for {} {}. Retrying...",
                            market_type_clone.as_str(),
                            symbol_clone
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });
        join_handle_vec.push(handle);
    }
    for handle in join_handle_vec {
        let _ = handle.await;
    }
}

async fn fetch_1s_klines(
    market_type: MarketType,
    provider: Arc<dyn MarketProvider>,
    symbols: Vec<String>,
    db: Arc<SQLiteDB>,
) {
    let mut join_handle_vec = vec![];
    for symbol in symbols.iter() {
        let from_open_time = _get_klines(
            db.clone(),
            &market_type,
            symbol,
            &KlineInterval::OneSecond,
            None,
            None,
            Some(1),
        )
        .ok()
        .and_then(|klines| klines.first().map(|kline| kline.open_time + 1))
        .unwrap_or(time::get_current_milli_timestamp() - 6 * 30 * 24 * 60 * 60 * 1000); // 最近6个月
        let now = time::get_current_milli_timestamp();

        let provider_clone = provider.clone();
        let symbol_clone = symbol.clone();
        let db_clone = db.clone();
        let market_type_clone = market_type.clone();
        let handle = tokio::spawn(async move {
            let mut from_open_time = from_open_time;
            while from_open_time < now {
                match provider_clone
                    .get_klines(GetKlinesRequest {
                        symbol: symbol_clone.clone(),
                        interval: KlineInterval::OneSecond,
                        start_time: Some(from_open_time),
                        end_time: None,
                        limit: Some(1000),
                    })
                    .await
                {
                    Ok(klines) => {
                        if klines.is_empty() {
                            break;
                        }
                        log::info!(
                            "Fetched {} 1s klines for {} {} starting from {}",
                            klines.len(),
                            market_type_clone.as_str(),
                            symbol_clone,
                            from_open_time
                        );
                        let last_open_time = klines.last().unwrap().open_time;
                        let res = _update_kline_data(db_clone.clone(), &market_type_clone, &klines);
                        if res.is_err() {
                            log::error!(
                                "Failed to update 1s kline data for {} {}: {:?}",
                                market_type_clone.as_str(),
                                symbol_clone,
                                res.err()
                            );
                            break;
                        }
                        from_open_time = last_open_time + 1;
                    }
                    Err(_) => {
                        log::warn!(
                            "Failed to fetch 1s kline data for {} {}. Retrying...",
                            market_type_clone.as_str(),
                            symbol_clone
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });
        join_handle_vec.push(handle);
    }
    for handle in join_handle_vec {
        let _ = handle.await;
    }
}

async fn fetch_trades(
    market_type: MarketType,
    provider: Arc<dyn MarketProvider>,
    symbols: Vec<String>,
    db: Arc<SQLiteDB>,
) {
    let mut join_handle_vec = vec![];
    for symbol in symbols.iter() {
        let from_id = _get_trades(db.clone(), &market_type, symbol, None, None, None, Some(1))
            .ok()
            .and_then(|trades| trades.first().map(|trade| trade.trade_id.clone()));
        let start_time = time::get_current_milli_timestamp() - 30 * 24 * 60 * 60 * 1000; // 最近1个月
        let now = time::get_current_milli_timestamp();

        let provider_clone = provider.clone();
        let symbol_clone = symbol.clone();
        let db_clone = db.clone();
        let market_type_clone = market_type.clone();
        let handle = tokio::spawn(async move {
            let mut current_from_id = from_id;
            let mut current_start_time = start_time;
            loop {
                match provider_clone
                    .get_trades(crate::models::GetTradesRequest {
                        symbol: symbol_clone.clone(),
                        from_id: current_from_id.clone(),
                        start_time: if current_from_id.is_none() {
                            Some(current_start_time)
                        } else {
                            None
                        },
                        end_time: None,
                        limit: Some(1000),
                    })
                    .await
                {
                    Ok(trades) => {
                        if trades.is_empty() {
                            break;
                        }
                        log::info!(
                            "Fetched {} trades for {} {} starting from {:?}",
                            trades.len(),
                            market_type_clone.as_str(),
                            symbol_clone,
                            current_from_id
                        );
                        let last_trade_id = trades.last().unwrap().trade_id.clone();
                        let last_timestamp = trades.last().unwrap().timestamp;

                        // 检查是否已经超过当前时间
                        if last_timestamp >= now {
                            // 只保存时间戳在范围内的交易
                            let valid_trades: Vec<_> =
                                trades.into_iter().filter(|t| t.timestamp < now).collect();
                            if !valid_trades.is_empty() {
                                let res = _update_trade_data(
                                    db_clone.clone(),
                                    &market_type_clone,
                                    &valid_trades,
                                );
                                if res.is_err() {
                                    log::error!(
                                        "Failed to update trade data for {} {}: {:?}",
                                        market_type_clone.as_str(),
                                        symbol_clone,
                                        res.err()
                                    );
                                }
                            }
                            break;
                        }

                        let res = _update_trade_data(db_clone.clone(), &market_type_clone, &trades);
                        if res.is_err() {
                            log::error!(
                                "Failed to update trade data for {} {}: {:?}",
                                market_type_clone.as_str(),
                                symbol_clone,
                                res.err()
                            );
                            break;
                        }
                        current_from_id = Some(last_trade_id);
                        current_start_time = last_timestamp + 1;
                    }
                    Err(_) => {
                        log::warn!(
                            "Failed to fetch trade data for {} {}. Retrying...",
                            market_type_clone.as_str(),
                            symbol_clone
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });
        join_handle_vec.push(handle);
    }
    for handle in join_handle_vec {
        let _ = handle.await;
    }
}

pub async fn market_dump(
    symbols: HashMap<MarketType, Vec<String>>,
    market_providers: HashMap<MarketType, Arc<dyn MarketProvider>>,
    db_path: &str,
) -> Result<()> {
    let db = Arc::new(
        SQLiteDB::new(db_path).map_err(|e| PlatformError::PlatformError {
            message: format!("Failed to open database: {}", e),
        })?,
    );

    // 创建表
    _create_kline_table(db.clone())?;
    _create_trade_table(db.clone())?;

    // 1. 获取1m kline：最近5年
    log::info!("Starting to fetch 1m klines for all symbols...");
    for (market_type, provider) in market_providers.iter() {
        if let Some(symbol_list) = symbols.get(market_type) {
            log::info!(
                "Fetching 1m klines for {} with {} symbols",
                market_type.as_str(),
                symbol_list.len()
            );
            fetch_1m_klines(
                market_type.clone(),
                provider.clone(),
                symbol_list.clone(),
                db.clone(),
            )
            .await;
        }
    }
    log::info!("Completed fetching 1m klines");

    // 2. 获取1s kline：最近6个月
    log::info!("Starting to fetch 1s klines for all symbols...");
    for (market_type, provider) in market_providers.iter() {
        if let Some(symbol_list) = symbols.get(market_type) {
            log::info!(
                "Fetching 1s klines for {} with {} symbols",
                market_type.as_str(),
                symbol_list.len()
            );
            fetch_1s_klines(
                market_type.clone(),
                provider.clone(),
                symbol_list.clone(),
                db.clone(),
            )
            .await;
        }
    }
    log::info!("Completed fetching 1s klines");

    // 3. 获取aggtrade：最近1个月
    log::info!("Starting to fetch trades for all symbols...");
    for (market_type, provider) in market_providers.iter() {
        if let Some(symbol_list) = symbols.get(market_type) {
            log::info!(
                "Fetching trades for {} with {} symbols",
                market_type.as_str(),
                symbol_list.len()
            );
            fetch_trades(
                market_type.clone(),
                provider.clone(),
                symbol_list.clone(),
                db.clone(),
            )
            .await;
        }
    }
    log::info!("Completed fetching trades");

    Ok(())
}
