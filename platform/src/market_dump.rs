use crate::{
    data_manager::db::*,
    errors::{PlatformError, Result},
    market_provider::MarketProvider,
    models::{GetKlinesRequest, KlineInterval, MarketType},
};
use db::sqlite::SQLiteDB;
use std::{collections::HashMap, sync::Arc};

async fn fetch_1m_klines(
    market_type: MarketType,
    provider: Arc<dyn MarketProvider>,
    symbols: Vec<String>,
    db: Arc<SQLiteDB>,
) {
    let mut join_handle_vec = vec![];
    for symbol in symbols.iter() {
        let from_open_time = get_klines(
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
                        let res = update_kline_data(db_clone.clone(), &market_type_clone, &klines);
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
        let from_open_time = get_klines(
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
                        let res = update_kline_data(db_clone.clone(), &market_type_clone, &klines);
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
        let from_id = get_trades(db.clone(), &market_type, symbol, None, None, None, Some(1))
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
                                let res = update_trade_data(
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

                        let res = update_trade_data(db_clone.clone(), &market_type_clone, &trades);
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
    create_kline_table(db.clone())?;
    create_trade_table(db.clone())?;

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
