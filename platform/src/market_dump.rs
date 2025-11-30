use crate::{
    data_manager::db::*,
    errors::{PlatformError, Result},
    market_provider::MarketProvider,
    models::{GetExchangeInfoRequest, GetKlinesRequest, KlineInterval, MarketType},
};
use db::sqlite::SQLiteDB;
use std::{collections::HashMap, sync::Arc};

async fn fetch_klines(
    market_type: MarketType,
    provider: Arc<dyn MarketProvider>,
    symbol: String,
    interval: KlineInterval,
    db: Arc<SQLiteDB>,
    from_ts: u64,
    to_ts: u64,
) -> Result<()> {
    let provider_clone = provider.clone();
    let symbol_clone = symbol.clone();
    let db_clone = db.clone();
    let market_type_clone = market_type.clone();
    let mut from_open_time = from_ts;
    loop {
        let klines = provider_clone
            .get_klines(GetKlinesRequest {
                symbol: symbol_clone.clone(),
                interval: interval.clone(),
                start_time: Some(from_open_time),
                end_time: None,
                limit: Some(1000),
            })
            .await?;
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
        update_kline_data(db_clone.clone(), &market_type_clone, &klines)?;
        from_open_time = last_open_time + 1;
        if klines.len() < 1000 {
            break;
        }
        if from_open_time >= to_ts {
            break;
        }
    }
    Ok(())
}

async fn check_update_klines(
    market_type: MarketType,
    provider: Arc<dyn MarketProvider>,
    symbol: String,
    interval: KlineInterval,
    db: Arc<SQLiteDB>,
    from_ts: u64,
    to_ts: u64,
) -> Result<()> {
    let mut current_from_ts = from_ts;
    let interval_ms = interval.to_millis();

    while current_from_ts < to_ts {
        let existing_klines = get_klines(
            db.clone(),
            &market_type,
            &symbol,
            &interval,
            Some(current_from_ts),
            Some(to_ts),
            Some(1000),
        )?;

        // 检查数据缺失
        if !existing_klines.is_empty() {
            let next_from_ts = existing_klines.last().unwrap().close_time + 1;
            if (next_from_ts - current_from_ts) / interval_ms != existing_klines.len() as u64 {
                log::info!(
                    "Data gap detected for {} {} from {} to {}. Fetching missing klines...",
                    market_type.as_str(),
                    symbol,
                    current_from_ts,
                    next_from_ts
                );
                fetch_klines(
                    market_type.clone(),
                    provider.clone(),
                    symbol.clone(),
                    interval.clone(),
                    db.clone(),
                    current_from_ts,
                    next_from_ts,
                )
                .await?;
            }
        }

        if existing_klines.is_empty() {
            log::info!(
                "No existing klines for {} {} from {}. Fetching data...",
                market_type.as_str(),
                symbol,
                current_from_ts
            );
            // 数据为空则从 current_from_ts 开始获取
            fetch_klines(
                market_type.clone(),
                provider.clone(),
                symbol.clone(),
                interval.clone(),
                db.clone(),
                current_from_ts,
                to_ts,
            )
            .await?;
            break;
        } else {
            let next_from_ts = existing_klines.last().unwrap().close_time + 1;
            current_from_ts = next_from_ts;
        }
    }
    Ok(())
}

async fn fetch_trades(
    market_type: MarketType,
    provider: Arc<dyn MarketProvider>,
    symbol: String,
    db: Arc<SQLiteDB>,
    from_seq_id: u64,
    to_seq_id: u64,
    from_ts: u64,
    to_ts: u64,
) -> Result<()> {
    let provider_clone = provider.clone();
    let symbol_clone = symbol.clone();
    let db_clone = db.clone();
    let market_type_clone = market_type.clone();
    let mut current_from_seq_id = from_seq_id;
    let mut current_from_ts = from_ts;
    loop {
        let trades = provider_clone
            .get_trades(crate::models::GetTradesRequest {
                symbol: symbol_clone.clone(),
                from_id: if current_from_seq_id > 0 {
                    Some(current_from_seq_id.to_string())
                } else {
                    None
                },
                start_time: if current_from_seq_id == 0 {
                    Some(current_from_ts)
                } else {
                    None
                },
                end_time: None,
                limit: Some(1000),
            })
            .await?;
        if trades.is_empty() {
            break;
        }
        log::info!(
            "Fetched {} trades for {} {} starting from seq_id {} or time {}",
            trades.len(),
            market_type_clone.as_str(),
            symbol_clone,
            current_from_seq_id,
            current_from_ts
        );
        let last_seq_id = trades.last().unwrap().seq_id;
        let last_ts = trades.last().unwrap().timestamp;
        update_trade_data(db_clone.clone(), &market_type_clone, &trades)?;
        current_from_seq_id = last_seq_id + 1;
        current_from_ts = last_ts + 1;
        if trades.len() < 1000 {
            break;
        }
        if current_from_seq_id >= to_seq_id {
            break;
        }
        if current_from_ts >= to_ts {
            break;
        }
    }
    Ok(())
}

async fn check_update_trades(
    market_type: MarketType,
    provider: Arc<dyn MarketProvider>,
    symbol: String,
    db: Arc<SQLiteDB>,
    from_ts: u64,
    to_ts: u64,
) -> Result<()> {
    let mut current_from_seq_id = 0u64;
    let mut current_from_ts = from_ts;

    while current_from_ts < to_ts {
        let existing_trades = get_trades(
            db.clone(),
            &market_type,
            &symbol,
            Some(current_from_ts),
            Some(to_ts),
            Some(current_from_seq_id),
            Some(1000),
        )?;

        // 如果是首次获取，检查是否从 from_ts 开始，如果差异60s以上，则补充数据
        // 检查数据缺失：验证 seq_id 是否严格递增连续
        if !existing_trades.is_empty() {
            if current_from_seq_id == 0 {
                let first_trade_time = existing_trades.first().unwrap().timestamp;
                if first_trade_time > from_ts + 60 * 1000 {
                    log::info!(
                        "Data gap detected for {} {} from time {} to {}. Fetching missing trades...",
                        market_type.as_str(),
                        symbol,
                        from_ts,
                        first_trade_time
                    );
                    fetch_trades(
                        market_type.clone(),
                        provider.clone(),
                        symbol.clone(),
                        db.clone(),
                        0,
                        u64::MAX,
                        from_ts,
                        first_trade_time,
                    )
                    .await?;
                }
                current_from_seq_id = existing_trades.first().unwrap().seq_id;
            }

            let to_seq_id = existing_trades.last().unwrap().seq_id;
            // 如果 seq_id 应该连续，则 (end - start + 1) 应该等于 len
            if (to_seq_id - current_from_seq_id + 1) as usize != existing_trades.len() {
                log::info!(
                    "Data gap detected for {} {} from seq_id {} to {}. Fetching missing trades...",
                    market_type.as_str(),
                    symbol,
                    current_from_seq_id,
                    to_seq_id
                );
                fetch_trades(
                    market_type.clone(),
                    provider.clone(),
                    symbol.clone(),
                    db.clone(),
                    current_from_seq_id,
                    to_seq_id,
                    from_ts,
                    to_ts,
                )
                .await?;
            }
        }

        if existing_trades.is_empty() {
            log::info!(
                "No existing trades for {} {} from {}. Fetching data...",
                market_type.as_str(),
                symbol,
                current_from_ts
            );
            fetch_trades(
                market_type.clone(),
                provider.clone(),
                symbol.clone(),
                db.clone(),
                current_from_seq_id,
                u64::MAX,
                from_ts,
                to_ts,
            )
            .await?;
            break;
        } else {
            let last_seq_id = existing_trades.last().unwrap().seq_id;
            current_from_seq_id = last_seq_id + 1;
            let last_timestamp = existing_trades.last().unwrap().timestamp;
            current_from_ts = last_timestamp;
        }
    }
    Ok(())
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
    create_symbol_info_table(db.clone())?;
    create_kline_table(db.clone())?;
    create_trade_table(db.clone())?;

    log::info!("Starting to fetch symbol info for all markets...");
    for (market_type, provider) in market_providers.iter() {
        log::info!("Fetching symbol info for {}", market_type.as_str());
        match provider
            .get_exchange_info(GetExchangeInfoRequest {
                symbols: Some(symbols.get(market_type).cloned().unwrap_or_default()),
                symbol: None,
            })
            .await
        {
            Ok(exchange_info) => {
                update_symbol_info(db.clone(), market_type, &exchange_info.symbols)?;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    // 并发获取加快获取效率
    let mut join_handlers = HashMap::new();

    log::info!("Starting to fetch 1m klines for all symbols...");
    for (market_type, provider) in market_providers.iter() {
        if let Some(symbol_list) = symbols.get(market_type) {
            log::info!(
                "Fetching 1m klines for {} with {} symbols",
                market_type.as_str(),
                symbol_list.len()
            );
            let now = time::get_current_milli_timestamp();
            for symbol in symbol_list.iter() {
                for interval in &[KlineInterval::OneMinute] {
                    let from_ts = match interval {
                        KlineInterval::OneMinute => now - 5 * 365 * 24 * 60 * 60 * 1000, // 最近5年
                        KlineInterval::OneSecond => now - 180 * 24 * 60 * 60 * 1000, // 最近6个月
                        _ => {
                            return Err(PlatformError::PlatformError {
                                message: format!(
                                    "Unsupported interval {:?} for market dump",
                                    interval
                                ),
                            })
                        }
                    };
                    let market_type_clone = market_type.clone();
                    let provider_clone = provider.clone();
                    let symbol_clone = symbol.clone();
                    let interval_clone = interval.clone();
                    let db_clone = db.clone();
                    join_handlers.insert(
                        format!(
                            "{}-{}-{}",
                            market_type_clone.as_str(),
                            symbol_clone,
                            interval_clone.as_str()
                        ),
                        tokio::spawn(async move {
                            check_update_klines(
                                market_type_clone,
                                provider_clone,
                                symbol_clone,
                                interval_clone,
                                db_clone,
                                from_ts,
                                u64::MAX,
                            )
                        }),
                    );
                }
            }
        }
    }

    for (key, handle) in join_handlers {
        log::info!("Waiting for task {} to complete...", key);
        match handle.await {
            Ok(res) => {
                if let Err(e) = res.await {
                    log::error!("Task {} failed: {:?}", key, e);
                }
            }
            Err(e) => {
                log::error!("Task {} panicked: {:?}", key, e);
            }
        }
    }

    log::info!("Starting to fetch trades for all symbols...");
    let mut trade_join_handlers = HashMap::new();
    for (market_type, provider) in market_providers.iter() {
        if let Some(symbol_list) = symbols.get(market_type) {
            log::info!(
                "Fetching trades for {} with {} symbols",
                market_type.as_str(),
                symbol_list.len()
            );
            let now = time::get_current_milli_timestamp();
            for symbol in symbol_list.iter() {
                let market_type_clone = market_type.clone();
                let provider_clone = provider.clone();
                let symbol_clone = symbol.clone();
                let db_clone = db.clone();
                let from_ts = now - 30 * 24 * 60 * 60 * 1000; // 最近1个月
                trade_join_handlers.insert(
                    format!("{}-{}-trade", market_type_clone.as_str(), symbol_clone),
                    tokio::spawn(async move {
                        check_update_trades(
                            market_type_clone,
                            provider_clone,
                            symbol_clone,
                            db_clone,
                            from_ts,
                            u64::MAX,
                        )
                    }),
                );
            }
        }
    }

    for (key, handle) in trade_join_handlers {
        log::info!("Waiting for trade task {} to complete...", key);
        match handle.await {
            Ok(res) => {
                if let Err(e) = res.await {
                    log::error!("Trade task {} failed: {:?}", key, e);
                }
            }
            Err(e) => {
                log::error!("Trade task {} panicked: {:?}", key, e);
            }
        }
    }
    log::info!("Completed fetching trades");

    Ok(())
}
