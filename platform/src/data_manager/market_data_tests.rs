use crate::{
    config::Config,
    data_manager::market_data::MarketData,
    market_provider::{binance_spot_market_provider::BinanceSpotMarketProvider, MarketProvider},
    models::{DepthData, KlineData, KlineInterval, MarketType, Ticker24hr, Trade},
};
use env_logger::Env;
use json::dump;
use log::info;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tempfile::NamedTempFile;
use tokio::{sync::Mutex, time::sleep};

#[tokio::test]
async fn test_market_data_initialization() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    // Create temporary config file
    let config_content = r#"
    {
        "data_manager": {
            "market_types": ["binance_spot"],
            "binance_spot": {
                "cache_capacity": 100,
                "symbols": ["BTCUSDT", "ETHUSDT"],
                "kline_intervals": ["1m", "5m"]
            }
        },
        "binance": {
            "spot": {
                "api_base_url": "https://api.binance.com",
                "stream_base_url": "wss://stream.binance.com:9443/stream",
                "subscribed_symbols": ["BTCUSDT", "ETHUSDT"],
                "subscribed_kline_intervals": ["1m", "5m"],
                "api_rate_limits": [[1000, 500], [60000, 5000]],
                "stream_rate_limits": [[1000, 500]],
                "kline_event_channel_capacity": 5000,
                "trade_event_channel_capacity": 5000,
                "depth_event_channel_capacity": 5000,
                "ticker_event_channel_capacity": 5000,
                "depth_cache_channel_capacity": 5000,
                "stream_reconnect_interval_milli_secs": 3000,
                "api_timeout_milli_secs": 10000
            }
        },
        "proxy": {
            "url": "socks5://127.0.0.1:10808"
        }
    }
    "#;

    let mut config_file = NamedTempFile::new().unwrap();
    std::io::Write::write_all(&mut config_file, config_content.as_bytes()).unwrap();
    let config_path = config_file.path().to_str().unwrap();

    // Load config
    let config = Config::from_json(config_path).unwrap();

    // Initialize market provider
    let mut provider = BinanceSpotMarketProvider::new(Arc::new(config.clone())).unwrap();
    provider.init().await.unwrap();

    // Create market providers map
    let mut market_providers: HashMap<MarketType, Arc<dyn MarketProvider>> = HashMap::new();
    market_providers.insert(MarketType::BinanceSpot, Arc::new(provider));

    // Initialize MarketData
    let market_data =
        MarketData::new(config.clone(), Arc::new(market_providers), Arc::new(vec![])).unwrap();

    info!("MarketData initialized successfully.");

    // Initialize market data
    market_data.init().await.unwrap();

    info!("MarketData init() completed. Waiting for initial data collection...");

    // Wait for initial data to be fetched
    sleep(Duration::from_secs(5)).await;

    // Test 1: Verify klines initialization
    info!("Testing klines initialization...");
    let klines_btc_1m = market_data
        .get_klines(
            &MarketType::BinanceSpot,
            &"BTCUSDT".to_string(),
            &KlineInterval::OneMinute,
            None,
        )
        .await
        .unwrap();
    assert!(
        !klines_btc_1m.is_empty(),
        "BTCUSDT 1m klines should not be empty"
    );
    info!("BTCUSDT 1m klines count: {}", klines_btc_1m.len());
    dump(&klines_btc_1m, "market_data_init_klines_btc_1m.json").unwrap();

    let klines_eth_5m = market_data
        .get_klines(
            &MarketType::BinanceSpot,
            &"ETHUSDT".to_string(),
            &KlineInterval::FiveMinutes,
            None,
        )
        .await
        .unwrap();
    assert!(
        !klines_eth_5m.is_empty(),
        "ETHUSDT 5m klines should not be empty"
    );
    info!("ETHUSDT 5m klines count: {}", klines_eth_5m.len());
    dump(&klines_eth_5m, "market_data_init_klines_eth_5m.json").unwrap();

    // Test 2: Verify trades initialization
    info!("Testing trades initialization...");
    let trades_btc = market_data
        .get_trades(&MarketType::BinanceSpot, &"BTCUSDT".to_string(), None)
        .await
        .unwrap();
    assert!(!trades_btc.is_empty(), "BTCUSDT trades should not be empty");
    info!("BTCUSDT trades count: {}", trades_btc.len());
    dump(&trades_btc, "market_data_init_trades_btc.json").unwrap();

    let trades_eth = market_data
        .get_trades(&MarketType::BinanceSpot, &"ETHUSDT".to_string(), None)
        .await
        .unwrap();
    assert!(!trades_eth.is_empty(), "ETHUSDT trades should not be empty");
    info!("ETHUSDT trades count: {}", trades_eth.len());
    dump(&trades_eth, "market_data_init_trades_eth.json").unwrap();

    // Test 3: Verify depth initialization
    info!("Testing depth initialization...");
    let depth_btc = market_data
        .get_depth(&MarketType::BinanceSpot, &"BTCUSDT".to_string())
        .await
        .unwrap();
    assert!(depth_btc.is_some(), "BTCUSDT depth should not be None");
    let depth_btc = depth_btc.unwrap();
    assert!(
        !depth_btc.bids.is_empty(),
        "BTCUSDT depth bids should not be empty"
    );
    assert!(
        !depth_btc.asks.is_empty(),
        "BTCUSDT depth asks should not be empty"
    );
    info!(
        "BTCUSDT depth: {} bids, {} asks",
        depth_btc.bids.len(),
        depth_btc.asks.len()
    );
    // Dump first 20 levels for readability
    let depth_btc_dump = DepthData {
        symbol: depth_btc.symbol.clone(),
        bids: depth_btc.bids.iter().take(20).cloned().collect(),
        asks: depth_btc.asks.iter().take(20).cloned().collect(),
        timestamp: depth_btc.timestamp,
    };
    dump(&depth_btc_dump, "market_data_init_depth_btc.json").unwrap();

    let depth_eth = market_data
        .get_depth(&MarketType::BinanceSpot, &"ETHUSDT".to_string())
        .await
        .unwrap();
    assert!(depth_eth.is_some(), "ETHUSDT depth should not be None");
    let depth_eth = depth_eth.unwrap();
    assert!(
        !depth_eth.bids.is_empty(),
        "ETHUSDT depth bids should not be empty"
    );
    assert!(
        !depth_eth.asks.is_empty(),
        "ETHUSDT depth asks should not be empty"
    );
    info!(
        "ETHUSDT depth: {} bids, {} asks",
        depth_eth.bids.len(),
        depth_eth.asks.len()
    );
    let depth_eth_dump = DepthData {
        symbol: depth_eth.symbol.clone(),
        bids: depth_eth.bids.iter().take(20).cloned().collect(),
        asks: depth_eth.asks.iter().take(20).cloned().collect(),
        timestamp: depth_eth.timestamp,
    };
    dump(&depth_eth_dump, "market_data_init_depth_eth.json").unwrap();

    // Test 4: Verify ticker initialization
    info!("Testing ticker initialization...");
    let ticker_btc = market_data
        .get_ticker(&MarketType::BinanceSpot, &"BTCUSDT".to_string())
        .await
        .unwrap();
    assert!(ticker_btc.is_some(), "BTCUSDT ticker should not be None");
    let ticker_btc = ticker_btc.unwrap();
    info!("BTCUSDT ticker: {}", ticker_btc.symbol);
    dump(&ticker_btc, "market_data_init_ticker_btc.json").unwrap();

    let ticker_eth = market_data
        .get_ticker(&MarketType::BinanceSpot, &"ETHUSDT".to_string())
        .await
        .unwrap();
    assert!(ticker_eth.is_some(), "ETHUSDT ticker should not be None");
    let ticker_eth = ticker_eth.unwrap();
    info!("ETHUSDT ticker: {}", ticker_eth.symbol);
    dump(&ticker_eth, "market_data_init_ticker_eth.json").unwrap();

    info!("All initialization tests passed successfully.");
}

#[tokio::test]
async fn test_market_data_streaming_updates() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    // Create temporary config file
    let config_content = r#"
    {
        "data_manager": {
            "market_types": ["binance_spot"],
            "binance_spot": {
                "cache_capacity": 50,
                "symbols": ["BTCUSDT"],
                "kline_intervals": ["1m"]
            }
        },
        "binance": {
            "spot": {
                "api_base_url": "https://api.binance.com",
                "stream_base_url": "wss://stream.binance.com:9443/stream",
                "subscribed_symbols": ["BTCUSDT"],
                "subscribed_kline_intervals": ["1m"],
                "api_rate_limits": [[1000, 500], [60000, 5000]],
                "stream_rate_limits": [[1000, 500]],
                "kline_event_channel_capacity": 5000,
                "trade_event_channel_capacity": 5000,
                "depth_event_channel_capacity": 5000,
                "ticker_event_channel_capacity": 5000,
                "depth_cache_channel_capacity": 5000,
                "stream_reconnect_interval_milli_secs": 3000,
                "api_timeout_milli_secs": 30000
            }
        },
        "proxy": {
            "url": "socks5://127.0.0.1:10808"
        }
    }
    "#;

    let mut config_file = NamedTempFile::new().unwrap();
    std::io::Write::write_all(&mut config_file, config_content.as_bytes()).unwrap();
    let config_path = config_file.path().to_str().unwrap();

    // Load config
    let config = Config::from_json(config_path).unwrap();

    // Initialize market provider
    let mut provider = BinanceSpotMarketProvider::new(Arc::new(config.clone())).unwrap();
    provider.init().await.unwrap();

    // Create market providers map
    let mut market_providers: HashMap<MarketType, Arc<dyn MarketProvider>> = HashMap::new();
    market_providers.insert(MarketType::BinanceSpot, Arc::new(provider));

    // Initialize MarketData
    let market_data = Arc::new(
        MarketData::new(config.clone(), Arc::new(market_providers), Arc::new(vec![])).unwrap(),
    );

    info!("MarketData initialized successfully.");

    // Initialize market data
    market_data.init().await.unwrap();

    info!("MarketData init() completed. Waiting for streaming updates...");

    // Wait for initial data
    sleep(Duration::from_secs(5)).await;

    // Record initial state
    let initial_klines = market_data
        .get_klines(
            &MarketType::BinanceSpot,
            &"BTCUSDT".to_string(),
            &KlineInterval::OneMinute,
            None,
        )
        .await
        .unwrap();
    let initial_klines_count = initial_klines.len();
    info!("Initial klines count: {}", initial_klines_count);

    let initial_trades = market_data
        .get_trades(&MarketType::BinanceSpot, &"BTCUSDT".to_string(), None)
        .await
        .unwrap();
    let initial_trades_count = initial_trades.len();
    info!("Initial trades count: {}", initial_trades_count);

    let initial_depth = market_data
        .get_depth(&MarketType::BinanceSpot, &"BTCUSDT".to_string())
        .await
        .unwrap()
        .unwrap();
    let initial_depth_timestamp = initial_depth.timestamp;
    info!("Initial depth timestamp: {}", initial_depth_timestamp);

    let initial_ticker = market_data
        .get_ticker(&MarketType::BinanceSpot, &"BTCUSDT".to_string())
        .await
        .unwrap()
        .unwrap();
    let initial_ticker_close_time = initial_ticker.close_time;
    info!("Initial ticker close_time: {}", initial_ticker_close_time);

    // Collect updates over time
    let klines_history = Arc::new(Mutex::new(Vec::<Vec<KlineData>>::new()));
    let trades_history = Arc::new(Mutex::new(Vec::<Vec<Trade>>::new()));
    let depths_history = Arc::new(Mutex::new(Vec::<DepthData>::new()));
    let tickers_history = Arc::new(Mutex::new(Vec::<Ticker24hr>::new()));

    let market_data_clone = market_data.clone();
    let klines_history_clone = klines_history.clone();
    let trades_history_clone = trades_history.clone();
    let depths_history_clone = depths_history.clone();
    let tickers_history_clone = tickers_history.clone();

    // Spawn task to collect snapshots every 5 seconds
    let collector_task = tokio::spawn(async move {
        for _ in 0..12 {
            // Collect for 60 seconds (12 * 5 seconds)
            sleep(Duration::from_secs(5)).await;

            // Collect klines
            if let Ok(klines) = market_data_clone
                .get_klines(
                    &MarketType::BinanceSpot,
                    &"BTCUSDT".to_string(),
                    &KlineInterval::OneMinute,
                    Some(1),
                )
                .await
            {
                klines_history_clone.lock().await.push(klines);
            }

            // Collect trades
            if let Ok(trades) = market_data_clone
                .get_trades(&MarketType::BinanceSpot, &"BTCUSDT".to_string(), Some(1))
                .await
            {
                trades_history_clone.lock().await.push(trades);
            }

            // Collect depth
            if let Ok(Some(depth)) = market_data_clone
                .get_depth(&MarketType::BinanceSpot, &"BTCUSDT".to_string())
                .await
            {
                let depth_dump = DepthData {
                    symbol: depth.symbol.clone(),
                    bids: depth.bids.iter().take(10).cloned().collect(),
                    asks: depth.asks.iter().take(10).cloned().collect(),
                    timestamp: depth.timestamp,
                };
                depths_history_clone.lock().await.push(depth_dump);
            }

            // Collect ticker
            if let Ok(Some(ticker)) = market_data_clone
                .get_ticker(&MarketType::BinanceSpot, &"BTCUSDT".to_string())
                .await
            {
                tickers_history_clone.lock().await.push(ticker);
            }
        }
    });

    collector_task.await.unwrap();

    info!("Finished collecting streaming updates.");

    // Verify and dump collected data
    let klines_history = klines_history.lock().await;
    assert!(
        !klines_history.is_empty(),
        "Klines history should not be empty"
    );
    info!("Collected {} klines snapshots", klines_history.len());
    dump(&*klines_history, "market_data_streaming_klines.json").unwrap();

    let trades_history = trades_history.lock().await;
    assert!(
        !trades_history.is_empty(),
        "Trades history should not be empty"
    );
    info!("Collected {} trades snapshots", trades_history.len());
    dump(&*trades_history, "market_data_streaming_trades.json").unwrap();

    let depths_history = depths_history.lock().await;
    assert!(
        !depths_history.is_empty(),
        "Depths history should not be empty"
    );
    info!("Collected {} depth snapshots", depths_history.len());
    dump(&*depths_history, "market_data_streaming_depths.json").unwrap();

    let tickers_history = tickers_history.lock().await;
    assert!(
        !tickers_history.is_empty(),
        "Tickers history should not be empty"
    );
    info!("Collected {} ticker snapshots", tickers_history.len());
    dump(&*tickers_history, "market_data_streaming_tickers.json").unwrap();

    // Verify that data is being updated
    let final_klines = market_data
        .get_klines(
            &MarketType::BinanceSpot,
            &"BTCUSDT".to_string(),
            &KlineInterval::OneMinute,
            None,
        )
        .await
        .unwrap();
    info!("Final klines count: {}", final_klines.len());

    let final_trades = market_data
        .get_trades(&MarketType::BinanceSpot, &"BTCUSDT".to_string(), None)
        .await
        .unwrap();
    info!("Final trades count: {}", final_trades.len());
    assert!(
        final_trades.len() >= initial_trades_count,
        "Trades should be updated (final: {}, initial: {})",
        final_trades.len(),
        initial_trades_count
    );

    let final_depth = market_data
        .get_depth(&MarketType::BinanceSpot, &"BTCUSDT".to_string())
        .await
        .unwrap()
        .unwrap();
    info!("Final depth timestamp: {}", final_depth.timestamp);
    assert!(
        final_depth.timestamp >= initial_depth_timestamp,
        "Depth should be updated (final: {}, initial: {})",
        final_depth.timestamp,
        initial_depth_timestamp
    );

    let final_ticker = market_data
        .get_ticker(&MarketType::BinanceSpot, &"BTCUSDT".to_string())
        .await
        .unwrap()
        .unwrap();
    info!("Final ticker close_time: {}", final_ticker.close_time);
    assert!(
        final_ticker.close_time >= initial_ticker_close_time,
        "Ticker should be updated (final: {}, initial: {})",
        final_ticker.close_time,
        initial_ticker_close_time
    );

    info!("All streaming update tests passed successfully.");
}

#[tokio::test]
async fn test_market_data_cache_capacity() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    // Create temporary config file with small cache capacity
    let config_content = r#"
    {
        "data_manager": {
            "market_types": ["binance_spot"],
            "binance_spot": {
                "cache_capacity": 10,
                "symbols": ["BTCUSDT"],
                "kline_intervals": ["1m"]
            }
        },
        "binance": {
            "spot": {
                "api_base_url": "https://api.binance.com",
                "stream_base_url": "wss://stream.binance.com:9443/stream",
                "subscribed_symbols": ["BTCUSDT"],
                "subscribed_kline_intervals": ["1m"],
                "api_rate_limits": [[1000, 500], [60000, 5000]],
                "stream_rate_limits": [[1000, 500]],
                "kline_event_channel_capacity": 5000,
                "trade_event_channel_capacity": 5000,
                "depth_event_channel_capacity": 5000,
                "ticker_event_channel_capacity": 5000,
                "depth_cache_channel_capacity": 5000,
                "stream_reconnect_interval_milli_secs": 3000,
                "api_timeout_milli_secs": 30000
            }
        },
        "proxy": {
            "url": "socks5://127.0.0.1:10808"
        }
    }
    "#;

    let mut config_file = NamedTempFile::new().unwrap();
    std::io::Write::write_all(&mut config_file, config_content.as_bytes()).unwrap();
    let config_path = config_file.path().to_str().unwrap();

    // Load config
    let config = Config::from_json(config_path).unwrap();

    // Initialize market provider
    let mut provider = BinanceSpotMarketProvider::new(Arc::new(config.clone())).unwrap();
    provider.init().await.unwrap();

    // Create market providers map
    let mut market_providers: HashMap<MarketType, Arc<dyn MarketProvider>> = HashMap::new();
    market_providers.insert(MarketType::BinanceSpot, Arc::new(provider));

    // Initialize MarketData
    let market_data =
        MarketData::new(config.clone(), Arc::new(market_providers), Arc::new(vec![])).unwrap();

    info!("MarketData initialized with cache_capacity=10.");

    // Initialize market data
    market_data.init().await.unwrap();

    info!("MarketData init() completed. Waiting for data...");

    // Wait for data collection
    sleep(Duration::from_secs(5)).await;

    // Verify cache capacity limits are respected
    let klines = market_data
        .get_klines(
            &MarketType::BinanceSpot,
            &"BTCUSDT".to_string(),
            &KlineInterval::OneMinute,
            None,
        )
        .await
        .unwrap();
    info!("Klines count: {}", klines.len());
    assert!(
        klines.len() <= 10,
        "Klines count should not exceed cache capacity"
    );
    dump(&klines, "market_data_cache_capacity_klines.json").unwrap();

    let trades = market_data
        .get_trades(&MarketType::BinanceSpot, &"BTCUSDT".to_string(), None)
        .await
        .unwrap();
    info!("Trades count: {}", trades.len());
    assert!(
        trades.len() <= 10,
        "Trades count should not exceed cache capacity"
    );
    dump(&trades, "market_data_cache_capacity_trades.json").unwrap();

    info!("Cache capacity test passed successfully.");
}
