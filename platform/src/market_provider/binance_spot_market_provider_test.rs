use super::binance_spot_market_provider::BinanceSpotMarketProvider;
use crate::{
    config::Config,
    market_provider::{MarketEvent, MarketProvider},
    models::KlineInterval,
};
use env_logger::Env;
use serde::Serialize;
use std::{collections::HashMap, path::Path, sync::Arc};
use tokio::{fs, sync::Mutex, time::Duration};

// Helper function to dump JSON to file
async fn dump_json<T: Serialize, P: AsRef<Path>>(data: &T, path: P) -> std::io::Result<()> {
    let json_str = serde_json::to_string_pretty(data).unwrap();
    fs::write(path, json_str).await
}

#[derive(Debug, Serialize, Clone)]
struct TestSnapshot {
    timestamp: u64,
    klines: HashMap<String, Vec<serde_json::Value>>,
    trades: HashMap<String, Vec<serde_json::Value>>,
    depth: HashMap<String, serde_json::Value>,
    ticker_24hr: HashMap<String, serde_json::Value>,
    exchange_info: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Clone)]
struct MarketEventLog {
    timestamp: u64,
    event_type: String,
    data: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct TestResults {
    test_name: String,
    duration_seconds: u64,
    snapshots: Vec<TestSnapshot>,
    events: Vec<MarketEventLog>,
    stats: TestStats,
}

#[derive(Debug, Serialize, Default, Clone)]
struct TestStats {
    total_kline_events: usize,
    total_trade_events: usize,
    total_depth_events: usize,
    total_ticker_events: usize,
    symbols_tested: Vec<String>,
    intervals_tested: Vec<String>,
}

async fn create_test_config() -> Arc<Config> {
    use std::io::Write;
    use tempfile::NamedTempFile;

    let mut tempfile = NamedTempFile::new().unwrap();
    writeln!(
        tempfile,
        r#"{{
        "binance": {{
            "spot": {{
                "api_base_url": "https://api.binance.com",
                "stream_base_url": "wss://stream.binance.com:9443/stream",
                "symbols": ["BTCUSDT", "ETHUSDT"],
                "kline_intervals": ["1m", "5m"],
                "max_trade_cnt": 100,
                "max_kline_cnt": 100,
                "market_event_channel_capacity": 1000,
                "exchange_info_refresh_interval_ms": 60000,
                "stream_retry_interval_ms": 5000,
                "api_rate_limits": [[60000, 1200]],
                "stream_rate_limits": [[3600000, 300]]
            }}
        }},
        "proxy": {{
            "url": "socks5://127.0.0.1:10808"
        }}
    }}"#
    )
    .unwrap();
    tempfile.flush().unwrap();

    Arc::new(Config::from_json(tempfile.path().to_str().unwrap()).unwrap())
}

async fn capture_snapshot(provider: &BinanceSpotMarketProvider) -> TestSnapshot {
    let mut snapshot = TestSnapshot {
        timestamp: time::get_current_milli_timestamp(),
        klines: HashMap::new(),
        trades: HashMap::new(),
        depth: HashMap::new(),
        ticker_24hr: HashMap::new(),
        exchange_info: None,
    };

    let symbols = vec!["BTCUSDT", "ETHUSDT"];
    let intervals = vec![KlineInterval::OneMinute, KlineInterval::FiveMinutes];

    // Capture klines
    for symbol in &symbols {
        for interval in &intervals {
            match provider
                .get_klines(symbol, interval.clone(), Some(10))
                .await
            {
                Ok(klines) => {
                    let key = format!("{}_{:?}", symbol, interval);
                    snapshot.klines.insert(
                        key,
                        klines
                            .iter()
                            .map(|k| serde_json::to_value(k.as_ref()).unwrap())
                            .collect(),
                    );
                }
                Err(e) => {
                    eprintln!("Failed to get klines for {} {:?}: {}", symbol, interval, e);
                }
            }
        }
    }

    // Capture trades
    for symbol in &symbols {
        match provider.get_trades(symbol, Some(10)).await {
            Ok(trades) => {
                snapshot.trades.insert(
                    symbol.to_string(),
                    trades
                        .iter()
                        .map(|t| serde_json::to_value(t.as_ref()).unwrap())
                        .collect(),
                );
            }
            Err(e) => {
                eprintln!("Failed to get trades for {}: {}", symbol, e);
            }
        }
    }

    // Capture depth
    for symbol in &symbols {
        match provider.get_depth(symbol).await {
            Ok(depth) => {
                snapshot.depth.insert(
                    symbol.to_string(),
                    serde_json::to_value(depth.as_ref()).unwrap(),
                );
            }
            Err(e) => {
                eprintln!("Failed to get depth for {}: {}", symbol, e);
            }
        }
    }

    // Capture ticker 24hr
    for symbol in &symbols {
        match provider.get_ticker_24hr(symbol).await {
            Ok(ticker) => {
                snapshot.ticker_24hr.insert(
                    symbol.to_string(),
                    serde_json::to_value(ticker.as_ref()).unwrap(),
                );
            }
            Err(e) => {
                eprintln!("Failed to get ticker for {}: {}", symbol, e);
            }
        }
    }

    // Capture exchange info
    match provider.get_exchange_info().await {
        Ok(info) => {
            snapshot.exchange_info = Some(serde_json::to_value(&info).unwrap());
        }
        Err(e) => {
            eprintln!("Failed to get exchange info: {}", e);
        }
    }

    snapshot
}

#[tokio::test]
async fn test_binance_spot_market_provider_comprehensive() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    // Create config and provider
    let config = create_test_config().await;
    let mut provider = BinanceSpotMarketProvider::new(config.clone()).unwrap();

    // Initialize provider
    log::info!("Initializing provider...");
    provider.init().await.unwrap();
    log::info!("Provider initialized successfully");

    // Subscribe to market events
    let mut event_receiver = provider.subscribe();

    // Shared data structures
    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let events_log = Arc::new(Mutex::new(Vec::new()));
    let stats = Arc::new(Mutex::new(TestStats::default()));

    // Event listener task
    let events_log_clone = events_log.clone();
    let stats_clone = stats.clone();
    let event_task = tokio::spawn(async move {
        while let Ok(event) = event_receiver.recv().await {
            let timestamp = time::get_current_milli_timestamp();
            let mut stats = stats_clone.lock().await;

            let (event_type, data) = match &event {
                MarketEvent::Kline(kline) => {
                    stats.total_kline_events += 1;
                    ("kline", serde_json::to_value(kline.as_ref()).unwrap())
                }
                MarketEvent::Trade(trade) => {
                    stats.total_trade_events += 1;
                    ("trade", serde_json::to_value(trade.as_ref()).unwrap())
                }
                MarketEvent::Depth(depth) => {
                    stats.total_depth_events += 1;
                    ("depth", serde_json::to_value(depth.as_ref()).unwrap())
                }
                MarketEvent::Ticker(ticker) => {
                    stats.total_ticker_events += 1;
                    ("ticker", serde_json::to_value(ticker.as_ref()).unwrap())
                }
            };

            drop(stats);

            let mut events = events_log_clone.lock().await;
            events.push(MarketEventLog {
                timestamp,
                event_type: event_type.to_string(),
                data,
            });
        }
    });

    // Periodic snapshot task
    let snapshots_clone = snapshots.clone();
    let snapshot_interval = Duration::from_secs(10);
    let test_duration = Duration::from_secs(30);
    let start_time = std::time::Instant::now();

    log::info!("Starting test for {} seconds...", test_duration.as_secs());
    log::info!(
        "Taking snapshots every {} second(s)",
        snapshot_interval.as_secs()
    );

    while start_time.elapsed() < test_duration {
        let snapshot = capture_snapshot(&provider).await;
        log::info!(
            "Snapshot taken at {} ms - klines: {}, trades: {}, depth: {}, ticker: {}",
            snapshot.timestamp,
            snapshot.klines.len(),
            snapshot.trades.len(),
            snapshot.depth.len(),
            snapshot.ticker_24hr.len()
        );

        let mut snapshots = snapshots_clone.lock().await;
        snapshots.push(snapshot);
        drop(snapshots);

        tokio::time::sleep(snapshot_interval).await;
    }

    log::info!("Test duration completed. Collecting results...");

    // Stop event listener
    drop(provider);
    event_task.abort();

    // Gather final statistics
    let mut final_stats = stats.lock().await;
    final_stats.symbols_tested = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
    final_stats.intervals_tested = vec!["1m".to_string(), "5m".to_string()];

    let stats_copy = final_stats.clone();
    drop(final_stats);

    // Prepare test results
    let snapshots_copy = snapshots.lock().await.clone();
    let events_copy = events_log.lock().await.clone();

    let test_results = TestResults {
        test_name: "binance_spot_market_provider_comprehensive".to_string(),
        duration_seconds: test_duration.as_secs(),
        snapshots: snapshots_copy,
        events: events_copy,
        stats: stats_copy.clone(),
    };

    // Print statistics
    log::info!("\n=== Test Statistics ===");
    log::info!(
        "Total Kline Events: {}",
        test_results.stats.total_kline_events
    );
    log::info!(
        "Total Trade Events: {}",
        test_results.stats.total_trade_events
    );
    log::info!(
        "Total Depth Events: {}",
        test_results.stats.total_depth_events
    );
    log::info!(
        "Total Ticker Events: {}",
        test_results.stats.total_ticker_events
    );
    log::info!("Snapshots Captured: {}", test_results.snapshots.len());
    log::info!("Events Logged: {}", test_results.events.len());

    // Dump results to JSON file
    let output_file = "binance_spot_market_provider_test_results.json";
    dump_json(&test_results, output_file).await.unwrap();
    log::info!("\nTest results saved to: {}", output_file);

    // Assertions
    assert!(
        test_results.stats.total_kline_events > 0,
        "Should receive kline events"
    );
    assert!(
        test_results.stats.total_trade_events > 0,
        "Should receive trade events"
    );
    assert!(
        test_results.stats.total_depth_events > 0,
        "Should receive depth events"
    );
    assert!(
        test_results.stats.total_ticker_events > 0,
        "Should receive ticker events"
    );
    assert!(
        test_results.snapshots.len() >= 2,
        "Should have at least 2 snapshots"
    );

    log::info!("\n=== All tests passed! ===");
}

#[tokio::test]
async fn test_get_klines() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    log::info!("=== Testing get_klines ===");

    let config = create_test_config().await;
    let mut provider = BinanceSpotMarketProvider::new(config).unwrap();
    provider.init().await.unwrap();

    // Wait for initial data
    tokio::time::sleep(Duration::from_secs(5)).await;

    let klines = provider
        .get_klines("BTCUSDT", KlineInterval::OneMinute, Some(10))
        .await
        .unwrap();

    log::info!("Retrieved {} klines", klines.len());
    assert!(!klines.is_empty(), "Should have klines data");

    // Dump to file
    let klines_json: Vec<_> = klines
        .iter()
        .map(|k| serde_json::to_value(k.as_ref()).unwrap())
        .collect();
    dump_json(&klines_json, "test_get_klines.json")
        .await
        .unwrap();

    log::info!("Klines data saved to test_get_klines.json");
    log::info!("Test passed!");
}

#[tokio::test]
async fn test_get_trades() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    log::info!("=== Testing get_trades ===");

    let config = create_test_config().await;
    let mut provider = BinanceSpotMarketProvider::new(config).unwrap();
    provider.init().await.unwrap();

    // Wait for initial data
    tokio::time::sleep(Duration::from_secs(5)).await;

    let trades = provider.get_trades("BTCUSDT", Some(20)).await.unwrap();

    log::info!("Retrieved {} trades", trades.len());
    assert!(!trades.is_empty(), "Should have trade data");

    // Dump to file
    let trades_json: Vec<_> = trades
        .iter()
        .map(|t| serde_json::to_value(t.as_ref()).unwrap())
        .collect();
    dump_json(&trades_json, "test_get_trades.json")
        .await
        .unwrap();

    log::info!("Trades data saved to test_get_trades.json");
    log::info!("Test passed!");
}

#[tokio::test]
async fn test_get_depth() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    log::info!("=== Testing get_depth ===");

    let config = create_test_config().await;
    let mut provider = BinanceSpotMarketProvider::new(config).unwrap();
    provider.init().await.unwrap();

    // Wait for initial depth snapshot
    tokio::time::sleep(Duration::from_secs(5)).await;

    let depth = provider.get_depth("BTCUSDT").await.unwrap();

    log::info!(
        "Retrieved depth - bids: {}, asks: {}",
        depth.bids.len(),
        depth.asks.len()
    );
    assert!(!depth.bids.is_empty(), "Should have bid data");
    assert!(!depth.asks.is_empty(), "Should have ask data");

    // Dump to file
    dump_json(depth.as_ref(), "test_get_depth.json")
        .await
        .unwrap();

    log::info!("Depth data saved to test_get_depth.json");
    log::info!("Test passed!");
}

#[tokio::test]
async fn test_get_ticker_24hr() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    log::info!("=== Testing get_ticker_24hr ===");

    let config = create_test_config().await;
    let mut provider = BinanceSpotMarketProvider::new(config).unwrap();
    provider.init().await.unwrap();

    // Wait for initial ticker data
    tokio::time::sleep(Duration::from_secs(5)).await;

    let ticker = provider.get_ticker_24hr("BTCUSDT").await.unwrap();

    log::info!(
        "Retrieved ticker - symbol: {}, last_price: {}",
        ticker.symbol,
        ticker.last_price
    );
    assert_eq!(ticker.symbol, "BTCUSDT", "Symbol should match");

    // Dump to file
    dump_json(ticker.as_ref(), "test_get_ticker_24hr.json")
        .await
        .unwrap();

    log::info!("Ticker data saved to test_get_ticker_24hr.json");
    log::info!("Test passed!");
}

#[tokio::test]
async fn test_get_exchange_info() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    log::info!("=== Testing get_exchange_info ===");

    let config = create_test_config().await;
    let mut provider = BinanceSpotMarketProvider::new(config).unwrap();
    provider.init().await.unwrap();

    let exchange_info = provider.get_exchange_info().await.unwrap();

    log::info!(
        "Retrieved exchange info - symbols count: {}",
        exchange_info.symbols.len()
    );
    assert!(!exchange_info.symbols.is_empty(), "Should have symbols");

    // Dump to file
    dump_json(&exchange_info, "test_get_exchange_info.json")
        .await
        .unwrap();

    log::info!("Exchange info saved to test_get_exchange_info.json");
    log::info!("Test passed!");
}

#[tokio::test]
async fn test_subscribe_events() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    log::info!("=== Testing subscribe events ===");

    let config = create_test_config().await;
    let mut provider = BinanceSpotMarketProvider::new(config).unwrap();
    provider.init().await.unwrap();

    let mut receiver = provider.subscribe();

    let mut kline_events = Vec::new();
    let mut trade_events = Vec::new();
    let mut depth_events = Vec::new();
    let mut ticker_events = Vec::new();

    let start = std::time::Instant::now();
    let duration = Duration::from_secs(10);

    log::info!("Collecting events for {} seconds...", duration.as_secs());

    while start.elapsed() < duration {
        tokio::select! {
            Ok(event) = receiver.recv() => {
                match event {
                    MarketEvent::Kline(kline) => {
                        log::info!("Kline: {} {:?} {}", kline.symbol, kline.interval, kline.close);
                        kline_events.push(serde_json::to_value(kline.as_ref()).unwrap());
                    }
                    MarketEvent::Trade(trade) => {
                        log::info!("Trade: {} price={} qty={}", trade.symbol, trade.price, trade.quantity);
                        trade_events.push(serde_json::to_value(trade.as_ref()).unwrap());
                    }
                    MarketEvent::Depth(depth) => {
                        log::info!("Depth: {} bids={} asks={}", depth.symbol, depth.bids.len(), depth.asks.len());
                        depth_events.push(serde_json::to_value(depth.as_ref()).unwrap());
                    }
                    MarketEvent::Ticker(ticker) => {
                        log::info!("Ticker: {} price={}", ticker.symbol, ticker.last_price);
                        ticker_events.push(serde_json::to_value(ticker.as_ref()).unwrap());
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }

    log::info!("\n=== Event Summary ===");
    log::info!("Kline events: {}", kline_events.len());
    log::info!("Trade events: {}", trade_events.len());
    log::info!("Depth events: {}", depth_events.len());
    log::info!("Ticker events: {}", ticker_events.len());

    // Dump events to files
    if !kline_events.is_empty() {
        dump_json(&kline_events, "test_subscribe_kline_events.json")
            .await
            .unwrap();
    }
    if !trade_events.is_empty() {
        dump_json(&trade_events, "test_subscribe_trade_events.json")
            .await
            .unwrap();
    }
    if !depth_events.is_empty() {
        dump_json(&depth_events, "test_subscribe_depth_events.json")
            .await
            .unwrap();
    }
    if !ticker_events.is_empty() {
        dump_json(&ticker_events, "test_subscribe_ticker_events.json")
            .await
            .unwrap();
    }

    // Assertions
    assert!(!kline_events.is_empty(), "Should receive kline events");
    assert!(!trade_events.is_empty(), "Should receive trade events");
    assert!(!depth_events.is_empty(), "Should receive depth events");
    assert!(!ticker_events.is_empty(), "Should receive ticker events");

    log::info!("\nTest passed!");
}
