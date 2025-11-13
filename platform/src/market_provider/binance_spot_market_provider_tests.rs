use crate::{
    config::Config,
    market_provider::{binance_spot_market_provider::BinanceSpotMarketProvider, MarketProvider},
    models::{
        DepthData, GetDepthRequest, GetKlinesRequest, GetTicker24hrRequest, GetTradesRequest,
        KlineInterval,
    },
};
use env_logger::Env;
use json::dump;
use log::info;
use std::{sync::Arc, time::Duration};
use tempfile::NamedTempFile;
use tokio::{sync::Mutex, time::sleep};

#[tokio::test]
async fn test_binance_spot_market_provider() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    // Create temporary config file
    let config_content = r#"
    {
        "binance_spot": {
            "api_base_url": "https://api.binance.com",
            "stream_base_url": "wss://stream.binance.com:9443/stream",
            "subscribed_symbols": ["BTCUSDT", "ETHUSDT"],
            "subscribed_kline_intervals": ["1m", "5m"],
            "api_rate_limits": [[1000, 10], [60000, 500]],
            "stream_rate_limits": [[1000, 10]],
            "kline_event_channel_capacity": 5000,
            "trade_event_channel_capacity": 5000,
            "depth_event_channel_capacity": 5000,
            "ticker_event_channel_capacity": 5000,
            "depth_cache_channel_capacity": 5000,
            "stream_reconnect_interval_milli_secs": 3000,
            "api_timeout_milli_secs": 30000
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
    let config = Arc::new(Config::from_json(config_path).unwrap());

    // Initialize provider
    let mut provider = BinanceSpotMarketProvider::new(config).unwrap();
    provider.init().await.unwrap();

    info!("Initialized BinanceSpotMarketProvider for testing.");

    // Test get_klines
    let klines_req = GetKlinesRequest {
        symbol: "BTCUSDT".to_string(),
        interval: KlineInterval::OneMinute,
        start_time: None,
        end_time: None,
        limit: Some(10),
    };
    let klines = provider.get_klines(klines_req).await.unwrap();
    assert!(!klines.is_empty());
    dump(&klines, "test_klines.json").unwrap();

    // Test get_trades
    let trades_req = GetTradesRequest {
        symbol: "BTCUSDT".to_string(),
        from_id: None,
        start_time: None,
        end_time: None,
        limit: Some(10),
    };
    let trades = provider.get_trades(trades_req).await.unwrap();
    assert!(!trades.is_empty());
    dump(&trades, "test_trades.json").unwrap();

    // Test get_depth
    let depth_req = GetDepthRequest {
        symbol: "BTCUSDT".to_string(),
        limit: Some(100),
    };
    let depth = provider.get_depth(depth_req).await.unwrap();
    assert!(!depth.bids.is_empty());
    assert!(!depth.asks.is_empty());
    dump(&depth, "test_depth.json").unwrap();

    // Test get_ticker_24hr
    let ticker_req = GetTicker24hrRequest {
        symbol: None,
        symbols: Some(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]),
    };
    let tickers = provider.get_ticker_24hr(ticker_req).await.unwrap();
    assert!(!tickers.is_empty());
    dump(&tickers, "test_tickers.json").unwrap();

    // Test get_exchange_info
    let exchange_info = provider.get_exchange_info().await.unwrap();
    dump(&exchange_info, "test_exchange_info.json").unwrap();

    info!("Completed API method tests.");

    // Test subscriptions
    let klines_data = Arc::new(Mutex::new(Vec::new()));
    let trades_data = Arc::new(Mutex::new(Vec::new()));
    let depths_data = Arc::new(Mutex::new(Vec::new()));
    let tickers_data = Arc::new(Mutex::new(Vec::new()));

    let klines_data_clone = klines_data.clone();
    let mut kline_rx = provider.subscribe_kline();
    tokio::spawn(async move {
        while let Ok(kline) = kline_rx.recv().await {
            klines_data_clone.lock().await.push(kline);
        }
    });

    let trades_data_clone = trades_data.clone();
    let mut trade_rx = provider.subscribe_trade();
    tokio::spawn(async move {
        while let Ok(trade) = trade_rx.recv().await {
            trades_data_clone.lock().await.push(trade);
        }
    });

    let depths_data_clone = depths_data.clone();
    let mut depth_rx = provider.subscribe_depth();
    tokio::spawn(async move {
        while let Ok(depth) = depth_rx.recv().await {
            let depth = DepthData {
                symbol: depth.symbol,
                bids: depth.bids.iter().take(10).cloned().collect(),
                asks: depth.asks.iter().take(10).cloned().collect(),
                timestamp: depth.timestamp,
            };
            depths_data_clone.lock().await.push(depth);
        }
    });

    let tickers_data_clone = tickers_data.clone();
    let mut ticker_rx = provider.subscribe_ticker();
    tokio::spawn(async move {
        while let Ok(ticker) = ticker_rx.recv().await {
            tickers_data_clone.lock().await.push(ticker);
        }
    });

    // Wait for 90 seconds
    sleep(Duration::from_secs(90)).await;

    info!("Finished collecting subscription data.");

    // Check and dump
    let klines_collected = klines_data.lock().await;
    assert!(!klines_collected.is_empty());
    dump(&*klines_collected, "collected_klines.json").unwrap();

    let trades_collected = trades_data.lock().await;
    assert!(!trades_collected.is_empty());
    dump(&*trades_collected, "collected_trades.json").unwrap();

    let depths_collected = depths_data.lock().await;
    assert!(!depths_collected.is_empty());
    dump(&*depths_collected, "collected_depths.json").unwrap();

    let tickers_collected = tickers_data.lock().await;
    assert!(!tickers_collected.is_empty());
    dump(&*tickers_collected, "collected_tickers.json").unwrap();
}
