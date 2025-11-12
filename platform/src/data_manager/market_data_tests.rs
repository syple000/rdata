use crate::{
    config::Config,
    data_manager::market_data::MarketData,
    data_manager::MarketDataManager,
    market_provider::{binance_spot_market_provider::BinanceSpotMarketProvider, MarketProvider},
    models::{DepthData, KlineData, KlineInterval, MarketType, Ticker24hr, Trade},
};
use env_logger::Env;
use json::dump;
use log::info;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tempfile::NamedTempFile;
use tokio::time::sleep;

#[tokio::test]
async fn test_market_data_initialization() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    let config_content = r#"
    {
        "data_manager": {
            "market_types": ["binance_spot"],
            "binance_spot": {
                "cache_capacity": 100
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
    let config = Config::from_json(config_path).unwrap();

    let mut provider = BinanceSpotMarketProvider::new(Arc::new(config.clone())).unwrap();
    provider.init().await.unwrap();
    let mut market_providers: HashMap<MarketType, Arc<dyn MarketProvider>> = HashMap::new();
    market_providers.insert(MarketType::BinanceSpot, Arc::new(provider));
    let market_data =
        Arc::new(MarketData::new(config.clone(), Arc::new(market_providers)).unwrap());

    market_data.init().await.unwrap();

    info!("MarketData init() completed. Waiting for initial data collection...");

    let old_klines_btc_1m: Vec<KlineData> = market_data
        .get_klines(
            &MarketType::BinanceSpot,
            &"BTCUSDT".to_string(),
            &KlineInterval::OneMinute,
            Some(1),
        )
        .await
        .unwrap();
    assert!(old_klines_btc_1m.len() == 1);
    let old_klines_eth_5m: Vec<KlineData> = market_data
        .get_klines(
            &MarketType::BinanceSpot,
            &"ETHUSDT".to_string(),
            &KlineInterval::FiveMinutes,
            Some(1),
        )
        .await
        .unwrap();
    assert!(old_klines_eth_5m.len() == 1);
    let old_trade_btc: Vec<Trade> = market_data
        .get_trades(&MarketType::BinanceSpot, &"BTCUSDT".to_string(), Some(1))
        .await
        .unwrap();
    assert!(old_trade_btc.len() == 1);
    let old_trade_eth: Vec<Trade> = market_data
        .get_trades(&MarketType::BinanceSpot, &"ETHUSDT".to_string(), Some(1))
        .await
        .unwrap();
    assert!(old_trade_eth.len() == 1);
    let old_btc_depth: DepthData = market_data
        .get_depth(&MarketType::BinanceSpot, &"BTCUSDT".to_string())
        .await
        .unwrap()
        .unwrap();
    let old_eth_depth: DepthData = market_data
        .get_depth(&MarketType::BinanceSpot, &"ETHUSDT".to_string())
        .await
        .unwrap()
        .unwrap();
    let old_btc_ticker: Ticker24hr = market_data
        .get_ticker(&MarketType::BinanceSpot, &"BTCUSDT".to_string())
        .await
        .unwrap()
        .unwrap();
    let old_eth_ticker: Ticker24hr = market_data
        .get_ticker(&MarketType::BinanceSpot, &"ETHUSDT".to_string())
        .await
        .unwrap()
        .unwrap();

    sleep(Duration::from_secs(120)).await;

    let klines_btc_1m: Vec<KlineData> = market_data
        .get_klines(
            &MarketType::BinanceSpot,
            &"BTCUSDT".to_string(),
            &KlineInterval::OneMinute,
            None,
        )
        .await
        .unwrap();
    assert!(klines_btc_1m.len() == 100);
    let open_times: Vec<u64> = klines_btc_1m[95..=99].iter().map(|e| e.open_time).collect();
    assert!(open_times.contains(&old_klines_btc_1m[0].open_time));
    let mut pre_open_time = None;
    klines_btc_1m.iter().for_each(|kline| {
        if let Some(pre) = pre_open_time {
            assert!(kline.open_time == pre + 60_000);
        }
        pre_open_time = Some(kline.open_time);
    });

    dump(&klines_btc_1m, "market_data_klines_btc_1m.json").unwrap();

    let klines_eth_5m: Vec<KlineData> = market_data
        .get_klines(
            &MarketType::BinanceSpot,
            &"ETHUSDT".to_string(),
            &KlineInterval::FiveMinutes,
            None,
        )
        .await
        .unwrap();
    assert!(klines_eth_5m.len() == 100);
    let open_times: Vec<u64> = klines_eth_5m[95..=99].iter().map(|e| e.open_time).collect();
    assert!(open_times.contains(&old_klines_eth_5m[0].open_time));
    let mut pre_open_time = None;
    klines_eth_5m.iter().for_each(|kline| {
        if let Some(pre) = pre_open_time {
            assert!(kline.open_time == pre + 300_000);
        }
        pre_open_time = Some(kline.open_time);
    });

    dump(&klines_eth_5m, "market_data_klines_eth_5m.json").unwrap();

    let trades_btc: Vec<Trade> = market_data
        .get_trades(&MarketType::BinanceSpot, &"BTCUSDT".to_string(), None)
        .await
        .unwrap();
    assert!(trades_btc.len() == 100);
    let lag = trades_btc.last().unwrap().timestamp - old_trade_btc[0].timestamp;
    assert!(lag > 100000 && lag < 140000);
    let mut pre_timestamp = None;
    trades_btc.iter().for_each(|trade| {
        if let Some(pre) = pre_timestamp {
            assert!(trade.timestamp >= pre);
        }
        pre_timestamp = Some(trade.timestamp);
    });
    dump(&trades_btc, "market_data_trades_btc.json").unwrap();

    let trades_eth = market_data
        .get_trades(&MarketType::BinanceSpot, &"ETHUSDT".to_string(), None)
        .await
        .unwrap();
    assert!(trades_eth.len() == 100);
    let lag = trades_eth.last().unwrap().timestamp - old_trade_eth[0].timestamp;
    assert!(lag > 100000 && lag < 140000);
    let mut pre_timestamp = None;
    trades_eth.iter().for_each(|trade| {
        if let Some(pre) = pre_timestamp {
            assert!(trade.timestamp >= pre);
        }
        pre_timestamp = Some(trade.timestamp);
    });
    dump(&trades_eth, "market_data_trades_eth.json").unwrap();

    let depth_btc: DepthData = market_data
        .get_depth(&MarketType::BinanceSpot, &"BTCUSDT".to_string())
        .await
        .unwrap()
        .unwrap();
    let lag = depth_btc.timestamp - old_btc_depth.timestamp;
    assert!(lag > 100000 && lag < 140000);
    let depth_btc_dump = DepthData {
        symbol: depth_btc.symbol.clone(),
        bids: depth_btc.bids.iter().take(20).cloned().collect(),
        asks: depth_btc.asks.iter().take(20).cloned().collect(),
        timestamp: depth_btc.timestamp,
    };
    dump(&depth_btc_dump, "market_data_depth_btc.json").unwrap();

    let depth_eth = market_data
        .get_depth(&MarketType::BinanceSpot, &"ETHUSDT".to_string())
        .await
        .unwrap()
        .unwrap();
    let lag = depth_eth.timestamp - old_eth_depth.timestamp;
    assert!(lag > 100000 && lag < 140000);
    let depth_eth_dump = DepthData {
        symbol: depth_eth.symbol.clone(),
        bids: depth_eth.bids.iter().take(20).cloned().collect(),
        asks: depth_eth.asks.iter().take(20).cloned().collect(),
        timestamp: depth_eth.timestamp,
    };
    dump(&depth_eth_dump, "market_data_depth_eth.json").unwrap();

    let ticker_btc = market_data
        .get_ticker(&MarketType::BinanceSpot, &"BTCUSDT".to_string())
        .await
        .unwrap()
        .unwrap();
    let lag = ticker_btc.close_time - old_btc_ticker.close_time;
    assert!(lag > 100000 && lag < 140000);
    dump(&ticker_btc, "market_data_ticker_btc.json").unwrap();

    let ticker_eth = market_data
        .get_ticker(&MarketType::BinanceSpot, &"ETHUSDT".to_string())
        .await
        .unwrap()
        .unwrap();
    let lag = ticker_eth.close_time - old_eth_ticker.close_time;
    assert!(lag > 100000 && lag < 140000);
    dump(&ticker_eth, "market_data_ticker_eth.json").unwrap();
}
