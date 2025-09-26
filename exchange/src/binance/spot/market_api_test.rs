use super::super::consts::*;
use super::market_api::MarketApi;
use super::requests::market::*;
use env_logger::Env;

#[tokio::test]
async fn test_market_get_klines() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();
    let market = MarketApi::new(SPOT_BASE_URL.to_string(), None);
    let resp = market
        .get_klines(GetKlinesRequest {
            symbol: "BTCUSDT".to_string(),
            interval: KlineInterval::OneMinute,
            start_time: Some(time::get_current_milli_timestamp() - 10 * 60 * 1000),
            end_time: Some(time::get_current_milli_timestamp()),
            limit: Some(5),
        })
        .await;

    assert!(resp.is_ok());
    json::dump(&resp.unwrap(), "klines.json").unwrap();
}

#[tokio::test]
async fn test_market_get_agg_trades() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();
    let market = MarketApi::new(SPOT_BASE_URL.to_string(), None);

    // 测试获取最近的归集交易
    let resp = market
        .get_agg_trades(GetAggTradesRequest {
            symbol: "BTCUSDT".to_string(),
            from_id: None,
            start_time: Some(time::get_current_milli_timestamp() - 60 * 1000),
            end_time: Some(time::get_current_milli_timestamp()),
            limit: Some(10),
        })
        .await;

    assert!(resp.is_ok());
    let agg_trades = resp.unwrap();
    assert!(!agg_trades.is_empty());
    println!("Got {} agg trades", agg_trades.len());
    json::dump(&agg_trades, "agg_trades.json").unwrap();
}

#[tokio::test]
async fn test_market_get_depth() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();
    let market = MarketApi::new(SPOT_BASE_URL.to_string(), None);

    // 测试获取深度信息
    let resp = market
        .get_depth(GetDepthRequest {
            symbol: "BTCUSDT".to_string(),
            limit: Some(5000),
        })
        .await;

    assert!(resp.is_ok());
    let depth = resp.unwrap();
    assert!(!depth.bids.is_empty());
    assert!(!depth.asks.is_empty());
    println!(
        "Got depth with {} bids and {} asks",
        depth.bids.len(),
        depth.asks.len()
    );
    json::dump(&depth, "depth.json").unwrap();
}
