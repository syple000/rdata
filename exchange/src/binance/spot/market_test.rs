use std::sync::Arc;

use env_logger::Env;
use tokio::sync::Mutex;

use super::super::consts::*;
use super::market::Market;
use super::market::MarketStream;
use super::requests::market::*;

#[tokio::test]
async fn test_market_get_klines() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();
    let market = Market::new(SPOT_BASE_URL.to_string(), None);
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
    json::dump(&resp.unwrap().klines, "klines.json").unwrap();
}

#[tokio::test]
async fn test_market_stream_depth_update() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    let mut market_stream = MarketStream::new(SPOT_WSS_URL.to_string() + "/stream", None);

    market_stream.subscribe_depth_update("BTCUSDT");

    let depth_updates = Arc::new(Mutex::new(Vec::<super::models::market::DepthUpdate>::new()));
    let depth_updates_clone = depth_updates.clone();
    market_stream.register_depth_update_callback(move |update| {
        let depth_updates_clone = depth_updates_clone.clone();
        Box::pin(async move {
            let mut depth_updates = depth_updates_clone.lock().await;
            depth_updates.push(update);
            Ok(())
        })
    });

    market_stream.init().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

    let depth_updates = depth_updates.lock().await;
    assert!(!depth_updates.is_empty());
    json::dump(&*depth_updates, "depth_updates.json").unwrap();
}
