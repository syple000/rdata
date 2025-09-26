use std::sync::Arc;

use env_logger::Env;
use tokio::sync::Mutex;

use super::super::consts::*;
use super::market_stream::MarketStream;

#[tokio::test]
async fn test_market_stream_depth_update() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    let mut market_stream = MarketStream::new(
        SPOT_WSS_URL.to_string() + "/stream",
        //None,
        Some("socks5://127.0.0.1:10808".to_string()),
        None,
    );

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

    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    let depth_updates = depth_updates.lock().await;
    assert!(!depth_updates.is_empty());
    json::dump(&*depth_updates, "depth_updates.json").unwrap();
}

#[tokio::test]
async fn test_market_stream_agg_trade() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    let mut market_stream = MarketStream::new(
        SPOT_WSS_URL.to_string() + "/stream",
        //None,
        Some("socks5://127.0.0.1:10808".to_string()),
        None,
    );

    market_stream.subscribe_agg_trade("BTCUSDT");

    let agg_trades = Arc::new(Mutex::new(Vec::<super::models::market::AggTrade>::new()));
    let agg_trades_clone = agg_trades.clone();
    market_stream.register_agg_trade_callback(move |trade| {
        let agg_trades_clone = agg_trades_clone.clone();
        Box::pin(async move {
            let mut agg_trades = agg_trades_clone.lock().await;
            agg_trades.push(trade);
            Ok(())
        })
    });

    market_stream.init().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    let agg_trades = agg_trades.lock().await;
    assert!(!agg_trades.is_empty());
    println!("Received {} agg trades", agg_trades.len());
    json::dump(&*agg_trades, "agg_trades_stream.json").unwrap();
}
