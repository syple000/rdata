use std::sync::Arc;
use std::time::Duration;

use super::super::consts::*;
use super::market_api::MarketApi;
use super::requests::market::*;
use env_logger::Env;
use rate_limiter::RateLimiter;

fn setup_test_market_api() -> MarketApi {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    let rate_limiter = RateLimiter::new(Duration::from_secs(60), 1200); // 1200 requests per minute

    let mut api = MarketApi::new(
        TEST_SPOT_BASE_URL.to_string(),
        Some("socks5://127.0.0.1:10808".to_string()),
        Some(Arc::new(vec![rate_limiter])),
    );
    api.init().unwrap();
    api
}

#[tokio::test]
async fn test_market_get_klines() {
    let market = setup_test_market_api();
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
    let market = setup_test_market_api();

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
    let market = setup_test_market_api();

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

#[tokio::test]
async fn test_market_get_exchange_info() {
    let market = setup_test_market_api();

    // 测试获取单个交易对的交易规范信息
    let resp = market
        .get_exchange_info(GetExchangeInfoRequest {
            symbol: None,
            symbols: None,
        })
        .await;

    assert!(resp.is_ok());
    let exchange_info = resp.unwrap();
    assert!(!exchange_info.symbols.is_empty());
    println!(
        "Got exchange info with {} symbols",
        exchange_info.symbols.len()
    );
    json::dump(&exchange_info, "exchange_info.json").unwrap();
}

#[tokio::test]
async fn test_market_get_ticker_24hr_single() {
    let market = setup_test_market_api();

    // 测试获取单个交易对的24小时价格变动情况
    let resp = market
        .get_ticker_24hr(GetTicker24hrRequest {
            symbol: Some("BTCUSDT".to_string()),
            symbols: None,
        })
        .await;

    assert!(resp.is_ok());
    let ticker = resp.unwrap();
    assert!(!ticker.is_empty());
    assert_eq!(ticker.len(), 1);
    assert_eq!(ticker[0].symbol, "BTCUSDT");
    println!(
        "Got ticker for {}: last_price={}",
        ticker[0].symbol, ticker[0].last_price
    );
    json::dump(&ticker, "ticker_24hr_single.json").unwrap();
}

#[tokio::test]
async fn test_market_get_ticker_24hr_multiple() {
    let market = setup_test_market_api();

    // 测试获取多个交易对的24小时价格变动情况
    let resp = market
        .get_ticker_24hr(GetTicker24hrRequest {
            symbol: None,
            symbols: Some(vec![
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "BNBUSDT".to_string(),
            ]),
        })
        .await;

    assert!(resp.is_ok());
    let tickers = resp.unwrap();
    assert_eq!(tickers.len(), 3);
    println!("Got {} tickers", tickers.len());
    for ticker in &tickers {
        println!(
            "  {}: last_price={}, volume={}, price_change_percent={}%",
            ticker.symbol, ticker.last_price, ticker.volume, ticker.price_change_percent
        );
    }
    json::dump(&tickers, "ticker_24hr_multiple.json").unwrap();
}
