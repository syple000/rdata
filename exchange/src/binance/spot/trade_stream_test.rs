use super::super::consts::*;
use super::trade_stream::TradeStream;
use crate::binance::spot::models::{OrderType, Side, TimeInForce};
use crate::binance::spot::requests::PlaceOrderRequest;
use crate::binance::spot::trade_api::TradeApi;
use env_logger::Env;
use rate_limiter::RateLimiter;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;

#[tokio::test]
async fn test_trade_stream_basic_functionality() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    // 创建 TradeStream 实例
    let mut trade_stream = TradeStream::new(
        TEST_SPOT_WSS_API_URL.to_string(),
        Some("socks5://127.0.0.1:10808".to_string()),
        None,
        TEST_SPOT_API_KEY.to_string(),
        TEST_SPOT_SECRET_KEY.to_string(),
    );

    let shutdown_token = trade_stream.init().await.unwrap();

    let rate_limiter = RateLimiter::new(Duration::from_secs(60), 1200); // 1200 requests per minute
    let trade_api = TradeApi::new(
        TEST_SPOT_BASE_URL.to_string(),
        Some(Arc::new(vec![rate_limiter])),
        TEST_SPOT_API_KEY.to_string(),
        TEST_SPOT_SECRET_KEY.to_string(),
    );
    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        r#type: OrderType::Limit,
        time_in_force: Some(TimeInForce::Gtc),
        quantity: Some(Decimal::from_str("0.0001").unwrap()),
        price: Some(Decimal::from_str("125000.00").unwrap()),
        new_client_order_id: Some(format!(
            "test_order_{}",
            time::get_current_milli_timestamp()
        )),
        stop_price: None,
        iceberg_qty: None,
    };
    let _ = trade_api.place_order(req).await;

    select! {
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
            shutdown_token.cancel();
        }
        _ = shutdown_token.cancelled() => {
            println!("Trade stream cancelled");
        }
    }
}
