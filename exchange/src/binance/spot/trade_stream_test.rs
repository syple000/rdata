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
use tokio::sync::Mutex;

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
    let outbound_account_positions = Arc::new(Mutex::new(vec![]));
    let outbound_account_positions_clone = outbound_account_positions.clone();
    let execution_reports = Arc::new(Mutex::new(vec![]));
    let execution_reports_clone = execution_reports.clone();
    trade_stream.register_outbound_account_position_callback(move |update| {
        let outbound_account_positions = outbound_account_positions_clone.clone();
        Box::pin(async move {
            let mut positions = outbound_account_positions.lock().await;
            positions.push(update);
            Ok(())
        })
    });
    trade_stream.register_execution_report_callback(move |report| {
        let execution_reports = execution_reports_clone.clone();
        Box::pin(async move {
            let mut reports = execution_reports.lock().await;
            reports.push(report);
            Ok(())
        })
    });

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

    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    shutdown_token.cancel();
    trade_stream.close().await.unwrap();

    let outbound_account_positions = outbound_account_positions.lock().await;
    json::dump(
        &*outbound_account_positions,
        "outbound_account_positions.json",
    )
    .unwrap();
    let execution_reports = execution_reports.lock().await;
    json::dump(&*execution_reports, "execution_reports.json").unwrap();
}
