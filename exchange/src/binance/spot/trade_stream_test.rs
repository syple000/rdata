use super::super::consts::*;
use super::trade_stream::TradeStream;
use crate::binance::spot::models::{OrderType, Side, TimeInForce};
use crate::binance::spot::requests::{CancelOrderRequest, PlaceOrderRequest};
use env_logger::{Env, Target};
use rust_decimal::Decimal;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_trade_stream() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .target(Target::Stdout)
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] {}:{} {}",
                buf.timestamp(),                    // 时间戳
                record.level(),                     // 日志级别
                record.file().unwrap_or("unknown"), // 文件名
                record.line().unwrap_or(0),         // 行号
                record.args()                       // 原始日志内容
            )
        })
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

    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        r#type: OrderType::Market,
        time_in_force: None,
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: None,
        new_client_order_id: Some(format!(
            "test_order_{}",
            time::get_current_milli_timestamp()
        )),
        stop_price: None,
        iceberg_qty: None,
    };
    let _ = trade_stream.place_order(req).await;
    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        r#type: OrderType::Limit,
        time_in_force: Some(TimeInForce::Gtc),
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("50000").unwrap()),
        new_client_order_id: Some(format!(
            "test_order_{}",
            time::get_current_milli_timestamp()
        )),
        stop_price: None,
        iceberg_qty: None,
    };
    let resp = trade_stream.place_order(req).await.unwrap();
    let req = CancelOrderRequest {
        symbol: "BTCUSDT".to_string(),
        order_id: Some(resp.order_id),
        orig_client_order_id: None,
        new_client_order_id: None,
    };
    let _ = trade_stream.cancel_order(req).await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    shutdown_token.cancel();

    let outbound_account_positions = outbound_account_positions.lock().await;
    json::dump(
        &*outbound_account_positions,
        "outbound_account_positions.json",
    )
    .unwrap();
    let execution_reports = execution_reports.lock().await;
    json::dump(&*execution_reports, "execution_reports.json").unwrap();
}
