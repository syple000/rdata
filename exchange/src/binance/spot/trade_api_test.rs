use super::super::consts::*;
use super::models::{OrderType, Side, TimeInForce};
use super::requests::trade::*;
use super::trade_api::TradeApi;
use env_logger::Env;
use rate_limiter::RateLimiter;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

fn setup_test_trade_api() -> TradeApi {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    let rate_limiter = RateLimiter::new(Duration::from_secs(60), 1200); // 1200 requests per minute

    let mut api = TradeApi::new(
        TEST_SPOT_BASE_URL.to_string(),
        // None,
        Some("socks5://127.0.0.1:10808".to_string()),
        Some(Arc::new(vec![rate_limiter])),
        TEST_SPOT_API_KEY.to_string(),
        TEST_SPOT_SECRET_KEY.to_string(),
    );
    api.init().unwrap();
    api
}

#[tokio::test]
async fn test_place_limit_order_parameter_validation() {
    let trade_api = setup_test_trade_api();

    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        r#type: OrderType::Limit,
        time_in_force: None, // 缺少必需参数
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("30000.00").unwrap()),
        new_client_order_id: None,
        stop_price: None,
        iceberg_qty: None,
    };

    let result = trade_api.place_order(req).await;
    assert!(result.is_err());
    if let Err(e) = result {
        println!("Expected error for missing time_in_force: {:?}", e);
    }
}

#[tokio::test]
async fn test_place_market_order_parameter_validation() {
    let trade_api = setup_test_trade_api();

    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        r#type: OrderType::Market,
        time_in_force: None,
        quantity: None,
        price: None,
        new_client_order_id: None,
        stop_price: None,
        iceberg_qty: None,
    };

    let result = trade_api.place_order(req).await;
    assert!(result.is_err());
    if let Err(e) = result {
        println!("Expected error for missing quantity: {:?}", e);
    }
}

#[tokio::test]
async fn test_place_stop_loss_order_parameter_validation() {
    let trade_api = setup_test_trade_api();

    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Sell,
        r#type: OrderType::StopLoss,
        time_in_force: None,
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: None,
        new_client_order_id: None,
        stop_price: None,
        iceberg_qty: None,
    };

    let result = trade_api.place_order(req).await;
    assert!(result.is_err());
    if let Err(e) = result {
        println!("Expected error for missing stop_price: {:?}", e);
    }
}

#[tokio::test]
async fn test_place_stop_loss_limit_order_parameter_validation() {
    let trade_api = setup_test_trade_api();

    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Sell,
        r#type: OrderType::StopLossLimit,
        time_in_force: Some(TimeInForce::Gtc),
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: None,
        new_client_order_id: None,
        stop_price: Some(Decimal::from_str("29000.00").unwrap()),
        iceberg_qty: None,
    };

    let result = trade_api.place_order(req).await;
    assert!(result.is_err());
    if let Err(e) = result {
        println!("Expected error for missing price: {:?}", e);
    }
}

#[tokio::test]
async fn test_place_take_profit_order_parameter_validation() {
    let trade_api = setup_test_trade_api();

    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Sell,
        r#type: OrderType::TakeProfit,
        time_in_force: None,
        quantity: None,
        price: None,
        new_client_order_id: None,
        stop_price: Some(Decimal::from_str("35000.00").unwrap()),
        iceberg_qty: None,
    };

    let result = trade_api.place_order(req).await;
    assert!(result.is_err());
    if let Err(e) = result {
        println!("Expected error for missing quantity: {:?}", e);
    }
}

#[tokio::test]
async fn test_place_take_profit_limit_order_parameter_validation() {
    let trade_api = setup_test_trade_api();

    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Sell,
        r#type: OrderType::TakeProfitLimit,
        time_in_force: None,
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("35000.00").unwrap()),
        new_client_order_id: None,
        stop_price: Some(Decimal::from_str("35000.00").unwrap()),
        iceberg_qty: None,
    };

    let result = trade_api.place_order(req).await;
    assert!(result.is_err());
    if let Err(e) = result {
        println!("Expected error for missing time_in_force: {:?}", e);
    }
}

#[tokio::test]
async fn test_place_limit_maker_order_parameter_validation() {
    let trade_api = setup_test_trade_api();

    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        r#type: OrderType::LimitMaker,
        time_in_force: None,
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: None,
        new_client_order_id: None,
        stop_price: None,
        iceberg_qty: None,
    };

    let result = trade_api.place_order(req).await;
    assert!(result.is_err());
    if let Err(e) = result {
        println!("Expected error for missing price: {:?}", e);
    }
}

#[tokio::test]
async fn test_iceberg_qty_validation() {
    let trade_api = setup_test_trade_api();

    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        r#type: OrderType::Market,
        time_in_force: None,
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: None,
        new_client_order_id: None,
        stop_price: None,
        iceberg_qty: Some(Decimal::from_str("0.0001").unwrap()), // 无效的组合
    };

    let result = trade_api.place_order(req).await;
    assert!(result.is_err());
    if let Err(e) = result {
        println!("Expected error for iceberg_qty on MARKET order: {:?}", e);
    }
}

#[tokio::test]
async fn test_iceberg_qty_time_in_force_validation() {
    let trade_api = setup_test_trade_api();

    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        r#type: OrderType::Limit,
        time_in_force: Some(TimeInForce::Ioc),
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("30000.00").unwrap()),
        new_client_order_id: None,
        stop_price: None,
        iceberg_qty: Some(Decimal::from_str("0.0001").unwrap()),
    };

    let result = trade_api.place_order(req).await;
    assert!(result.is_err());
    if let Err(e) = result {
        println!("Expected error for iceberg_qty without GTC: {:?}", e);
    }
}

#[tokio::test]
async fn test_place_valid_limit_order() {
    let trade_api = setup_test_trade_api();

    let client_order_id = format!("test_order_{}", time::get_current_milli_timestamp());
    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        r#type: OrderType::Limit,
        time_in_force: Some(TimeInForce::Gtc),
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("25000.00").unwrap()), // 设置一个较低的价格以避免意外成交
        new_client_order_id: Some(client_order_id.clone()),
        stop_price: None,
        iceberg_qty: None,
    };

    let result = trade_api.place_order(req).await;

    match result {
        Ok(order) => {
            println!("Successfully placed limit order: {:?}", order);
            json::dump(&order, "placed_limit_order.json").unwrap();

            let cancel_req = CancelOrderRequest {
                symbol: "BTCUSDT".to_string(),
                order_id: Some(order.order_id),
                orig_client_order_id: Some(client_order_id.clone()),
                new_client_order_id: Some(client_order_id.clone()),
            };

            let cancel_result = trade_api.cancel_order(cancel_req).await;
            match cancel_result {
                Ok(cancelled_order) => {
                    println!("Successfully cancelled order: {:?}", cancelled_order);
                }
                Err(e) => {
                    panic!("Failed to cancel the placed order: {:?}", e);
                }
            }

            let get_order_req = GetOrderRequest {
                symbol: "BTCUSDT".to_string(),
                order_id: Some(order.order_id),
                orig_client_order_id: None,
            };
            let get_order_result = trade_api.get_order(get_order_req).await;
            match get_order_result {
                Ok(fetched_order) => {
                    println!("Fetched order: {:?}", fetched_order);
                    json::dump(&fetched_order, "fetched_order.json").unwrap();
                }
                Err(e) => {
                    panic!("Failed to fetch the placed order: {:?}", e);
                }
            }
            let get_orders_result = trade_api
                .get_all_orders(GetAllOrdersRequest {
                    symbol: "BTCUSDT".to_string(),
                    order_id: None,
                    start_time: Some(time::get_current_milli_timestamp() - 60 * 1000),
                    end_time: Some(time::get_current_milli_timestamp()),
                    limit: Some(10),
                })
                .await;
            match get_orders_result {
                Ok(orders) => {
                    println!(
                        "Got {} orders including the placed limit order",
                        orders.len()
                    );
                    json::dump(&orders, "all_orders_after_limit_order.json").unwrap();
                }
                Err(e) => {
                    panic!("Failed to get all orders: {:?}", e);
                }
            }
        }
        Err(e) => {
            panic!("Failed to place limit order: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_place_valid_market_order() {
    let trade_api = setup_test_trade_api();

    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        r#type: OrderType::Market,
        time_in_force: None,
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: None,
        new_client_order_id: Some(format!(
            "test_market_order_{}",
            time::get_current_milli_timestamp()
        )),
        stop_price: None,
        iceberg_qty: None,
    };

    let result = trade_api.place_order(req).await;

    match result {
        Ok(order) => {
            println!("Successfully placed market order: {:?}", order);
            json::dump(&order, "placed_market_order.json").unwrap();

            let get_order_req = GetOrderRequest {
                symbol: "BTCUSDT".to_string(),
                order_id: Some(order.order_id),
                orig_client_order_id: None,
            };

            let get_order_result = trade_api.get_order(get_order_req).await;
            match get_order_result {
                Ok(fetched_order) => {
                    println!("Fetched order: {:?}", fetched_order);
                    json::dump(&fetched_order, "fetched_market_order.json").unwrap();
                }
                Err(e) => {
                    panic!("Failed to fetch the placed market order: {:?}", e);
                }
            }
        }
        Err(e) => {
            panic!("Failed to place market order: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_cancel_order_parameter_validation() {
    let trade_api = setup_test_trade_api();

    let req = CancelOrderRequest {
        symbol: "BTCUSDT".to_string(),
        order_id: None,
        orig_client_order_id: None,
        new_client_order_id: None,
    };

    let result = trade_api.cancel_order(req).await;
    assert!(result.is_err());
    if let Err(e) = result {
        println!("Expected error for missing order identification: {:?}", e);
    }
}

#[tokio::test]
async fn test_cancel_order_by_order_id() {
    let trade_api = setup_test_trade_api();

    // 测试通过订单ID取消订单
    let req = CancelOrderRequest {
        symbol: "BTCUSDT".to_string(),
        order_id: Some(12345),
        orig_client_order_id: None,
        new_client_order_id: None,
    };

    let result = trade_api.cancel_order(req).await;

    match result {
        Ok(cancelled_order) => {
            panic!("Unexpectedly cancelled order: {:?}", cancelled_order);
        }
        Err(e) => {
            println!(
                "Failed to cancel order by ID (expected for non-existent order): {:?}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_cancel_order_by_client_order_id() {
    let trade_api = setup_test_trade_api();

    let req = CancelOrderRequest {
        symbol: "BTCUSDT".to_string(),
        order_id: None,
        orig_client_order_id: Some("non_existent_order".to_string()),
        new_client_order_id: None,
    };

    let result = trade_api.cancel_order(req).await;

    match result {
        Ok(cancelled_order) => {
            panic!("Unexpectedly cancelled order: {:?}", cancelled_order);
        }
        Err(e) => {
            println!(
                "Failed to cancel order by client ID (expected for non-existent order): {:?}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_get_open_orders() {
    let trade_api = setup_test_trade_api();

    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        r#type: OrderType::Limit,
        time_in_force: Some(TimeInForce::Gtc),
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("25000.00").unwrap()), // 设置一个较低的价格以避免意外成交
        new_client_order_id: Some(format!(
            "test_order_{}",
            time::get_current_milli_timestamp()
        )),
        stop_price: None,
        iceberg_qty: None,
    };

    let _ = trade_api.place_order(req).await;

    let req = GetOpenOrdersRequest {
        symbol: Some("BTCUSDT".to_string()),
    };

    let result = trade_api.get_open_orders(req).await;

    match result {
        Ok(orders) => {
            println!("Got {} open orders", orders.len());
            json::dump(&orders, "open_orders.json").unwrap();
        }
        Err(e) => {
            panic!("Failed to get open orders: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_get_all_orders() {
    let trade_api = setup_test_trade_api();

    let req = GetAllOrdersRequest {
        symbol: "BTCUSDT".to_string(),
        order_id: None,
        start_time: Some(time::get_current_milli_timestamp() - 24 * 60 * 60 * 1000), // 24小时前
        end_time: Some(time::get_current_milli_timestamp()),
        limit: Some(4),
    };

    let result = trade_api.get_all_orders(req).await;

    match result {
        Ok(orders) => {
            println!("Got {} orders in history", orders.len());
            json::dump(&orders, "all_orders.json").unwrap();
        }
        Err(e) => {
            panic!("Failed to get all orders: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_get_trades() {
    let trade_api = setup_test_trade_api();

    let req = GetTradesRequest {
        symbol: "BTCUSDT".to_string(),
        order_id: None,
        from_id: None,
        start_time: Some(time::get_current_milli_timestamp() - 24 * 60 * 60 * 1000), // 24小时前
        end_time: Some(time::get_current_milli_timestamp()),
        limit: Some(3),
    };

    let result = trade_api.get_trades(req).await;

    match result {
        Ok(trades) => {
            println!("Got {} trades for BTCUSDT", trades.len());
            json::dump(&trades, "trades.json").unwrap();
        }
        Err(e) => {
            panic!("Failed to get trades for BTCUSDT: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_get_account() {
    let trade_api = setup_test_trade_api();

    let req = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Sell,
        r#type: OrderType::Limit,
        time_in_force: Some(TimeInForce::Gtc),
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("200000.00").unwrap()),
        new_client_order_id: Some(format!(
            "test_order_{}",
            time::get_current_milli_timestamp()
        )),
        stop_price: None,
        iceberg_qty: None,
    };

    let resp = trade_api.place_order(req).await.unwrap();

    let req = GetAccountRequest {};

    let result = trade_api.get_account(req).await;

    match result {
        Ok(account) => {
            println!("Got account info: {:?}", account);
            json::dump(&account, "account_info.json").unwrap();
        }
        Err(e) => {
            panic!("Failed to get account info: {:?}", e);
        }
    }

    trade_api
        .cancel_order(CancelOrderRequest {
            symbol: "BTCUSDT".to_string(),
            order_id: Some(resp.order_id),
            orig_client_order_id: None,
            new_client_order_id: None,
        })
        .await
        .unwrap();
}
