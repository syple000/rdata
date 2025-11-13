use crate::{
    config::Config,
    models::{
        CancelOrderRequest, GetAllOrdersRequest, GetOpenOrdersRequest, GetOrderRequest,
        GetUserTradesRequest, OrderSide, OrderStatus, OrderType, PlaceOrderRequest, TimeInForce,
    },
    trade_provider::{binance_spot_trade_provider::BinanceSpotTradeProvider, TradeProvider},
};
use env_logger::Env;
use log::info;
use rust_decimal::Decimal;
use std::{str::FromStr, sync::Arc, time::Duration};
use tempfile::NamedTempFile;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_binance_spot_trade_provider() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    let config_content = r#"
    {
        "binance_spot": {
            "api_base_url": "https://testnet.binance.vision",
            "stream_api_base_url": "wss://ws-api.testnet.binance.vision/ws-api/v3",
            "api_key": "GMh8WTFiTiRPpbt1EFwYaDEunKN9gJy9qgRyYF8irvSYCdgjYcIaACDeyfKFOMcq",
            "secret_key": "NgIxnbabjf6cTnPYZpyVDAP7UoVNm3wzhJcLh89FYWSA5SkXJlCZD0yDCQcA4R33",
            "api_rate_limits": [[1000, 10], [60000, 500]],
            "stream_api_rate_limits": [[1000, 10]],
            "order_event_channel_capacity": 5000,
            "user_trade_event_channel_capacity": 5000,
            "account_event_channel_capacity": 5000,
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

    let config = Arc::new(Config::from_json(config_path).unwrap());

    let mut provider = BinanceSpotTradeProvider::new(config).unwrap();
    provider.init().await.unwrap();

    info!("Initialized BinanceSpotTradeProvider for testing.");

    let orders_data = Arc::new(Mutex::new(Vec::new()));
    let user_trades_data = Arc::new(Mutex::new(Vec::new()));
    let account_updates_data = Arc::new(Mutex::new(Vec::new()));

    let orders_data_clone = orders_data.clone();
    let mut order_rx = provider.subscribe_order();
    tokio::spawn(async move {
        while let Ok(order) = order_rx.recv().await {
            orders_data_clone.lock().await.push(order);
        }
    });

    let user_trades_data_clone = user_trades_data.clone();
    let mut user_trade_rx = provider.subscribe_user_trade();
    tokio::spawn(async move {
        while let Ok(trade) = user_trade_rx.recv().await {
            user_trades_data_clone.lock().await.push(trade);
        }
    });

    let account_updates_data_clone = account_updates_data.clone();
    let mut account_rx = provider.subscribe_account_update();
    tokio::spawn(async move {
        while let Ok(update) = account_rx.recv().await {
            account_updates_data_clone.lock().await.push(update);
        }
    });

    let account = provider.get_account().await.unwrap();
    json::dump(&account, "init_account.json").unwrap();

    // Test 1: 买单成功
    info!("buy order, expect success");
    let buy_limit_order = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: OrderSide::Buy,
        r#type: OrderType::Limit,
        time_in_force: Some(TimeInForce::Gtc),
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("140000.0").unwrap()),
        client_order_id: format!("test_buy_limit_{}", time::get_current_milli_timestamp()),
        stop_price: None,
        iceberg_qty: None,
    };

    let order = provider.place_order(buy_limit_order.clone()).await.unwrap();
    assert_eq!(order.order_status, OrderStatus::New);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let order = provider
        .get_order(GetOrderRequest {
            symbol: order.symbol.to_string(),
            order_id: Some(order.order_id.to_string()),
            client_order_id: None,
        })
        .await
        .unwrap();
    assert_eq!(order.order_status, OrderStatus::Filled);

    // Test 2: 买单未成撤单
    info!("buy order then cancel, expect cancel success");
    let buy_limit_order = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: OrderSide::Buy,
        r#type: OrderType::Limit,
        time_in_force: Some(TimeInForce::Gtc),
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("100000.0").unwrap()),
        client_order_id: format!("test_buy_limit_{}", time::get_current_milli_timestamp()),
        stop_price: None,
        iceberg_qty: None,
    };

    let order = provider.place_order(buy_limit_order.clone()).await.unwrap();
    assert_eq!(order.order_status, OrderStatus::New);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let open_orders_req = GetOpenOrdersRequest {
        symbol: Some("BTCUSDT".to_string()),
    };
    let open_orders = provider.get_open_orders(open_orders_req).await.unwrap();
    json::dump(&open_orders, "open_orders.json").unwrap();

    let order = provider
        .get_order(GetOrderRequest {
            symbol: order.symbol.to_string(),
            order_id: Some(order.order_id.to_string()),
            client_order_id: None,
        })
        .await
        .unwrap();
    assert_eq!(order.order_status, OrderStatus::New);
    provider
        .cancel_order(CancelOrderRequest {
            symbol: order.symbol.to_string(),
            order_id: Some(order.order_id.to_string()),
            client_order_id: order.client_order_id,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let order = provider
        .get_order(GetOrderRequest {
            symbol: order.symbol.to_string(),
            order_id: Some(order.order_id.to_string()),
            client_order_id: None,
        })
        .await
        .unwrap();
    assert_eq!(order.order_status, OrderStatus::Canceled);

    // Test 3: 卖单成功
    info!("sell order, expect success");
    let sell_market_order = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: OrderSide::Sell,
        r#type: OrderType::Market,
        time_in_force: None,
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: None,
        client_order_id: format!("test_sell_market_{}", time::get_current_milli_timestamp()),
        stop_price: None,
        iceberg_qty: None,
    };
    let order = provider
        .place_order(sell_market_order.clone())
        .await
        .unwrap();
    assert_eq!(order.order_status, OrderStatus::New);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let order = provider
        .get_order(GetOrderRequest {
            symbol: order.symbol.to_string(),
            order_id: Some(order.order_id.to_string()),
            client_order_id: None,
        })
        .await
        .unwrap();
    assert_eq!(order.order_status, OrderStatus::Filled);

    // Test 4: 卖单失败数量不够
    info!("Test 4: Place sell order with insufficient quantity (should fail)");
    let large_sell_order = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: OrderSide::Sell,
        r#type: OrderType::Market,
        time_in_force: None,
        quantity: Some(Decimal::from_str("80.0").unwrap()),
        price: None,
        client_order_id: format!("test_sell_fail_{}", time::get_current_milli_timestamp()),
        stop_price: None,
        iceberg_qty: None,
    };
    provider
        .place_order(large_sell_order.clone())
        .await
        .expect_err("expect err: account has insufficient balance");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let all_orders_req = GetAllOrdersRequest {
        symbol: "BTCUSDT".to_string(),
        from_id: None,
        start_time: Some(time::get_current_milli_timestamp() - 120 * 1000),
        end_time: None,
        limit: Some(100),
    };
    let all_orders = provider.get_all_orders(all_orders_req).await.unwrap();
    assert!(!all_orders.is_empty());
    json::dump(&all_orders, "all_orders.json").unwrap();

    let user_trades_req = GetUserTradesRequest {
        symbol: "BTCUSDT".to_string(),
        order_id: None,
        from_id: None,
        start_time: Some(time::get_current_milli_timestamp() - 120 * 1000),
        end_time: None,
        limit: Some(100),
    };
    let user_trades = provider.get_user_trades(user_trades_req).await.unwrap();
    assert!(!user_trades.is_empty());
    json::dump(&user_trades, "user_trades.json").unwrap();

    let account = provider.get_account().await.unwrap();
    json::dump(&account, "final_account.json").unwrap();

    let orders_collected = orders_data.lock().await;
    assert!(!orders_collected.is_empty());
    json::dump(&*orders_collected, "collected_orders.json").unwrap();

    let user_trades_collected = user_trades_data.lock().await;
    assert!(!user_trades_collected.is_empty());
    json::dump(&*user_trades_collected, "collected_user_trades.json").unwrap();

    let account_updates_collected = account_updates_data.lock().await;
    assert!(!account_updates_collected.is_empty());
    json::dump(
        &*account_updates_collected,
        "collected_account_updates.json",
    )
    .unwrap();
}
