use crate::{
    config::Config,
    data_manager::{trade_data::TradeData, TradeDataManager},
    models::{
        Account, Balance, CancelOrderRequest, GetOpenOrdersRequest, GetUserTradesRequest,
        MarketType, Order, OrderSide, OrderType, PlaceOrderRequest, TimeInForce, UserTrade,
    },
    trade_provider::{binance_spot_trade_provider::BinanceSpotTradeProvider, TradeProvider},
};
use env_logger::Env;
use json::dump;
use log::info;
use rust_decimal::Decimal;
use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_trade_data_with_binance_operations_and_persistence() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    let db_file = NamedTempFile::new().unwrap();
    let db_path = db_file.path().to_str().unwrap();

    let config_content = format!(
        r#"
    {{
        "data_manager": {{
            "market_types": ["binance_spot"],
            "binance_spot": {{
                "symbols": ["BTCUSDT"]
            }},
            "db_path": "{}",
            "refresh_interval_secs": 5
        }},
        "binance": {{
            "spot": {{
                "api_base_url": "https://testnet.binance.vision",
                "stream_api_base_url": "wss://ws-api.testnet.binance.vision/ws-api/v3",
                "api_key": "GMh8WTFiTiRPpbt1EFwYaDEunKN9gJy9qgRyYF8irvSYCdgjYcIaACDeyfKFOMcq",
                "secret_key": "NgIxnbabjf6cTnPYZpyVDAP7UoVNm3wzhJcLh89FYWSA5SkXJlCZD0yDCQcA4R33",
                "api_rate_limits": [[1000, 500], [60000, 5000]],
                "stream_rate_limits": [[1000, 500]],
                "order_event_channel_capacity": 5000,
                "user_trade_event_channel_capacity": 5000,
                "account_event_channel_capacity": 5000,
                "stream_reconnect_interval_milli_secs": 3000,
                "api_timeout_milli_secs": 30000
            }}
        }},
        "proxy": {{
            "url": "socks5://127.0.0.1:10808"
        }}
    }}
    "#,
        db_path
    );

    let mut config_file = NamedTempFile::new().unwrap();
    std::io::Write::write_all(&mut config_file, config_content.as_bytes()).unwrap();
    let config_path = config_file.path().to_str().unwrap();

    // Load config
    let config = Config::from_json(config_path).unwrap();

    info!("Creating BinanceSpotTradeProvider...");

    // Initialize Binance trade provider
    let mut provider = BinanceSpotTradeProvider::new(Arc::new(config.clone())).unwrap();
    provider.init().await.unwrap();
    let provider = Arc::new(provider);

    // Create trade providers map
    let mut trade_providers: HashMap<MarketType, Arc<dyn TradeProvider>> = HashMap::new();
    trade_providers.insert(MarketType::BinanceSpot, provider.clone());

    // Initialize TradeData
    let trade_data = TradeData::new(config.clone(), Arc::new(trade_providers)).unwrap();
    let trade_data: Arc<dyn TradeDataManager> = Arc::new(trade_data);

    info!("TradeData initialized successfully.");

    trade_data.init().await.unwrap();

    info!("TradeData init() phase complete. Waiting for initial setup...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ============ Test 1: Place a buy limit order (will be filled or remain open) ============
    info!("Test 1: Placing buy limit order...");
    let buy_limit_order = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: OrderSide::Buy,
        r#type: OrderType::Limit,
        time_in_force: Some(TimeInForce::Gtc),
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("140000.0").unwrap()),
        new_client_order_id: Some(format!(
            "test_buy_limit_{}",
            time::get_current_milli_timestamp()
        )),
        stop_price: None,
        iceberg_qty: None,
    };

    let order_1: Order = trade_data
        .place_order(&MarketType::BinanceSpot, buy_limit_order.clone())
        .await
        .unwrap();
    info!(
        "Buy limit order placed: order_id={}, status={:?}",
        order_1.order_id, order_1.order_status
    );
    tokio::time::sleep(Duration::from_secs(1)).await;

    // ============ Test 2: Place another buy order that will be cancelled ============
    info!("Test 2: Placing buy order to be cancelled...");
    let buy_order_to_cancel = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: OrderSide::Buy,
        r#type: OrderType::Limit,
        time_in_force: Some(TimeInForce::Gtc),
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("100000.0").unwrap()),
        new_client_order_id: Some(format!(
            "test_cancel_{}",
            time::get_current_milli_timestamp()
        )),
        stop_price: None,
        iceberg_qty: None,
    };

    let order_2: Order = trade_data
        .place_order(&MarketType::BinanceSpot, buy_order_to_cancel.clone())
        .await
        .unwrap();
    info!(
        "Buy order for cancellation placed: order_id={}, status={:?}",
        order_2.order_id, order_2.order_status
    );
    tokio::time::sleep(Duration::from_secs(1)).await;

    // ============ Test 3: Cancel the second order ============
    info!("Test 3: Cancelling the second order...");
    trade_data
        .cancel_order(
            &MarketType::BinanceSpot,
            CancelOrderRequest {
                symbol: order_2.symbol.clone(),
                order_id: Some(order_2.order_id.clone()),
                orig_client_order_id: Some(order_2.client_order_id.clone()),
                new_client_order_id: None,
            },
        )
        .await
        .unwrap();
    info!("Order cancelled: order_id={}", order_2.order_id);
    tokio::time::sleep(Duration::from_secs(1)).await;

    // ============ Test 4: Place a market sell order ============
    info!("Test 4: Placing market sell order...");
    let sell_market_order = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: OrderSide::Sell,
        r#type: OrderType::Market,
        time_in_force: None,
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: None,
        new_client_order_id: Some(format!("test_sell_{}", time::get_current_milli_timestamp())),
        stop_price: None,
        iceberg_qty: None,
    };

    let order_3 = trade_data
        .place_order(&MarketType::BinanceSpot, sell_market_order.clone())
        .await
        .unwrap();
    info!(
        "Market sell order placed: order_id={}, status={:?}",
        order_3.order_id, order_3.order_status
    );
    tokio::time::sleep(Duration::from_secs(1)).await;

    // ============ Test 5: sell order limit, not filled ============
    let sell_order_to_cancel = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: OrderSide::Sell,
        r#type: OrderType::Limit,
        time_in_force: Some(TimeInForce::Gtc),
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("140000.0").unwrap()),
        new_client_order_id: Some(format!("test_{}", time::get_current_milli_timestamp())),
        stop_price: None,
        iceberg_qty: None,
    };

    let order_4: Order = trade_data
        .place_order(&MarketType::BinanceSpot, sell_order_to_cancel.clone())
        .await
        .unwrap();
    info!(
        "Sell order not filled: order_id={}, status={:?}",
        order_4.order_id, order_4.order_status
    );
    tokio::time::sleep(Duration::from_secs(1)).await;

    // ============ Wait for all operations to be synced to TradeData ============
    info!("Waiting for operations to be synced to TradeData (3 seconds)...");
    tokio::time::sleep(Duration::from_secs(20)).await;

    // Test account data consistency
    let account_from_cache = trade_data
        .get_account(&MarketType::BinanceSpot)
        .await
        .unwrap()
        .unwrap();

    let account_from_api = provider.get_account().await.unwrap();

    assert!(account_equal(&account_from_cache, &account_from_api));
    dump(&account_from_cache, "trade_data_account.json").unwrap();

    // Test open orders data consistency
    let open_orders_from_cache = trade_data
        .get_open_orders(&MarketType::BinanceSpot)
        .await
        .unwrap();

    let open_orders_from_api = provider
        .get_open_orders(GetOpenOrdersRequest { symbol: None })
        .await
        .unwrap();

    assert!(orders_equal(
        open_orders_from_cache.clone(),
        open_orders_from_api.clone()
    ));
    dump(&open_orders_from_api, "trade_data_open_orders.json").unwrap();

    // Test user trades data consistency
    // 假定测试运行间隔大于1minute，以确保可以获取到历史交易数据
    let user_trades_from_data_manager = trade_data
        .get_user_trades(&MarketType::BinanceSpot, "BTCUSDT", Some(200))
        .await
        .unwrap();
    let user_trades_from_api = provider
        .get_user_trades(GetUserTradesRequest {
            symbol: "BTCUSDT".to_string(),
            order_id: None,
            limit: Some(200),
            from_id: None,
            start_time: Some(time::get_current_milli_timestamp() - 24 * 60 * 60 * 1000 + 1),
            end_time: None,
        })
        .await
        .unwrap();
    assert!(trades_equal(
        user_trades_from_data_manager.clone(),
        user_trades_from_api.clone()
    ));
    dump(&user_trades_from_api, "trade_data_user_trades.json").unwrap();
}

fn account_equal(a1: &Account, a2: &Account) -> bool {
    if a1.balances.len() != a2.balances.len() {
        return false;
    }
    if a1.timestamp != a2.timestamp {
        return false;
    }

    let mut balances1 = HashMap::<String, Balance>::new();
    for b in &a1.balances {
        balances1.insert(b.asset.clone(), b.clone());
    }

    for b in &a2.balances {
        match balances1.get(&b.asset) {
            Some(b1) => {
                if b1 != b {
                    return false;
                }
            }
            None => {
                return false;
            }
        }
    }

    return true;
}

fn orders_equal(o1: Vec<Order>, o2: Vec<Order>) -> bool {
    if o1.len() != o2.len() {
        return false;
    }

    let mut orders1 = HashMap::<String, Order>::new();
    for o in o1 {
        orders1.insert(o.order_id.clone(), o);
    }

    for o in o2 {
        match orders1.get(&o.order_id) {
            Some(o1) => {
                if o1 != &o {
                    return false;
                }
            }
            None => {
                return false;
            }
        }
    }

    return true;
}

fn trades_equal(t1: Vec<UserTrade>, t2: Vec<UserTrade>) -> bool {
    if t1.len() != t2.len() {
        return false;
    }

    let mut trades1 = HashMap::<String, UserTrade>::new();
    for t in t1 {
        trades1.insert(t.trade_id.clone(), t);
    }

    for t in t2 {
        match trades1.get(&t.trade_id) {
            Some(t1) => {
                if t1 != &t {
                    return false;
                }
            }
            None => {
                return false;
            }
        }
    }

    return true;
}
