use crate::{
    config::Config,
    data_manager::{trade_data::TradeData, TradeDataManager},
    models::{
        Account, Balance, CancelOrderRequest, GetAllOrdersRequest, GetOpenOrdersRequest,
        GetUserTradesRequest, MarketType, Order, OrderSide, OrderType, PlaceOrderRequest,
        TimeInForce, UserTrade,
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
        "markets": ["binance_spot"],
        "db_path": "{}",
        "binance_spot": {{
            "trade_refresh_interval_secs": 5,
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
    let config = Config::from_json(config_path).unwrap();

    let mut provider = BinanceSpotTradeProvider::new(Arc::new(config.clone())).unwrap();
    provider.init().await.unwrap();
    let provider = Arc::new(provider);
    let mut trade_providers: HashMap<MarketType, Arc<dyn TradeProvider>> = HashMap::new();
    trade_providers.insert(MarketType::BinanceSpot, provider.clone());

    let trade_data = TradeData::new(Arc::new(config.clone()), Arc::new(trade_providers)).unwrap();
    let trade_data_ptr = Arc::new(trade_data);
    let trade_data: Arc<dyn TradeDataManager> = trade_data_ptr.clone();

    trade_data.init().await.unwrap();

    // 记录开始时间，睡眠5秒后进行交易
    let start_ts = time::get_current_milli_timestamp();
    tokio::time::sleep(Duration::from_millis(5000)).await;

    info!("Test 1: Placing buy limit order...");
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
    let order_1: Order = trade_data
        .place_order(&MarketType::BinanceSpot, buy_limit_order.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    info!("Test 2: Placing buy order to be cancelled...");
    let buy_order_to_cancel = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: OrderSide::Buy,
        r#type: OrderType::Limit,
        time_in_force: Some(TimeInForce::Gtc),
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("80000.0").unwrap()),
        client_order_id: format!("test_cancel_{}", time::get_current_milli_timestamp()),
        stop_price: None,
        iceberg_qty: None,
    };
    let order_2: Order = trade_data
        .place_order(&MarketType::BinanceSpot, buy_order_to_cancel.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    trade_data
        .cancel_order(
            &MarketType::BinanceSpot,
            CancelOrderRequest {
                symbol: order_2.symbol.clone(),
                order_id: Some(order_2.order_id.clone()),
                client_order_id: order_2.client_order_id.clone(),
            },
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    info!("Test 3: Placing market sell order...");
    let sell_market_order = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: OrderSide::Sell,
        r#type: OrderType::Market,
        time_in_force: None,
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: None,
        client_order_id: format!("test_sell_{}", time::get_current_milli_timestamp()),
        stop_price: None,
        iceberg_qty: None,
    };
    let order_3: Order = trade_data
        .place_order(&MarketType::BinanceSpot, sell_market_order.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    info!("Test 4: Placing sell limit order not to be filled...");
    let sell_order_to_cancel = PlaceOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: OrderSide::Sell,
        r#type: OrderType::Limit,
        time_in_force: Some(TimeInForce::Gtc),
        quantity: Some(Decimal::from_str("0.001").unwrap()),
        price: Some(Decimal::from_str("140000.0").unwrap()),
        client_order_id: format!("test_{}", time::get_current_milli_timestamp()),
        stop_price: None,
        iceberg_qty: None,
    };
    let order_4: Order = trade_data
        .place_order(&MarketType::BinanceSpot, sell_order_to_cancel.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    info!("Waiting for operations to be synced to TradeData (20 seconds)...");
    tokio::time::sleep(Duration::from_secs(20)).await;
    let end_ts = time::get_current_milli_timestamp();

    // 账户最终一致性校验
    let account1 = trade_data
        .get_account(&MarketType::BinanceSpot)
        .await
        .unwrap()
        .unwrap();
    let account2 = trade_data_ptr
        .get_account_from_db(&MarketType::BinanceSpot)
        .unwrap()
        .unwrap();
    let account3 = provider.get_account().await.unwrap();

    assert!(account_equal(&account1, &account2) && account_equal(&account1, &account3));
    dump(&account1, "trade_data_account.json").unwrap();

    // 开订单状态校验
    let open_orders1 = trade_data
        .get_open_orders(&MarketType::BinanceSpot)
        .await
        .unwrap();
    let open_orders2 = trade_data_ptr
        .get_open_orders_from_db(&MarketType::BinanceSpot)
        .unwrap();
    let open_orders3 = provider
        .get_open_orders(GetOpenOrdersRequest { symbol: None })
        .await
        .unwrap();
    assert!(
        orders_equal(open_orders1.clone(), open_orders2.clone())
            && orders_equal(open_orders1.clone(), open_orders3.clone())
    );
    assert!(open_orders1
        .iter()
        .map(|e| e.order_id.clone())
        .collect::<Vec<String>>()
        .contains(&order_4.order_id));
    dump(&open_orders1, "trade_data_open_orders.json").unwrap();

    // 全部订单 & 成交记录校验（本次测试时间区间）
    let orders1 = trade_data
        .get_orders(
            &MarketType::BinanceSpot,
            "BTCUSDT",
            Some(start_ts),
            Some(end_ts),
            None,
        )
        .await
        .unwrap()
        .into_iter()
        .filter(|e| e.update_time >= start_ts && e.update_time <= end_ts)
        .collect::<Vec<Order>>();
    let orders2 = provider
        .get_all_orders(GetAllOrdersRequest {
            symbol: "BTCUSDT".to_string(),
            from_id: None,
            start_time: Some(start_ts),
            end_time: Some(end_ts),
            limit: None,
        })
        .await
        .unwrap();
    assert!(orders_equal(orders1.clone(), orders2.clone()));
    let order_ids = orders1
        .iter()
        .map(|e| e.order_id.clone())
        .collect::<Vec<String>>();
    assert!(
        order_ids.contains(&order_1.order_id)
            && order_ids.contains(&order_2.order_id)
            && order_ids.contains(&order_3.order_id)
            && order_ids.contains(&order_4.order_id)
    );
    dump(&orders1, "trade_data_orders.json").unwrap();

    let trades1 = trade_data
        .get_user_trades(&MarketType::BinanceSpot, "BTCUSDT", None, None, None)
        .await
        .unwrap()
        .into_iter()
        .filter(|e| e.timestamp >= start_ts && e.timestamp <= end_ts)
        .collect::<Vec<UserTrade>>();
    let trades2 = provider
        .get_user_trades(GetUserTradesRequest {
            symbol: "BTCUSDT".to_string(),
            order_id: None,
            from_id: None,
            start_time: Some(start_ts),
            end_time: Some(end_ts),
            limit: None,
        })
        .await
        .unwrap();
    assert!(trades_equal(trades1.clone(), trades2.clone()));
    dump(&trades1, "trade_data_user_trades.json").unwrap();
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
