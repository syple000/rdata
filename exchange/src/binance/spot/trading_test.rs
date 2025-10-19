#[cfg(test)]
mod tests {
    use crate::binance::errors::BinanceError;
    use crate::binance::spot::models::{
        Account, Balance, Order, OrderStatus, OrderType, OutboundAccountPosition, Side,
        TimeInForce, Trade,
    };
    use crate::binance::spot::trading::Trading;
    use rust_decimal::Decimal;
    use std::collections::HashMap;
    use std::str::FromStr;

    fn temp_db() -> sled::Db {
        sled::Config::default().temporary(true).open().unwrap()
    }

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn sample_order(order_id: u64, update_time: u64, status: OrderStatus) -> Order {
        Order {
            order_id,
            client_order_id: format!("client-{order_id}"),
            symbol: "BTCUSDT".to_string(),
            order_side: Side::Buy,
            order_type: OrderType::Limit,
            order_quantity: dec("1.5"),
            order_price: dec("30000"),
            executed_qty: dec("0"),
            cummulative_quote_qty: dec("0"),
            order_status: status,
            time_in_force: TimeInForce::Gtc,
            stop_price: dec("0"),
            iceberg_qty: dec("0"),
            create_time: update_time.saturating_sub(10),
            update_time,
        }
    }

    fn sample_trade(trade_id: u64, order_id: u64) -> Trade {
        Trade {
            trade_id,
            order_id,
            symbol: "BTCUSDT".to_string(),
            order_side: Side::Buy,
            trade_price: dec("30100"),
            trade_quantity: dec("0.5"),
            commission: dec("0.0005"),
            commission_asset: "USDT".to_string(),
            is_maker: false,
            timestamp: 1_700_000_000 + trade_id,
        }
    }

    fn sample_account(update_time: u64) -> Account {
        Account {
            maker_commission_rate: dec("0.001"),
            taker_commission_rate: dec("0.001"),
            buyer_commission_rate: dec("0.001"),
            seller_commission_rate: dec("0.001"),
            balances: vec![
                Balance {
                    asset: "BTC".to_string(),
                    free: dec("0.5"),
                    locked: dec("0.1"),
                },
                Balance {
                    asset: "USDT".to_string(),
                    free: dec("1000"),
                    locked: dec("0"),
                },
            ],
            can_trade: true,
            update_time,
        }
    }

    #[test]
    fn update_by_outbound_account_position_requires_existing_account() {
        let db = temp_db();
        let trading = Trading::new(&db, None, None, None, None).unwrap();

        let outbound = OutboundAccountPosition {
            balances: vec![Balance {
                asset: "BTC".to_string(),
                free: dec("0.6"),
                locked: dec("0.2"),
            }],
            transaction_time: 1,
            update_time: 200,
        };

        let result = trading.update_by_outbound_account_position(&outbound);
        assert!(matches!(
            result,
            Err(BinanceError::ClientError { message }) if message.contains("Account is None")
        ));
    }

    #[test]
    fn update_by_outbound_account_position_merges_and_updates_balances() {
        let db = temp_db();
        let trading = Trading::new(&db, None, None, None, None).unwrap();

        let initial_account = sample_account(100);
        trading.update_account(&initial_account).unwrap();

        let outbound = OutboundAccountPosition {
            balances: vec![
                Balance {
                    asset: "BTC".to_string(),
                    free: dec("0.6"),
                    locked: dec("0.2"),
                },
                Balance {
                    asset: "ETH".to_string(),
                    free: dec("2"),
                    locked: dec("0"),
                },
            ],
            transaction_time: 2,
            update_time: 200,
        };

        trading
            .update_by_outbound_account_position(&outbound)
            .unwrap();

        let updated_account = trading.get_account().unwrap();
        assert_eq!(updated_account.update_time, 200);

        let balances: HashMap<_, _> = updated_account
            .balances
            .into_iter()
            .map(|balance| (balance.asset, (balance.free, balance.locked)))
            .collect();

        assert_eq!(balances.get("BTC"), Some(&(dec("0.6"), dec("0.2"))));
        assert_eq!(balances.get("USDT"), Some(&(dec("1000"), dec("0"))));
        assert_eq!(balances.get("ETH"), Some(&(dec("2"), dec("0"))));
    }

    #[test]
    fn update_by_outbound_account_position_ignores_older_updates() {
        let db = temp_db();
        let trading = Trading::new(&db, None, None, None, None).unwrap();

        let initial_account = sample_account(200);
        trading.update_account(&initial_account).unwrap();

        let outbound = OutboundAccountPosition {
            balances: vec![Balance {
                asset: "BTC".to_string(),
                free: dec("1"),
                locked: dec("0.3"),
            }],
            transaction_time: 3,
            update_time: 150,
        };

        trading
            .update_by_outbound_account_position(&outbound)
            .unwrap();

        let account_after = trading.get_account().unwrap();
        assert_eq!(account_after.update_time, 200);

        let btc_balance = account_after
            .balances
            .into_iter()
            .find(|balance| balance.asset == "BTC")
            .unwrap();
        assert_eq!(btc_balance.free, dec("0.5"));
        assert_eq!(btc_balance.locked, dec("0.1"));
    }

    #[test]
    fn update_order_rejects_stale_payloads() {
        let db = temp_db();
        let trading = Trading::new(&db, None, None, None, None).unwrap();

        let latest_order = sample_order(1, 200, OrderStatus::New);
        trading.update_order(&latest_order).unwrap();

        let stale_order = sample_order(1, 150, OrderStatus::PartiallyFilled);
        trading.update_order(&stale_order).unwrap();

        let stored_order = trading.get_order_by_exchange_order_id(1).unwrap();
        assert_eq!(stored_order.update_time, 200);
        assert!(matches!(&stored_order.order_status, OrderStatus::New));
    }

    #[test]
    fn trading_persists_and_recovers_state_from_storage() {
        let db = temp_db();
        let trading = Trading::new(&db, None, None, None, None).unwrap();

        let order = sample_order(42, 400, OrderStatus::New);
        let want_price = dec("30500.5");
        trading
            .update_want_price(&order.client_order_id, want_price)
            .unwrap();
        trading.update_order(&order).unwrap();

        let trade = sample_trade(1001, order.order_id);
        trading.update_trade(&trade).unwrap();

        let account = sample_account(500);
        trading.update_account(&account).unwrap();

        drop(trading);

        let trading_reloaded = Trading::new(&db, None, None, None, None).unwrap();

        assert_eq!(
            trading_reloaded
                .get_want_price_by_client_order_id(&order.client_order_id)
                .unwrap(),
            want_price
        );
        assert_eq!(
            trading_reloaded
                .get_want_price_by_exchange_order_id(order.order_id)
                .unwrap(),
            want_price
        );

        let reloaded_order = trading_reloaded
            .get_order_by_exchange_order_id(order.order_id)
            .unwrap();
        assert_eq!(reloaded_order.client_order_id, order.client_order_id);
        assert_eq!(reloaded_order.update_time, order.update_time);

        let trades = trading_reloaded
            .get_trades_by_exchange_order_id(order.order_id)
            .unwrap();
        assert_eq!(trades.len(), 1);
        let reloaded_trade = trades.into_iter().next().unwrap();
        assert_eq!(reloaded_trade.trade_id, trade.trade_id);
        assert_eq!(reloaded_trade.trade_price, trade.trade_price);

        let reloaded_account = trading_reloaded.get_account().unwrap();
        assert_eq!(reloaded_account.update_time, account.update_time);

        let balances: HashMap<_, _> = reloaded_account
            .balances
            .into_iter()
            .map(|balance| (balance.asset, (balance.free, balance.locked)))
            .collect();
        assert_eq!(balances.get("BTC"), Some(&(dec("0.5"), dec("0.1"))));
        assert_eq!(balances.get("USDT"), Some(&(dec("1000"), dec("0"))));
    }
}
