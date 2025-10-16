#[cfg(test)]
mod tests {
    use crate::binance::spot::{models::AggTrade, trade::Trade};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn create_test_agg_trade(
        symbol: &str,
        agg_trade_id: u64,
        price: &str,
        quantity: &str,
        first_trade_id: u64,
        last_trade_id: u64,
        timestamp: u64,
        is_buyer_maker: bool,
    ) -> AggTrade {
        AggTrade {
            symbol: symbol.to_string(),
            agg_trade_id,
            price: Decimal::from_str(price).unwrap(),
            quantity: Decimal::from_str(quantity).unwrap(),
            first_trade_id,
            last_trade_id,
            timestamp,
            is_buyer_maker,
        }
    }

    #[tokio::test]
    async fn test_trade_new() {
        let result = Trade::new(
            "BTCUSDT",
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
            None,
        );
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_trade_update_basic() {
        let trade_manager = Trade::new(
            "BTCUSDT",
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
            None,
        )
        .unwrap();

        let trade = create_test_agg_trade(
            "BTCUSDT",
            1,
            "50000.0",
            "1.5",
            100,
            102,
            1000000000000,
            false,
        );

        let result = trade_manager.update(&trade).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true); // Should return true for new trade

        let view = trade_manager.get_trades().await;
        assert_eq!(view.len(), 1);

        let trades: Vec<_> = view.iter().collect();
        assert_eq!(trades[0].agg_trade_id, 1);
        assert_eq!(trades[0].price, Decimal::from_str("50000.0").unwrap());
        assert_eq!(trades[0].quantity, Decimal::from_str("1.5").unwrap());
    }

    #[tokio::test]
    async fn test_trade_update_wrong_symbol() {
        let trade_manager = Trade::new(
            "BTCUSDT",
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
            None,
        )
        .unwrap();

        let trade = create_test_agg_trade(
            "ETHUSDT",
            1,
            "3000.0",
            "10.0",
            100,
            100,
            1000000000000,
            false,
        );

        let result = trade_manager.update(&trade).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_trade_update_sequential() {
        let trade_manager = Trade::new(
            "BTCUSDT",
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
            None,
        )
        .unwrap();

        for i in 1..=5 {
            let trade = create_test_agg_trade(
                "BTCUSDT",
                i,
                "50000.0",
                "1.0",
                100 + i - 1,
                100 + i - 1,
                1000000000000 + i * 1000,
                false,
            );
            let result = trade_manager.update(&trade).await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), true);
        }

        let view = trade_manager.get_trades().await;
        assert_eq!(view.len(), 5);
    }

    #[tokio::test]
    async fn test_trade_update_duplicate() {
        let trade_manager = Trade::new(
            "BTCUSDT",
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
            None,
        )
        .unwrap();

        let trade = create_test_agg_trade(
            "BTCUSDT",
            1,
            "50000.0",
            "1.5",
            100,
            102,
            1000000000000,
            false,
        );

        // First update should succeed
        let result1 = trade_manager.update(&trade).await;
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap(), true);

        // Second update with same ID should return false
        let result2 = trade_manager.update(&trade).await;
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), false);

        let view = trade_manager.get_trades().await;
        assert_eq!(view.len(), 1);
    }

    #[tokio::test]
    async fn test_trade_update_old_trade_ignored() {
        let trade_manager = Trade::new(
            "BTCUSDT",
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
            None,
        )
        .unwrap();

        // Add recent trade
        let trade1 = create_test_agg_trade(
            "BTCUSDT",
            10,
            "50000.0",
            "1.5",
            110,
            112,
            1000000000000,
            false,
        );
        trade_manager.update(&trade1).await.unwrap();

        // Try to add older trade
        let trade2 = create_test_agg_trade(
            "BTCUSDT",
            5,
            "49000.0",
            "2.0",
            105,
            107,
            999999000000,
            false,
        );
        let result = trade_manager.update(&trade2).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);

        let view = trade_manager.get_trades().await;
        assert_eq!(view.len(), 1);

        let trades: Vec<_> = view.iter().collect();
        assert_eq!(trades[0].agg_trade_id, 10); // Should keep the newer one
    }

    #[tokio::test]
    async fn test_trade_update_with_gap() {
        let trade_manager = Trade::new(
            "BTCUSDT",
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
            None,
        )
        .unwrap();

        // Add first trade
        let trade1 = create_test_agg_trade(
            "BTCUSDT",
            1,
            "50000.0",
            "1.0",
            100,
            100,
            1000000000000,
            false,
        );
        trade_manager.update(&trade1).await.unwrap();

        // Add trade with gap (ID 5, skipping 2, 3, 4)
        let trade2 = create_test_agg_trade(
            "BTCUSDT",
            5,
            "51000.0",
            "2.0",
            104,
            104,
            1000000004000,
            false,
        );
        trade_manager.update(&trade2).await.unwrap();

        let view = trade_manager.get_trades().await;
        // Should have entries for IDs 1, 2, 3, 4, 5 but 2, 3, 4 will be None
        let trades = view.trades();
        assert_eq!(trades.len(), 2); // Only 2 actual trades (gaps are filtered out)
    }

    #[tokio::test]
    async fn test_trade_update_fill_gap() {
        let trade_manager = Trade::new(
            "BTCUSDT",
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
            None,
        )
        .unwrap();

        // Add trades with gaps
        let trade1 = create_test_agg_trade(
            "BTCUSDT",
            1,
            "50000.0",
            "1.0",
            100,
            100,
            1000000000000,
            false,
        );
        trade_manager.update(&trade1).await.unwrap();

        let trade3 = create_test_agg_trade(
            "BTCUSDT",
            5,
            "51000.0",
            "2.0",
            104,
            104,
            1000000004000,
            false,
        );
        trade_manager.update(&trade3).await.unwrap();

        // Fill the gap
        let trade2 = create_test_agg_trade(
            "BTCUSDT",
            3,
            "50500.0",
            "1.5",
            102,
            102,
            1000000002000,
            false,
        );
        let result = trade_manager.update(&trade2).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true); // Should successfully fill the gap

        let view = trade_manager.get_trades().await;
        let trades = view.trades();
        assert_eq!(trades.len(), 3); // Now have 3 actual trades
    }

    #[tokio::test]
    async fn test_trade_archive() {
        let trade_manager = Trade::new(
            "BTCUSDT",
            10,
            5,
            5,
            &sled::Config::default().temporary(true).open().unwrap(),
            None,
        )
        .unwrap();

        for i in 1..=20 {
            let trade = create_test_agg_trade(
                "BTCUSDT",
                1000 + i,
                "50000.0",
                "1.0",
                100 + i - 1,
                100 + i - 1,
                1000000000000 + i * 1000,
                false,
            );
            trade_manager.update(&trade).await.unwrap();
        }

        let view_before = trade_manager.get_trades().await;
        assert_eq!(view_before.len(), 14);

        trade_manager.archive(1020).await;

        let view_after = trade_manager.get_trades().await;
        assert_eq!(view_after.len(), 5);

        let trades = trade_manager.get_trades_with_limit(100).await;
        assert_eq!(trades.len(), 20);
    }

    #[tokio::test]
    async fn test_trade_archive_idempotent() {
        let trade_manager = Trade::new(
            "BTCUSDT",
            10,
            5,
            5,
            &sled::Config::default().temporary(true).open().unwrap(),
            None,
        )
        .unwrap();

        // Add trades
        for i in 1..=10 {
            let trade = create_test_agg_trade(
                "BTCUSDT",
                i,
                "50000.0",
                "1.0",
                100 + i - 1,
                100 + i - 1,
                1000000000000 + i * 1000,
                false,
            );
            trade_manager.update(&trade).await.unwrap();
        }

        // Archive to ID 5
        trade_manager.archive(5).await;

        let view1 = trade_manager.get_trades().await;
        let archived_count1 = view1.archived_trades.len();
        let latest_count1 = view1.latest_trades.len();

        // Archive again with same or lower ID
        trade_manager.archive(3).await;

        let view2 = trade_manager.get_trades().await;
        let archived_count2 = view2.archived_trades.len();
        let latest_count2 = view2.latest_trades.len();

        // Should be the same
        assert_eq!(archived_count1, archived_count2);
        assert_eq!(latest_count1, latest_count2);
    }

    #[tokio::test]
    async fn test_trade_view_iter() {
        let trade_manager = Trade::new(
            "BTCUSDT",
            100,
            50,
            100,
            &sled::Config::default().temporary(true).open().unwrap(),
            None,
        )
        .unwrap();

        for i in 1..=5 {
            let trade = create_test_agg_trade(
                "BTCUSDT",
                i,
                "50000.0",
                "1.0",
                100 + i - 1,
                100 + i - 1,
                1000000000000 + i * 1000,
                false,
            );
            trade_manager.update(&trade).await.unwrap();
        }

        let view = trade_manager.get_trades().await;
        let count = view.iter().count();
        assert_eq!(count, 5);

        let trades = view.trades();
        assert_eq!(trades.len(), 5);
    }

    #[tokio::test]
    async fn test_trade_view_len() {
        let trade_manager = Trade::new(
            "BTCUSDT",
            100,
            50,
            100,
            &sled::Config::default().temporary(true).open().unwrap(),
            None,
        )
        .unwrap();

        let view_empty = trade_manager.get_trades().await;
        assert_eq!(view_empty.len(), 0);

        for i in 1..=3 {
            let trade = create_test_agg_trade(
                "BTCUSDT",
                i,
                "50000.0",
                "1.0",
                100 + i - 1,
                100 + i - 1,
                1000000000000 + i * 1000,
                false,
            );
            trade_manager.update(&trade).await.unwrap();
        }

        let view = trade_manager.get_trades().await;
        assert_eq!(view.len(), 3);
    }

    #[tokio::test]
    async fn test_trade_is_buyer_maker() {
        let trade_manager = Trade::new(
            "BTCUSDT",
            100,
            50,
            100,
            &sled::Config::default().temporary(true).open().unwrap(),
            None,
        )
        .unwrap();

        let trade_buy = create_test_agg_trade(
            "BTCUSDT",
            1,
            "50000.0",
            "1.0",
            100,
            100,
            1000000000000,
            false,
        );
        trade_manager.update(&trade_buy).await.unwrap();

        let trade_sell = create_test_agg_trade(
            "BTCUSDT",
            2,
            "50100.0",
            "1.5",
            101,
            101,
            1000000001000,
            true,
        );
        trade_manager.update(&trade_sell).await.unwrap();

        let view = trade_manager.get_trades().await;
        let trades: Vec<_> = view.iter().collect();

        assert_eq!(trades[0].is_buyer_maker, false);
        assert_eq!(trades[1].is_buyer_maker, true);
    }
}
