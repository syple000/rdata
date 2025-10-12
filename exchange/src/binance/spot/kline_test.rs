#[cfg(test)]
mod tests {
    use crate::binance::spot::{
        kline::Kline,
        models::{AggTrade, KlineData},
    };
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn create_test_kline_data(
        symbol: &str,
        open_time: u64,
        interval: u64,
        open: &str,
        high: &str,
        low: &str,
        close: &str,
        volume: &str,
    ) -> KlineData {
        KlineData {
            symbol: symbol.to_string(),
            open_time,
            close_time: open_time + interval,
            open: Decimal::from_str(open).unwrap(),
            high: Decimal::from_str(high).unwrap(),
            low: Decimal::from_str(low).unwrap(),
            close: Decimal::from_str(close).unwrap(),
            volume: Decimal::from_str(volume).unwrap(),
            quote_volume: Decimal::from_str("0").unwrap(),
            trade_count: 0,
        }
    }

    fn create_test_agg_trade(
        symbol: &str,
        agg_trade_id: u64,
        price: &str,
        quantity: &str,
        first_trade_id: u64,
        last_trade_id: u64,
        timestamp: u64,
    ) -> AggTrade {
        AggTrade {
            symbol: symbol.to_string(),
            agg_trade_id,
            price: Decimal::from_str(price).unwrap(),
            quantity: Decimal::from_str(quantity).unwrap(),
            first_trade_id,
            last_trade_id,
            timestamp,
            is_buyer_maker: false,
        }
    }

    #[tokio::test]
    async fn test_kline_new() {
        let result = Kline::new(
            "BTCUSDT".to_string(),
            60000,
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
        );
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_kline_update_by_kline_basic() {
        let kline_manager = Kline::new(
            "BTCUSDT".to_string(),
            60000,
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
        )
        .unwrap();

        let kline_data = create_test_kline_data(
            "BTCUSDT",
            1000000020000, // Aligned with 60000 interval
            60000,
            "50000.0",
            "51000.0",
            "49000.0",
            "50500.0",
            "10.5",
        );

        let result = kline_manager.update_by_kline(&kline_data).await;
        assert!(result.is_ok());

        let view = kline_manager.get_klines().await;
        assert_eq!(view.len(), 1);

        let klines: Vec<_> = view.iter().collect();
        assert_eq!(klines[0].symbol, "BTCUSDT");
        assert_eq!(klines[0].open_time, 1000000020000);
    }

    #[tokio::test]
    async fn test_kline_update_by_kline_wrong_symbol() {
        let kline_manager = Kline::new(
            "BTCUSDT".to_string(),
            60000,
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
        )
        .unwrap();

        let kline_data = create_test_kline_data(
            "ETHUSDT",
            1000000000000,
            60000,
            "3000.0",
            "3100.0",
            "2900.0",
            "3050.0",
            "100.0",
        );

        let result = kline_manager.update_by_kline(&kline_data).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_kline_update_by_kline_wrong_interval() {
        let kline_manager = Kline::new(
            "BTCUSDT".to_string(),
            60000,
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
        )
        .unwrap();

        let kline_data = create_test_kline_data(
            "BTCUSDT",
            1000000020000, // Aligned
            30000,         // Wrong interval
            "50000.0",
            "51000.0",
            "49000.0",
            "50500.0",
            "10.5",
        );

        let result = kline_manager.update_by_kline(&kline_data).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_kline_update_by_kline_sequential() {
        let kline_manager = Kline::new(
            "BTCUSDT".to_string(),
            60000,
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
        )
        .unwrap();

        // Add first kline
        let kline1 = create_test_kline_data(
            "BTCUSDT",
            1000000020000, // Aligned with 60000
            60000,
            "50000.0",
            "51000.0",
            "49000.0",
            "50500.0",
            "10.5",
        );
        kline_manager.update_by_kline(&kline1).await.unwrap();

        // Add second kline
        let kline2 = create_test_kline_data(
            "BTCUSDT",
            1000000080000, // 1000000020000 + 60000
            60000,
            "50500.0",
            "52000.0",
            "50000.0",
            "51500.0",
            "15.5",
        );
        kline_manager.update_by_kline(&kline2).await.unwrap();

        let view = kline_manager.get_klines().await;
        assert_eq!(view.len(), 2);
        assert_eq!(
            view.iter().rev().collect::<Vec<_>>()[0].open_time,
            1000000080000
        );
    }

    #[tokio::test]
    async fn test_kline_update_by_trade_basic() {
        let kline_manager = Kline::new(
            "BTCUSDT".to_string(),
            60000,
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
        )
        .unwrap();

        let trade = create_test_agg_trade("BTCUSDT", 1, "50000.0", "1.5", 100, 102, 1000000000000);

        let result = kline_manager.update_by_trade(&trade).await;
        assert!(result.is_ok());

        let view = kline_manager.get_klines().await;
        assert_eq!(view.len(), 1);

        let klines: Vec<_> = view.iter().collect();
        assert_eq!(klines[0].open, Decimal::from_str("50000.0").unwrap());
        assert_eq!(klines[0].high, Decimal::from_str("50000.0").unwrap());
        assert_eq!(klines[0].low, Decimal::from_str("50000.0").unwrap());
        assert_eq!(klines[0].close, Decimal::from_str("50000.0").unwrap());
        assert_eq!(klines[0].volume, Decimal::from_str("1.5").unwrap());
        assert_eq!(klines[0].trade_count, 3); // 102 - 100 + 1
    }

    #[tokio::test]
    async fn test_kline_update_by_trade_multiple_in_same_interval() {
        let kline_manager = Kline::new(
            "BTCUSDT".to_string(),
            60000,
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
        )
        .unwrap();

        let trade1 = create_test_agg_trade(
            "BTCUSDT",
            1,
            "50000.0",
            "1.0",
            100,
            100,
            1000000020000, // Aligned with 60000
        );
        kline_manager.update_by_trade(&trade1).await.unwrap();

        let trade2 = create_test_agg_trade(
            "BTCUSDT",
            2,
            "51000.0",
            "2.0",
            101,
            101,
            1000000050000, // Same interval
        );
        kline_manager.update_by_trade(&trade2).await.unwrap();

        let trade3 = create_test_agg_trade(
            "BTCUSDT",
            3,
            "49000.0",
            "1.5",
            102,
            102,
            1000000070000, // Same interval
        );
        kline_manager.update_by_trade(&trade3).await.unwrap();

        let view = kline_manager.get_klines().await;
        assert_eq!(view.len(), 1);

        let klines: Vec<_> = view.iter().collect();
        assert_eq!(klines[0].open, Decimal::from_str("50000.0").unwrap());
        assert_eq!(klines[0].high, Decimal::from_str("51000.0").unwrap());
        assert_eq!(klines[0].low, Decimal::from_str("49000.0").unwrap());
        assert_eq!(klines[0].close, Decimal::from_str("49000.0").unwrap());
        assert_eq!(klines[0].volume, Decimal::from_str("4.5").unwrap());
        assert_eq!(klines[0].trade_count, 3);
    }

    #[tokio::test]
    async fn test_kline_update_by_trade_wrong_symbol() {
        let kline_manager = Kline::new(
            "BTCUSDT".to_string(),
            60000,
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
        )
        .unwrap();

        let trade = create_test_agg_trade("ETHUSDT", 1, "3000.0", "10.0", 100, 100, 1000000000000);

        let result = kline_manager.update_by_trade(&trade).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_kline_archive() {
        let kline_manager = Kline::new(
            "BTCUSDT".to_string(),
            60000,
            10,
            5,
            5,
            &sled::Config::default().temporary(true).open().unwrap(),
        )
        .unwrap();

        for i in 0..20 {
            let kline = create_test_kline_data(
                "BTCUSDT",
                1000000020000 + i * 60000, // Aligned
                60000,
                "50000.0",
                "51000.0",
                "49000.0",
                "50500.0",
                "10.5",
            );
            kline_manager.update_by_kline(&kline).await.unwrap();
        }

        let view_before = kline_manager.get_klines().await;
        assert_eq!(view_before.len(), 14);

        kline_manager.archive(1000000020000 + 18 * 60000).await;

        let view_after = kline_manager.get_klines().await;
        assert_eq!(view_after.len(), 6);

        let klines = kline_manager.get_klines_with_limit(1000).await;
        assert_eq!(klines.len(), 20);
    }

    #[tokio::test]
    async fn test_kline_view_iter() {
        let kline_manager = Kline::new(
            "BTCUSDT".to_string(),
            60000,
            100,
            50,
            100,
            &sled::Config::default().temporary(true).open().unwrap(),
        )
        .unwrap();

        for i in 0..5 {
            let kline = create_test_kline_data(
                "BTCUSDT",
                1000000020000 + i * 60000, // Aligned
                60000,
                "50000.0",
                "51000.0",
                "49000.0",
                "50500.0",
                "10.5",
            );
            kline_manager.update_by_kline(&kline).await.unwrap();
        }

        let view = kline_manager.get_klines().await;
        let count = view.iter().count();
        assert_eq!(count, 5);

        let klines = view.klines();
        assert_eq!(klines.len(), 5);
    }

    #[tokio::test]
    async fn test_kline_gap_filling() {
        let kline_manager = Kline::new(
            "BTCUSDT".to_string(),
            60000,
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
        )
        .unwrap();

        // Add first kline
        let kline1 = create_test_kline_data(
            "BTCUSDT",
            1000000020000, // Aligned
            60000,
            "50000.0",
            "51000.0",
            "49000.0",
            "50500.0",
            "10.5",
        );
        kline_manager.update_by_kline(&kline1).await.unwrap();

        // Add kline with gap (skip 2 intervals)
        let kline2 = create_test_kline_data(
            "BTCUSDT",
            1000000020000 + 3 * 60000, // Aligned
            60000,
            "51000.0",
            "52000.0",
            "50500.0",
            "51500.0",
            "15.5",
        );
        kline_manager.update_by_kline(&kline2).await.unwrap();

        let view = kline_manager.get_klines().await;
        assert_eq!(view.len(), 4); // Should fill the gap
    }

    #[tokio::test]
    async fn test_kline_old_data_ignored() {
        let kline_manager = Kline::new(
            "BTCUSDT".to_string(),
            60000,
            1000,
            500,
            5000,
            &sled::Config::default().temporary(true).open().unwrap(),
        )
        .unwrap();

        // Add recent kline
        let kline1 = create_test_kline_data(
            "BTCUSDT",
            1000000080000, // Aligned, later time
            60000,
            "50000.0",
            "51000.0",
            "49000.0",
            "50500.0",
            "10.5",
        );
        kline_manager.update_by_kline(&kline1).await.unwrap();

        // Try to add older kline
        let kline2 = create_test_kline_data(
            "BTCUSDT",
            1000000020000, // Aligned, earlier time
            60000,
            "48000.0",
            "49000.0",
            "47000.0",
            "48500.0",
            "5.5",
        );
        kline_manager.update_by_kline(&kline2).await.unwrap();

        let view = kline_manager.get_klines().await;
        assert_eq!(view.len(), 1);

        let klines: Vec<_> = view.iter().collect();
        assert_eq!(klines[0].open_time, 1000000080000); // Should keep the newer one
    }
}
