#[cfg(test)]
mod tests {
    use crate::binance::spot::{
        depth::Depth,
        models::{DepthData, DepthUpdate, PriceLevel},
    };
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn create_test_price_level(price: &str, quantity: &str) -> PriceLevel {
        PriceLevel {
            price: Decimal::from_str(price).unwrap(),
            quantity: Decimal::from_str(quantity).unwrap(),
        }
    }

    fn create_test_depth_data(
        symbol: &str,
        last_update_id: u64,
        bids: Vec<(&str, &str)>,
        asks: Vec<(&str, &str)>,
    ) -> DepthData {
        DepthData {
            symbol: symbol.to_string(),
            last_update_id,
            bids: bids
                .iter()
                .map(|(p, q)| create_test_price_level(p, q))
                .collect(),
            asks: asks
                .iter()
                .map(|(p, q)| create_test_price_level(p, q))
                .collect(),
        }
    }

    fn create_test_depth_update(
        symbol: &str,
        first_update_id: u64,
        last_update_id: u64,
        bids: Vec<(&str, &str)>,
        asks: Vec<(&str, &str)>,
        timestamp: u64,
    ) -> DepthUpdate {
        DepthUpdate {
            symbol: symbol.to_string(),
            first_update_id,
            last_update_id,
            bids: bids
                .iter()
                .map(|(p, q)| create_test_price_level(p, q))
                .collect(),
            asks: asks
                .iter()
                .map(|(p, q)| create_test_price_level(p, q))
                .collect(),
            timestamp,
        }
    }

    #[tokio::test]
    async fn test_depth_new() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let result = Depth::new("BTCUSDT".to_string(), &db);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_depth_get_depth_empty() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let depth = Depth::new("BTCUSDT".to_string(), &db).unwrap();

        let result = depth.get_depth().await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_basic() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let depth = Depth::new("BTCUSDT".to_string(), &db).unwrap();

        let depth_data = create_test_depth_data(
            "BTCUSDT",
            1000,
            vec![("50000.0", "1.5"), ("49900.0", "2.0")],
            vec![("50100.0", "1.0"), ("50200.0", "1.5")],
        );

        let result = depth.update_by_depth(&depth_data).await;
        assert!(result.is_ok());

        let depth_arc = depth.get_depth().await;
        assert!(depth_arc.is_some());

        let depth_result = (*depth_arc).as_ref().unwrap();
        assert_eq!(depth_result.symbol, "BTCUSDT");
        assert_eq!(depth_result.last_update_id, 1000);
        assert_eq!(depth_result.bids.len(), 2);
        assert_eq!(depth_result.asks.len(), 2);

        // Check bids are sorted in descending order
        assert_eq!(
            depth_result.bids[0].price,
            Decimal::from_str("50000.0").unwrap()
        );
        assert_eq!(
            depth_result.bids[1].price,
            Decimal::from_str("49900.0").unwrap()
        );

        // Check asks are sorted in ascending order
        assert_eq!(
            depth_result.asks[0].price,
            Decimal::from_str("50100.0").unwrap()
        );
        assert_eq!(
            depth_result.asks[1].price,
            Decimal::from_str("50200.0").unwrap()
        );
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_wrong_symbol() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let depth = Depth::new("BTCUSDT".to_string(), &db).unwrap();

        let depth_data = create_test_depth_data(
            "ETHUSDT",
            1000,
            vec![("3000.0", "10.0")],
            vec![("3100.0", "5.0")],
        );

        let result = depth.update_by_depth(&depth_data).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_update_basic() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let depth = Depth::new("BTCUSDT".to_string(), &db).unwrap();

        // Initialize with depth data
        let depth_data = create_test_depth_data(
            "BTCUSDT",
            1000,
            vec![("50000.0", "1.5"), ("49900.0", "2.0")],
            vec![("50100.0", "1.0"), ("50200.0", "1.5")],
        );
        depth.update_by_depth(&depth_data).await.unwrap();

        // Update with depth update
        let depth_update = create_test_depth_update(
            "BTCUSDT",
            1001,
            1002,
            vec![("50000.0", "2.0"), ("49800.0", "3.0")],
            vec![("50100.0", "1.5")],
            1234567890,
        );

        let result = depth.update_by_depth_update(&depth_update).await;
        assert!(result.is_ok());

        let depth_arc = depth.get_depth().await;
        assert!(depth_arc.is_some());

        let depth_result = (*depth_arc).as_ref().unwrap();
        assert_eq!(depth_result.last_update_id, 1002);

        // Check updated bid
        let bid_50000 = depth_result
            .bids
            .iter()
            .find(|b| b.price == Decimal::from_str("50000.0").unwrap());
        assert!(bid_50000.is_some());
        assert_eq!(
            bid_50000.unwrap().quantity,
            Decimal::from_str("2.0").unwrap()
        );

        // Check new bid
        let bid_49800 = depth_result
            .bids
            .iter()
            .find(|b| b.price == Decimal::from_str("49800.0").unwrap());
        assert!(bid_49800.is_some());
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_update_without_init() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let depth = Depth::new("BTCUSDT".to_string(), &db).unwrap();

        let depth_update = create_test_depth_update(
            "BTCUSDT",
            1001,
            1002,
            vec![("50000.0", "2.0")],
            vec![("50100.0", "1.5")],
            1234567890,
        );

        let result = depth.update_by_depth_update(&depth_update).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_update_wrong_symbol() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let depth = Depth::new("BTCUSDT".to_string(), &db).unwrap();

        // Initialize with depth data
        let depth_data = create_test_depth_data(
            "BTCUSDT",
            1000,
            vec![("50000.0", "1.5")],
            vec![("50100.0", "1.0")],
        );
        depth.update_by_depth(&depth_data).await.unwrap();

        // Try to update with wrong symbol
        let depth_update = create_test_depth_update(
            "ETHUSDT",
            1001,
            1002,
            vec![("3000.0", "10.0")],
            vec![("3100.0", "5.0")],
            1234567890,
        );

        let result = depth.update_by_depth_update(&depth_update).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_update_old_data_ignored() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let depth = Depth::new("BTCUSDT".to_string(), &db).unwrap();

        // Initialize with depth data
        let depth_data = create_test_depth_data(
            "BTCUSDT",
            1000,
            vec![("50000.0", "1.5")],
            vec![("50100.0", "1.0")],
        );
        depth.update_by_depth(&depth_data).await.unwrap();

        // Try to update with older data
        let depth_update = create_test_depth_update(
            "BTCUSDT",
            995,
            999,
            vec![("50000.0", "2.0")],
            vec![("50100.0", "1.5")],
            1234567890,
        );

        let result = depth.update_by_depth_update(&depth_update).await;
        assert!(result.is_ok());

        let depth_arc = depth.get_depth().await;
        let depth_result = (*depth_arc).as_ref().unwrap();
        // Should still have old update_id
        assert_eq!(depth_result.last_update_id, 1000);
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_update_out_of_order() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let depth = Depth::new("BTCUSDT".to_string(), &db).unwrap();

        // Initialize with depth data
        let depth_data = create_test_depth_data(
            "BTCUSDT",
            1000,
            vec![("50000.0", "1.5")],
            vec![("50100.0", "1.0")],
        );
        depth.update_by_depth(&depth_data).await.unwrap();

        // Try to update with gap in sequence
        let depth_update = create_test_depth_update(
            "BTCUSDT",
            1005,
            1010,
            vec![("50000.0", "2.0")],
            vec![("50100.0", "1.5")],
            1234567890,
        );

        let result = depth.update_by_depth_update(&depth_update).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_update_remove_level() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let depth = Depth::new("BTCUSDT".to_string(), &db).unwrap();

        // Initialize with depth data
        let depth_data = create_test_depth_data(
            "BTCUSDT",
            1000,
            vec![("50000.0", "1.5"), ("49900.0", "2.0")],
            vec![("50100.0", "1.0"), ("50200.0", "1.5")],
        );
        depth.update_by_depth(&depth_data).await.unwrap();

        // Update with zero quantity to remove level
        let depth_update = create_test_depth_update(
            "BTCUSDT",
            1001,
            1002,
            vec![("50000.0", "0")],
            vec![("50100.0", "0")],
            1234567890,
        );

        depth.update_by_depth_update(&depth_update).await.unwrap();

        let depth_arc = depth.get_depth().await;
        let depth_result = (*depth_arc).as_ref().unwrap();

        // Check that levels with zero quantity are removed
        assert_eq!(depth_result.bids.len(), 1);
        assert_eq!(depth_result.asks.len(), 1);

        assert_eq!(
            depth_result.bids[0].price,
            Decimal::from_str("49900.0").unwrap()
        );
        assert_eq!(
            depth_result.asks[0].price,
            Decimal::from_str("50200.0").unwrap()
        );
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_update_sequential() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let depth = Depth::new("BTCUSDT".to_string(), &db).unwrap();

        // Initialize with depth data
        let depth_data = create_test_depth_data(
            "BTCUSDT",
            1000,
            vec![("50000.0", "1.5")],
            vec![("50100.0", "1.0")],
        );
        depth.update_by_depth(&depth_data).await.unwrap();

        // First update
        let depth_update1 = create_test_depth_update(
            "BTCUSDT",
            1001,
            1002,
            vec![("50000.0", "2.0")],
            vec![],
            1234567890,
        );
        depth.update_by_depth_update(&depth_update1).await.unwrap();

        // Second update
        let depth_update2 = create_test_depth_update(
            "BTCUSDT",
            1003,
            1004,
            vec![("49900.0", "3.0")],
            vec![("50200.0", "2.0")],
            1234567891,
        );
        depth.update_by_depth_update(&depth_update2).await.unwrap();

        let depth_arc = depth.get_depth().await;
        let depth_result = (*depth_arc).as_ref().unwrap();

        assert_eq!(depth_result.last_update_id, 1004);
        assert_eq!(depth_result.bids.len(), 2);
        assert_eq!(depth_result.asks.len(), 2);
    }

    #[tokio::test]
    async fn test_depth_archive() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let depth = Depth::new("BTCUSDT".to_string(), &db).unwrap();

        let depth_data = create_test_depth_data(
            "BTCUSDT",
            1000,
            vec![("50000.0", "1.5")],
            vec![("50100.0", "1.0")],
        );
        depth.update_by_depth(&depth_data).await.unwrap();

        // Archive should not panic
        depth.archive().await;
    }

    #[tokio::test]
    async fn test_depth_archive_empty() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let depth = Depth::new("BTCUSDT".to_string(), &db).unwrap();

        // Archive empty depth should not panic
        depth.archive().await;
    }

    #[tokio::test]
    async fn test_depth_sorting() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let depth = Depth::new("BTCUSDT".to_string(), &db).unwrap();

        // Create depth data with unsorted prices
        let depth_data = create_test_depth_data(
            "BTCUSDT",
            1000,
            vec![
                ("49900.0", "2.0"),
                ("50000.0", "1.5"),
                ("49800.0", "3.0"),
                ("50100.0", "1.0"),
            ],
            vec![
                ("50300.0", "1.5"),
                ("50100.0", "1.0"),
                ("50500.0", "2.0"),
                ("50200.0", "1.2"),
            ],
        );

        depth.update_by_depth(&depth_data).await.unwrap();

        let depth_arc = depth.get_depth().await;
        let depth_result = (*depth_arc).as_ref().unwrap();

        // Verify bids are sorted in descending order (highest price first)
        for i in 0..depth_result.bids.len() - 1 {
            assert!(depth_result.bids[i].price > depth_result.bids[i + 1].price);
        }

        // Verify asks are sorted in ascending order (lowest price first)
        for i in 0..depth_result.asks.len() - 1 {
            assert!(depth_result.asks[i].price < depth_result.asks[i + 1].price);
        }
    }
}
