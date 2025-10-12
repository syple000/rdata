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
        let depth = Depth::new("BTCUSDT".to_string());
        let depth_data = depth.get_depth().await;
        assert!(depth_data.is_none());
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_basic() {
        let depth = Depth::new("BTCUSDT".to_string());

        let depth_data = create_test_depth_data(
            "BTCUSDT",
            100,
            vec![("50000.0", "1.5"), ("49900.0", "2.0")],
            vec![("50100.0", "1.2"), ("50200.0", "1.8")],
        );

        let result = depth.update_by_depth(&depth_data).await;
        assert!(result.is_ok());

        let retrieved = depth.get_depth().await;
        assert!(retrieved.is_some());

        let data = retrieved.as_ref().as_ref().unwrap();
        assert_eq!(data.symbol, "BTCUSDT");
        assert_eq!(data.last_update_id, 100);
        assert_eq!(data.bids.len(), 2);
        assert_eq!(data.asks.len(), 2);

        // Check bids are sorted descending by price
        assert!(data.bids[0].price > data.bids[1].price);
        assert_eq!(data.bids[0].price, Decimal::from_str("50000.0").unwrap());
        assert_eq!(data.bids[1].price, Decimal::from_str("49900.0").unwrap());

        // Check asks are sorted ascending by price
        assert!(data.asks[0].price < data.asks[1].price);
        assert_eq!(data.asks[0].price, Decimal::from_str("50100.0").unwrap());
        assert_eq!(data.asks[1].price, Decimal::from_str("50200.0").unwrap());
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_wrong_symbol() {
        let depth = Depth::new("BTCUSDT".to_string());

        let depth_data = create_test_depth_data(
            "ETHUSDT",
            100,
            vec![("3000.0", "10.0")],
            vec![("3100.0", "12.0")],
        );

        let result = depth.update_by_depth(&depth_data).await;
        assert!(result.is_err());

        if let Err(e) = result {
            assert!(e.to_string().contains("symbol mismatch"));
        }
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_replace() {
        let depth = Depth::new("BTCUSDT".to_string());

        // First update
        let depth_data1 = create_test_depth_data(
            "BTCUSDT",
            100,
            vec![("50000.0", "1.5")],
            vec![("50100.0", "1.2")],
        );
        depth.update_by_depth(&depth_data1).await.unwrap();

        // Second update should replace the first
        let depth_data2 = create_test_depth_data(
            "BTCUSDT",
            200,
            vec![("51000.0", "2.5"), ("50900.0", "3.0")],
            vec![("51100.0", "2.2")],
        );
        depth.update_by_depth(&depth_data2).await.unwrap();

        let retrieved = depth.get_depth().await;
        let data = retrieved.as_ref().as_ref().unwrap();
        assert_eq!(data.last_update_id, 200);
        assert_eq!(data.bids.len(), 2);
        assert_eq!(data.asks.len(), 1);
        assert_eq!(data.bids[0].price, Decimal::from_str("51000.0").unwrap());
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_update_basic() {
        let depth = Depth::new("BTCUSDT".to_string());

        // Initialize depth
        let initial_depth = create_test_depth_data(
            "BTCUSDT",
            100,
            vec![("50000.0", "1.5"), ("49900.0", "2.0")],
            vec![("50100.0", "1.2"), ("50200.0", "1.8")],
        );
        depth.update_by_depth(&initial_depth).await.unwrap();

        // Update with new data
        let update = create_test_depth_update(
            "BTCUSDT",
            101,
            101,
            vec![("50000.0", "2.5")], // Update existing bid
            vec![("50300.0", "1.0")], // Add new ask
            1234567890,
        );

        let result = depth.update_by_depth_update(&update).await;
        assert!(result.is_ok());

        let retrieved = depth.get_depth().await;
        let data = retrieved.as_ref().as_ref().unwrap();
        assert_eq!(data.last_update_id, 101);
        assert_eq!(data.bids.len(), 2);
        assert_eq!(data.asks.len(), 3);

        // Check the bid was updated
        let bid_50000 = data
            .bids
            .iter()
            .find(|b| b.price == Decimal::from_str("50000.0").unwrap())
            .unwrap();
        assert_eq!(bid_50000.quantity, Decimal::from_str("2.5").unwrap());
    }

    #[tokio::test]
    async fn test_depth_update_by_depth_update_remove_level() {
        let depth = Depth::new("BTCUSDT".to_string());

        // Initialize depth
        let initial_depth = create_test_depth_data(
            "BTCUSDT",
            100,
            vec![("50000.0", "1.5"), ("49900.0", "2.0")],
            vec![("50100.0", "1.2"), ("50200.0", "1.8")],
        );
        depth.update_by_depth(&initial_depth).await.unwrap();

        // Update to remove a price level (quantity = 0)
        let update = create_test_depth_update(
            "BTCUSDT",
            101,
            101,
            vec![("50000.0", "0")], // Remove bid
            vec![("50100.0", "0")], // Remove ask
            1234567890,
        );

        depth.update_by_depth_update(&update).await.unwrap();

        let retrieved = depth.get_depth().await;
        let data = retrieved.as_ref().as_ref().unwrap();
        assert_eq!(data.bids.len(), 1);
        assert_eq!(data.asks.len(), 1);
        assert_eq!(data.bids[0].price, Decimal::from_str("49900.0").unwrap());
        assert_eq!(data.asks[0].price, Decimal::from_str("50200.0").unwrap());
    }

    #[tokio::test]
    async fn test_depth_update_not_initialized() {
        let depth = Depth::new("BTCUSDT".to_string());

        // Try to update without initializing
        let update = create_test_depth_update(
            "BTCUSDT",
            101,
            101,
            vec![("50000.0", "1.5")],
            vec![("50100.0", "1.2")],
            1234567890,
        );

        let result = depth.update_by_depth_update(&update).await;
        assert!(result.is_err());

        if let Err(e) = result {
            assert!(e.to_string().contains("not initialized"));
        }
    }

    #[tokio::test]
    async fn test_depth_update_wrong_symbol() {
        let depth = Depth::new("BTCUSDT".to_string());

        // Initialize depth
        let initial_depth = create_test_depth_data(
            "BTCUSDT",
            100,
            vec![("50000.0", "1.5")],
            vec![("50100.0", "1.2")],
        );
        depth.update_by_depth(&initial_depth).await.unwrap();

        // Try to update with wrong symbol
        let update = create_test_depth_update(
            "ETHUSDT",
            101,
            101,
            vec![("3000.0", "10.0")],
            vec![("3100.0", "12.0")],
            1234567890,
        );

        let result = depth.update_by_depth_update(&update).await;
        assert!(result.is_err());

        if let Err(e) = result {
            assert!(e.to_string().contains("symbol mismatch"));
        }
    }

    #[tokio::test]
    async fn test_depth_update_out_of_order() {
        let depth = Depth::new("BTCUSDT".to_string());

        // Initialize depth
        let initial_depth = create_test_depth_data(
            "BTCUSDT",
            100,
            vec![("50000.0", "1.5")],
            vec![("50100.0", "1.2")],
        );
        depth.update_by_depth(&initial_depth).await.unwrap();

        // Try to update with out-of-order update (first_update_id > last_update_id + 1)
        let update = create_test_depth_update(
            "BTCUSDT",
            105, // Gap: 105 > 100 + 1
            110,
            vec![("50000.0", "2.0")],
            vec![("50100.0", "2.5")],
            1234567890,
        );

        let result = depth.update_by_depth_update(&update).await;
        assert!(result.is_err());

        if let Err(e) = result {
            assert!(e.to_string().contains("out of order"));
        }
    }

    #[tokio::test]
    async fn test_depth_update_ignore_old_update() {
        let depth = Depth::new("BTCUSDT".to_string());

        // Initialize depth
        let initial_depth = create_test_depth_data(
            "BTCUSDT",
            100,
            vec![("50000.0", "1.5")],
            vec![("50100.0", "1.2")],
        );
        depth.update_by_depth(&initial_depth).await.unwrap();

        // Try to update with older update (last_update_id <= current last_update_id)
        let update = create_test_depth_update(
            "BTCUSDT",
            95,
            100,
            vec![("50000.0", "2.0")],
            vec![("50100.0", "2.5")],
            1234567890,
        );

        let result = depth.update_by_depth_update(&update).await;
        assert!(result.is_ok());

        // Verify depth was not updated
        let retrieved = depth.get_depth().await;
        let data = retrieved.as_ref().as_ref().unwrap();
        assert_eq!(data.last_update_id, 100);
        assert_eq!(data.bids[0].quantity, Decimal::from_str("1.5").unwrap());
    }

    #[tokio::test]
    async fn test_depth_update_sequential_updates() {
        let depth = Depth::new("BTCUSDT".to_string());

        // Initialize depth
        let initial_depth = create_test_depth_data(
            "BTCUSDT",
            100,
            vec![("50000.0", "1.0"), ("49900.0", "2.0"), ("49800.0", "3.0")],
            vec![("50100.0", "1.0"), ("50200.0", "2.0"), ("50300.0", "3.0")],
        );
        depth.update_by_depth(&initial_depth).await.unwrap();

        // Sequential update 1
        let update1 = create_test_depth_update(
            "BTCUSDT",
            101,
            102,
            vec![("50000.0", "1.5"), ("49700.0", "1.0")], // Update one, add one
            vec![("50100.0", "0")],                       // Remove one
            1234567890,
        );
        depth.update_by_depth_update(&update1).await.unwrap();

        let retrieved = depth.get_depth().await;
        let data = retrieved.as_ref().as_ref().unwrap();
        assert_eq!(data.last_update_id, 102);
        assert_eq!(data.bids.len(), 4); // 3 original - 0 + 1 new
        assert_eq!(data.asks.len(), 2); // 3 original - 1

        // Sequential update 2
        let update2 = create_test_depth_update(
            "BTCUSDT",
            103,
            104,
            vec![("49800.0", "0"), ("50100.0", "2.0")], // Remove one, add new
            vec![("50200.0", "5.0")],                   // Update one
            1234567891,
        );
        depth.update_by_depth_update(&update2).await.unwrap();

        let retrieved = depth.get_depth().await;
        let data = retrieved.as_ref().as_ref().unwrap();
        assert_eq!(data.last_update_id, 104);
        assert_eq!(data.bids.len(), 4); // 4 - 1 + 1
        assert_eq!(data.asks.len(), 2);

        // Verify specific updates
        let ask_50200 = data
            .asks
            .iter()
            .find(|a| a.price == Decimal::from_str("50200.0").unwrap())
            .unwrap();
        assert_eq!(ask_50200.quantity, Decimal::from_str("5.0").unwrap());
    }

    #[tokio::test]
    async fn test_depth_sorting() {
        let depth = Depth::new("BTCUSDT".to_string());

        // Create depth with unsorted data
        let depth_data = create_test_depth_data(
            "BTCUSDT",
            100,
            vec![
                ("49900.0", "2.0"),
                ("50000.0", "1.5"),
                ("49800.0", "3.0"),
                ("50100.0", "0.5"),
            ],
            vec![
                ("50300.0", "1.8"),
                ("50100.0", "1.2"),
                ("50500.0", "2.5"),
                ("50200.0", "1.0"),
            ],
        );

        depth.update_by_depth(&depth_data).await.unwrap();

        let retrieved = depth.get_depth().await;
        let data = retrieved.as_ref().as_ref().unwrap();

        // Verify bids are sorted descending
        for i in 0..data.bids.len() - 1 {
            assert!(data.bids[i].price > data.bids[i + 1].price);
        }
        assert_eq!(data.bids[0].price, Decimal::from_str("50100.0").unwrap());
        assert_eq!(
            data.bids[data.bids.len() - 1].price,
            Decimal::from_str("49800.0").unwrap()
        );

        // Verify asks are sorted ascending
        for i in 0..data.asks.len() - 1 {
            assert!(data.asks[i].price < data.asks[i + 1].price);
        }
        assert_eq!(data.asks[0].price, Decimal::from_str("50100.0").unwrap());
        assert_eq!(
            data.asks[data.asks.len() - 1].price,
            Decimal::from_str("50500.0").unwrap()
        );
    }

    #[tokio::test]
    async fn test_depth_empty_orderbook() {
        let depth = Depth::new("BTCUSDT".to_string());

        // Create depth with empty bids and asks
        let depth_data = create_test_depth_data("BTCUSDT", 100, vec![], vec![]);

        depth.update_by_depth(&depth_data).await.unwrap();

        let retrieved = depth.get_depth().await;
        let data = retrieved.as_ref().as_ref().unwrap();
        assert_eq!(data.bids.len(), 0);
        assert_eq!(data.asks.len(), 0);
    }

    #[tokio::test]
    async fn test_depth_get_depth_before_update() {
        let depth = Depth::new("BTCUSDT".to_string());

        // Call get_depth multiple times before any update
        let data1 = depth.get_depth().await;
        assert!(data1.is_none());

        let data2 = depth.get_depth().await;
        assert!(data2.is_none());
    }

    #[tokio::test]
    async fn test_depth_concurrent_updates() {
        let depth = Depth::new("BTCUSDT".to_string());

        // Initialize depth
        let initial_depth = create_test_depth_data(
            "BTCUSDT",
            100,
            vec![("50000.0", "1.0")],
            vec![("50100.0", "1.0")],
        );
        depth.update_by_depth(&initial_depth).await.unwrap();

        // Simulate multiple updates in sequence
        let updates = vec![
            create_test_depth_update("BTCUSDT", 101, 101, vec![("50000.0", "2.0")], vec![], 1000),
            create_test_depth_update("BTCUSDT", 102, 102, vec![("50000.0", "3.0")], vec![], 2000),
            create_test_depth_update("BTCUSDT", 103, 103, vec![("50000.0", "4.0")], vec![], 3000),
        ];

        for update in updates {
            depth.update_by_depth_update(&update).await.unwrap();
        }

        let retrieved = depth.get_depth().await;
        let data = retrieved.as_ref().as_ref().unwrap();
        assert_eq!(data.last_update_id, 103);
        assert_eq!(data.bids[0].quantity, Decimal::from_str("4.0").unwrap());
    }
}
