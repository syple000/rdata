#[cfg(test)]
mod tests {
    use crate::binance::spot::{
        depth::Depth,
        models::{DepthData, DepthUpdate, PriceLevel},
    };
    use rust_decimal::Decimal;

    fn create_test_depth_data(symbol: &str, last_update_id: u64) -> DepthData {
        DepthData {
            symbol: symbol.to_string(),
            last_update_id,
            bids: vec![
                PriceLevel {
                    price: Decimal::new(10000, 0),
                    quantity: Decimal::new(1, 0),
                },
                PriceLevel {
                    price: Decimal::new(9900, 0),
                    quantity: Decimal::new(2, 0),
                },
                PriceLevel {
                    price: Decimal::new(9800, 0),
                    quantity: Decimal::new(3, 0),
                },
            ],
            asks: vec![
                PriceLevel {
                    price: Decimal::new(10100, 0),
                    quantity: Decimal::new(1, 0),
                },
                PriceLevel {
                    price: Decimal::new(10200, 0),
                    quantity: Decimal::new(2, 0),
                },
                PriceLevel {
                    price: Decimal::new(10300, 0),
                    quantity: Decimal::new(3, 0),
                },
            ],
        }
    }

    fn create_test_depth_update(
        symbol: &str,
        first_update_id: u64,
        last_update_id: u64,
    ) -> DepthUpdate {
        DepthUpdate {
            symbol: symbol.to_string(),
            first_update_id,
            last_update_id,
            bids: vec![PriceLevel {
                price: Decimal::new(9950, 0),
                quantity: Decimal::new(5, 0),
            }],
            asks: vec![PriceLevel {
                price: Decimal::new(10150, 0),
                quantity: Decimal::new(5, 0),
            }],
            timestamp: 1234567890,
        }
    }

    #[tokio::test]
    async fn test_depth_new() {
        let depth = Depth::new("BTCUSDT".to_string());
        let depth_data = depth.get_depth().await;
        assert!(depth_data.is_none());
    }

    #[tokio::test]
    async fn test_update_by_depth() {
        let depth = Depth::new("BTCUSDT".to_string());
        let depth_data = create_test_depth_data("BTCUSDT", 100);

        let result = depth.update_by_depth(&depth_data).await;
        assert!(result.is_ok());

        let loaded_depth = depth.get_depth().await;
        assert!(loaded_depth.is_some());
        let loaded_depth = loaded_depth.as_ref().as_ref().unwrap();
        assert_eq!(loaded_depth.symbol, "BTCUSDT");
        assert_eq!(loaded_depth.last_update_id, 100);
        assert_eq!(loaded_depth.bids.len(), 3);
        assert_eq!(loaded_depth.asks.len(), 3);

        // 验证 bids 是降序排列
        assert!(loaded_depth.bids[0].price > loaded_depth.bids[1].price);
        assert!(loaded_depth.bids[1].price > loaded_depth.bids[2].price);

        // 验证 asks 是升序排列
        assert!(loaded_depth.asks[0].price < loaded_depth.asks[1].price);
        assert!(loaded_depth.asks[1].price < loaded_depth.asks[2].price);
    }

    #[tokio::test]
    async fn test_update_by_depth_symbol_mismatch() {
        let depth = Depth::new("BTCUSDT".to_string());
        let depth_data = create_test_depth_data("ETHUSDT", 100);

        let result = depth.update_by_depth(&depth_data).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_by_depth_update_success() {
        let depth = Depth::new("BTCUSDT".to_string());
        let depth_data = create_test_depth_data("BTCUSDT", 100);

        // 首先初始化深度数据
        depth.update_by_depth(&depth_data).await.unwrap();

        // 更新深度数据
        let depth_update = create_test_depth_update("BTCUSDT", 101, 102);
        let result = depth.update_by_depth_update(&depth_update).await;
        assert!(result.is_ok());

        let loaded_depth = depth.get_depth().await;
        assert!(loaded_depth.is_some());
        let loaded_depth = loaded_depth.as_ref().as_ref().unwrap();
        assert_eq!(loaded_depth.last_update_id, 102);

        // 验证更新后的数据中包含新增的价格档位
        let has_new_bid = loaded_depth
            .bids
            .iter()
            .any(|b| b.price == Decimal::new(9950, 0));
        let has_new_ask = loaded_depth
            .asks
            .iter()
            .any(|a| a.price == Decimal::new(10150, 0));
        assert!(has_new_bid);
        assert!(has_new_ask);
    }

    #[tokio::test]
    async fn test_update_by_depth_update_not_initialized() {
        let depth = Depth::new("BTCUSDT".to_string());
        let depth_update = create_test_depth_update("BTCUSDT", 101, 102);

        let result = depth.update_by_depth_update(&depth_update).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_by_depth_update_symbol_mismatch() {
        let depth = Depth::new("BTCUSDT".to_string());
        let depth_data = create_test_depth_data("BTCUSDT", 100);
        depth.update_by_depth(&depth_data).await.unwrap();

        let depth_update = create_test_depth_update("ETHUSDT", 101, 102);
        let result = depth.update_by_depth_update(&depth_update).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_by_depth_update_old_update() {
        let depth = Depth::new("BTCUSDT".to_string());
        let depth_data = create_test_depth_data("BTCUSDT", 100);
        depth.update_by_depth(&depth_data).await.unwrap();

        // 尝试使用旧的 update_id 更新
        let depth_update = create_test_depth_update("BTCUSDT", 90, 95);
        let result = depth.update_by_depth_update(&depth_update).await;
        assert!(result.is_ok()); // 应该忽略旧的更新

        let loaded_depth = depth.get_depth().await;
        assert_eq!(loaded_depth.as_ref().as_ref().unwrap().last_update_id, 100);
    }

    #[tokio::test]
    async fn test_update_by_depth_update_out_of_order() {
        let depth = Depth::new("BTCUSDT".to_string());
        let depth_data = create_test_depth_data("BTCUSDT", 100);
        depth.update_by_depth(&depth_data).await.unwrap();

        // 跳过一些更新，导致顺序不连续
        let depth_update = create_test_depth_update("BTCUSDT", 110, 120);
        let result = depth.update_by_depth_update(&depth_update).await;
        assert!(result.is_err()); // 应该返回错误
    }

    #[tokio::test]
    async fn test_update_by_depth_update_remove_price_level() {
        let depth = Depth::new("BTCUSDT".to_string());
        let depth_data = create_test_depth_data("BTCUSDT", 100);
        depth.update_by_depth(&depth_data).await.unwrap();

        // 移除一个价格档位（quantity 为 0）
        let depth_update = DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            first_update_id: 101,
            last_update_id: 102,
            bids: vec![PriceLevel {
                price: Decimal::new(10000, 0), // 移除原有的 bid
                quantity: Decimal::new(0, 0),
            }],
            asks: vec![PriceLevel {
                price: Decimal::new(10100, 0), // 移除原有的 ask
                quantity: Decimal::new(0, 0),
            }],
            timestamp: 1234567890,
        };

        depth.update_by_depth_update(&depth_update).await.unwrap();

        let loaded_depth = depth.get_depth().await;
        let loaded_depth = loaded_depth.as_ref().as_ref().unwrap();

        // 验证价格档位已被移除
        let has_removed_bid = loaded_depth
            .bids
            .iter()
            .any(|b| b.price == Decimal::new(10000, 0));
        let has_removed_ask = loaded_depth
            .asks
            .iter()
            .any(|a| a.price == Decimal::new(10100, 0));
        assert!(!has_removed_bid);
        assert!(!has_removed_ask);
    }

    #[tokio::test]
    async fn test_multiple_updates() {
        let depth = Depth::new("BTCUSDT".to_string());
        let depth_data = create_test_depth_data("BTCUSDT", 100);
        depth.update_by_depth(&depth_data).await.unwrap();

        // 连续更新多次
        for i in 1..=5 {
            let depth_update = DepthUpdate {
                symbol: "BTCUSDT".to_string(),
                first_update_id: 100 + i,
                last_update_id: 100 + i,
                bids: vec![PriceLevel {
                    price: Decimal::new((9900 + i * 10) as i64, 0),
                    quantity: Decimal::new(i as i64, 0),
                }],
                asks: vec![PriceLevel {
                    price: Decimal::new((10100 + i * 10) as i64, 0),
                    quantity: Decimal::new(i as i64, 0),
                }],
                timestamp: 1234567890 + i,
            };
            depth.update_by_depth_update(&depth_update).await.unwrap();
        }

        let loaded_depth = depth.get_depth().await;
        let loaded_depth = loaded_depth.as_ref().as_ref().unwrap();
        assert_eq!(loaded_depth.last_update_id, 105);
    }
}
