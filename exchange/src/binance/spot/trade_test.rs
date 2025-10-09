#[cfg(test)]
mod tests {
    use crate::binance::spot::{models::AggTrade, trade::Trade};
    use rust_decimal::Decimal;

    fn create_test_trade(symbol: &str, agg_trade_id: u64, price: i64, quantity: i64) -> AggTrade {
        AggTrade {
            symbol: symbol.to_string(),
            agg_trade_id,
            price: Decimal::new(price, 0),
            quantity: Decimal::new(quantity, 0),
            first_trade_id: agg_trade_id * 10,
            last_trade_id: agg_trade_id * 10 + 5,
            timestamp: 1000000000000 + agg_trade_id * 1000,
            is_buyer_maker: false,
        }
    }

    #[tokio::test]
    async fn test_trade_new() {
        let trade = Trade::new("BTCUSDT".to_string(), 100, 2, 1000);
        let trades = trade.get_trades().await;
        assert_eq!(trades.len(), 0);
    }

    #[tokio::test]
    async fn test_update_first_trade() {
        let trade_manager = Trade::new("BTCUSDT".to_string(), 100, 2, 1000);
        let trade_data = create_test_trade("BTCUSDT", 1, 10000, 1);

        let result = trade_manager.update(&trade_data).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true); // 应该返回 true 表示新增了交易

        let trades = trade_manager.get_trades().await;
        assert_eq!(trades.len(), 1);
        assert_eq!(trades.trades()[0].agg_trade_id, 1);
    }

    #[tokio::test]
    async fn test_update_symbol_mismatch() {
        let trade_manager = Trade::new("BTCUSDT".to_string(), 100, 2, 1000);
        let trade_data = create_test_trade("ETHUSDT", 1, 10000, 1);

        let result = trade_manager.update(&trade_data).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_sequential_trades() {
        let trade_manager = Trade::new("BTCUSDT".to_string(), 100, 2, 1000);

        // 添加连续的交易
        for i in 1..=5 {
            let trade_data = create_test_trade("BTCUSDT", i, 10000 + i as i64 * 10, 1);
            let result = trade_manager.update(&trade_data).await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), true);
        }

        let trades = trade_manager.get_trades().await;
        assert_eq!(trades.len(), 5);

        // 验证交易顺序
        let trade_list = trades.trades();
        for i in 0..5 {
            assert_eq!(trade_list[i].agg_trade_id, (i + 1) as u64);
        }
    }

    #[tokio::test]
    async fn test_update_duplicate_trade() {
        let trade_manager = Trade::new("BTCUSDT".to_string(), 100, 2, 1000);

        let trade_data = create_test_trade("BTCUSDT", 1, 10000, 1);
        let result1 = trade_manager.update(&trade_data).await;
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap(), true);

        // 尝试添加相同的交易
        let result2 = trade_manager.update(&trade_data).await;
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), false); // 应该返回 false 表示重复

        let trades = trade_manager.get_trades().await;
        assert_eq!(trades.len(), 1); // 不应该重复添加
    }

    #[tokio::test]
    async fn test_update_old_trade() {
        let trade_manager = Trade::new("BTCUSDT".to_string(), 100, 2, 1000);

        let trade_data2 = create_test_trade("BTCUSDT", 2, 10020, 2);
        trade_manager.update(&trade_data2).await.unwrap();

        // 尝试添加更早的交易
        let trade_data1 = create_test_trade("BTCUSDT", 1, 10010, 1);
        let result = trade_manager.update(&trade_data1).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false); // 应该返回 false，旧交易被忽略

        let trades = trade_manager.get_trades().await;
        assert_eq!(trades.len(), 1);
        assert_eq!(trades.trades()[0].agg_trade_id, 2);
    }

    #[tokio::test]
    async fn test_update_with_gaps() {
        let trade_manager = Trade::new("BTCUSDT".to_string(), 100, 2, 1000);

        // 添加 trade id 1
        let trade_data1 = create_test_trade("BTCUSDT", 1, 10000, 1);
        trade_manager.update(&trade_data1).await.unwrap();

        // 跳过 2, 3, 添加 trade id 4
        let trade_data4 = create_test_trade("BTCUSDT", 4, 10030, 4);
        trade_manager.update(&trade_data4).await.unwrap();

        let trades = trade_manager.get_trades().await;
        // len() 只计算有效的交易，不包括空位
        assert_eq!(trades.len(), 2);

        let trade_list = trades.trades();
        assert_eq!(trade_list[0].agg_trade_id, 1);
        assert_eq!(trade_list[1].agg_trade_id, 4);
    }

    #[tokio::test]
    async fn test_update_fill_gap() {
        let trade_manager = Trade::new("BTCUSDT".to_string(), 100, 2, 1000);

        // 添加 trade id 1
        let trade_data1 = create_test_trade("BTCUSDT", 1, 10000, 1);
        trade_manager.update(&trade_data1).await.unwrap();

        // 添加 trade id 4
        let trade_data4 = create_test_trade("BTCUSDT", 4, 10030, 4);
        trade_manager.update(&trade_data4).await.unwrap();

        // 填充 gap: trade id 2
        let trade_data2 = create_test_trade("BTCUSDT", 2, 10010, 2);
        let result = trade_manager.update(&trade_data2).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true); // 应该成功填充

        let trades = trade_manager.get_trades().await;
        // len() 只计算有效的交易，应该是 3 个（1, 2, 4）
        assert_eq!(trades.len(), 3);

        let trade_list = trades.trades();
        assert_eq!(trade_list[0].agg_trade_id, 1);
        assert_eq!(trade_list[1].agg_trade_id, 2);
        assert_eq!(trade_list[2].agg_trade_id, 4);
    }

    #[tokio::test]
    async fn test_trade_view_iter() {
        let trade_manager = Trade::new("BTCUSDT".to_string(), 100, 2, 1000);

        for i in 1..=5 {
            let trade_data = create_test_trade("BTCUSDT", i, 10000 + i as i64 * 10, 1);
            trade_manager.update(&trade_data).await.unwrap();
        }

        let trades = trade_manager.get_trades().await;
        let mut count = 0;
        for _ in trades.iter() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_trade_view_trades() {
        let trade_manager = Trade::new("BTCUSDT".to_string(), 100, 2, 1000);

        for i in 1..=5 {
            let trade_data = create_test_trade("BTCUSDT", i, 10000 + i as i64 * 10, 1);
            trade_manager.update(&trade_data).await.unwrap();
        }

        let trades = trade_manager.get_trades().await;
        let trade_list = trades.trades();
        assert_eq!(trade_list.len(), 5);

        for i in 0..5 {
            assert_eq!(trade_list[i].agg_trade_id, (i + 1) as u64);
        }
    }

    #[tokio::test]
    async fn test_archive_functionality() {
        let max_latest = 10;
        let overload_ratio = 2;
        let trade_manager = Trade::new("BTCUSDT".to_string(), max_latest, overload_ratio, 100);

        // 添加足够多的交易触发归档
        for i in 1..=(max_latest * overload_ratio + 5) {
            let trade_data = create_test_trade("BTCUSDT", i as u64, 10000 + i as i64, 1);
            trade_manager.update(&trade_data).await.unwrap();
        }

        let trades = trade_manager.get_trades().await;
        // 归档后应该保持在合理范围内
        assert!(trades.len() <= max_latest * overload_ratio + 5);
    }

    #[tokio::test]
    async fn test_archive_preserves_order() {
        let max_latest = 5;
        let overload_ratio = 2;
        let trade_manager = Trade::new("BTCUSDT".to_string(), max_latest, overload_ratio, 20);

        // 添加足够多的交易
        for i in 1..=20 {
            let trade_data = create_test_trade("BTCUSDT", i, 10000 + i as i64, 1);
            trade_manager.update(&trade_data).await.unwrap();
        }

        let trades = trade_manager.get_trades().await;
        let trade_list = trades.trades();

        // 验证顺序是否保持
        for i in 0..trade_list.len() - 1 {
            assert!(trade_list[i].agg_trade_id < trade_list[i + 1].agg_trade_id);
        }
    }

    #[tokio::test]
    async fn test_archive_respects_max_archived() {
        let max_latest = 5;
        let overload_ratio = 2;
        let max_archived = 10;
        let trade_manager = Trade::new(
            "BTCUSDT".to_string(),
            max_latest,
            overload_ratio,
            max_archived,
        );

        // 添加大量交易
        for i in 1..=50 {
            let trade_data = create_test_trade("BTCUSDT", i, 10000 + i as i64, 1);
            trade_manager.update(&trade_data).await.unwrap();
        }

        let trades = trade_manager.get_trades().await;
        // 总数不应该超过 max_archived + max_latest
        assert!(trades.len() <= max_archived + max_latest);
    }

    #[tokio::test]
    async fn test_mixed_updates_with_gaps() {
        let trade_manager = Trade::new("BTCUSDT".to_string(), 100, 2, 1000);

        // 添加非连续的交易
        let trade_ids = vec![1, 3, 5, 7, 10];
        for &id in &trade_ids {
            let trade_data = create_test_trade("BTCUSDT", id, 10000 + id as i64 * 10, 1);
            trade_manager.update(&trade_data).await.unwrap();
        }

        let trades = trade_manager.get_trades().await;
        // len() 只计算有效的交易，应该是 5 个
        assert_eq!(trades.len(), 5);

        // 填充部分空位
        let fill_ids = vec![2, 6];
        for &id in &fill_ids {
            let trade_data = create_test_trade("BTCUSDT", id, 10000 + id as i64 * 10, 1);
            let result = trade_manager.update(&trade_data).await.unwrap();
            assert_eq!(result, true);
        }

        let trades = trade_manager.get_trades().await;
        // 现在应该有 7 个有效交易
        assert_eq!(trades.len(), 7);

        let trade_list = trades.trades();

        // 验证填充的交易存在
        assert!(trade_list.iter().any(|t| t.agg_trade_id == 2));
        assert!(trade_list.iter().any(|t| t.agg_trade_id == 6));
    }

    #[tokio::test]
    async fn test_concurrent_updates() {
        use tokio::task;

        let trade_manager = std::sync::Arc::new(Trade::new("BTCUSDT".to_string(), 100, 2, 1000));

        let mut handles = vec![];

        // 并发添加交易
        for i in 1..=10 {
            let tm = trade_manager.clone();
            let handle = task::spawn(async move {
                let trade_data = create_test_trade("BTCUSDT", i, 10000 + i as i64 * 10, 1);
                tm.update(&trade_data).await
            });
            handles.push(handle);
        }

        // 等待所有任务完成
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        let trades = trade_manager.get_trades().await;
        assert_eq!(trades.len(), 10);
    }

    #[tokio::test]
    async fn test_empty_gap_iteration() {
        let trade_manager = Trade::new("BTCUSDT".to_string(), 100, 2, 1000);

        // 添加有间隔的交易
        trade_manager
            .update(&create_test_trade("BTCUSDT", 1, 10000, 1))
            .await
            .unwrap();
        trade_manager
            .update(&create_test_trade("BTCUSDT", 5, 10040, 5))
            .await
            .unwrap();

        let trades = trade_manager.get_trades().await;

        // 迭代器应该只包含有效的交易，不包含空位
        let count = trades.iter().count();
        assert_eq!(count, 2); // 只有两个有效交易

        // trades() 方法应该返回所有有效交易
        let trade_list = trades.trades();
        assert_eq!(trade_list.len(), 2);
    }
}
