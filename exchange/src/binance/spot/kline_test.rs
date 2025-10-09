#[cfg(test)]
mod tests {
    use crate::binance::spot::{
        kline::Kline,
        models::{AggTrade, KlineData},
    };
    use rust_decimal::Decimal;

    fn create_test_kline(symbol: &str, open_time: u64, interval: u64) -> KlineData {
        KlineData {
            symbol: symbol.to_string(),
            open_time,
            close_time: open_time + interval,
            open: Decimal::new(10000, 0),
            high: Decimal::new(10100, 0),
            low: Decimal::new(9900, 0),
            close: Decimal::new(10050, 0),
            volume: Decimal::new(100, 0),
            quote_volume: Decimal::new(1000000, 0),
            trade_count: 1000,
        }
    }

    fn create_test_trade(
        symbol: &str,
        agg_trade_id: u64,
        timestamp: u64,
        price: i64,
        quantity: i64,
    ) -> AggTrade {
        AggTrade {
            symbol: symbol.to_string(),
            agg_trade_id,
            price: Decimal::new(price, 0),
            quantity: Decimal::new(quantity, 0),
            first_trade_id: agg_trade_id * 10,
            last_trade_id: agg_trade_id * 10 + 5,
            timestamp,
            is_buyer_maker: false,
        }
    }

    #[tokio::test]
    async fn test_kline_new() {
        let kline = Kline::new("BTCUSDT".to_string(), 60000, 100, 2, 1000);
        let klines = kline.get_klines().await;
        assert_eq!(klines.len(), 0);
    }

    #[tokio::test]
    async fn test_update_by_kline_success() {
        let interval = 60000; // 1分钟
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let kline_data = create_test_kline("BTCUSDT", 60000, interval);
        let result = kline.update_by_kline(&kline_data).await;
        assert!(result.is_ok());

        let klines = kline.get_klines().await;
        assert_eq!(klines.len(), 1);
        assert_eq!(klines.klines()[0].open_time, 60000);
    }

    #[tokio::test]
    async fn test_update_by_kline_interval_mismatch() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let mut kline_data = create_test_kline("BTCUSDT", 1000000000000, interval);
        kline_data.close_time = kline_data.open_time + 120000; // 错误的间隔

        let result = kline.update_by_kline(&kline_data).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_by_kline_time_not_aligned() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let kline_data = create_test_kline("BTCUSDT", 60001, interval); // 未对齐
        let result = kline.update_by_kline(&kline_data).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_by_kline_symbol_mismatch() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let kline_data = create_test_kline("ETHUSDT", 1000000000000, interval);
        let result = kline.update_by_kline(&kline_data).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_by_kline_multiple() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        // 添加多个连续的 kline
        for i in 0..5 {
            let kline_data = create_test_kline("BTCUSDT", i * interval, interval);
            kline.update_by_kline(&kline_data).await.unwrap();
        }

        let klines = kline.get_klines().await;
        assert_eq!(klines.len(), 5);
    }

    #[tokio::test]
    async fn test_update_by_kline_with_gaps() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        // 添加第一个 kline
        let kline_data1 = create_test_kline("BTCUSDT", 0, interval);
        kline.update_by_kline(&kline_data1).await.unwrap();

        // 跳过一个间隔，添加第三个 kline
        let kline_data3 = create_test_kline("BTCUSDT", 2 * interval, interval);
        kline.update_by_kline(&kline_data3).await.unwrap();

        let klines = kline.get_klines().await;
        assert_eq!(klines.len(), 3); // 应该自动填充缺失的 kline
    }

    #[tokio::test]
    async fn test_update_by_kline_old_data() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let kline_data2 = create_test_kline("BTCUSDT", interval, interval);
        kline.update_by_kline(&kline_data2).await.unwrap();

        // 尝试添加更早的 kline
        let kline_data1 = create_test_kline("BTCUSDT", 0, interval);
        kline.update_by_kline(&kline_data1).await.unwrap();

        let klines = kline.get_klines().await;
        // 旧数据应该被忽略
        assert_eq!(klines.len(), 1);
        assert_eq!(klines.klines()[0].open_time, interval);
    }

    #[tokio::test]
    async fn test_update_by_trade_first_trade() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let trade = create_test_trade("BTCUSDT", 1, 60000, 10000, 1);
        let result = kline.update_by_trade(&trade).await;
        assert!(result.is_ok());

        let klines = kline.get_klines().await;
        assert_eq!(klines.len(), 1);
        let kline_data = &klines.klines()[0];
        assert_eq!(kline_data.open, Decimal::new(10000, 0));
        assert_eq!(kline_data.high, Decimal::new(10000, 0));
        assert_eq!(kline_data.low, Decimal::new(10000, 0));
        assert_eq!(kline_data.close, Decimal::new(10000, 0));
        assert_eq!(kline_data.volume, Decimal::new(1, 0));
    }

    #[tokio::test]
    async fn test_update_by_trade_symbol_mismatch() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let trade = create_test_trade("ETHUSDT", 1, 1000000000000, 10000, 1);
        let result = kline.update_by_trade(&trade).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_by_trade_multiple_in_same_kline() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let base_time = 60000;
        let trades = vec![
            create_test_trade("BTCUSDT", 1, base_time, 10000, 1),
            create_test_trade("BTCUSDT", 2, base_time + 10000, 10100, 2),
            create_test_trade("BTCUSDT", 3, base_time + 20000, 9900, 1),
        ];

        for trade in trades {
            kline.update_by_trade(&trade).await.unwrap();
        }

        let klines = kline.get_klines().await;
        assert_eq!(klines.len(), 1);
        let kline_data = &klines.klines()[0];
        assert_eq!(kline_data.open, Decimal::new(10000, 0));
        assert_eq!(kline_data.high, Decimal::new(10100, 0));
        assert_eq!(kline_data.low, Decimal::new(9900, 0));
        assert_eq!(kline_data.close, Decimal::new(9900, 0));
        assert_eq!(kline_data.volume, Decimal::new(4, 0)); // 1 + 2 + 1
    }

    #[tokio::test]
    async fn test_update_by_trade_across_intervals() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let base_time = 60000;
        let trades = vec![
            create_test_trade("BTCUSDT", 1, base_time, 10000, 1),
            create_test_trade("BTCUSDT", 2, base_time + interval, 10100, 2),
            create_test_trade("BTCUSDT", 3, base_time + 2 * interval, 9900, 3),
        ];

        for trade in trades {
            kline.update_by_trade(&trade).await.unwrap();
        }

        let klines = kline.get_klines().await;
        assert_eq!(klines.len(), 3);
    }

    #[tokio::test]
    async fn test_update_by_trade_with_gaps() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let base_time = 60000;
        let trade1 = create_test_trade("BTCUSDT", 1, base_time, 10000, 1);
        kline.update_by_trade(&trade1).await.unwrap();

        // 跳过一个间隔
        let trade2 = create_test_trade("BTCUSDT", 2, base_time + 2 * interval, 10100, 2);
        kline.update_by_trade(&trade2).await.unwrap();

        let klines = kline.get_klines().await;
        assert_eq!(klines.len(), 3); // 应该自动填充空的 kline
    }

    #[tokio::test]
    async fn test_update_by_trade_old_trade() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let base_time = 60000;
        let trade2 = create_test_trade("BTCUSDT", 2, base_time + interval, 10100, 2);
        kline.update_by_trade(&trade2).await.unwrap();

        // 尝试添加更早的交易
        let trade1 = create_test_trade("BTCUSDT", 1, base_time, 10000, 1);
        kline.update_by_trade(&trade1).await.unwrap();

        let klines = kline.get_klines().await;
        // 旧交易应该被忽略
        assert_eq!(klines.len(), 1);
    }

    #[tokio::test]
    async fn test_mixed_update_kline_and_trade() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let base_time = 60000;

        // 先用 trade 更新
        let trade1 = create_test_trade("BTCUSDT", 1, base_time, 10000, 1);
        kline.update_by_trade(&trade1).await.unwrap();

        // 然后用完整的 kline 覆盖
        let kline_data = create_test_kline("BTCUSDT", base_time, interval);
        kline.update_by_kline(&kline_data).await.unwrap();

        let klines = kline.get_klines().await;
        assert_eq!(klines.len(), 1);
        // 完整的 kline 数据应该覆盖 trade 构建的数据
        assert_eq!(klines.klines()[0].trade_count, 1000);
    }

    #[tokio::test]
    async fn test_archive_functionality() {
        let interval = 60000;
        let max_building = 10;
        let overload_ratio = 2;
        let kline = Kline::new(
            "BTCUSDT".to_string(),
            interval,
            max_building,
            overload_ratio,
            100,
        );

        let base_time = 60000;

        // 添加足够多的 kline 触发归档
        for i in 0..(max_building * overload_ratio + 5) {
            let kline_data =
                create_test_kline("BTCUSDT", base_time + i as u64 * interval, interval);
            kline.update_by_kline(&kline_data).await.unwrap();
        }

        let klines = kline.get_klines().await;
        // 归档后应该保持在合理范围内
        assert!(klines.len() <= max_building * overload_ratio + 5);
    }

    #[tokio::test]
    async fn test_kline_view_iter() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let base_time = 60000;
        for i in 0..5 {
            let kline_data = create_test_kline("BTCUSDT", base_time + i * interval, interval);
            kline.update_by_kline(&kline_data).await.unwrap();
        }

        let klines = kline.get_klines().await;
        let mut count = 0;
        for _ in klines.iter() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_update_by_trade_finalized_kline() {
        let interval = 60000;
        let kline = Kline::new("BTCUSDT".to_string(), interval, 100, 2, 1000);

        let base_time = 60000;

        // 先用完整的 kline 更新
        let kline_data = create_test_kline("BTCUSDT", base_time, interval);
        kline.update_by_kline(&kline_data).await.unwrap();

        // 尝试用 trade 更新已完成的 kline
        let trade = create_test_trade("BTCUSDT", 1, base_time + 1000, 10200, 5);
        kline.update_by_trade(&trade).await.unwrap();

        let klines = kline.get_klines().await;
        let kline_data = &klines.klines()[0];
        // 已完成的 kline 不应该被 trade 更新
        assert_eq!(kline_data.trade_count, 1000); // 保持原来的值
    }
}
