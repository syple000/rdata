#[cfg(test)]
mod tests_market_data_cache {
    use crate::data_manager::market_data_cache::MarketDataCache;
    use crate::models::{KlineData, KlineInterval, Trade};
    use rust_decimal::Decimal;

    // ============================================================================
    // Helper functions
    // ============================================================================

    fn create_kline(
        symbol: &str,
        interval: KlineInterval,
        open_time: u64,
        close_time: u64,
    ) -> KlineData {
        KlineData {
            symbol: symbol.to_string(),
            interval,
            open_time,
            close_time,
            open: Decimal::from(100),
            high: Decimal::from(110),
            low: Decimal::from(90),
            close: Decimal::from(105),
            volume: Decimal::from(1000),
            quote_volume: Decimal::from(100000),
            is_closed: true,
        }
    }

    fn create_trade(symbol: &str, seq_id: u64, price: u64, quantity: u64, timestamp: u64) -> Trade {
        Trade {
            symbol: symbol.to_string(),
            trade_id: format!("trade_{}", seq_id),
            price: Decimal::from(price),
            quantity: Decimal::from(quantity),
            timestamp,
            is_buyer_maker: seq_id % 2 == 0,
            seq_id,
        }
    }

    // ============================================================================
    // KlineCache Tests
    // ============================================================================

    #[tokio::test]
    async fn test_kline_add_single() {
        let cache = MarketDataCache::new(100, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();
        let kline = create_kline(
            "BTCUSDT",
            interval.clone(),
            interval_ms,
            2 * interval_ms - 1,
        );

        let result = cache.add_kline(kline.clone()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0, "No eviction on first add");
    }

    #[tokio::test]
    async fn test_kline_get_single() {
        let cache = MarketDataCache::new(100, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();
        let open_time = interval_ms;
        let kline = create_kline("BTCUSDT", interval.clone(), open_time, 2 * interval_ms - 1);

        cache.add_kline(kline).await.unwrap();

        let result = cache.get_klines("BTCUSDT", interval.clone(), Some(1)).await;
        assert!(result.is_ok());
        let klines = result.unwrap();
        assert_eq!(klines.len(), 1);
        assert!(klines[0].is_some());
        assert_eq!(klines[0].as_ref().unwrap().open_time, open_time);
    }

    #[tokio::test]
    async fn test_kline_sequential_add() {
        let cache = MarketDataCache::new(100, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // Add 10 sequential klines, starting from interval_ms (not 0)
        for i in 1..=10 {
            let open_time = i * interval_ms;
            let kline = create_kline(
                "BTCUSDT",
                interval.clone(),
                open_time,
                open_time + interval_ms - 1,
            );
            let result = cache.add_kline(kline).await;
            assert!(result.is_ok(), "Failed to add kline at i={}", i);
            assert_eq!(result.unwrap().len(), 0);
        }

        // Get all 10 klines
        let result = cache
            .get_klines("BTCUSDT", interval.clone(), Some(10))
            .await;
        assert!(result.is_ok());
        let klines = result.unwrap();
        assert_eq!(klines.len(), 10);

        // Verify all are present and in order
        for i in 0..10 {
            assert!(klines[i].is_some());
            assert_eq!(
                klines[i].as_ref().unwrap().open_time,
                (i as u64 + 1) * interval_ms
            );
        }
    }

    #[tokio::test]
    async fn test_kline_out_of_order_small() {
        let cache = MarketDataCache::new(100, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // Add klines 1, 3, 2 (small disorder)
        let kline1 = create_kline(
            "BTCUSDT",
            interval.clone(),
            1 * interval_ms,
            2 * interval_ms - 1,
        );
        let kline3 = create_kline(
            "BTCUSDT",
            interval.clone(),
            3 * interval_ms,
            4 * interval_ms - 1,
        );
        let kline2 = create_kline(
            "BTCUSDT",
            interval.clone(),
            2 * interval_ms,
            3 * interval_ms - 1,
        );

        cache.add_kline(kline1).await.unwrap();
        cache.add_kline(kline3).await.unwrap();
        cache.add_kline(kline2).await.unwrap();

        // Get all 3 klines
        let result = cache.get_klines("BTCUSDT", interval.clone(), Some(3)).await;
        assert!(result.is_ok());
        let klines = result.unwrap();
        assert_eq!(klines.len(), 3);

        // Verify all are present
        assert!(klines[0].is_some());
        assert!(klines[1].is_some());
        assert!(klines[2].is_some());

        // Verify correct times
        assert_eq!(klines[0].as_ref().unwrap().open_time, 1 * interval_ms);
        assert_eq!(klines[1].as_ref().unwrap().open_time, 2 * interval_ms);
        assert_eq!(klines[2].as_ref().unwrap().open_time, 3 * interval_ms);
    }

    #[tokio::test]
    async fn test_kline_out_of_order_large() {
        let cache = MarketDataCache::new(100, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // Add klines in reverse order (large disorder), starting from 1
        for i in (1..=10).rev() {
            let open_time = i * interval_ms;
            let kline = create_kline(
                "BTCUSDT",
                interval.clone(),
                open_time,
                open_time + interval_ms - 1,
            );
            let result = cache.add_kline(kline).await;
            assert!(result.is_ok());
        }

        // Get all 10 klines
        let result = cache
            .get_klines("BTCUSDT", interval.clone(), Some(10))
            .await;
        assert!(result.is_ok());
        let klines = result.unwrap();
        assert_eq!(klines.len(), 10);

        // Verify all are present and properly ordered
        for i in 0..10 {
            assert!(klines[i].is_some(), "Kline at index {} should exist", i);
            assert_eq!(
                klines[i].as_ref().unwrap().open_time,
                (i as u64 + 1) * interval_ms,
                "Kline at index {} has wrong open_time",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_kline_gap_small() {
        let cache = MarketDataCache::new(100, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // Add klines 1, 3, 5 (small gaps)
        for i in [1, 3, 5].iter() {
            let open_time = i * interval_ms;
            let kline = create_kline(
                "BTCUSDT",
                interval.clone(),
                open_time,
                open_time + interval_ms - 1,
            );
            cache.add_kline(kline).await.unwrap();
        }

        // Get 5 klines
        let result = cache.get_klines("BTCUSDT", interval.clone(), Some(5)).await;
        assert!(result.is_ok());
        let klines = result.unwrap();
        assert_eq!(klines.len(), 5);

        // Verify gaps: position 0 (id 1), none (gap), position 2 (id 3), none (gap), position 4 (id 5)
        assert!(klines[0].is_some(), "Index 0 should have data");
        assert!(klines[1].is_none(), "Index 1 should be None (gap)");
        assert!(klines[2].is_some(), "Index 2 should have data");
        assert!(klines[3].is_none(), "Index 3 should be None (gap)");
        assert!(klines[4].is_some(), "Index 4 should have data");
    }

    #[tokio::test]
    async fn test_kline_gap_large() {
        let cache = MarketDataCache::new(100, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // Add kline at 1
        let kline1 = create_kline(
            "BTCUSDT",
            interval.clone(),
            1 * interval_ms,
            2 * interval_ms - 1,
        );
        cache.add_kline(kline1).await.unwrap();

        // Add kline at index 51 (large gap)
        let kline51 = create_kline(
            "BTCUSDT",
            interval.clone(),
            51 * interval_ms,
            52 * interval_ms - 1,
        );
        let result = cache.add_kline(kline51).await;
        assert!(result.is_ok());

        // Get 51 klines
        let result = cache
            .get_klines("BTCUSDT", interval.clone(), Some(51))
            .await;
        assert!(result.is_ok());
        let klines = result.unwrap();
        assert_eq!(klines.len(), 51);

        // First should exist
        assert!(klines[0].is_some());
        assert_eq!(klines[0].as_ref().unwrap().open_time, 1 * interval_ms);

        // Middle should be None
        for i in 1..50 {
            assert!(
                klines[i].is_none(),
                "Kline at index {} should be None (gap)",
                i
            );
        }

        // Last should exist
        assert!(klines[50].is_some());
        assert_eq!(klines[50].as_ref().unwrap().open_time, 51 * interval_ms);
    }

    #[tokio::test]
    async fn test_kline_capacity_eviction() {
        let capacity = 10;
        let cache = MarketDataCache::new(capacity, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // Add 15 klines (should evict the first 5)
        for i in 1..=15 {
            let open_time = i * interval_ms;
            let kline = create_kline(
                "BTCUSDT",
                interval.clone(),
                open_time,
                open_time + interval_ms - 1,
            );
            let result = cache.add_kline(kline).await;
            assert!(result.is_ok());

            if i <= capacity as u64 {
                // First `capacity` should not cause eviction
                assert_eq!(result.unwrap().len(), 0, "No eviction at i={}", i);
            } else {
                // After capacity, each new kline should cause eviction of oldest
                assert_eq!(
                    result.unwrap().len(),
                    1,
                    "Should evict 1 kline at iteration {}",
                    i
                );
            }
        }

        // Get capacity number of klines
        let result = cache
            .get_klines("BTCUSDT", interval.clone(), Some(capacity))
            .await;
        assert!(result.is_ok());
        let klines = result.unwrap();
        assert_eq!(klines.len(), capacity);

        // Verify we have the last 10 klines (6-15)
        for i in 0..capacity {
            assert!(klines[i].is_some());
            assert_eq!(
                klines[i].as_ref().unwrap().open_time,
                (6 + i as u64) * interval_ms
            );
        }
    }

    #[tokio::test]
    async fn test_kline_replace_existing() {
        let cache = MarketDataCache::new(100, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // Add initial kline
        let kline1 = create_kline(
            "BTCUSDT",
            interval.clone(),
            1 * interval_ms,
            2 * interval_ms - 1,
        );
        cache.add_kline(kline1).await.unwrap();

        // Replace with same open_time but different data
        let mut kline2 = create_kline(
            "BTCUSDT",
            interval.clone(),
            1 * interval_ms,
            2 * interval_ms - 1,
        );
        kline2.close = Decimal::from(200);
        let result = cache.add_kline(kline2).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0, "No eviction on replace");

        // Verify replaced data
        let result = cache.get_klines("BTCUSDT", interval.clone(), Some(1)).await;
        assert!(result.is_ok());
        let klines = result.unwrap();
        assert_eq!(klines[0].as_ref().unwrap().close, Decimal::from(200));
    }

    #[tokio::test]
    async fn test_kline_multiple_symbols() {
        let cache = MarketDataCache::new(100, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // Add klines for different symbols
        let kline_btc = create_kline(
            "BTCUSDT",
            interval.clone(),
            1 * interval_ms,
            2 * interval_ms - 1,
        );
        let kline_eth = create_kline(
            "ETHUSDT",
            interval.clone(),
            1 * interval_ms,
            2 * interval_ms - 1,
        );

        cache.add_kline(kline_btc).await.unwrap();
        cache.add_kline(kline_eth).await.unwrap();

        // Get klines for each symbol
        let result_btc = cache.get_klines("BTCUSDT", interval.clone(), Some(1)).await;
        let result_eth = cache.get_klines("ETHUSDT", interval.clone(), Some(1)).await;

        assert!(result_btc.is_ok());
        assert!(result_eth.is_ok());

        let klines_btc = result_btc.unwrap();
        let klines_eth = result_eth.unwrap();

        assert_eq!(klines_btc[0].as_ref().unwrap().symbol, "BTCUSDT");
        assert_eq!(klines_eth[0].as_ref().unwrap().symbol, "ETHUSDT");
    }

    #[tokio::test]
    async fn test_kline_empty_cache() {
        let cache = MarketDataCache::new(100, 100);
        let interval = KlineInterval::OneMinute;

        // Get from empty cache
        let result = cache
            .get_klines("BTCUSDT", interval.clone(), Some(10))
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_kline_limit_parameter() {
        let cache = MarketDataCache::new(100, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // Add 10 klines
        for i in 1..=10 {
            let kline = create_kline(
                "BTCUSDT",
                interval.clone(),
                i * interval_ms,
                (i + 1) * interval_ms - 1,
            );
            cache.add_kline(kline).await.unwrap();
        }

        // Get with different limits
        let result_3 = cache.get_klines("BTCUSDT", interval.clone(), Some(3)).await;
        let klines_3 = result_3.unwrap();
        assert_eq!(klines_3.len(), 3);
        // Should get the last 3 klines: 8, 9, 10
        assert!(klines_3[0].is_some(), "Third-to-last kline should exist");
        assert!(klines_3[1].is_some(), "Second-to-last kline should exist");
        assert!(klines_3[2].is_some(), "Last kline should exist");

        let result_5 = cache.get_klines("BTCUSDT", interval.clone(), Some(5)).await;
        let klines_5 = result_5.unwrap();
        assert_eq!(klines_5.len(), 5);

        // Get with None limit should return all in capacity
        let result_all = cache.get_klines("BTCUSDT", interval.clone(), None).await;
        let klines = result_all.unwrap();
        assert_eq!(klines.len(), 100, "Should return capacity size");
        // Should have some klines from the end of the sequence
        assert!(
            klines.iter().any(|k| k.is_some()),
            "Should have at least some klines"
        );
    }

    // ============================================================================
    // TradeCache Tests
    // ============================================================================

    #[tokio::test]
    async fn test_trade_add_single() {
        let cache = MarketDataCache::new(100, 100);
        let trade = create_trade("BTCUSDT", 1, 50000, 1, 1000);

        let result = cache.add_trade(trade).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_trade_get_single() {
        let cache = MarketDataCache::new(100, 100);
        let trade = create_trade("BTCUSDT", 1, 50000, 1, 1000);

        cache.add_trade(trade.clone()).await.unwrap();

        let result = cache.get_trades("BTCUSDT", Some(1)).await;
        assert!(result.is_ok());
        let trades = result.unwrap();
        assert_eq!(trades.len(), 1);
        assert!(trades[0].is_some());
        assert_eq!(trades[0].as_ref().unwrap().seq_id, 1);
    }

    #[tokio::test]
    async fn test_trade_sequential_add() {
        let cache = MarketDataCache::new(100, 100);

        // Add 10 sequential trades
        for i in 1..=10 {
            let trade = create_trade("BTCUSDT", i, 50000 + i as u64, i as u64, 1000 + i as u64);
            let result = cache.add_trade(trade).await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap().len(), 0);
        }

        // Get all 10 trades
        let result = cache.get_trades("BTCUSDT", Some(10)).await;
        assert!(result.is_ok());
        let trades = result.unwrap();
        assert_eq!(trades.len(), 10);

        // Verify all are present and in order
        for i in 0..10 {
            assert!(trades[i].is_some());
            assert_eq!(trades[i].as_ref().unwrap().seq_id, (i + 1) as u64);
        }
    }

    #[tokio::test]
    async fn test_trade_out_of_order_small() {
        let cache = MarketDataCache::new(100, 100);

        // Add trades 1, 3, 2 (small disorder)
        let trade1 = create_trade("BTCUSDT", 1, 50000, 1, 1000);
        let trade3 = create_trade("BTCUSDT", 3, 50002, 1, 1002);
        let trade2 = create_trade("BTCUSDT", 2, 50001, 1, 1001);

        cache.add_trade(trade1).await.unwrap();
        cache.add_trade(trade3).await.unwrap();
        cache.add_trade(trade2).await.unwrap();

        // Get all 3 trades
        let result = cache.get_trades("BTCUSDT", Some(3)).await;
        assert!(result.is_ok());
        let trades = result.unwrap();
        assert_eq!(trades.len(), 3);

        // Verify all are present
        assert!(trades[0].is_some());
        assert!(trades[1].is_some());
        assert!(trades[2].is_some());

        // Verify seq_ids are correct
        assert_eq!(trades[0].as_ref().unwrap().seq_id, 1);
        assert_eq!(trades[1].as_ref().unwrap().seq_id, 2);
        assert_eq!(trades[2].as_ref().unwrap().seq_id, 3);
    }

    #[tokio::test]
    async fn test_trade_out_of_order_large() {
        let cache = MarketDataCache::new(100, 100);

        // Add trades in reverse order (large disorder)
        for i in (1..=10).rev() {
            let trade = create_trade("BTCUSDT", i, 50000 + i as u64, i as u64, 1000 + i as u64);
            let result = cache.add_trade(trade).await;
            assert!(result.is_ok());
        }

        // Get all 10 trades
        let result = cache.get_trades("BTCUSDT", Some(10)).await;
        assert!(result.is_ok());
        let trades = result.unwrap();
        assert_eq!(trades.len(), 10);

        // Verify all are present and properly ordered by seq_id
        for i in 0..10 {
            assert!(trades[i].is_some(), "Trade at index {} should exist", i);
            assert_eq!(
                trades[i].as_ref().unwrap().seq_id,
                (i + 1) as u64,
                "Trade at index {} has wrong seq_id",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_trade_gap_small() {
        let cache = MarketDataCache::new(100, 100);

        // Add trades 1, 3, 5 (small gaps)
        for seq_id in [1, 3, 5].iter() {
            let trade = create_trade("BTCUSDT", *seq_id, 50000 + seq_id, *seq_id, 1000 + seq_id);
            cache.add_trade(trade).await.unwrap();
        }

        // Get 5 trades
        let result = cache.get_trades("BTCUSDT", Some(5)).await;
        assert!(result.is_ok());
        let trades = result.unwrap();
        assert_eq!(trades.len(), 5);

        // Verify gaps: position 0 (id 1), none (gap), position 2 (id 3), none (gap), position 4 (id 5)
        assert!(trades[0].is_some(), "Index 0 should have data");
        assert!(trades[1].is_none(), "Index 1 should be None (gap)");
        assert!(trades[2].is_some(), "Index 2 should have data");
        assert!(trades[3].is_none(), "Index 3 should be None (gap)");
        assert!(trades[4].is_some(), "Index 4 should have data");
    }

    #[tokio::test]
    async fn test_trade_gap_large() {
        let cache = MarketDataCache::new(100, 100);

        // Add trade at seq_id 1
        let trade1 = create_trade("BTCUSDT", 1, 50000, 1, 1000);
        cache.add_trade(trade1).await.unwrap();

        // Add trade at seq_id 51 (large gap)
        let trade51 = create_trade("BTCUSDT", 51, 50050, 1, 1050);
        let result = cache.add_trade(trade51).await;
        assert!(result.is_ok());

        // Get 51 trades
        let result = cache.get_trades("BTCUSDT", Some(51)).await;
        assert!(result.is_ok());
        let trades = result.unwrap();
        assert_eq!(trades.len(), 51);

        // First should exist
        assert!(trades[0].is_some());
        assert_eq!(trades[0].as_ref().unwrap().seq_id, 1);

        // Middle should be None
        for i in 1..50 {
            assert!(
                trades[i].is_none(),
                "Trade at index {} should be None (gap)",
                i
            );
        }

        // Last should exist
        assert!(trades[50].is_some());
        assert_eq!(trades[50].as_ref().unwrap().seq_id, 51);
    }

    #[tokio::test]
    async fn test_trade_capacity_eviction() {
        let capacity = 10;
        let cache = MarketDataCache::new(100, capacity);

        // Add 15 trades
        for i in 1..=15 {
            let trade = create_trade("BTCUSDT", i, 50000 + i as u64, i as u64, 1000 + i as u64);
            let result = cache.add_trade(trade).await;
            assert!(result.is_ok());
            let removed = result.unwrap();

            // After reaching capacity, we should start evicting
            if i <= capacity as u64 {
                assert_eq!(
                    removed.len(),
                    0,
                    "No eviction until capacity reached at i={}",
                    i
                );
            } else {
                // Once we exceed capacity, we may get evictions
                assert!(
                    removed.len() <= 1,
                    "Should evict at most 1 trade at i={}",
                    i
                );
            }
        }

        // Get capacity number of trades
        let result = cache.get_trades("BTCUSDT", Some(capacity)).await;
        assert!(result.is_ok());
        let trades = result.unwrap();
        assert_eq!(trades.len(), capacity);

        // Verify the trades have reasonable seq_ids
        for trade_opt in trades.iter() {
            if let Some(trade) = trade_opt {
                assert!(
                    trade.seq_id >= 1 && trade.seq_id <= 15,
                    "Trade has unexpected seq_id {}",
                    trade.seq_id
                );
            }
        }
    }

    #[tokio::test]
    async fn test_trade_replace_existing() {
        let cache = MarketDataCache::new(100, 100);

        // Add initial trade
        let trade1 = create_trade("BTCUSDT", 1, 50000, 1, 1000);
        cache.add_trade(trade1).await.unwrap();

        // Replace with same seq_id but different data
        let mut trade2 = create_trade("BTCUSDT", 1, 51000, 2, 1000);
        trade2.quantity = Decimal::from(5);
        let result = cache.add_trade(trade2).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0, "No eviction on replace");

        // Verify replaced data
        let result = cache.get_trades("BTCUSDT", Some(1)).await;
        assert!(result.is_ok());
        let trades = result.unwrap();
        assert_eq!(trades[0].as_ref().unwrap().quantity, Decimal::from(5));
    }

    #[tokio::test]
    async fn test_trade_multiple_symbols() {
        let cache = MarketDataCache::new(100, 100);

        // Add trades for different symbols
        let trade_btc = create_trade("BTCUSDT", 1, 50000, 1, 1000);
        let trade_eth = create_trade("ETHUSDT", 1, 3000, 1, 1000);

        cache.add_trade(trade_btc).await.unwrap();
        cache.add_trade(trade_eth).await.unwrap();

        // Get trades for each symbol
        let result_btc = cache.get_trades("BTCUSDT", Some(1)).await;
        let result_eth = cache.get_trades("ETHUSDT", Some(1)).await;

        assert!(result_btc.is_ok());
        assert!(result_eth.is_ok());

        let trades_btc = result_btc.unwrap();
        let trades_eth = result_eth.unwrap();

        assert_eq!(trades_btc[0].as_ref().unwrap().symbol, "BTCUSDT");
        assert_eq!(trades_eth[0].as_ref().unwrap().symbol, "ETHUSDT");
    }

    #[tokio::test]
    async fn test_trade_empty_cache() {
        let cache = MarketDataCache::new(100, 100);

        // Get from empty cache
        let result = cache.get_trades("BTCUSDT", Some(10)).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_trade_limit_parameter() {
        let cache = MarketDataCache::new(100, 100);

        // Add 10 trades
        for i in 1..=10 {
            let trade = create_trade("BTCUSDT", i, 50000 + i as u64, i as u64, 1000 + i as u64);
            cache.add_trade(trade).await.unwrap();
        }

        // Get with different limits
        let result_3 = cache.get_trades("BTCUSDT", Some(3)).await;
        let trades_3 = result_3.unwrap();
        assert_eq!(trades_3.len(), 3);
        // Should get the last 3 trades: 8, 9, 10
        assert!(trades_3[0].is_some(), "Third-to-last trade should exist");
        assert!(trades_3[1].is_some(), "Second-to-last trade should exist");
        assert!(trades_3[2].is_some(), "Last trade should exist");

        let result_5 = cache.get_trades("BTCUSDT", Some(5)).await;
        let trades_5 = result_5.unwrap();
        assert_eq!(trades_5.len(), 5);

        // Get with None limit should return all in capacity
        let result_all = cache.get_trades("BTCUSDT", None).await;
        let trades = result_all.unwrap();
        assert_eq!(trades.len(), 100, "Should return capacity size");
        // Should have some trades from the end of the sequence
        assert!(
            trades.iter().any(|t| t.is_some()),
            "Should have at least some trades"
        );
    }

    // ============================================================================
    // Complex Scenarios
    // ============================================================================

    #[tokio::test]
    async fn test_mixed_kline_and_trade() {
        let cache = MarketDataCache::new(50, 50);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // Add klines, starting from 1
        for i in 1..=5 {
            let kline = create_kline(
                "BTCUSDT",
                interval.clone(),
                i * interval_ms,
                (i + 1) * interval_ms - 1,
            );
            cache.add_kline(kline).await.unwrap();
        }

        // Add trades
        for i in 1..=5 {
            let trade = create_trade(
                "BTCUSDT",
                i as u64,
                50000 + i as u64,
                i as u64,
                1000 + i as u64,
            );
            cache.add_trade(trade).await.unwrap();
        }

        // Verify both work independently
        let klines = cache
            .get_klines("BTCUSDT", interval, Some(5))
            .await
            .unwrap();
        assert_eq!(klines.len(), 5);
        assert_eq!(klines.iter().filter(|k| k.is_some()).count(), 5);

        let trades = cache.get_trades("BTCUSDT", Some(5)).await.unwrap();
        assert_eq!(trades.len(), 5);
        assert_eq!(trades.iter().filter(|t| t.is_some()).count(), 5);
    }

    #[tokio::test]
    async fn test_kline_symbol_mismatch_error() {
        let cache = MarketDataCache::new(100, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // Add kline for BTCUSDT
        let kline1 = create_kline(
            "BTCUSDT",
            interval.clone(),
            1 * interval_ms,
            2 * interval_ms - 1,
        );
        cache.add_kline(kline1).await.unwrap();

        // Try to add kline with different symbol to the same cache slot
        // (This should succeed as they're different symbols)
        let kline2 = create_kline(
            "ETHUSDT",
            interval.clone(),
            1 * interval_ms,
            2 * interval_ms - 1,
        );
        let result = cache.add_kline(kline2).await;
        assert!(result.is_ok(), "Should succeed with different symbol");
    }

    #[tokio::test]
    async fn test_kline_misaligned_open_time_error() {
        let cache = MarketDataCache::new(100, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // Try to add kline with misaligned open_time
        let mut kline = create_kline(
            "BTCUSDT",
            interval.clone(),
            1 * interval_ms,
            2 * interval_ms - 1,
        );
        kline.open_time = 1500; // Not aligned to minute boundary
        let result = cache.add_kline(kline).await;
        assert!(result.is_err(), "Should reject misaligned open_time");
    }

    #[tokio::test]
    async fn test_kline_very_old_rejected() {
        let cache = MarketDataCache::new(10, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // Add recent klines, starting from a high base value
        for i in 1..=10 {
            let open_time = 10000 * interval_ms + i * interval_ms;
            let kline = create_kline(
                "BTCUSDT",
                interval.clone(),
                open_time,
                open_time + interval_ms - 1,
            );
            cache.add_kline(kline).await.unwrap();
        }

        // Try to add very old kline (before the capacity window)
        let old_kline = create_kline(
            "BTCUSDT",
            interval.clone(),
            1 * interval_ms,
            2 * interval_ms - 1,
        );
        let result = cache.add_kline(old_kline).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0, "Very old kline should be ignored");
    }

    #[tokio::test]
    async fn test_trade_rapid_disorder() {
        let cache = MarketDataCache::new(100, 100);

        // Add trades with rapid disorder: 10, 5, 15, 1, 20, 3, etc.
        let seq_ids = vec![10, 5, 15, 1, 20, 3, 25, 7, 12, 18];
        for seq_id in seq_ids.iter() {
            let trade = create_trade("BTCUSDT", *seq_id, 50000 + seq_id, *seq_id, 1000 + seq_id);
            cache.add_trade(trade).await.unwrap();
        }

        // Get all trades
        let result = cache.get_trades("BTCUSDT", Some(25)).await;
        assert!(result.is_ok());
        let trades = result.unwrap();

        // Verify the ones that should exist
        let expected_ids = vec![1u64, 3, 5, 7, 10, 12, 15, 18, 20, 25];
        for &seq_id in expected_ids.iter() {
            let index = (seq_id - 1) as usize;
            assert!(
                trades[index].is_some(),
                "Trade with seq_id {} should exist",
                seq_id
            );
        }
    }

    #[tokio::test]
    async fn test_kline_different_intervals() {
        let cache = MarketDataCache::new(100, 100);
        let interval_ms_1m = KlineInterval::OneMinute.to_millis();
        let interval_ms_5m = KlineInterval::FiveMinutes.to_millis();

        let kline_1m = create_kline(
            "BTCUSDT",
            KlineInterval::OneMinute,
            1 * interval_ms_1m,
            2 * interval_ms_1m - 1,
        );
        let kline_5m = create_kline(
            "BTCUSDT",
            KlineInterval::FiveMinutes,
            1 * interval_ms_5m,
            2 * interval_ms_5m - 1,
        );

        cache.add_kline(kline_1m).await.unwrap();
        cache.add_kline(kline_5m).await.unwrap();

        // Get klines for different intervals
        let result_1m = cache
            .get_klines("BTCUSDT", KlineInterval::OneMinute, Some(1))
            .await;
        let result_5m = cache
            .get_klines("BTCUSDT", KlineInterval::FiveMinutes, Some(1))
            .await;

        assert!(result_1m.is_ok());
        assert!(result_5m.is_ok());

        let klines_1m = result_1m.unwrap();
        let klines_5m = result_5m.unwrap();

        assert!(klines_1m[0].is_some());
        assert!(klines_5m[0].is_some());
        assert_eq!(
            klines_1m[0].as_ref().unwrap().interval,
            KlineInterval::OneMinute
        );
        assert_eq!(
            klines_5m[0].as_ref().unwrap().interval,
            KlineInterval::FiveMinutes
        );
    }

    #[tokio::test]
    async fn test_kline_evict_multiple_on_large_jump() {
        let capacity = 10;
        let cache = MarketDataCache::new(capacity, 100);
        let interval = KlineInterval::OneMinute;
        let interval_ms = interval.to_millis();

        // 1. Fill the cache completely
        for i in 1..=capacity as u64 {
            let kline = create_kline(
                "BTCUSDT",
                interval.clone(),
                i * interval_ms,
                i * interval_ms + 1,
            );
            let evicted = cache.add_kline(kline).await.unwrap();
            assert!(evicted.is_empty(), "Should not evict while filling");
        }

        // 2. Add a kline far in the future, which should evict multiple klines
        // This jump should evict 5 klines (1, 2, 3, 4, 5)
        let jump_kline = create_kline(
            "BTCUSDT",
            interval.clone(),
            (capacity as u64 + 5) * interval_ms,
            (capacity as u64 + 5) * interval_ms + 1,
        );
        let evicted = cache.add_kline(jump_kline).await.unwrap();

        assert_eq!(evicted.len(), 5, "Should evict 5 klines on large jump");
        let mut evicted_times: Vec<u64> = evicted.iter().map(|k| k.open_time).collect();
        evicted_times.sort();
        let expected_times: Vec<u64> = (1..=5).map(|i| i * interval_ms).collect();
        assert_eq!(
            evicted_times, expected_times,
            "Evicted klines should be the oldest ones"
        );

        // 3. Verify the cache contains the correct klines (6-10 and 15)
        let klines = cache
            .get_klines("BTCUSDT", interval.clone(), Some(capacity))
            .await
            .unwrap();
        let present_times: Vec<u64> = klines
            .iter()
            .filter_map(|k| k.as_ref().map(|d| d.open_time))
            .collect();
        let mut expected_present_times: Vec<u64> = (6..=10).map(|i| i * interval_ms).collect();
        expected_present_times.push(15 * interval_ms);

        assert_eq!(present_times.len(), expected_present_times.len());
        assert!(present_times
            .iter()
            .all(|t| expected_present_times.contains(t)));
    }

    #[tokio::test]
    async fn test_trade_evict_multiple_on_large_jump() {
        let capacity = 10;
        let cache = MarketDataCache::new(100, capacity);

        // 1. Fill the cache
        for i in 1..=capacity as u64 {
            let trade = create_trade("BTCUSDT", i, 50000, 1, 1000 + i);
            let evicted = cache.add_trade(trade).await.unwrap();
            assert!(evicted.is_empty());
        }

        // 2. Add a trade far in the future, evicting multiple
        let jump_trade = create_trade("BTCUSDT", capacity as u64 + 5, 50000, 1, 2000);
        let evicted = cache.add_trade(jump_trade).await.unwrap();

        assert_eq!(evicted.len(), 5, "Should evict 5 trades on large jump");
        let mut evicted_ids: Vec<u64> = evicted.iter().map(|t| t.seq_id).collect();
        evicted_ids.sort();
        let expected_ids: Vec<u64> = (1..=5).collect();
        assert_eq!(
            evicted_ids, expected_ids,
            "Evicted trades should be the oldest ones"
        );

        // 3. Verify cache content
        let trades = cache.get_trades("BTCUSDT", Some(capacity)).await.unwrap();
        let present_ids: Vec<u64> = trades
            .iter()
            .filter_map(|t| t.as_ref().map(|d| d.seq_id))
            .collect();
        let mut expected_present_ids: Vec<u64> = (6..=10).collect();
        expected_present_ids.push(15);

        assert_eq!(present_ids.len(), expected_present_ids.len());
        assert!(present_ids
            .iter()
            .all(|id| expected_present_ids.contains(id)));
    }
}
