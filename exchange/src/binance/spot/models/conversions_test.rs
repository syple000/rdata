#[cfg(test)]
mod tests {
    use crate::binance::spot::models::*;
    use platform::models as platform;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn test_side_conversion() {
        let buy: platform::OrderSide = Side::Buy.into();
        assert_eq!(buy, platform::OrderSide::Buy);

        let sell: platform::OrderSide = Side::Sell.into();
        assert_eq!(sell, platform::OrderSide::Sell);
    }

    #[test]
    fn test_order_status_conversion() {
        let new: platform::OrderStatus = OrderStatus::New.into();
        assert_eq!(new, platform::OrderStatus::New);

        let filled: platform::OrderStatus = OrderStatus::Filled.into();
        assert_eq!(filled, platform::OrderStatus::Filled);
    }

    #[test]
    fn test_balance_conversion() {
        let binance_balance = Balance {
            asset: "BTC".to_string(),
            free: Decimal::from_str("1.5").unwrap(),
            locked: Decimal::from_str("0.5").unwrap(),
        };

        let platform_balance: platform::Balance = binance_balance.into();
        assert_eq!(platform_balance.asset, "BTC");
        assert_eq!(platform_balance.free, Decimal::from_str("1.5").unwrap());
        assert_eq!(platform_balance.locked, Decimal::from_str("0.5").unwrap());
    }

    #[test]
    fn test_kline_conversion() {
        let binance_kline = KlineData {
            symbol: "BTCUSDT".to_string(),
            interval: KlineInterval::OneMinute,
            open_time: 1000,
            close_time: 2000,
            open: Decimal::from_str("50000").unwrap(),
            high: Decimal::from_str("51000").unwrap(),
            low: Decimal::from_str("49000").unwrap(),
            close: Decimal::from_str("50500").unwrap(),
            volume: Decimal::from_str("100").unwrap(),
            quote_volume: Decimal::from_str("5000000").unwrap(),
            trade_count: 500,
            taker_buy_volume: Decimal::from_str("60").unwrap(),
            taker_buy_quote_volume: Decimal::from_str("3000000").unwrap(),
            first_trade_id: 1,
            last_trade_id: 500,
            is_closed: true,
        };

        let platform_kline: platform::KlineData = binance_kline.into();
        assert_eq!(platform_kline.symbol, "BTCUSDT");
        assert_eq!(platform_kline.interval, platform::KlineInterval::OneMinute);
        assert_eq!(platform_kline.open, Decimal::from_str("50000").unwrap());
        assert_eq!(platform_kline.close, Decimal::from_str("50500").unwrap());
    }

    #[test]
    fn test_symbol_info_conversion() {
        // 创建一个完整的 Symbol 结构，模拟从 exchange_info.json 解析的数据
        let binance_symbol = Symbol {
            symbol: "BTCUSDT".to_string(),
            status: "TRADING".to_string(),
            base_asset: "BTC".to_string(),
            base_asset_precision: 8,
            quote_asset: "USDT".to_string(),
            quote_asset_precision: 8,
            order_types: vec![
                OrderType::Limit,
                OrderType::LimitMaker,
                OrderType::Market,
                OrderType::StopLoss,
                OrderType::StopLossLimit,
                OrderType::TakeProfit,
                OrderType::TakeProfitLimit,
            ],
            iceberg_allowed: true,
            oco_allowed: true,
            is_spot_trading_allowed: true,
            is_margin_trading_allowed: false,
            filters: vec![
                Filter {
                    filter_type: "PRICE_FILTER".to_string(),
                    min_price: Some(Decimal::from_str("0.01").unwrap()),
                    max_price: Some(Decimal::from_str("1000000").unwrap()),
                    tick_size: Some(Decimal::from_str("0.01").unwrap()),
                    min_qty: None,
                    max_qty: None,
                    step_size: None,
                    min_notional: None,
                    apply_to_market: None,
                    avg_price_mins: None,
                    limit: None,
                    max_num_orders: None,
                    max_num_algo_orders: None,
                },
                Filter {
                    filter_type: "LOT_SIZE".to_string(),
                    min_price: None,
                    max_price: None,
                    tick_size: None,
                    min_qty: Some(Decimal::from_str("0.00001").unwrap()),
                    max_qty: Some(Decimal::from_str("9000").unwrap()),
                    step_size: Some(Decimal::from_str("0.00001").unwrap()),
                    min_notional: None,
                    apply_to_market: None,
                    avg_price_mins: None,
                    limit: None,
                    max_num_orders: None,
                    max_num_algo_orders: None,
                },
                Filter {
                    filter_type: "MARKET_LOT_SIZE".to_string(),
                    min_price: None,
                    max_price: None,
                    tick_size: None,
                    min_qty: Some(Decimal::from_str("0.00000000").unwrap()),
                    max_qty: Some(Decimal::from_str("134.23809950").unwrap()),
                    step_size: Some(Decimal::from_str("0.00000000").unwrap()),
                    min_notional: None,
                    apply_to_market: None,
                    avg_price_mins: None,
                    limit: None,
                    max_num_orders: None,
                    max_num_algo_orders: None,
                },
                Filter {
                    filter_type: "NOTIONAL".to_string(),
                    min_price: None,
                    max_price: None,
                    tick_size: None,
                    min_qty: None,
                    max_qty: None,
                    step_size: None,
                    min_notional: Some(Decimal::from_str("5").unwrap()),
                    apply_to_market: None,
                    avg_price_mins: Some(5),
                    limit: None,
                    max_num_orders: None,
                    max_num_algo_orders: None,
                },
            ],
            permissions: vec![],
        };

        // 测试基础转换（不包含 extra 字段）
        let platform_symbol: platform::SymbolInfo = binance_symbol.clone().into();
        assert_eq!(platform_symbol.symbol, "BTCUSDT");
        assert_eq!(platform_symbol.status, platform::SymbolStatus::Trading);
        assert_eq!(platform_symbol.base_asset, "BTC");
        assert_eq!(platform_symbol.quote_asset, "USDT");
        assert_eq!(platform_symbol.base_asset_precision, Some(8));
        assert_eq!(platform_symbol.quote_asset_precision, Some(8));

        // 验证价格过滤器
        assert_eq!(
            platform_symbol.min_price,
            Some(Decimal::from_str("0.01").unwrap())
        );
        assert_eq!(
            platform_symbol.max_price,
            Some(Decimal::from_str("1000000").unwrap())
        );
        assert_eq!(
            platform_symbol.price_tick_size,
            Some(Decimal::from_str("0.01").unwrap())
        );

        // 验证数量过滤器 (LOT_SIZE)
        assert_eq!(
            platform_symbol.min_quantity,
            Some(Decimal::from_str("0.00001").unwrap())
        );
        assert_eq!(
            platform_symbol.max_quantity,
            Some(Decimal::from_str("9000").unwrap())
        );
        assert_eq!(
            platform_symbol.quantity_step_size,
            Some(Decimal::from_str("0.00001").unwrap())
        );

        // 验证市价单数量过滤器 (MARKET_LOT_SIZE)
        assert_eq!(
            platform_symbol.min_market_quantity,
            Some(Decimal::from_str("0.00000000").unwrap())
        );
        assert_eq!(
            platform_symbol.max_market_quantity,
            Some(Decimal::from_str("134.23809950").unwrap())
        );
        assert_eq!(
            platform_symbol.market_quantity_step_size,
            Some(Decimal::from_str("0.00000000").unwrap())
        );

        // 验证最小名义价值
        assert_eq!(
            platform_symbol.min_notional,
            Some(Decimal::from_str("5").unwrap())
        );

        // 测试带 extra 字段的转换
        let platform_symbol_with_extra: platform::SymbolInfo<SymbolInfoExtra> =
            binance_symbol.clone().into();
        assert_eq!(platform_symbol_with_extra.symbol, "BTCUSDT");
        assert_eq!(platform_symbol_with_extra.extra.order_types.len(), 7);
        assert_eq!(platform_symbol_with_extra.extra.iceberg_allowed, true);
        assert_eq!(platform_symbol_with_extra.extra.oco_allowed, true);
        assert_eq!(
            platform_symbol_with_extra.extra.is_spot_trading_allowed,
            true
        );
        assert_eq!(
            platform_symbol_with_extra.extra.is_margin_trading_allowed,
            false
        );
        assert_eq!(platform_symbol_with_extra.extra.filters.len(), 4);
    }

    #[test]
    fn test_exchange_info_conversion() {
        let binance_info = ExchangeInfo {
            timezone: "UTC".to_string(),
            server_time: 1761312954876,
            symbols: vec![Symbol {
                symbol: "BTCUSDT".to_string(),
                status: "TRADING".to_string(),
                base_asset: "BTC".to_string(),
                base_asset_precision: 8,
                quote_asset: "USDT".to_string(),
                quote_asset_precision: 8,
                order_types: vec![OrderType::Market, OrderType::Limit],
                iceberg_allowed: true,
                oco_allowed: true,
                is_spot_trading_allowed: true,
                is_margin_trading_allowed: false,
                filters: vec![],
                permissions: vec![],
            }],
        };

        // 测试基础转换
        let platform_info: platform::ExchangeInfo = binance_info.clone().into();
        assert_eq!(platform_info.symbols.len(), 1);
        assert_eq!(platform_info.symbols[0].symbol, "BTCUSDT");

        // 测试带 extra 字段的转换
        let platform_info_with_extra: platform::ExchangeInfo<SymbolInfoExtra, ExchangeInfoExtra> =
            binance_info.clone().into();
        assert_eq!(platform_info_with_extra.extra.timezone, "UTC");
        assert_eq!(platform_info_with_extra.extra.server_time, 1761312954876);
        assert_eq!(platform_info_with_extra.symbols.len(), 1);
    }
}
