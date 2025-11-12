use crate::models::{enums::*, market::*, market_reqs::*, trade::*, trade_reqs::*};
use exchange::binance::spot::{models as ex_models, requests as ex_requests};

// ============================================================================
// Market Data Conversions: exchange -> platform
// ============================================================================

impl From<ex_models::KlineData> for KlineData {
    fn from(value: ex_models::KlineData) -> Self {
        KlineData {
            symbol: value.symbol,
            interval: value.interval.into(),
            open_time: value.open_time,
            close_time: value.close_time,
            open: value.open,
            high: value.high,
            low: value.low,
            close: value.close,
            volume: value.volume,
            quote_volume: value.quote_volume,
            is_closed: value.is_closed,
        }
    }
}

impl From<ex_models::Ticker24hr> for Ticker24hr {
    fn from(value: ex_models::Ticker24hr) -> Self {
        Ticker24hr {
            symbol: value.symbol,
            last_price: value.last_price,
            last_qty: value.last_qty,
            bid_price: value.bid_price,
            bid_qty: value.bid_qty,
            ask_price: value.ask_price,
            ask_qty: value.ask_qty,
            open_price: value.open_price,
            high_price: value.high_price,
            low_price: value.low_price,
            volume: value.volume,
            quote_volume: value.quote_volume,
            open_time: value.open_time,
            close_time: value.close_time,
            count: value.count,
        }
    }
}

impl From<ex_models::PriceLevel> for PriceLevel {
    fn from(value: ex_models::PriceLevel) -> Self {
        PriceLevel {
            price: value.price,
            quantity: value.quantity,
        }
    }
}

impl From<ex_models::DepthData> for DepthData {
    fn from(value: ex_models::DepthData) -> Self {
        DepthData {
            symbol: value.symbol,
            bids: value.bids.into_iter().map(|b| b.into()).collect(),
            asks: value.asks.into_iter().map(|a| a.into()).collect(),
            timestamp: value.timestamp,
        }
    }
}

impl From<ex_models::AggTrade> for Trade {
    fn from(value: ex_models::AggTrade) -> Self {
        Trade {
            symbol: value.symbol,
            trade_id: value.agg_trade_id.to_string(),
            price: value.price,
            quantity: value.quantity,
            timestamp: value.timestamp,
            is_buyer_maker: value.is_buyer_maker,
            seq_id: value.agg_trade_id,
        }
    }
}

impl From<ex_models::Symbol> for SymbolInfo {
    fn from(value: ex_models::Symbol) -> Self {
        // Extract filter values
        let mut min_price = None;
        let mut max_price = None;
        let mut price_tick_size = None;
        let mut min_market_quantity = None;
        let mut max_market_quantity = None;
        let mut market_quantity_step_size = None;
        let mut min_quantity = None;
        let mut max_quantity = None;
        let mut quantity_step_size = None;
        let mut min_notional = None;

        for filter in &value.filters {
            match filter.filter_type.as_str() {
                "PRICE_FILTER" => {
                    min_price = filter.min_price;
                    max_price = filter.max_price;
                    price_tick_size = filter.tick_size;
                }
                "LOT_SIZE" => {
                    min_quantity = filter.min_qty;
                    max_quantity = filter.max_qty;
                    quantity_step_size = filter.step_size;
                }
                "MARKET_LOT_SIZE" => {
                    min_market_quantity = filter.min_qty;
                    max_market_quantity = filter.max_qty;
                    market_quantity_step_size = filter.step_size;
                }
                "NOTIONAL" => {
                    min_notional = filter.min_notional;
                }
                _ => {}
            }
        }

        let status = match value.status.as_str() {
            "TRADING" => SymbolStatus::Trading,
            "HALT" => SymbolStatus::Halted,
            "BREAK" => SymbolStatus::Break,
            "END_OF_DAY" => SymbolStatus::EndOfDay,
            _ => SymbolStatus::Trading, // default
        };

        SymbolInfo {
            symbol: value.symbol,
            status,
            base_asset: value.base_asset,
            quote_asset: value.quote_asset,
            base_asset_precision: Some(value.base_asset_precision),
            quote_asset_precision: Some(value.quote_asset_precision),
            min_price,
            max_price,
            price_tick_size,
            min_market_quantity,
            max_market_quantity,
            market_quantity_step_size,
            min_quantity,
            max_quantity,
            quantity_step_size,
            min_notional,
        }
    }
}

impl From<ex_models::ExchangeInfo> for ExchangeInfo {
    fn from(value: ex_models::ExchangeInfo) -> Self {
        ExchangeInfo {
            symbols: value.symbols.into_iter().map(|s| s.into()).collect(),
        }
    }
}

// ============================================================================
// Trade Data Conversions: exchange -> platform
// ============================================================================

impl From<ex_models::Side> for OrderSide {
    fn from(value: ex_models::Side) -> Self {
        match value {
            ex_models::Side::Buy => OrderSide::Buy,
            ex_models::Side::Sell => OrderSide::Sell,
        }
    }
}

impl From<ex_models::OrderType> for OrderType {
    fn from(value: ex_models::OrderType) -> Self {
        match value {
            ex_models::OrderType::Limit => OrderType::Limit,
            ex_models::OrderType::Market => OrderType::Market,
            ex_models::OrderType::StopLoss => OrderType::StopLoss,
            ex_models::OrderType::StopLossLimit => OrderType::StopLossLimit,
            ex_models::OrderType::TakeProfit => OrderType::TakeProfit,
            ex_models::OrderType::TakeProfitLimit => OrderType::TakeProfitLimit,
            ex_models::OrderType::LimitMaker => OrderType::LimitMaker,
        }
    }
}

impl From<ex_models::OrderStatus> for OrderStatus {
    fn from(value: ex_models::OrderStatus) -> Self {
        match value {
            ex_models::OrderStatus::New => OrderStatus::New,
            ex_models::OrderStatus::PendingNew => OrderStatus::PendingNew,
            ex_models::OrderStatus::PartiallyFilled => OrderStatus::PartiallyFilled,
            ex_models::OrderStatus::Filled => OrderStatus::Filled,
            ex_models::OrderStatus::Canceled => OrderStatus::Canceled,
            ex_models::OrderStatus::PendingCancel => OrderStatus::PendingCancel,
            ex_models::OrderStatus::Rejected => OrderStatus::Rejected,
            ex_models::OrderStatus::Expired => OrderStatus::Expired,
            ex_models::OrderStatus::ExpiredInMatch => OrderStatus::ExpiredInMatch,
        }
    }
}

impl From<ex_models::TimeInForce> for TimeInForce {
    fn from(value: ex_models::TimeInForce) -> Self {
        match value {
            ex_models::TimeInForce::Gtc => TimeInForce::Gtc,
            ex_models::TimeInForce::Ioc => TimeInForce::Ioc,
            ex_models::TimeInForce::Fok => TimeInForce::Fok,
        }
    }
}

impl From<ex_models::KlineInterval> for KlineInterval {
    fn from(value: ex_models::KlineInterval) -> Self {
        match value {
            ex_models::KlineInterval::OneSecond => KlineInterval::OneSecond,
            ex_models::KlineInterval::OneMinute => KlineInterval::OneMinute,
            ex_models::KlineInterval::ThreeMinutes => KlineInterval::ThreeMinutes,
            ex_models::KlineInterval::FiveMinutes => KlineInterval::FiveMinutes,
            ex_models::KlineInterval::FifteenMinutes => KlineInterval::FifteenMinutes,
            ex_models::KlineInterval::ThirtyMinutes => KlineInterval::ThirtyMinutes,
            ex_models::KlineInterval::OneHour => KlineInterval::OneHour,
            ex_models::KlineInterval::TwoHours => KlineInterval::TwoHours,
            ex_models::KlineInterval::FourHours => KlineInterval::FourHours,
            ex_models::KlineInterval::SixHours => KlineInterval::SixHours,
            ex_models::KlineInterval::EightHours => KlineInterval::EightHours,
            ex_models::KlineInterval::TwelveHours => KlineInterval::TwelveHours,
            ex_models::KlineInterval::OneDay => KlineInterval::OneDay,
            ex_models::KlineInterval::ThreeDays => KlineInterval::ThreeDays,
            ex_models::KlineInterval::OneWeek => KlineInterval::OneWeek,
            ex_models::KlineInterval::OneMonth => KlineInterval::OneMonth,
        }
    }
}

impl From<ex_models::Order> for Order {
    fn from(value: ex_models::Order) -> Self {
        Order {
            symbol: value.symbol,
            order_id: value.order_id.to_string(),
            client_order_id: value.client_order_id,
            order_side: value.order_side.into(),
            order_type: value.order_type.into(),
            order_status: value.order_status.into(),
            order_price: value.order_price,
            order_quantity: value.order_quantity,
            executed_qty: value.executed_qty,
            cummulative_quote_qty: value.cummulative_quote_qty,
            time_in_force: value.time_in_force.into(),
            stop_price: value.stop_price,
            iceberg_qty: value.iceberg_qty,
            create_time: value.create_time,
            update_time: value.update_time,
        }
    }
}

impl From<ex_models::Trade> for UserTrade {
    fn from(value: ex_models::Trade) -> Self {
        UserTrade {
            trade_id: value.trade_id.to_string(),
            order_id: value.order_id.to_string(),
            symbol: value.symbol,
            order_side: value.order_side.into(),
            trade_price: value.trade_price,
            trade_quantity: value.trade_quantity,
            commission: value.commission,
            commission_asset: value.commission_asset,
            is_maker: if value.is_maker { 1 } else { 0 },
            timestamp: value.timestamp,
        }
    }
}

impl From<ex_models::Balance> for Balance {
    fn from(value: ex_models::Balance) -> Self {
        Balance {
            asset: value.asset,
            free: value.free,
            locked: value.locked,
        }
    }
}

impl From<ex_models::OutboundAccountPosition> for AccountUpdate {
    fn from(value: ex_models::OutboundAccountPosition) -> Self {
        AccountUpdate {
            balances: value.balances.into_iter().map(|b| b.into()).collect(),
            timestamp: value.update_time,
        }
    }
}

impl From<ex_models::Account> for Account {
    fn from(value: ex_models::Account) -> Self {
        Account {
            balances: value.balances.into_iter().map(|b| b.into()).collect(),
            timestamp: value.update_time,
        }
    }
}

// ============================================================================
// Request Conversions: platform -> exchange
// ============================================================================

impl From<OrderSide> for ex_models::Side {
    fn from(value: OrderSide) -> Self {
        match value {
            OrderSide::Buy => ex_models::Side::Buy,
            OrderSide::Sell => ex_models::Side::Sell,
        }
    }
}

impl From<OrderType> for ex_models::OrderType {
    fn from(value: OrderType) -> Self {
        match value {
            OrderType::Limit => ex_models::OrderType::Limit,
            OrderType::Market => ex_models::OrderType::Market,
            OrderType::StopLoss => ex_models::OrderType::StopLoss,
            OrderType::StopLossLimit => ex_models::OrderType::StopLossLimit,
            OrderType::TakeProfit => ex_models::OrderType::TakeProfit,
            OrderType::TakeProfitLimit => ex_models::OrderType::TakeProfitLimit,
            OrderType::LimitMaker => ex_models::OrderType::LimitMaker,
        }
    }
}

impl From<TimeInForce> for ex_models::TimeInForce {
    fn from(value: TimeInForce) -> Self {
        match value {
            TimeInForce::Gtc => ex_models::TimeInForce::Gtc,
            TimeInForce::Ioc => ex_models::TimeInForce::Ioc,
            TimeInForce::Fok => ex_models::TimeInForce::Fok,
        }
    }
}

impl From<KlineInterval> for ex_models::KlineInterval {
    fn from(value: KlineInterval) -> Self {
        match value {
            KlineInterval::OneSecond => ex_models::KlineInterval::OneSecond,
            KlineInterval::OneMinute => ex_models::KlineInterval::OneMinute,
            KlineInterval::ThreeMinutes => ex_models::KlineInterval::ThreeMinutes,
            KlineInterval::FiveMinutes => ex_models::KlineInterval::FiveMinutes,
            KlineInterval::FifteenMinutes => ex_models::KlineInterval::FifteenMinutes,
            KlineInterval::ThirtyMinutes => ex_models::KlineInterval::ThirtyMinutes,
            KlineInterval::OneHour => ex_models::KlineInterval::OneHour,
            KlineInterval::TwoHours => ex_models::KlineInterval::TwoHours,
            KlineInterval::FourHours => ex_models::KlineInterval::FourHours,
            KlineInterval::SixHours => ex_models::KlineInterval::SixHours,
            KlineInterval::EightHours => ex_models::KlineInterval::EightHours,
            KlineInterval::TwelveHours => ex_models::KlineInterval::TwelveHours,
            KlineInterval::OneDay => ex_models::KlineInterval::OneDay,
            KlineInterval::ThreeDays => ex_models::KlineInterval::ThreeDays,
            KlineInterval::OneWeek => ex_models::KlineInterval::OneWeek,
            KlineInterval::OneMonth => ex_models::KlineInterval::OneMonth,
        }
    }
}

// Market Request Conversions

impl From<GetKlinesRequest> for ex_requests::GetKlinesRequest {
    fn from(value: GetKlinesRequest) -> Self {
        ex_requests::GetKlinesRequest {
            symbol: value.symbol,
            interval: value.interval.into(),
            start_time: value.start_time,
            end_time: value.end_time,
            limit: value.limit,
        }
    }
}

impl From<GetTradesRequest> for ex_requests::GetAggTradesRequest {
    fn from(value: GetTradesRequest) -> Self {
        ex_requests::GetAggTradesRequest {
            symbol: value.symbol,
            from_id: value.from_id.and_then(|id| id.parse().ok()),
            start_time: value.start_time,
            end_time: value.end_time,
            limit: value.limit,
        }
    }
}

impl From<GetDepthRequest> for ex_requests::GetDepthRequest {
    fn from(value: GetDepthRequest) -> Self {
        ex_requests::GetDepthRequest {
            symbol: value.symbol,
            limit: value.limit,
        }
    }
}

impl From<GetExchangeInfoRequest> for ex_requests::GetExchangeInfoRequest {
    fn from(value: GetExchangeInfoRequest) -> Self {
        ex_requests::GetExchangeInfoRequest {
            symbol: value.symbol,
            symbols: value.symbols,
        }
    }
}

impl From<GetTicker24hrRequest> for ex_requests::GetTicker24hrRequest {
    fn from(value: GetTicker24hrRequest) -> Self {
        ex_requests::GetTicker24hrRequest {
            symbol: value.symbol,
            symbols: value.symbols,
        }
    }
}

// Trade Request Conversions

impl From<PlaceOrderRequest> for ex_requests::PlaceOrderRequest {
    fn from(value: PlaceOrderRequest) -> Self {
        ex_requests::PlaceOrderRequest {
            symbol: value.symbol,
            side: value.side.into(),
            r#type: value.r#type.into(),
            time_in_force: value.time_in_force.map(|tif| tif.into()),
            quantity: value.quantity,
            price: value.price,
            new_client_order_id: Some(value.client_order_id),
            stop_price: value.stop_price,
            iceberg_qty: value.iceberg_qty,
        }
    }
}

impl From<CancelOrderRequest> for ex_requests::CancelOrderRequest {
    fn from(value: CancelOrderRequest) -> Self {
        ex_requests::CancelOrderRequest {
            symbol: value.symbol,
            order_id: value.order_id.and_then(|id| id.parse().ok()),
            orig_client_order_id: Some(value.client_order_id.clone()),
            new_client_order_id: Some(value.client_order_id),
        }
    }
}

impl From<GetOrderRequest> for ex_requests::GetOrderRequest {
    fn from(value: GetOrderRequest) -> Self {
        ex_requests::GetOrderRequest {
            symbol: value.symbol,
            order_id: value.order_id.and_then(|id| id.parse().ok()),
            orig_client_order_id: value.client_order_id,
        }
    }
}

impl From<GetOpenOrdersRequest> for ex_requests::GetOpenOrdersRequest {
    fn from(value: GetOpenOrdersRequest) -> Self {
        ex_requests::GetOpenOrdersRequest {
            symbol: value.symbol,
        }
    }
}

impl From<GetAllOrdersRequest> for ex_requests::GetAllOrdersRequest {
    fn from(value: GetAllOrdersRequest) -> Self {
        ex_requests::GetAllOrdersRequest {
            symbol: value.symbol,
            from_id: value.from_id.and_then(|id| id.parse().ok()),
            start_time: value.start_time,
            end_time: value.end_time,
            limit: value.limit,
        }
    }
}

impl From<GetUserTradesRequest> for ex_requests::GetTradesRequest {
    fn from(value: GetUserTradesRequest) -> Self {
        ex_requests::GetTradesRequest {
            symbol: value.symbol,
            order_id: value.order_id.and_then(|id| id.parse().ok()),
            from_id: value.from_id.and_then(|id| id.parse().ok()),
            start_time: value.start_time,
            end_time: value.end_time,
            limit: value.limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_kline_conversion() {
        let ex_kline = ex_models::KlineData {
            symbol: "BTCUSDT".to_string(),
            interval: ex_models::KlineInterval::OneMinute,
            open_time: 1000,
            close_time: 2000,
            open: dec!(100.0),
            high: dec!(110.0),
            low: dec!(90.0),
            close: dec!(105.0),
            volume: dec!(1000.0),
            quote_volume: dec!(100000.0),
            trade_count: 100,
            taker_buy_volume: dec!(500.0),
            taker_buy_quote_volume: dec!(50000.0),
            first_trade_id: 1,
            last_trade_id: 100,
            is_closed: true,
        };

        let platform_kline: KlineData = ex_kline.into();
        assert_eq!(platform_kline.symbol, "BTCUSDT");
        assert_eq!(platform_kline.open, dec!(100.0));
        assert_eq!(platform_kline.is_closed, true);
    }

    #[test]
    fn test_order_side_conversion() {
        let ex_side = ex_models::Side::Buy;
        let platform_side: OrderSide = ex_side.into();
        assert_eq!(platform_side, OrderSide::Buy);

        let platform_side = OrderSide::Sell;
        let ex_side: ex_models::Side = platform_side.into();
        assert!(matches!(ex_side, ex_models::Side::Sell));
    }

    #[test]
    fn test_trade_conversion() {
        let ex_agg_trade = ex_models::AggTrade {
            symbol: "BTCUSDT".to_string(),
            agg_trade_id: 12345,
            price: dec!(50000.0),
            quantity: dec!(0.1),
            first_trade_id: 100,
            last_trade_id: 105,
            timestamp: 1000000,
            is_buyer_maker: true,
        };

        let platform_trade: Trade = ex_agg_trade.into();
        assert_eq!(platform_trade.symbol, "BTCUSDT");
        assert_eq!(platform_trade.trade_id, "12345");
        assert_eq!(platform_trade.price, dec!(50000.0));
    }

    #[test]
    fn test_user_trade_conversion() {
        let ex_trade = ex_models::Trade {
            trade_id: 999,
            order_id: 888,
            symbol: "ETHUSDT".to_string(),
            order_side: ex_models::Side::Buy,
            trade_price: dec!(3000.0),
            trade_quantity: dec!(1.5),
            commission: dec!(0.001),
            commission_asset: "BNB".to_string(),
            is_maker: false,
            timestamp: 2000000,
        };

        let platform_user_trade: UserTrade = ex_trade.into();
        assert_eq!(platform_user_trade.trade_id, "999");
        assert_eq!(platform_user_trade.order_id, "888");
        assert_eq!(platform_user_trade.symbol, "ETHUSDT");
    }

    #[test]
    fn test_request_conversion() {
        let platform_req = GetKlinesRequest {
            symbol: "BTCUSDT".to_string(),
            interval: KlineInterval::OneHour,
            start_time: Some(1000),
            end_time: Some(2000),
            limit: Some(100),
        };

        let ex_req: ex_requests::GetKlinesRequest = platform_req.into();
        assert_eq!(ex_req.symbol, "BTCUSDT");
        assert_eq!(ex_req.start_time, Some(1000));
        assert_eq!(ex_req.limit, Some(100));
    }
}
