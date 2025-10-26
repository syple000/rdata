use crate::models as platform;
use exchange::binance::spot::models::*;

impl From<Side> for platform::OrderSide {
    fn from(side: Side) -> Self {
        match side {
            Side::Buy => platform::OrderSide::Buy,
            Side::Sell => platform::OrderSide::Sell,
        }
    }
}

impl From<OrderStatus> for platform::OrderStatus {
    fn from(status: OrderStatus) -> Self {
        match status {
            OrderStatus::New => platform::OrderStatus::New,
            OrderStatus::PendingNew => platform::OrderStatus::New,
            OrderStatus::PartiallyFilled => platform::OrderStatus::PartiallyFilled,
            OrderStatus::Filled => platform::OrderStatus::Filled,
            OrderStatus::Canceled => platform::OrderStatus::Canceled,
            OrderStatus::PendingCancel => platform::OrderStatus::PendingCancel,
            OrderStatus::Rejected => platform::OrderStatus::Rejected,
            OrderStatus::Expired => platform::OrderStatus::Expired,
            OrderStatus::ExpiredInMatch => platform::OrderStatus::ExpiredInMatch,
        }
    }
}

impl From<TimeInForce> for platform::TimeInForce {
    fn from(tif: TimeInForce) -> Self {
        match tif {
            TimeInForce::Gtc => platform::TimeInForce::Gtc,
            TimeInForce::Ioc => platform::TimeInForce::Ioc,
            TimeInForce::Fok => platform::TimeInForce::Fok,
        }
    }
}

impl From<OrderType> for platform::OrderType {
    fn from(order_type: OrderType) -> Self {
        match order_type {
            OrderType::Market => platform::OrderType::Market,
            OrderType::Limit => platform::OrderType::Limit,
            OrderType::TakeProfit => platform::OrderType::TakeProfit,
            OrderType::StopLoss => platform::OrderType::StopLoss,
            OrderType::StopLossLimit => platform::OrderType::StopLossLimit,
            OrderType::TakeProfitLimit => platform::OrderType::TakeProfitLimit,
            OrderType::LimitMaker => platform::OrderType::LimitMaker,
        }
    }
}

impl From<KlineInterval> for platform::KlineInterval {
    fn from(interval: KlineInterval) -> Self {
        match interval {
            KlineInterval::OneSecond => platform::KlineInterval::OneSecond,
            KlineInterval::OneMinute => platform::KlineInterval::OneMinute,
            KlineInterval::ThreeMinutes => platform::KlineInterval::ThreeMinutes,
            KlineInterval::FiveMinutes => platform::KlineInterval::FiveMinutes,
            KlineInterval::FifteenMinutes => platform::KlineInterval::FifteenMinutes,
            KlineInterval::ThirtyMinutes => platform::KlineInterval::ThirtyMinutes,
            KlineInterval::OneHour => platform::KlineInterval::OneHour,
            KlineInterval::TwoHours => platform::KlineInterval::TwoHours,
            KlineInterval::FourHours => platform::KlineInterval::FourHours,
            KlineInterval::SixHours => platform::KlineInterval::SixHours,
            KlineInterval::EightHours => platform::KlineInterval::EightHours,
            KlineInterval::TwelveHours => platform::KlineInterval::TwelveHours,
            KlineInterval::OneDay => platform::KlineInterval::OneDay,
            KlineInterval::ThreeDays => platform::KlineInterval::ThreeDays,
            KlineInterval::OneWeek => platform::KlineInterval::OneWeek,
            KlineInterval::OneMonth => platform::KlineInterval::OneMonth,
        }
    }
}

impl From<platform::KlineInterval> for KlineInterval {
    fn from(interval: platform::KlineInterval) -> Self {
        match interval {
            platform::KlineInterval::OneSecond => KlineInterval::OneSecond,
            platform::KlineInterval::OneMinute => KlineInterval::OneMinute,
            platform::KlineInterval::ThreeMinutes => KlineInterval::ThreeMinutes,
            platform::KlineInterval::FiveMinutes => KlineInterval::FiveMinutes,
            platform::KlineInterval::FifteenMinutes => KlineInterval::FifteenMinutes,
            platform::KlineInterval::ThirtyMinutes => KlineInterval::ThirtyMinutes,
            platform::KlineInterval::OneHour => KlineInterval::OneHour,
            platform::KlineInterval::TwoHours => KlineInterval::TwoHours,
            platform::KlineInterval::FourHours => KlineInterval::FourHours,
            platform::KlineInterval::SixHours => KlineInterval::SixHours,
            platform::KlineInterval::EightHours => KlineInterval::EightHours,
            platform::KlineInterval::TwelveHours => KlineInterval::TwelveHours,
            platform::KlineInterval::OneDay => KlineInterval::OneDay,
            platform::KlineInterval::ThreeDays => KlineInterval::ThreeDays,
            platform::KlineInterval::OneWeek => KlineInterval::OneWeek,
            platform::KlineInterval::OneMonth => KlineInterval::OneMonth,
        }
    }
}

impl From<KlineData> for platform::KlineData {
    fn from(kline: KlineData) -> Self {
        platform::KlineData {
            symbol: kline.symbol,
            interval: kline.interval.into(),
            open_time: kline.open_time,
            close_time: kline.close_time,
            open: kline.open,
            high: kline.high,
            low: kline.low,
            close: kline.close,
            volume: kline.volume,
            quote_volume: kline.quote_volume,
        }
    }
}

impl From<Ticker24hr> for platform::Ticker24hr {
    fn from(ticker: Ticker24hr) -> Self {
        platform::Ticker24hr {
            symbol: ticker.symbol,
            last_price: ticker.last_price,
            last_qty: ticker.last_qty,
            bid_price: ticker.bid_price,
            bid_qty: ticker.bid_qty,
            ask_price: ticker.ask_price,
            ask_qty: ticker.ask_qty,
            open_price: ticker.open_price,
            high_price: ticker.high_price,
            low_price: ticker.low_price,
            volume: ticker.volume,
            quote_volume: ticker.quote_volume,
            open_time: ticker.open_time,
            close_time: ticker.close_time,
            count: ticker.count,
        }
    }
}

impl From<PriceLevel> for platform::PriceLevel {
    fn from(level: PriceLevel) -> Self {
        platform::PriceLevel {
            price: level.price,
            quantity: level.quantity,
        }
    }
}

impl From<DepthData> for platform::DepthData {
    fn from(depth: DepthData) -> Self {
        platform::DepthData {
            symbol: depth.symbol,
            bids: depth.bids.into_iter().map(Into::into).collect(),
            asks: depth.asks.into_iter().map(Into::into).collect(),
            timestamp: depth.timestamp,
        }
    }
}

impl From<AggTrade> for platform::Trade {
    fn from(trade: AggTrade) -> Self {
        platform::Trade {
            symbol: trade.symbol,
            trade_id: trade.agg_trade_id.to_string(),
            price: trade.price,
            quantity: trade.quantity,
            timestamp: trade.timestamp,
            is_buyer_maker: trade.is_buyer_maker,
        }
    }
}

fn parse_symbol_status(status: &str) -> platform::SymbolStatus {
    match status {
        "TRADING" => platform::SymbolStatus::Trading,
        "HALT" => platform::SymbolStatus::Halted,
        "BREAK" => platform::SymbolStatus::Break,
        "END_OF_DAY" => platform::SymbolStatus::EndOfDay,
        _ => platform::SymbolStatus::Trading, // Default to Trading
    }
}

impl From<Symbol> for platform::SymbolInfo {
    fn from(symbol: Symbol) -> Self {
        let mut min_price = None;
        let mut max_price = None;
        let mut price_tick_size = None;
        let mut min_quantity = None;
        let mut max_quantity = None;
        let mut quantity_step_size = None;
        let mut min_market_quantity = None;
        let mut max_market_quantity = None;
        let mut market_quantity_step_size = None;
        let mut min_notional = None;

        for filter in &symbol.filters {
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

        platform::SymbolInfo {
            symbol: symbol.symbol,
            status: parse_symbol_status(&symbol.status),
            base_asset: symbol.base_asset,
            quote_asset: symbol.quote_asset,
            base_asset_precision: Some(symbol.base_asset_precision),
            quote_asset_precision: Some(symbol.quote_asset_precision),
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

impl From<ExchangeInfo> for platform::ExchangeInfo {
    fn from(info: ExchangeInfo) -> Self {
        platform::ExchangeInfo {
            symbols: info.symbols.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<Order> for platform::Order {
    fn from(order: Order) -> Self {
        platform::Order {
            symbol: order.symbol,
            order_id: order.order_id.to_string(),
            client_order_id: order.client_order_id,
            order_side: order.order_side.into(),
            order_type: order.order_type.into(),
            order_status: order.order_status.into(),
            order_price: order.order_price,
            order_quantity: order.order_quantity,
            executed_qty: order.executed_qty,
            cummulative_quote_qty: order.cummulative_quote_qty,
            time_in_force: order.time_in_force.into(),
            stop_price: order.stop_price,
            iceberg_qty: order.iceberg_qty,
            create_time: order.create_time,
            update_time: order.update_time,
        }
    }
}

impl From<Trade> for platform::UserTrade {
    fn from(trade: Trade) -> Self {
        platform::UserTrade {
            trade_id: trade.trade_id.to_string(),
            order_id: trade.order_id.to_string(),
            symbol: trade.symbol,
            order_side: trade.order_side.into(),
            trade_price: trade.trade_price,
            trade_quantity: trade.trade_quantity,
            commission: trade.commission,
            commission_asset: trade.commission_asset,
            is_maker: trade.is_maker,
            timestamp: trade.timestamp,
        }
    }
}

impl From<Balance> for platform::Balance {
    fn from(balance: Balance) -> Self {
        platform::Balance {
            asset: balance.asset,
            free: balance.free,
            locked: balance.locked,
        }
    }
}
