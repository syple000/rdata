use crate::{
    errors::{PlatformError, Result},
    models::Ticker24hr,
};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerFactors {
    // 价格相关
    pub return_from_open: f64, // (last_price - open_price)/open_price
    pub return_from_low: f64,  // (last_price - low_price)/low_price
    pub return_from_high: f64, // (last_price - high_price)/high_price
    pub intraday_range: f64,   // (high_price - low_price)/open_price

    // 盘口(top-of-book)
    pub spread: f64,            // best_ask - best_bid
    pub rel_spread: f64,        // spread / mid
    pub bid_ask_imbalance: f64, // (bid_qty - ask_qty)/(bid_qty + ask_qty)

    // 量价
    pub vwap_intraday: f64, // 当日成交额/成交量
    pub last_vs_vwap: f64,  // (last_price - vwap_intraday)/vwap_intraday
    pub volume_rate: f64,   // 单位时间成交量(按24h折算近似): volume/(close_time-open_time)
}

pub fn calc_ticker_factors(ticker: &Ticker24hr) -> Result<TickerFactors> {
    let last = ticker.last_price.to_f64().unwrap_or(0.0);
    let open = ticker.open_price.to_f64().unwrap_or(0.0);
    let high = ticker.high_price.to_f64().unwrap_or(0.0);
    let low = ticker.low_price.to_f64().unwrap_or(0.0);

    let bid_price = ticker.bid_price.to_f64().unwrap_or(0.0);
    let ask_price = ticker.ask_price.to_f64().unwrap_or(0.0);
    let bid_qty = ticker.bid_qty.to_f64().unwrap_or(0.0);
    let ask_qty = ticker.ask_qty.to_f64().unwrap_or(0.0);

    let volume = ticker.volume.to_f64().unwrap_or(0.0);
    let quote_volume = ticker.quote_volume.to_f64().unwrap_or(0.0);

    if open <= 0.0 {
        return Err(PlatformError::FactorError {
            message: "Ticker open_price must be positive".to_string(),
        });
    }

    let return_from_open = (last - open) / open;
    let return_from_low = if low > 0.0 { (last - low) / low } else { 0.0 };
    let return_from_high = if high > 0.0 {
        (last - high) / high
    } else {
        0.0
    };
    let intraday_range = if open > 0.0 { (high - low) / open } else { 0.0 };

    let mid = (bid_price + ask_price) / 2.0;
    let spread = ask_price - bid_price;
    let rel_spread = if mid.abs() > 1e-10 { spread / mid } else { 0.0 };

    let qty_sum = bid_qty + ask_qty;
    let bid_ask_imbalance = if qty_sum > 0.0 {
        (bid_qty - ask_qty) / qty_sum
    } else {
        0.0
    };

    let vwap_intraday = if volume > 0.0 {
        quote_volume / volume
    } else {
        0.0
    };

    let last_vs_vwap = if vwap_intraday > 0.0 {
        (last - vwap_intraday) / vwap_intraday
    } else {
        0.0
    };

    let time_span_ms = if ticker.close_time > ticker.open_time {
        (ticker.close_time - ticker.open_time) as f64
    } else {
        0.0
    };
    let volume_rate = if time_span_ms > 0.0 {
        volume / (time_span_ms / 1000.0)
    } else {
        0.0
    };

    Ok(TickerFactors {
        return_from_open,
        return_from_low,
        return_from_high,
        intraday_range,
        spread,
        rel_spread,
        bid_ask_imbalance,
        vwap_intraday,
        last_vs_vwap,
        volume_rate,
    })
}
