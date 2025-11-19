use crate::{
    errors::{PlatformError, Result},
    models::DepthData,
};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthFactors {
    // 盘口价差
    pub spread: f64,     // 绝对价差 best_ask - best_bid
    pub rel_spread: f64, // 相对价差 spread / mid

    // 深度与不平衡
    pub bid_depth_5: f64,       // 买一到买五总量
    pub ask_depth_5: f64,       // 卖一到卖五总量
    pub depth_imbalance_5: f64, // (bid_depth_5 - ask_depth_5)/(bid_depth_5 + ask_depth_5)

    // VWAP 与溢价
    pub bid_vwap_5: f64,
    pub ask_vwap_5: f64,
    pub bid_vwap_discount: f64, // (mid - bid_vwap_5)/mid
    pub ask_vwap_premium: f64,  // (ask_vwap_5 - mid)/mid

    // 挂单墙强度
    pub max_bid_wall_ratio_5: f64, // 最大单档占买盘比例
    pub max_ask_wall_ratio_5: f64, // 最大单档占卖盘比例
}

pub fn calc_depth_factors(depth: &DepthData) -> Result<DepthFactors> {
    if depth.bids.is_empty() || depth.asks.is_empty() {
        return Err(PlatformError::FactorError {
            message: "Depth data must contain at least one bid and one ask".to_string(),
        });
    }

    let best_bid = depth.bids[0].price.to_f64().unwrap_or(0.0);
    let best_ask = depth.asks[0].price.to_f64().unwrap_or(0.0);
    let mid = (best_bid + best_ask) / 2.0;

    let spread = best_ask - best_bid;
    let rel_spread = if mid.abs() > 1e-10 { spread / mid } else { 0.0 };

    let n_bid = depth.bids.len().min(5);
    let n_ask = depth.asks.len().min(5);

    let mut bid_depth_5 = 0.0;
    let mut ask_depth_5 = 0.0;

    let mut bid_weighted_price = 0.0;
    let mut ask_weighted_price = 0.0;

    let mut max_bid_level = 0.0;
    let mut max_ask_level = 0.0;

    for i in 0..n_bid {
        let level = &depth.bids[i];
        let size = level.quantity.to_f64().unwrap_or(0.0);
        let price = level.price.to_f64().unwrap_or(0.0);
        bid_depth_5 += size;
        bid_weighted_price += price * size;
        if size > max_bid_level {
            max_bid_level = size;
        }
    }

    for i in 0..n_ask {
        let level = &depth.asks[i];
        let size = level.quantity.to_f64().unwrap_or(0.0);
        let price = level.price.to_f64().unwrap_or(0.0);
        ask_depth_5 += size;
        ask_weighted_price += price * size;
        if size > max_ask_level {
            max_ask_level = size;
        }
    }

    let depth_sum = bid_depth_5 + ask_depth_5;
    let depth_imbalance_5 = if depth_sum > 0.0 {
        (bid_depth_5 - ask_depth_5) / depth_sum
    } else {
        0.0
    };

    let bid_vwap_5 = if bid_depth_5 > 0.0 {
        bid_weighted_price / bid_depth_5
    } else {
        0.0
    };

    let ask_vwap_5 = if ask_depth_5 > 0.0 {
        ask_weighted_price / ask_depth_5
    } else {
        0.0
    };

    let bid_vwap_discount = if mid.abs() > 1e-10 {
        (mid - bid_vwap_5) / mid
    } else {
        0.0
    };

    let ask_vwap_premium = if mid.abs() > 1e-10 {
        (ask_vwap_5 - mid) / mid
    } else {
        0.0
    };

    let max_bid_wall_ratio_5 = if bid_depth_5 > 0.0 {
        max_bid_level / bid_depth_5
    } else {
        0.0
    };

    let max_ask_wall_ratio_5 = if ask_depth_5 > 0.0 {
        max_ask_level / ask_depth_5
    } else {
        0.0
    };

    Ok(DepthFactors {
        spread,
        rel_spread,
        bid_depth_5,
        ask_depth_5,
        depth_imbalance_5,
        bid_vwap_5,
        ask_vwap_5,
        bid_vwap_discount,
        ask_vwap_premium,
        max_bid_wall_ratio_5,
        max_ask_wall_ratio_5,
    })
}
