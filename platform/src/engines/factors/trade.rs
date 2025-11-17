use crate::{
    errors::{PlatformError, Result},
    models::Trade,
};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeFactors {
    // 价格动量因子
    pub price_return: f64,       // 1. 价格变化率：最新价格/最早价格
    pub trend_strength: f64, // 2. 价格趋势强度：计算价格diff率，sum(1 if diff > 0) - sum(1 if diff < 0) / (N - 1)
    pub price_volatility: f64, // 3. 价格波动率: 标准差sqrt(sum((x - mean(x))^2)/N)
    pub price_range: f64,    // 4. 价格区间幅度: (max_price - min_price) / mean_price
    pub price_acceleration: f64, // 5. 价格加速度：对价格diff率求diff后求平均
    pub price_position: f64, // 6. 价格位置： (最新价格 - min_price) / (max_price - min_price)

    // 成交量因子
    pub avg_vol: f64,           // 平均成交量：成交量均值
    pub vol_volatility: f64,    // 成交量波动率：成交量标准差
    pub vol_skew: f64, // 成交量偏度：衡量成交量分布的对称性，np.mean((volumes - np.mean(volumes)) ** 3) / (np.std(volumes) ** 3 + 1e-10)
    pub large_trade_ratio: f64, // 大单比例：大单数(1.5倍平均) / 总成交数
    pub vol_trend: f64, // 成交量趋势：最近N次平均成交量/之前的平均成交量

    // 买卖压力因子
    pub buy_count: u64,            // buy maker计数
    pub sell_count: u64,           // sell maker计数
    pub trade_imbalance: f64, // 成交不平衡度: (buy_count - sell_count) / (buy_count + sell_count + 1e-10)
    pub buy_vol: f64,         // buy maker成交量
    pub sell_vol: f64,        // sell maker成交量
    pub vol_imbalance: f64,   // 成交量不平衡度: (buy_vol - sell_vol) / (buy_vol + sell_vol + 1e-10
    pub net_buy_ratio: f64,   // 净主动买入比例: sum(buy_vol) / sum(buy_vol + sell_vol + 1e-10)
    pub avg_trade_size_ratio: f64, // 买卖平均单量对比

    // 价量因子
    pub vwap: f64,                     // 成交量加权平均价
    pub price_vwap_deviation: f64,     // 最新价格与VWAP的偏离
    pub vwap_slope: f64,               // vwap斜率，最近N条的vwap 和 往前N条的vwap 的变化率
    pub obv: f64,                      // On-Balance Vol，价格上涨加本次成交量，下跌减本次成交量
    pub price_volume_correlation: f64, // 价格与成交量的相关系数

    // 时间序列因子
    pub trade_frequency: f64,    // 单位时间内的成交次数
    pub avg_trade_interval: f64, // 平均成交间隔时间
    pub trade_interval_std: f64, // 成交间隔时间标准差
}

/// 共享的预计算数据结构，用于优化性能
struct PrecomputedData {
    prices: Vec<f64>,
    volumes: Vec<f64>,
    timestamps: Vec<u64>,

    // 价格统计
    price_mean: f64,
    price_min: f64,
    price_max: f64,
    price_first: f64,
    price_last: f64,

    // 成交量统计
    vol_mean: f64,

    // 买卖统计
    buy_count: u64,
    sell_count: u64,
    buy_vol: f64,
    sell_vol: f64,

    // 价量统计
    vwap: f64,

    // 时间统计
    time_span: f64, // 毫秒
}

impl PrecomputedData {
    fn new(trades: &[Trade]) -> Self {
        let n = trades.len();
        let mut prices = Vec::with_capacity(n);
        let mut volumes = Vec::with_capacity(n);
        let mut timestamps = Vec::with_capacity(n);
        let mut is_buyer_makers = Vec::with_capacity(n);

        let mut price_sum = 0.0;
        let mut price_min = f64::MAX;
        let mut price_max = f64::MIN;
        let mut vol_sum = 0.0;
        let mut buy_count = 0u64;
        let mut sell_count = 0u64;
        let mut buy_vol = 0.0;
        let mut sell_vol = 0.0;
        let mut weighted_price_sum = 0.0;

        for trade in trades {
            let price = trade.price.to_f64().unwrap_or(0.0);
            let volume = trade.quantity.to_f64().unwrap_or(0.0);
            let is_buyer_maker = trade.is_buyer_maker;

            prices.push(price);
            volumes.push(volume);
            timestamps.push(trade.timestamp);
            is_buyer_makers.push(is_buyer_maker);

            price_sum += price;
            vol_sum += volume;
            price_min = price_min.min(price);
            price_max = price_max.max(price);
            weighted_price_sum += price * volume;

            // 买单做市提供流动性，说明吃单方向是卖单
            if is_buyer_maker == 1 {
                sell_count += 1;
                sell_vol += volume;
            } else {
                buy_count += 1;
                buy_vol += volume;
            }
        }

        let price_mean = price_sum / n as f64;
        let vol_mean = vol_sum / n as f64;
        let vwap = if vol_sum > 0.0 {
            weighted_price_sum / vol_sum
        } else {
            0.0
        };

        let price_first = prices[0];
        let price_last = prices[n - 1];

        let time_span = if n > 1 {
            timestamps[n - 1] as f64 - timestamps[0] as f64
        } else {
            0.0
        };

        Self {
            prices,
            volumes,
            timestamps,
            price_mean,
            price_min,
            price_max,
            price_first,
            price_last,
            vol_mean,
            buy_count,
            sell_count,
            buy_vol,
            sell_vol,
            vwap,
            time_span,
        }
    }
}

/// 1. 价格变化率：最新价格/最早价格 - 1
fn calc_price_return(data: &PrecomputedData) -> f64 {
    (data.price_last / (data.price_first + 1e-10)) - 1.0
}

/// 2. 价格趋势强度：(上涨次数 - 下跌次数) / 总变化次数
fn calc_trend_strength(data: &PrecomputedData) -> f64 {
    let mut up_count = 0;
    let mut down_count = 0;

    for i in 1..data.prices.len() {
        let diff = data.prices[i] - data.prices[i - 1];
        if diff > 0.0 {
            up_count += 1;
        } else if diff < 0.0 {
            down_count += 1;
        }
    }

    let total = (data.prices.len() - 1) as f64;
    (up_count as f64 - down_count as f64) / total
}

/// 3. 价格波动率：标准差
fn calc_price_volatility(data: &PrecomputedData) -> f64 {
    calc_std(&data.prices, data.price_mean)
}

/// 4. 价格区间幅度: (max_price - min_price) / mean_price
fn calc_price_range(data: &PrecomputedData) -> f64 {
    (data.price_max - data.price_min) / (data.price_mean + 1e-10)
}

/// 5. 价格加速度：对价格变化率再求变化的平均值
fn calc_price_acceleration(data: &PrecomputedData) -> f64 {
    // 先计算一阶差分（速度）
    let mut first_diff = Vec::with_capacity(data.prices.len() - 1);
    for i in 1..data.prices.len() {
        first_diff.push((data.prices[i] - data.prices[i - 1]) / (data.prices[i - 1] + 1e-10));
    }

    // 再计算二阶差分（加速度）
    let mut second_diff_sum = 0.0;
    for i in 1..first_diff.len() {
        second_diff_sum += first_diff[i] - first_diff[i - 1];
    }

    second_diff_sum / (first_diff.len() - 1) as f64
}

/// 6. 价格位置：(最新价格 - min_price) / (max_price - min_price)
fn calc_price_position(data: &PrecomputedData) -> f64 {
    let range = data.price_max - data.price_min;
    if range > 0.0 {
        (data.price_last - data.price_min) / range
    } else {
        0.5 // 如果没有价格变化，返回中间位置
    }
}

/// 平均成交量（已在预计算中）
fn calc_avg_vol(data: &PrecomputedData) -> f64 {
    data.vol_mean
}

/// 成交量波动率：标准差
fn calc_vol_volatility(data: &PrecomputedData) -> f64 {
    calc_std(&data.volumes, data.vol_mean)
}

/// 成交量偏度：衡量成交量分布的对称性
fn calc_vol_skew(data: &PrecomputedData) -> f64 {
    let mean = data.vol_mean;
    let std = calc_vol_volatility(data);

    let mut skew_sum = 0.0;
    for &vol in &data.volumes {
        let diff = vol - mean;
        skew_sum += diff.powi(3);
    }

    let n = data.volumes.len() as f64;
    (skew_sum / n) / (std.powi(3) + 1e-10)
}

/// 大单比例：成交量 > 1.5倍平均的交易数 / 总交易数
fn calc_large_trade_ratio(data: &PrecomputedData) -> f64 {
    let threshold = data.vol_mean * 1.5;
    let large_count = data.volumes.iter().filter(|&&v| v > threshold).count();

    large_count as f64 / data.volumes.len() as f64
}

/// 成交量趋势：最近10条的平均成交量 / 10条前的平均成交量
fn calc_vol_trend(data: &PrecomputedData) -> f64 {
    let n = data.volumes.len();
    let first_half_avg: f64 = data.volumes[..n - 10].iter().sum::<f64>() / (n - 10) as f64;
    let second_half_avg: f64 = data.volumes[n - 10..].iter().sum::<f64>() / 10.0;

    if first_half_avg > 0.0 {
        second_half_avg / first_half_avg
    } else {
        1.0
    }
}

/// 成交不平衡度: (buy_count - sell_count) / (buy_count + sell_count)
fn calc_trade_imbalance(data: &PrecomputedData) -> f64 {
    let total = (data.buy_count + data.sell_count) as f64;
    if total > 0.0 {
        (data.buy_count as f64 - data.sell_count as f64) / total
    } else {
        0.0
    }
}

/// 成交量不平衡度: (buy_vol - sell_vol) / (buy_vol + sell_vol)
fn calc_vol_imbalance(data: &PrecomputedData) -> f64 {
    let total = data.buy_vol + data.sell_vol;
    if total > 0.0 {
        (data.buy_vol - data.sell_vol) / total
    } else {
        0.0
    }
}

/// 净主动买入比例: buy_vol / (buy_vol + sell_vol)
fn calc_net_buy_ratio(data: &PrecomputedData) -> f64 {
    let total = data.buy_vol + data.sell_vol;
    if total > 0.0 {
        data.buy_vol / total
    } else {
        0.0
    }
}

/// 买卖平均单量对比: (buy_vol/buy_count) / (sell_vol/sell_count)
fn calc_avg_trade_size_ratio(data: &PrecomputedData) -> f64 {
    let avg_buy_size = if data.buy_count > 0 {
        data.buy_vol / data.buy_count as f64
    } else {
        0.0
    };

    let avg_sell_size = if data.sell_count > 0 {
        data.sell_vol / data.sell_count as f64
    } else {
        0.0
    };

    if avg_sell_size > 0.0 {
        avg_buy_size / avg_sell_size
    } else if avg_buy_size > 0.0 {
        2.0 // 如果只有买单，返回一个大值
    } else {
        1.0
    }
}

/// 最新价格与VWAP的偏离: (price_last - vwap) / vwap
fn calc_price_vwap_deviation(data: &PrecomputedData) -> f64 {
    if data.vwap > 0.0 {
        (data.price_last - data.vwap) / data.vwap
    } else {
        0.0
    }
}

/// VWAP斜率：最近10条的VWAP与前20-10条的VWAP的变化率
fn calc_vwap_slope(data: &PrecomputedData) -> f64 {
    let n = data.prices.len();

    // 计算前半段VWAP
    let mut first_weighted_sum = 0.0;
    let mut first_vol_sum = 0.0;
    for i in n - 20..n - 10 {
        first_weighted_sum += data.prices[i] * data.volumes[i];
        first_vol_sum += data.volumes[i];
    }
    let first_vwap = if first_vol_sum > 0.0 {
        first_weighted_sum / first_vol_sum
    } else {
        0.0
    };

    // 计算后半段VWAP
    let mut second_weighted_sum = 0.0;
    let mut second_vol_sum = 0.0;
    for i in n - 10..n {
        second_weighted_sum += data.prices[i] * data.volumes[i];
        second_vol_sum += data.volumes[i];
    }
    let second_vwap = if second_vol_sum > 0.0 {
        second_weighted_sum / second_vol_sum
    } else {
        0.0
    };

    if first_vwap > 0.0 {
        (second_vwap - first_vwap) / first_vwap
    } else {
        0.0
    }
}

/// OBV (On-Balance Volume)：价格上涨加成交量，下跌减成交量
fn calc_obv(data: &PrecomputedData) -> f64 {
    if data.prices.len() < 2 {
        return 0.0;
    }

    let mut obv = 0.0;
    for i in 1..data.prices.len() {
        if data.prices[i] > data.prices[i - 1] {
            obv += data.volumes[i];
        } else if data.prices[i] < data.prices[i - 1] {
            obv -= data.volumes[i];
        }
    }

    obv
}

/// 价格与成交量的相关系数
fn calc_price_volume_correlation(data: &PrecomputedData) -> f64 {
    if data.prices.len() < 2 {
        return 0.0;
    }

    let price_mean = data.price_mean;
    let vol_mean = data.vol_mean;

    let mut covariance = 0.0;
    let mut price_var = 0.0;
    let mut vol_var = 0.0;

    for i in 0..data.prices.len() {
        let price_diff = data.prices[i] - price_mean;
        let vol_diff = data.volumes[i] - vol_mean;
        covariance += price_diff * vol_diff;
        price_var += price_diff * price_diff;
        vol_var += vol_diff * vol_diff;
    }

    let denominator = (price_var * vol_var).sqrt();
    covariance / (denominator + 1e-10)
}

/// 交易频率：单位时间（秒）内的成交次数
fn calc_trade_frequency(data: &PrecomputedData) -> f64 {
    if data.time_span <= 0.0 {
        return 0.0;
    }

    let time_span_seconds = data.time_span / 1000.0;
    data.prices.len() as f64 / time_span_seconds
}

/// 平均成交间隔时间（毫秒）
fn calc_avg_trade_interval(data: &PrecomputedData) -> f64 {
    if data.timestamps.len() < 2 {
        return 0.0;
    }

    data.time_span / (data.timestamps.len() - 1) as f64
}

/// 成交间隔时间标准差（毫秒）
fn calc_trade_interval_std(data: &PrecomputedData) -> f64 {
    if data.timestamps.len() < 2 {
        return 0.0;
    }

    let mut intervals = Vec::with_capacity(data.timestamps.len() - 1);
    for i in 1..data.timestamps.len() {
        intervals.push((data.timestamps[i] - data.timestamps[i - 1]) as f64);
    }

    let mean = intervals.iter().sum::<f64>() / intervals.len() as f64;
    calc_std(&intervals, mean)
}

/// 计算标准差
fn calc_std(values: &[f64], mean: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let variance: f64 =
        values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64;

    variance.sqrt()
}

pub fn calc_trade_factors(trades: &[Trade]) -> Result<TradeFactors> {
    if trades.len() < 20 {
        return Err(PlatformError::FactorError {
            message: format!("Not enough trades to calculate factors"),
        });
    }

    let data = PrecomputedData::new(trades);

    Ok(TradeFactors {
        // 价格动量因子
        price_return: calc_price_return(&data),
        trend_strength: calc_trend_strength(&data),
        price_volatility: calc_price_volatility(&data),
        price_range: calc_price_range(&data),
        price_acceleration: calc_price_acceleration(&data),
        price_position: calc_price_position(&data),

        // 成交量因子
        avg_vol: calc_avg_vol(&data),
        vol_volatility: calc_vol_volatility(&data),
        vol_skew: calc_vol_skew(&data),
        large_trade_ratio: calc_large_trade_ratio(&data),
        vol_trend: calc_vol_trend(&data),

        // 买卖压力因子
        buy_count: data.buy_count,
        sell_count: data.sell_count,
        trade_imbalance: calc_trade_imbalance(&data),
        buy_vol: data.buy_vol,
        sell_vol: data.sell_vol,
        vol_imbalance: calc_vol_imbalance(&data),
        net_buy_ratio: calc_net_buy_ratio(&data),
        avg_trade_size_ratio: calc_avg_trade_size_ratio(&data),

        // 价量因子
        vwap: data.vwap,
        price_vwap_deviation: calc_price_vwap_deviation(&data),
        vwap_slope: calc_vwap_slope(&data),
        obv: calc_obv(&data),
        price_volume_correlation: calc_price_volume_correlation(&data),

        // 时间序列因子
        trade_frequency: calc_trade_frequency(&data),
        avg_trade_interval: calc_avg_trade_interval(&data),
        trade_interval_std: calc_trade_interval_std(&data),
    })
}
