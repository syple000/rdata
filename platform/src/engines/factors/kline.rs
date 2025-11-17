use crate::{
    errors::{PlatformError, Result},
    models::KlineData,
};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineFactors {
    // 价格动量与趋势
    pub price_return: f64,     // 收益率: close/prev_close - 1（序列首尾）
    pub trend_strength: f64,   // 趋势强度: (上涨k线数-下跌k线数)/(N-1)
    pub price_volatility: f64, // 收益率波动率
    pub price_range: f64,      // 价格区间幅度: (max_close-min_close)/mean_close
    pub price_position: f64,   // 位置: (最新close-min_close)/(max_close-min_close)

    // 量价
    pub avg_volume: f64,               // 平均成交量
    pub volume_volatility: f64,        // 成交量波动率
    pub volume_trend: f64,             // 成交量趋势: 后半段均量/前半段均量
    pub obv: f64,                      // OBV: 收盘涨加量、跌减量
    pub price_volume_correlation: f64, // 收盘价与成交量相关系数

    // K线形态/波动
    pub avg_intraday_range: f64, // 平均日内振幅 (high-low)/close_prev
    pub avg_body_ratio: f64,     // 平均实体占比 |close-open|/(high-low)
}

struct PrecomputedKlineData {
    closes: Vec<f64>,
    opens: Vec<f64>,
    highs: Vec<f64>,
    lows: Vec<f64>,
    volumes: Vec<f64>,

    close_mean: f64,
    close_min: f64,
    close_max: f64,

    volume_mean: f64,
}

impl PrecomputedKlineData {
    fn new(klines: &[KlineData]) -> Self {
        let n = klines.len();
        let mut closes = Vec::with_capacity(n);
        let mut opens = Vec::with_capacity(n);
        let mut highs = Vec::with_capacity(n);
        let mut lows = Vec::with_capacity(n);
        let mut volumes = Vec::with_capacity(n);

        let mut close_sum = 0.0;
        let mut close_min = f64::MAX;
        let mut close_max = f64::MIN;
        let mut volume_sum = 0.0;

        for k in klines {
            let o = k.open.to_f64().unwrap_or(0.0);
            let h = k.high.to_f64().unwrap_or(0.0);
            let l = k.low.to_f64().unwrap_or(0.0);
            let c = k.close.to_f64().unwrap_or(0.0);
            let v = k.volume.to_f64().unwrap_or(0.0);

            opens.push(o);
            highs.push(h);
            lows.push(l);
            closes.push(c);
            volumes.push(v);

            close_sum += c;
            close_min = close_min.min(c);
            close_max = close_max.max(c);
            volume_sum += v;
        }

        let close_mean = close_sum / n as f64;
        let volume_mean = volume_sum / n as f64;

        Self {
            closes,
            opens,
            highs,
            lows,
            volumes,
            close_mean,
            close_min,
            close_max,
            volume_mean,
        }
    }
}

fn calc_std(values: &[f64], mean: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let variance: f64 =
        values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64;

    variance.sqrt()
}

fn calc_price_return(data: &PrecomputedKlineData) -> f64 {
    let first = data.closes.first().cloned().unwrap_or(0.0);
    let last = data.closes.last().cloned().unwrap_or(0.0);
    if first.abs() < 1e-10 {
        0.0
    } else {
        last / first - 1.0
    }
}

fn calc_trend_strength(data: &PrecomputedKlineData) -> f64 {
    let mut up = 0i64;
    let mut down = 0i64;
    for i in 1..data.closes.len() {
        let diff = data.closes[i] - data.closes[i - 1];
        if diff > 0.0 {
            up += 1;
        } else if diff < 0.0 {
            down += 1;
        }
    }
    let total = (data.closes.len().saturating_sub(1)) as f64;
    if total > 0.0 {
        (up as f64 - down as f64) / total
    } else {
        0.0
    }
}

fn calc_price_volatility(data: &PrecomputedKlineData) -> f64 {
    let mut rets = Vec::with_capacity(data.closes.len().saturating_sub(1));
    for i in 1..data.closes.len() {
        let prev = data.closes[i - 1];
        if prev.abs() < 1e-10 {
            continue;
        }
        rets.push(data.closes[i] / prev - 1.0);
    }
    if rets.is_empty() {
        return 0.0;
    }
    let mean = rets.iter().sum::<f64>() / rets.len() as f64;
    calc_std(&rets, mean)
}

fn calc_price_range(data: &PrecomputedKlineData) -> f64 {
    (data.close_max - data.close_min) / (data.close_mean + 1e-10)
}

fn calc_price_position(data: &PrecomputedKlineData) -> f64 {
    let last = data.closes.last().cloned().unwrap_or(0.0);
    let range = data.close_max - data.close_min;
    if range > 0.0 {
        (last - data.close_min) / range
    } else {
        0.5
    }
}

fn calc_avg_volume(data: &PrecomputedKlineData) -> f64 {
    data.volume_mean
}

fn calc_volume_volatility(data: &PrecomputedKlineData) -> f64 {
    calc_std(&data.volumes, data.volume_mean)
}

fn calc_volume_trend(data: &PrecomputedKlineData) -> f64 {
    let n = data.volumes.len();
    if n < 4 {
        return 1.0;
    }
    let half = n / 2;
    let first_avg: f64 = data.volumes[..half].iter().sum::<f64>() / half as f64;
    let second_avg: f64 = data.volumes[half..].iter().sum::<f64>() / (n - half) as f64;
    if first_avg > 0.0 {
        second_avg / first_avg
    } else {
        1.0
    }
}

fn calc_obv(data: &PrecomputedKlineData) -> f64 {
    if data.closes.len() < 2 {
        return 0.0;
    }
    let mut obv = 0.0;
    for i in 1..data.closes.len() {
        if data.closes[i] > data.closes[i - 1] {
            obv += data.volumes[i];
        } else if data.closes[i] < data.closes[i - 1] {
            obv -= data.volumes[i];
        }
    }
    obv
}

fn calc_price_volume_correlation(data: &PrecomputedKlineData) -> f64 {
    let n = data.closes.len();
    if n < 2 {
        return 0.0;
    }
    let price_mean = data.close_mean;
    let vol_mean = data.volume_mean;

    let mut cov = 0.0;
    let mut price_var = 0.0;
    let mut vol_var = 0.0;
    for i in 0..n {
        let dp = data.closes[i] - price_mean;
        let dv = data.volumes[i] - vol_mean;
        cov += dp * dv;
        price_var += dp * dp;
        vol_var += dv * dv;
    }
    let denom = (price_var * vol_var).sqrt();
    cov / (denom + 1e-10)
}

fn calc_avg_intraday_range(data: &PrecomputedKlineData) -> f64 {
    if data.closes.len() < 2 {
        return 0.0;
    }
    let mut ranges = Vec::with_capacity(data.closes.len());
    for i in 0..data.closes.len() {
        let h = data.highs[i];
        let l = data.lows[i];
        let prev_close = if i == 0 {
            data.opens[i]
        } else {
            data.closes[i - 1]
        };
        if prev_close.abs() < 1e-10 {
            continue;
        }
        ranges.push((h - l) / prev_close.abs());
    }
    if ranges.is_empty() {
        return 0.0;
    }
    ranges.iter().sum::<f64>() / ranges.len() as f64
}

fn calc_avg_body_ratio(data: &PrecomputedKlineData) -> f64 {
    let mut ratios = Vec::with_capacity(data.closes.len());
    for i in 0..data.closes.len() {
        let body = (data.closes[i] - data.opens[i]).abs();
        let range = (data.highs[i] - data.lows[i]).abs();
        if range <= 0.0 {
            continue;
        }
        ratios.push(body / range);
    }
    if ratios.is_empty() {
        return 0.0;
    }
    ratios.iter().sum::<f64>() / ratios.len() as f64
}

pub fn calc_kline_factors(klines: &[KlineData]) -> Result<KlineFactors> {
    if klines.len() < 5 {
        return Err(PlatformError::FactorError {
            message: "Not enough klines to calculate factors".to_string(),
        });
    }

    let data = PrecomputedKlineData::new(klines);

    Ok(KlineFactors {
        price_return: calc_price_return(&data),
        trend_strength: calc_trend_strength(&data),
        price_volatility: calc_price_volatility(&data),
        price_range: calc_price_range(&data),
        price_position: calc_price_position(&data),

        avg_volume: calc_avg_volume(&data),
        volume_volatility: calc_volume_volatility(&data),
        volume_trend: calc_volume_trend(&data),
        obv: calc_obv(&data),
        price_volume_correlation: calc_price_volume_correlation(&data),

        avg_intraday_range: calc_avg_intraday_range(&data),
        avg_body_ratio: calc_avg_body_ratio(&data),
    })
}
