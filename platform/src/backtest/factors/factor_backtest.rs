use crate::{
    backtest::factors::traits::{FactorCalculator, PriceProvider},
    data_manager::local_data_manager::{Clock, LocalMarketDataManager},
    errors::Result,
    models::MarketType,
};
use std::{collections::HashMap, sync::Arc};

/// 因子回测记录
#[derive(Debug, Clone)]
pub struct FactorRecord {
    pub timestamp: u64,              // 回测时钟时间
    pub factor_value: f64,           // 因子值
    pub factor_timestamp: u64,       // 因子数据的行情时间戳
    pub price: f64,                  // 基准价格
    pub price_timestamp: u64,        // 价格数据的行情时间戳
    pub forward_return: Option<f64>, // 未来 N 个周期的收益率
}

pub struct FactorBacktester {
    market_mgr: Arc<LocalMarketDataManager>,
    clock: Arc<Clock>,
}

impl FactorBacktester {
    pub fn new(market_mgr: Arc<LocalMarketDataManager>, clock: Arc<Clock>) -> Self {
        Self { market_mgr, clock }
    }

    /// 执行回测
    /// - calculator: 实现了 FactorCalculator 的因子计算逻辑
    /// - price_provider: 实现了 PriceProvider 的价格获取逻辑
    /// - market_type: 市场类型
    /// - symbol: 交易对
    /// - start_ts: 开始时间戳
    /// - end_ts: 结束时间戳
    /// - step_ms: 步进时间（例如 1分钟 = 60000ms）
    /// - forward_steps: 计算未来多少个 step 的收益率
    pub async fn run_test(
        &self,
        calculator: &dyn FactorCalculator,
        price_provider: &dyn PriceProvider,
        market_type: MarketType,
        symbol: &str,
        start_ts: u64,
        end_ts: u64,
        step_ms: u64,
        forward_steps: usize,
    ) -> Result<Vec<FactorRecord>> {
        let mut records = Vec::new();
        let mut cur_ts = start_ts;

        // 允许的最大时间间隔：step_ms
        let max_lag_ms = step_ms;
        let mut loop_cnt = 0;

        // 1. 遍历时间轴，计算因子值和价格
        while cur_ts <= end_ts {
            if loop_cnt % 1000 == 0 {
                log::info!(
                    "Backtesting {} at time {}, progress: {:.2}%",
                    symbol,
                    cur_ts,
                    (cur_ts - start_ts) as f64 / (end_ts - start_ts) as f64 * 100.0
                );
            }
            loop_cnt += 1;

            // 设置模拟时钟，LocalMarketDataManager 会根据这个时间过滤数据
            self.clock.set_cur_ts(cur_ts);

            // 获取因子值和因子行情时间戳
            let factor_result = calculator
                .calculate(&self.market_mgr, &market_type, symbol)
                .await;

            // 获取价格和价格行情时间戳
            let price_result = price_provider
                .get_price(&self.market_mgr, &market_type, symbol)
                .await;

            // 只有当因子和价格都成功获取时才记录
            if let (Ok((factor_value, factor_ts)), Ok((price, price_ts))) =
                (factor_result, price_result)
            {
                // 检查因子行情时间戳的延迟
                let factor_lag = if cur_ts >= factor_ts {
                    cur_ts - factor_ts
                } else {
                    // 行情时间戳不应该超前当前时钟
                    log::warn!(
                        "Factor timestamp {} is ahead of current time {} for {}",
                        factor_ts,
                        cur_ts,
                        symbol
                    );
                    cur_ts += step_ms;
                    continue;
                };

                // 检查价格行情时间戳的延迟
                let price_lag = if cur_ts >= price_ts {
                    cur_ts - price_ts
                } else {
                    // 行情时间戳不应该超前当前时钟
                    log::warn!(
                        "Price timestamp {} is ahead of current time {} for {}",
                        price_ts,
                        cur_ts,
                        symbol
                    );
                    cur_ts += step_ms;
                    continue;
                };

                // 如果因子或价格的行情时间戳延迟超过 step_ms/2，则跳过
                if factor_lag > max_lag_ms {
                    log::warn!(
                        "Factor data lag ({} ms) exceeds threshold ({} ms) for {} at cur_ts={}, factor_ts={}, skipping",
                        factor_lag,
                        max_lag_ms,
                        symbol,
                        cur_ts,
                        factor_ts
                    );
                    cur_ts += step_ms;
                    continue;
                }

                if price_lag > max_lag_ms {
                    log::warn!(
                        "Price data lag ({} ms) exceeds threshold ({} ms) for {} at cur_ts={}, price_ts={}, skipping",
                        price_lag,
                        max_lag_ms,
                        symbol,
                        cur_ts,
                        price_ts
                    );
                    cur_ts += step_ms;
                    continue;
                }

                // 数据有效性检查
                if !factor_value.is_nan() && price > 0.0 {
                    records.push(FactorRecord {
                        timestamp: cur_ts,
                        factor_value,
                        factor_timestamp: factor_ts,
                        price,
                        price_timestamp: price_ts,
                        forward_return: None, // 稍后填充
                    });
                }
            } else {
                log::warn!(
                    "Failed to get factor or price for {} at {} in {:?}, skipping this timestamp.",
                    symbol,
                    cur_ts,
                    market_type
                );
            }

            cur_ts += step_ms;
        }

        // 2. 计算 Forward Return (未来收益率)
        // 构建一个 时间戳 -> 价格 的快速查找表
        let price_map: HashMap<u64, f64> = records.iter().map(|r| (r.timestamp, r.price)).collect();

        let target_duration = step_ms * forward_steps as u64;

        for record in records.iter_mut() {
            let target_ts = record.timestamp + target_duration;

            // 只有当严格对应的未来时间点有价格时，才计算收益
            // 如果未来那个时间点数据缺失，那么这个样本的 forward_return 就是 None，
            // 在计算 IC 时会被自动剔除。这是最严谨的做法。
            if let Some(future_price) = price_map.get(&target_ts) {
                if record.price > 0.0 {
                    let ret = (future_price - record.price) / record.price;
                    record.forward_return = Some(ret);
                }
            }
        }

        Ok(records)
    }

    /// 计算 IC (Information Coefficient)
    pub fn calculate_ic(&self, records: &[FactorRecord]) -> f64 {
        let valid_records: Vec<&FactorRecord> = records
            .iter()
            .filter(|r| r.forward_return.is_some() && !r.factor_value.is_nan())
            .collect();

        if valid_records.len() < 2 {
            return 0.0;
        }

        let n = valid_records.len() as f64;
        let mean_factor = valid_records.iter().map(|r| r.factor_value).sum::<f64>() / n;
        let mean_return = valid_records
            .iter()
            .map(|r| r.forward_return.unwrap())
            .sum::<f64>()
            / n;

        let mut numerator = 0.0;
        let mut var_factor = 0.0;
        let mut var_return = 0.0;

        for r in valid_records {
            let f_diff = r.factor_value - mean_factor;
            let r_diff = r.forward_return.unwrap() - mean_return;

            numerator += f_diff * r_diff;
            var_factor += f_diff * f_diff;
            var_return += r_diff * r_diff;
        }

        if var_factor == 0.0 || var_return == 0.0 {
            return 0.0;
        }

        numerator / (var_factor.sqrt() * var_return.sqrt())
    }

    /// 计算 IC 和 IR (Information Ratio)
    /// 将数据按天分组，计算每日 IC，然后计算 IC 的均值和标准差
    /// 返回 (IC_Mean, IC_IR)
    pub fn calculate_ic_ir(&self, records: &[FactorRecord]) -> (f64, f64) {
        let mut daily_records: HashMap<u64, Vec<FactorRecord>> = HashMap::new();

        for record in records {
            if record.forward_return.is_some() && !record.factor_value.is_nan() {
                // 按天分组 (UTC)
                let day = record.timestamp / 86_400_000;
                daily_records
                    .entry(day)
                    .or_insert_with(Vec::new)
                    .push(record.clone());
            }
        }

        let mut daily_ics = Vec::new();
        for (_day, day_records) in daily_records {
            // 样本太少不计算
            if day_records.len() < 2 {
                continue;
            }
            let ic = self.calculate_ic(&day_records);
            if !ic.is_nan() {
                daily_ics.push(ic);
            }
        }

        if daily_ics.is_empty() {
            return (0.0, 0.0);
        }

        let n = daily_ics.len() as f64;
        let mean_ic = daily_ics.iter().sum::<f64>() / n;

        let variance = daily_ics.iter().map(|x| (x - mean_ic).powi(2)).sum::<f64>() / n;
        let std_dev = variance.sqrt();

        let ir = if std_dev != 0.0 {
            mean_ic / std_dev
        } else {
            0.0
        };

        (mean_ic, ir)
    }
}
