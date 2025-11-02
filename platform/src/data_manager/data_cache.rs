use std::{collections::VecDeque, sync::Arc};

use dashmap::DashMap;
use rust_decimal::Decimal;
use tokio::sync::RwLock;

use crate::models::{KlineData, KlineInterval};

struct KlineCache {
    klines: DashMap<(String, KlineInterval), Arc<RwLock<VecDeque<KlineData>>>>,
    max_capacity: usize,
}

impl KlineCache {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            klines: DashMap::new(),
            max_capacity,
        }
    }

    pub async fn add_kline(&self, symbol: String, kline: KlineData) {
        let key = (symbol.clone(), kline.interval.clone());
        let klines = self
            .klines
            .entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(VecDeque::with_capacity(self.max_capacity))))
            .clone();

        let from_kline = |kline: KlineData| -> KlineData {
            KlineData {
                symbol: kline.symbol,
                interval: kline.interval.clone(),
                open_time: kline.open_time + kline.interval.to_millis(),
                close_time: kline.close_time + kline.interval.to_millis(),
                open: kline.close,
                high: kline.close,
                low: kline.close,
                close: kline.close,
                volume: Decimal::from(0),
                quote_volume: Decimal::from(0),
                is_closed: false, // 从前一个kline填充的新kline都默认未关闭
            }
        };

        let mut klines_lock = klines.write().await;
        if klines_lock.is_empty() {
            klines_lock.push_back(kline);
        } else {
            let mut first_kline_time = klines_lock.front().unwrap().open_time;
            let mut last_kline_time = klines_lock.back().unwrap().open_time;
            if kline.open_time < first_kline_time {
                while first_kline_time > kline.open_time && klines_lock.len() < self.max_capacity {
                    let new_kline = from_pre_kline(kline);
                    klines_lock.push_front(new_kline);
                    first_kline_time
                }
            } else if kline.open_time > last_kline_time {
                let mut current_time = last_kline_time;
                while current_time < kline.open_time {
                    let pre_kline = klines_lock.back().unwrap().clone();
                    let new_kline = from_pre_kline(pre_kline);
                    klines_lock.push_back(new_kline);
                    current_time = klines_lock.back().unwrap().open_time;
                    if klines_lock.len() > self.max_capacity {
                        klines_lock.pop_front();
                    }
                }
            }
        }
    }

    pub async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
    ) -> Option<Vec<KlineData>> {
        let key = (symbol.to_string(), interval);
        if let Some(kline_list) = self.klines.get(&key) {
            let klines = kline_list.value().read().await;
            Some(klines.iter().cloned().collect())
        } else {
            None
        }
    }
}
