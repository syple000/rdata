use crate::binance::{
    errors::Result,
    spot::models::{AggTrade, KlineData},
};
use arc_swap::ArcSwap;
use rust_decimal::Decimal;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone)]
struct BuildingStat {
    kline: Arc<KlineData>,
    is_fin: bool,
    first_trade_id: Option<u64>,
    last_trade_id: Option<u64>,
}

pub struct Kline {
    symbol: String,
    interval: u64,

    max_building_cnt: usize, // 最大正确构建数限制，会强制归档更早的kline
    overload_ratio: usize, // 超过最大正确构建数的多少倍时，强制归档更早的kline。根据更新/读取频率自定义调整
    building: RwLock<VecDeque<BuildingStat>>,
    max_archived_cnt: usize, // 归档的最大数量
    archived: ArcSwap<VecDeque<Arc<KlineData>>>,
}

pub struct KlineView {
    archived: Arc<VecDeque<Arc<KlineData>>>,
    building: VecDeque<Arc<KlineData>>,
}

impl KlineView {
    pub fn iter(&self) -> impl Iterator<Item = &KlineData> {
        self.archived
            .iter()
            .chain(self.building.iter())
            .map(|k| k.as_ref())
    }

    pub fn klines(&self) -> Vec<Arc<KlineData>> {
        self.archived
            .iter()
            .chain(self.building.iter())
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.archived.len() + self.building.len()
    }
}

impl Kline {
    pub fn new(
        symbol: String,
        interval: u64,
        max_building_cnt: usize,
        overload_ratio: usize,
        max_archived_cnt: usize,
    ) -> Self {
        Self {
            symbol,
            interval,
            max_building_cnt,
            overload_ratio,
            building: RwLock::new(VecDeque::with_capacity(max_building_cnt)),
            max_archived_cnt,
            archived: ArcSwap::from_pointee(VecDeque::with_capacity(max_archived_cnt)),
        }
    }

    pub async fn get_klines(&self) -> KlineView {
        KlineView {
            archived: self.archived.load_full(),
            building: self
                .building
                .read()
                .await
                .iter()
                .map(|k| k.kline.clone())
                .collect(),
        }
    }

    pub async fn archive(&self) {
        let building_klines = self.building.read().await;
        if building_klines.len() <= self.max_building_cnt * self.overload_ratio {
            return;
        }
        drop(building_klines);

        let mut building_klines = self.building.write().await;
        let mut archived_ptr = self.archived.load_full();
        let archived = Arc::make_mut(&mut archived_ptr);
        while building_klines.len() > self.max_building_cnt {
            let kline = building_klines.pop_front().unwrap();
            archived.push_back(kline.kline.clone());
            if archived.len() > self.max_archived_cnt {
                archived.pop_front();
            }
        }
        self.archived.store(archived_ptr);
    }

    pub async fn update_by_kline(&self, kline: &KlineData) -> Result<()> {
        if kline.close_time - kline.open_time != self.interval {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Kline interval mismatch".to_string(),
            });
        }
        if kline.open_time % self.interval != 0 {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Kline open time not aligned with interval".to_string(),
            });
        }
        if kline.symbol != self.symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Kline symbol mismatch".to_string(),
            });
        }

        let mut building_klines = self.building.write().await;

        let mut index = 0;
        if let Some(front) = building_klines.front() {
            if kline.open_time < front.kline.open_time {
                return Ok(());
            }
            index = ((kline.open_time - front.kline.open_time) / self.interval) as usize;
        }

        if index < building_klines.len() {
            let stat = BuildingStat {
                kline: Arc::new(kline.clone()),
                is_fin: true,
                first_trade_id: None,
                last_trade_id: None,
            };
            building_klines[index] = stat;
            return Ok(());
        }

        while building_klines.len() < index {
            let mut kline = building_klines.back().unwrap().clone();
            let kline_inner = Arc::make_mut(&mut kline.kline);
            kline_inner.open_time += self.interval;
            kline_inner.close_time += self.interval;
            kline_inner.open = kline_inner.close;
            kline_inner.high = kline_inner.close;
            kline_inner.low = kline_inner.close;
            kline_inner.volume = Decimal::new(0, 0);
            kline_inner.quote_volume = Decimal::new(0, 0);
            kline_inner.trade_count = 0;
            kline.is_fin = false;
            kline.first_trade_id = None;
            kline.last_trade_id = None;
            building_klines.push_back(kline);
        }

        let stat = BuildingStat {
            kline: Arc::new(kline.clone()),
            is_fin: true,
            first_trade_id: None,
            last_trade_id: None,
        };
        building_klines.push_back(stat);
        drop(building_klines);

        self.archive().await;

        Ok(())
    }

    pub async fn update_by_trade(&self, trade: &AggTrade) -> Result<()> {
        if trade.symbol != self.symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Trade symbol mismatch".to_string(),
            });
        }

        let mut building_klines = self.building.write().await;
        let mut index = 0;
        if let Some(front) = building_klines.front() {
            if trade.timestamp < front.kline.open_time {
                return Ok(());
            }
            index = ((trade.timestamp - front.kline.open_time) / self.interval) as usize;
        }

        if index < building_klines.len() {
            let kline = &mut building_klines[index];
            if kline.is_fin {
                return Ok(());
            }

            if kline.first_trade_id.is_none() && kline.last_trade_id.is_none() {
                kline.first_trade_id = Some(trade.first_trade_id);
                kline.last_trade_id = Some(trade.last_trade_id);
                let kline_inner = Arc::make_mut(&mut kline.kline);
                kline_inner.open = trade.price.clone();
                kline_inner.high = trade.price.clone();
                kline_inner.low = trade.price.clone();
                kline_inner.close = trade.price.clone();
                kline_inner.volume = trade.quantity.clone();
                kline_inner.quote_volume = trade.price * trade.quantity;
                kline_inner.trade_count = trade.last_trade_id - trade.first_trade_id + 1;
            } else {
                let kline_inner = Arc::make_mut(&mut kline.kline);
                if kline.first_trade_id.unwrap() > trade.first_trade_id {
                    kline_inner.open = trade.price.clone();
                    kline.first_trade_id = Some(trade.first_trade_id);
                }
                if kline.last_trade_id.unwrap() < trade.last_trade_id {
                    kline_inner.close = trade.price.clone();
                    kline.last_trade_id = Some(trade.last_trade_id);
                }
                if kline_inner.high < trade.price {
                    kline_inner.high = trade.price.clone();
                }
                if kline_inner.low > trade.price {
                    kline_inner.low = trade.price.clone();
                }
                kline_inner.volume += trade.quantity.clone();
                kline_inner.quote_volume += trade.price * trade.quantity;
                kline_inner.trade_count += trade.last_trade_id - trade.first_trade_id + 1;
            }

            return Ok(());
        }

        while building_klines.len() < index {
            let mut kline = building_klines.back().unwrap().clone();
            let kline_inner = Arc::make_mut(&mut kline.kline);
            kline_inner.open_time += self.interval;
            kline_inner.close_time += self.interval;
            kline_inner.open = kline_inner.close;
            kline_inner.high = kline_inner.close;
            kline_inner.low = kline_inner.close;
            kline_inner.volume = Decimal::new(0, 0);
            kline_inner.quote_volume = Decimal::new(0, 0);
            kline_inner.trade_count = 0;
            kline.is_fin = false;
            kline.first_trade_id = None;
            kline.last_trade_id = None;
            building_klines.push_back(kline);
        }

        let open_time = trade.timestamp - (trade.timestamp % self.interval);
        let close_time = open_time + self.interval;
        building_klines.push_back(BuildingStat {
            kline: Arc::new(KlineData {
                symbol: trade.symbol.clone(),
                open_time,
                close_time,
                open: trade.price.clone(),
                high: trade.price.clone(),
                low: trade.price.clone(),
                close: trade.price.clone(),
                volume: trade.quantity.clone(),
                quote_volume: trade.price * trade.quantity,
                trade_count: trade.last_trade_id - trade.first_trade_id + 1,
            }),
            is_fin: false,
            first_trade_id: Some(trade.first_trade_id),
            last_trade_id: Some(trade.last_trade_id),
        });
        drop(building_klines);

        self.archive().await;

        Ok(())
    }
}
