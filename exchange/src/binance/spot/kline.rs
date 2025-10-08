use crate::binance::spot::models::KlineData;
use arc_swap::ArcSwap;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::RwLock;

struct BuildingStat {
    kline: Arc<KlineData>,
    allow_update_by_trade: bool,
    first_trade_id: Option<u64>,
    last_trade_id: Option<u64>,
}

struct Kline {
    symbol: String,
    interval: u64,

    max_building_cnt: usize, // 如果超过这个限制，强制归档最早的kline
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

    pub fn len(&self) -> usize {
        self.archived.len() + self.building.len()
    }
}

impl Kline {
    fn new(
        symbol: String,
        interval: u64,
        max_building_cnt: usize,
        max_archived_cnt: usize,
    ) -> Self {
        Self {
            symbol,
            interval,
            max_building_cnt,
            building: RwLock::new(VecDeque::new()),
            max_archived_cnt,
            archived: ArcSwap::from_pointee(VecDeque::new()),
        }
    }

    async fn get_klines(&self) -> KlineView {
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

    fn _push_back(&self, klines: &mut VecDeque<Kline>, kline: Kline) {
        if klines.len() >= self.max_cnt {
            klines.pop_front();
        }
        klines.push_back(kline);
    }

    async fn update_by_kline(&self, kline: KlineData) -> Result<()> {
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

        let klines = &mut *self.klines.write().await;

        if let Some(front) = klines.front() {
            if kline.open_time < front.kline.open_time {
                return Ok(());
            }
            let index = ((kline.open_time - front.kline.open_time) / self.interval) as usize;
            while klines.len() <= index {
                let mut kline = klines.back().unwrap().clone();
                kline.kline.open_time += self.interval;
                kline.kline.close_time += self.interval;
                kline.kline.volume = Decimal::new(0, 0);
                kline.kline.quote_volume = Decimal::new(0, 0);
                kline.kline.trade_count = 0;
                kline.allow_update_by_trade = true;
                kline.first_trade_id = None;
                kline.last_trade_id = None;
                self._push_back(klines, kline);
            }
            klines[index] = Kline {
                kline: kline,
                allow_update_by_trade: false,
                first_trade_id: None,
                last_trade_id: None,
            };
        } else {
            klines.push_back(Kline {
                kline: kline,
                allow_update_by_trade: false,
                first_trade_id: None,
                last_trade_id: None,
            });
        }
        Ok(())
    }

    async fn update_by_trade(&self, trade: AggTrade) -> Result<()> {
        if trade.symbol != self.symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Trade symbol mismatch".to_string(),
            });
        }

        let klines = &mut *self.klines.write().await;

        if let Some(front) = klines.front() {
            if trade.timestamp < front.kline.open_time {
                return Ok(());
            }
            let index = ((trade.timestamp - front.kline.open_time) / self.interval) as usize;
            while klines.len() <= index {
                let mut kline = klines.back().unwrap().clone();
                kline.kline.open_time += self.interval;
                kline.kline.close_time += self.interval;
                kline.kline.volume = Decimal::new(0, 0);
                kline.kline.quote_volume = Decimal::new(0, 0);
                kline.kline.trade_count = 0;
                kline.allow_update_by_trade = true;
                kline.first_trade_id = None;
                kline.last_trade_id = None;
                self._push_back(klines, kline);
            }
            let kline = &mut klines[index];
            if !kline.allow_update_by_trade {
                return Ok(());
            }
            if kline.first_trade_id.is_none() && kline.last_trade_id.is_none() {
                kline.first_trade_id = Some(trade.first_trade_id);
                kline.last_trade_id = Some(trade.last_trade_id);
                kline.kline.symbol = trade.symbol;
                kline.kline.open = trade.price.clone();
                kline.kline.high = trade.price.clone();
                kline.kline.low = trade.price.clone();
                kline.kline.close = trade.price.clone();
                kline.kline.volume = trade.quantity.clone();
                kline.kline.quote_volume = trade.price * trade.quantity;
                kline.kline.trade_count = trade.last_trade_id - trade.first_trade_id + 1;
            } else {
                if kline.first_trade_id.unwrap() > trade.first_trade_id {
                    kline.kline.open = trade.price.clone();
                }
                if kline.last_trade_id.unwrap() < trade.last_trade_id {
                    kline.kline.close = trade.price.clone();
                }
                if kline.kline.high < trade.price {
                    kline.kline.high = trade.price.clone();
                }
                if kline.kline.low > trade.price {
                    kline.kline.low = trade.price.clone();
                }
                kline.kline.volume += trade.quantity.clone();
                kline.kline.quote_volume += trade.price * trade.quantity;
                kline.kline.trade_count += trade.last_trade_id - trade.first_trade_id + 1;
            }
        } else {
            let open_time = trade.timestamp - (trade.timestamp % self.interval);
            let close_time = open_time + self.interval;
            self._push_back(
                klines,
                Kline {
                    kline: KlineData {
                        symbol: trade.symbol,
                        open_time,
                        close_time,
                        open: trade.price.clone(),
                        high: trade.price.clone(),
                        low: trade.price.clone(),
                        close: trade.price.clone(),
                        volume: trade.quantity.clone(),
                        quote_volume: trade.price * trade.quantity,
                        trade_count: trade.last_trade_id - trade.first_trade_id + 1,
                    },
                    allow_update_by_trade: true,
                    first_trade_id: Some(trade.first_trade_id),
                    last_trade_id: Some(trade.last_trade_id),
                },
            );
        }
        Ok(())
    }
}
