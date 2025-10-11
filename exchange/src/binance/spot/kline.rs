use crate::binance::{
    errors::Result,
    spot::models::{AggTrade, KlineData},
};
use arc_swap::ArcSwap;
use csv::{Writer, WriterBuilder};
use log::{error, warn};
use rust_decimal::Decimal;
use std::{
    collections::VecDeque,
    fs::OpenOptions,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use time::get_current_milli_timestamp;
use tokio::sync::{Mutex, RwLock};

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

    max_building_cache_cnt: usize, // 长时间未手动进行归档，在触发容量阈值后，自动归档
    target_building_cache_cnt: usize, // 归档时，将数据长度缩减到该值
    building_klines: RwLock<VecDeque<BuildingStat>>,
    max_archived_cache_cnt: usize,
    latest_archived_kline_time: AtomicU64,
    archived_klines: ArcSwap<VecDeque<Arc<KlineData>>>,
    archived_csv: Mutex<Writer<std::fs::File>>,
}

pub struct KlineView {
    archived_klines: Arc<VecDeque<Arc<KlineData>>>,
    building_klines: VecDeque<Arc<KlineData>>,
}

impl KlineView {
    pub fn iter(&self) -> impl Iterator<Item = &KlineData> {
        self.archived_klines
            .iter()
            .chain(self.building_klines.iter())
            .map(|k| k.as_ref())
    }

    pub fn klines(&self) -> Vec<Arc<KlineData>> {
        self.archived_klines
            .iter()
            .chain(self.building_klines.iter())
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.archived_klines.len() + self.building_klines.len()
    }
}

impl Kline {
    pub fn new(
        symbol: String,
        interval: u64,
        max_building_cache_cnt: usize,
        target_building_cache_cnt: usize,
        max_archived_cache_cnt: usize,
    ) -> Result<Self> {
        let csv_file = format!(
            "{}_trades_{}.csv",
            symbol,
            get_current_milli_timestamp().to_string()
        );
        let path = Path::new(&csv_file);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| crate::binance::errors::BinanceError::ClientError {
                message: format!("Failed to new Trade, open CSV file fail: {}", e),
            })?;
        let csv_writer = WriterBuilder::new().has_headers(true).from_writer(file);

        Ok(Self {
            symbol,
            interval,
            max_building_cache_cnt,
            target_building_cache_cnt,
            building_klines: RwLock::new(VecDeque::with_capacity(max_building_cache_cnt)),
            max_archived_cache_cnt,
            latest_archived_kline_time: AtomicU64::new(0),
            archived_klines: ArcSwap::from_pointee(VecDeque::with_capacity(max_archived_cache_cnt)),
            archived_csv: Mutex::new(csv_writer),
        })
    }

    pub async fn get_klines(&self) -> KlineView {
        KlineView {
            archived_klines: self.archived_klines.load_full(),
            building_klines: self
                .building_klines
                .read()
                .await
                .iter()
                .map(|k| k.kline.clone())
                .collect(),
        }
    }

    pub async fn archive(&self, archived_kline_time: u64) {
        if archived_kline_time <= self.latest_archived_kline_time.load(Ordering::Relaxed) {
            return;
        }

        let mut building_klines = self.building_klines.write().await;

        self.latest_archived_kline_time
            .store(archived_kline_time, Ordering::Release);

        let mut to_archive_klines = vec![];
        loop {
            let front = building_klines.front();
            if front.is_none() {
                break;
            }
            let front = front.unwrap();
            if front.kline.open_time > archived_kline_time {
                break;
            }
            let front = building_klines.pop_front().unwrap();
            to_archive_klines.push(front.kline);
        }

        if to_archive_klines.is_empty() {
            return;
        }

        let mut archived_klines_ptr = self.archived_klines.load_full();
        let archived_klines = Arc::make_mut(&mut archived_klines_ptr);
        let mut archived_csv = self.archived_csv.lock().await;
        for kline in to_archive_klines {
            if let Err(e) = archived_csv.serialize(&*kline) {
                error!("Failed to archive kline to csv, serialize error: {}", e);
            }
            if archived_klines.len() >= self.max_archived_cache_cnt {
                archived_klines.pop_front().unwrap();
            }
            archived_klines.push_back(kline);
        }
        self.archived_klines.store(archived_klines_ptr);
        if let Err(e) = archived_csv.flush() {
            error!("Failed to archive kline to csv, flush error: {}", e);
        }
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

        let mut building_klines = self.building_klines.write().await;

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

        if kline
            .open_time
            .saturating_sub(self.latest_archived_kline_time.load(Ordering::Relaxed))
            >= self.max_building_cache_cnt as u64 * self.interval
        {
            warn!(
                "Kline building cache cnt exceed max {}, archive to {}",
                self.max_building_cache_cnt, self.target_building_cache_cnt
            );
            self.archive(kline.open_time - self.target_building_cache_cnt as u64 * self.interval)
                .await;
        }

        Ok(())
    }

    pub async fn update_by_trade(&self, trade: &AggTrade) -> Result<()> {
        if trade.symbol != self.symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Trade symbol mismatch".to_string(),
            });
        }

        let mut building_klines = self.building_klines.write().await;
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

        if open_time.saturating_sub(self.latest_archived_kline_time.load(Ordering::Relaxed))
            >= self.max_building_cache_cnt as u64 * self.interval
        {
            warn!(
                "Kline building cache cnt exceed max {}, archive to {}",
                self.max_building_cache_cnt, self.target_building_cache_cnt
            );
            self.archive(open_time - self.target_building_cache_cnt as u64 * self.interval)
                .await;
        }

        Ok(())
    }
}
