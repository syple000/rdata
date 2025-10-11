use crate::binance::{errors::Result, spot::models::AggTrade};
use arc_swap::ArcSwap;
use csv::{Writer, WriterBuilder};
use log::{error, warn};
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

struct TradeStat {
    id: u64,
    trade: Option<Arc<AggTrade>>,
}

pub struct Trade {
    symbol: String,

    max_latest_cache_cnt: usize, // 长时间未手动进行归档，在触发容量阈值后，自动归档
    target_latest_cache_cnt: usize, // 归档时，将数据长度缩减到该值
    latest_trades: RwLock<VecDeque<TradeStat>>,

    max_archived_cache_cnt: usize,
    latest_archived_trade_id: AtomicU64,
    archived_trades: ArcSwap<VecDeque<Arc<AggTrade>>>,
    archived_csv: Mutex<Writer<std::fs::File>>,
}

pub struct TradeView {
    pub archived_trades: Arc<VecDeque<Arc<AggTrade>>>,
    pub latest_trades: VecDeque<Arc<AggTrade>>,
}

impl TradeView {
    pub fn iter(&self) -> impl Iterator<Item = &AggTrade> {
        self.archived_trades
            .iter()
            .chain(self.latest_trades.iter())
            .map(|k| k.as_ref())
    }

    pub fn trades(&self) -> Vec<Arc<AggTrade>> {
        self.archived_trades
            .iter()
            .chain(self.latest_trades.iter())
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.archived_trades.len() + self.latest_trades.len()
    }
}

impl Trade {
    pub fn new(
        symbol: String,
        max_latest_cache_cnt: usize,
        target_latest_cache_cnt: usize,
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
                message: format!(
                    "Failed to new Trade, open CSV file fail: {}, {}",
                    e, csv_file
                ),
            })?;
        let csv_writer = WriterBuilder::new().has_headers(true).from_writer(file);

        Ok(Self {
            symbol: symbol.clone(),
            max_latest_cache_cnt,
            target_latest_cache_cnt,
            max_archived_cache_cnt,
            latest_archived_trade_id: AtomicU64::new(0),
            latest_trades: RwLock::new(VecDeque::with_capacity(max_latest_cache_cnt)),
            archived_trades: ArcSwap::from_pointee(VecDeque::with_capacity(max_archived_cache_cnt)),
            archived_csv: Mutex::new(csv_writer),
        })
    }

    pub async fn get_trades(&self) -> TradeView {
        TradeView {
            archived_trades: self.archived_trades.load_full(),
            latest_trades: self
                .latest_trades
                .read()
                .await
                .iter()
                .filter_map(|t| t.trade.clone())
                .collect(),
        }
    }

    pub async fn archive(&self, archived_trade_id: u64) {
        if archived_trade_id <= self.latest_archived_trade_id.load(Ordering::Relaxed) {
            return;
        }

        let mut latest_trades = self.latest_trades.write().await;

        self.latest_archived_trade_id
            .store(archived_trade_id, Ordering::Release);

        let mut to_archive_trades = vec![];
        loop {
            let front = latest_trades.front();
            if front.is_none() {
                break;
            }
            let front = front.unwrap();
            if front.id > archived_trade_id {
                break;
            }
            let front = latest_trades.pop_front().unwrap();
            if front.trade.is_none() {
                continue;
            }
            let trade = front.trade.unwrap();
            to_archive_trades.push(trade);
        }

        if to_archive_trades.is_empty() {
            return;
        }

        let mut archived_trades_ptr = self.archived_trades.load_full();
        let archived_trades = Arc::make_mut(&mut archived_trades_ptr);
        let mut archived_csv = self.archived_csv.lock().await;
        for trade in to_archive_trades {
            if let Err(e) = archived_csv.serialize(&*trade) {
                error!("Failed to archive trade to CSV, serialize fail: {}", e);
            }
            if archived_trades.len() >= self.max_archived_cache_cnt {
                archived_trades.pop_front().unwrap();
            }
            archived_trades.push_back(trade.clone());
        }
        self.archived_trades.store(archived_trades_ptr);
        if let Err(e) = archived_csv.flush() {
            error!("Failed to archive trade to CSV, flush fail: {}", e);
        }
    }

    pub async fn update(&self, trade: &AggTrade) -> Result<bool> {
        if trade.symbol != self.symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Trade symbol mismatch".to_string(),
            });
        }

        let mut latest_trades = self.latest_trades.write().await;
        let mut index = 0;
        if let Some(stat) = latest_trades.front() {
            if trade.agg_trade_id <= stat.id {
                return Ok(false);
            }
            index = (trade.agg_trade_id - stat.id) as usize;
        }

        if index < latest_trades.len() {
            let stat = &mut latest_trades[index];
            if stat.trade.is_none() {
                stat.trade = Some(Arc::new(trade.clone()));
                return Ok(true);
            } else {
                return Ok(false);
            }
        }

        while latest_trades.len() < index {
            let id = latest_trades.back().unwrap().id + 1;
            latest_trades.push_back(TradeStat { id, trade: None });
        }
        latest_trades.push_back(TradeStat {
            id: trade.agg_trade_id,
            trade: Some(Arc::new(trade.clone())),
        });
        drop(latest_trades);

        if trade
            .agg_trade_id
            .saturating_sub(self.latest_archived_trade_id.load(Ordering::Relaxed))
            >= self.max_latest_cache_cnt as u64
        {
            warn!(
                "Trade latest cache cnt exceed max {}, archive to {}",
                self.max_latest_cache_cnt, self.target_latest_cache_cnt
            );
            self.archive(trade.agg_trade_id - self.target_latest_cache_cnt as u64)
                .await;
        }

        Ok(true)
    }
}
