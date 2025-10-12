use crate::binance::{errors::Result, spot::models::AggTrade};
use arc_swap::ArcSwap;
use log::warn;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::RwLock;

struct TradeStat {
    id: u64,
    trade: Option<Arc<AggTrade>>,
}

// 来源1：websocket推送
// 来源2：api获取（从latest_archived_trade_id开始获取，如果该值不存在，获取最新的trades，使用最新的trade id更新latest_archived_trade_id）
pub struct Trade {
    symbol: String,

    max_latest_cache_cnt: usize, // 长时间未手动进行归档，在触发容量阈值后，自动归档
    target_latest_cache_cnt: usize, // 归档时，将数据长度缩减到该值
    latest_trades: RwLock<VecDeque<TradeStat>>,

    max_archived_cache_cnt: usize,
    latest_archived_trade_id: AtomicU64,
    archived_trades: ArcSwap<VecDeque<Arc<AggTrade>>>,
    db_tree: sled::Tree,
}

pub struct TradeView {
    pub archived_trades: Arc<VecDeque<Arc<AggTrade>>>,
    pub latest_trades: VecDeque<Arc<AggTrade>>,
}

impl TradeView {
    pub fn iter(&self) -> impl Iterator<Item = &AggTrade> + DoubleEndedIterator + '_ {
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
        db: &sled::Db,
    ) -> Result<Self> {
        let db_tree = db.open_tree(format!("{}_trade", symbol)).map_err(|e| {
            crate::binance::errors::BinanceError::ClientError {
                message: format!("Failed to new Trade, open sled tree fail: {}", e),
            }
        })?;

        Ok(Self {
            symbol: symbol.clone(),
            max_latest_cache_cnt,
            target_latest_cache_cnt,
            max_archived_cache_cnt,
            latest_archived_trade_id: AtomicU64::new(0),
            latest_trades: RwLock::new(VecDeque::with_capacity(max_latest_cache_cnt)),
            archived_trades: ArcSwap::from_pointee(VecDeque::with_capacity(max_archived_cache_cnt)),
            db_tree,
        })
    }

    pub fn get_latest_archived_trade_id(&self) -> Option<u64> {
        let id = self.latest_archived_trade_id.load(Ordering::Relaxed);
        if id == 0 {
            None
        } else {
            Some(id)
        }
    }

    pub async fn get_trades(&self) -> TradeView {
        let latest_trades = self.latest_trades.read().await;
        let archived_trades = self.archived_trades.load_full();
        TradeView {
            archived_trades: archived_trades,
            latest_trades: latest_trades
                .iter()
                .filter_map(|t| t.trade.clone())
                .collect(),
        }
    }

    pub async fn get_trades_with_limit(&self, limit: usize) -> VecDeque<Arc<AggTrade>> {
        let latest_trades = self.latest_trades.read().await;
        let mut result = latest_trades
            .iter()
            .filter_map(|t| t.trade.clone())
            .collect::<VecDeque<_>>();
        if result.len() >= limit {
            return result
                .iter()
                .rev()
                .take(limit)
                .cloned()
                .collect::<VecDeque<_>>();
        }

        let archived_trades = self.archived_trades.load_full();
        for trade in archived_trades.iter().rev() {
            if result.len() >= limit {
                return result;
            }
            result.push_front(trade.clone());
        }

        drop(latest_trades);

        let mut to_id = std::u64::MAX;
        if !result.is_empty() {
            to_id = result.front().unwrap().agg_trade_id;
        }
        self.db_tree
            .range(..to_id.to_be_bytes())
            .rev()
            .take(limit - result.len())
            .for_each(|item| {
                if let Ok((_, v)) = item {
                    if let Ok(trade) = serde_json::from_slice::<AggTrade>(&v) {
                        result.push_front(Arc::new(trade));
                    }
                }
            });

        return result;
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
        let mut batch = sled::Batch::default();
        for trade in to_archive_trades {
            batch.insert(
                &trade.agg_trade_id.to_be_bytes()[..],
                serde_json::to_string(&*trade).unwrap().as_bytes(),
            );
            if archived_trades.len() >= self.max_archived_cache_cnt {
                archived_trades.pop_front().unwrap();
            }
            archived_trades.push_back(trade.clone());
        }
        self.archived_trades.store(archived_trades_ptr);
        self.db_tree.apply_batch(batch).unwrap();
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
