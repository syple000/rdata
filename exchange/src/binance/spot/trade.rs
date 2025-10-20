use crate::binance::{errors::Result, spot::models::AggTrade};
use arc_swap::ArcSwap;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    vec,
};
use tokio::sync::RwLock;

struct TradeStat {
    id: u64,
    trade: Option<Arc<AggTrade>>,
}

pub struct Trade {
    symbol: String,

    latest_trades: RwLock<VecDeque<TradeStat>>,

    max_archived_cache_cnt: usize,
    latest_archived_trade_id: AtomicU64,
    archived_trades: ArcSwap<VecDeque<Arc<AggTrade>>>,
    trade_db: db::SledTreeProxy<AggTrade>,
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
        symbol: &str,
        max_archived_cache_cnt: usize,
        db: &sled::Db,
        hook: Option<Arc<dyn db::SledTreeProxyHook<Item = AggTrade> + Send + Sync>>,
    ) -> Result<Self> {
        let trade_db =
            db::SledTreeProxy::new(db, &format!("{}_trade", symbol), hook).map_err(|e| {
                crate::binance::errors::BinanceError::ClientError {
                    message: format!("Failed to create trade db for symbol {}: {}", symbol, e),
                }
            })?;
        if max_archived_cache_cnt == 0 {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "max_archived_cache_cnt must be greater than 0".to_string(),
            });
        }

        let mut archived_trades = VecDeque::with_capacity(max_archived_cache_cnt);
        let mut latest_archived_trade_id = 0u64;
        for item in trade_db.iter().rev().take(max_archived_cache_cnt) {
            if let Ok((_, trade)) = item {
                latest_archived_trade_id = latest_archived_trade_id.max(trade.agg_trade_id);
                archived_trades.push_front(Arc::new(trade));
            }
        }

        Ok(Self {
            symbol: symbol.to_string(),
            latest_trades: RwLock::new(VecDeque::new()),
            max_archived_cache_cnt,
            latest_archived_trade_id: AtomicU64::new(latest_archived_trade_id),
            archived_trades: ArcSwap::from_pointee(archived_trades),
            trade_db: trade_db,
        })
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
        self.trade_db
            .range(..to_id.to_be_bytes())
            .rev()
            .take(limit - result.len())
            .for_each(|item| {
                if let Ok((_, trade)) = item {
                    result.push_front(Arc::new(trade));
                }
            });

        return result;
    }

    pub async fn archive(&self, archived_trade_id: u64) {
        if archived_trade_id <= self.latest_archived_trade_id.load(Ordering::Relaxed) {
            return;
        }
        let mut latest_trades = self.latest_trades.write().await;
        if archived_trade_id <= self.latest_archived_trade_id.load(Ordering::Relaxed) {
            return;
        }

        self.latest_archived_trade_id
            .store(archived_trade_id, Ordering::Release);

        let mut to_archive_trades = vec![];
        let mut to_archive_trade_ids = vec![];
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
            let trade_id = trade.agg_trade_id.to_be_bytes();
            to_archive_trades.push(trade);
            to_archive_trade_ids.push(trade_id);
        }

        if to_archive_trades.is_empty() {
            return;
        }

        let mut archived_trades_ptr = self.archived_trades.load_full();
        let archived_trades = Arc::make_mut(&mut archived_trades_ptr);
        let mut batch = vec![];
        for (index, trade) in to_archive_trades.iter().enumerate() {
            batch.push((
                &to_archive_trade_ids.get(index).unwrap()[..],
                Some(&**trade),
            ));
            if archived_trades.len() >= self.max_archived_cache_cnt {
                archived_trades.pop_front().unwrap();
            }
            archived_trades.push_back(trade.clone());
        }
        self.archived_trades.store(archived_trades_ptr);
        self.trade_db.apply_batch(batch).unwrap();
    }

    pub async fn update(&self, trade: &AggTrade) -> Result<bool> {
        if trade.symbol != self.symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Trade symbol mismatch".to_string(),
            });
        }

        if trade.agg_trade_id <= self.latest_archived_trade_id.load(Ordering::Relaxed) {
            return Ok(false);
        }
        let mut latest_trades = self.latest_trades.write().await;
        if trade.agg_trade_id <= self.latest_archived_trade_id.load(Ordering::Relaxed) {
            return Ok(false);
        }

        if latest_trades.is_empty() {
            latest_trades.push_back(TradeStat {
                id: trade.agg_trade_id,
                trade: Some(Arc::new(trade.clone())),
            });
            return Ok(true);
        }

        while let Some(stat) = latest_trades.back() {
            if stat.id >= trade.agg_trade_id {
                break;
            }
            let id = latest_trades.back().unwrap().id + 1;
            latest_trades.push_back(TradeStat { id, trade: None });
        }
        while let Some(stat) = latest_trades.front() {
            if stat.id <= trade.agg_trade_id {
                break;
            }
            let id = latest_trades.front().unwrap().id - 1;
            latest_trades.push_front(TradeStat { id, trade: None });
        }

        let index = (trade.agg_trade_id - latest_trades.front().unwrap().id) as usize;

        let stat = &mut latest_trades[index];
        if stat.trade.is_none() {
            stat.trade = Some(Arc::new(trade.clone()));
            return Ok(true);
        } else {
            return Ok(false);
        }
    }
}
