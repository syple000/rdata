use crate::binance::{errors::Result, spot::models::AggTrade};
use arc_swap::ArcSwap;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::RwLock;

struct TradeStat {
    id: u64,
    trade: Option<Arc<AggTrade>>,
}

pub struct Trade {
    symbol: String,

    max_latest_trades_cnt: usize,
    overload_ratio: usize,
    latest_trades: RwLock<VecDeque<TradeStat>>,

    max_archived_cnt: usize,
    archived: ArcSwap<VecDeque<Arc<AggTrade>>>,
}

pub struct TradeView {
    pub archived: Arc<VecDeque<Arc<AggTrade>>>,
    pub latest_trades: VecDeque<Arc<AggTrade>>,
}

impl TradeView {
    pub fn iter(&self) -> impl Iterator<Item = &AggTrade> {
        self.archived
            .iter()
            .chain(self.latest_trades.iter())
            .map(|k| k.as_ref())
    }

    pub fn trades(&self) -> Vec<Arc<AggTrade>> {
        self.archived
            .iter()
            .chain(self.latest_trades.iter())
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.archived.len() + self.latest_trades.len()
    }
}

impl Trade {
    pub fn new(
        symbol: String,
        max_latest_trades_cnt: usize,
        overload_ratio: usize,
        max_archived_cnt: usize,
    ) -> Self {
        Self {
            symbol: symbol.clone(),
            max_latest_trades_cnt,
            overload_ratio,
            latest_trades: RwLock::new(VecDeque::with_capacity(max_latest_trades_cnt)),
            max_archived_cnt,
            archived: ArcSwap::from_pointee(VecDeque::with_capacity(max_archived_cnt)),
        }
    }

    pub async fn get_trades(&self) -> TradeView {
        TradeView {
            archived: self.archived.load_full(),
            latest_trades: self
                .latest_trades
                .read()
                .await
                .iter()
                .filter_map(|t| t.trade.clone())
                .collect(),
        }
    }

    pub async fn archive(&self) {
        let latest_trades = self.latest_trades.read().await;
        if latest_trades.len() <= self.max_latest_trades_cnt / self.overload_ratio {
            return;
        }
        drop(latest_trades);

        let mut latest_trades = self.latest_trades.write().await;
        let mut archived_ptr = self.archived.load_full();
        let archived = Arc::make_mut(&mut archived_ptr);
        while latest_trades.len() > self.max_latest_trades_cnt {
            if let Some(stat) = latest_trades.pop_front() {
                if let Some(trade) = stat.trade {
                    archived.push_back(trade);
                    if archived.len() > self.max_archived_cnt {
                        archived.pop_front();
                    }
                }
            }
        }
        self.archived.store(archived_ptr);
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

        self.archive().await;

        Ok(true)
    }
}
