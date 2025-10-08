use crate::binance::{
    errors::Result,
    spot::models::{DepthData, DepthUpdate, PriceLevel},
};
use arc_swap::ArcSwap;
use rust_decimal::Decimal;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::RwLock;

struct DepthStat {
    last_update_id: u64,
    bids: HashMap<Decimal, PriceLevel>,
    asks: HashMap<Decimal, PriceLevel>,
    timestamp: u64,
}

struct Depth {
    symbol: String,
    stat: RwLock<Option<DepthStat>>,
    is_updated: AtomicBool,
    depth_data: ArcSwap<Option<DepthData>>,
}

impl Depth {
    fn new(symbol: String) -> Self {
        Self {
            symbol: symbol.clone(),
            stat: RwLock::new(None),
            is_updated: AtomicBool::new(false),
            depth_data: ArcSwap::from_pointee(None),
        }
    }

    async fn get_depth(&self) -> Arc<Option<DepthData>> {
        if !self.is_updated.load(Ordering::Relaxed) {
            return self.depth_data.load_full();
        }

        let mut bids: Vec<PriceLevel>;
        let mut asks: Vec<PriceLevel>;
        let last_update_id: u64;
        {
            let stat = self.stat.read().await;
            if stat.is_none() {
                return self.depth_data.load_full();
            }
            let stat = stat.as_ref().unwrap();
            bids = stat.bids.values().cloned().collect();
            asks = stat.asks.values().cloned().collect();
            last_update_id = stat.last_update_id;
        }

        bids.sort_by(|a, b| b.price.cmp(&a.price));
        asks.sort_by(|a, b| a.price.cmp(&b.price));
        self.depth_data.store(Arc::new(Some(DepthData {
            symbol: self.symbol.clone(),
            last_update_id,
            bids,
            asks,
        })));
        self.is_updated.store(false, Ordering::Release);
        return self.depth_data.load_full();
    }

    async fn update_by_depth(&self, depth: DepthData) -> Result<()> {
        if self.symbol != depth.symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Depth symbol mismatch".to_string(),
            });
        }

        let mut bids = HashMap::new();
        let mut asks = HashMap::new();
        for bid in depth.bids {
            bids.insert(bid.price.clone(), bid);
        }
        for ask in depth.asks {
            asks.insert(ask.price.clone(), ask);
        }

        let mut stat = self.stat.write().await;
        *stat = Some(DepthStat {
            bids: bids,
            asks: asks,
            last_update_id: depth.last_update_id,
            timestamp: 0,
        });

        self.is_updated.store(true, Ordering::Release);
        Ok(())
    }

    async fn update_by_depth_update(&self, update: DepthUpdate) -> Result<()> {
        if self.symbol != update.symbol {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Depth update symbol mismatch".to_string(),
            });
        }

        let mut stat = self.stat.write().await;
        if stat.is_none() {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Depth not initialized".to_string(),
            });
        }
        let stat = stat.as_mut().unwrap();

        if update.last_update_id <= stat.last_update_id {
            return Ok(());
        }

        if update.first_update_id <= stat.last_update_id + 1 {
            for bid in update.bids {
                if bid.quantity.is_zero() {
                    stat.bids.remove(&bid.price);
                } else {
                    stat.bids.insert(bid.price.clone(), bid);
                }
            }
            for ask in update.asks {
                if ask.quantity.is_zero() {
                    stat.asks.remove(&ask.price);
                } else {
                    stat.asks.insert(ask.price.clone(), ask);
                }
            }
            stat.last_update_id = update.last_update_id;
            stat.timestamp = update.timestamp;
            self.is_updated.store(true, Ordering::Release);
            return Ok(());
        }

        Err(crate::binance::errors::BinanceError::ClientError {
            message: format!(
                "Depth update out of order. Last update id: {}, update first id: {}",
                stat.last_update_id, update.first_update_id
            ),
        })
    }
}
