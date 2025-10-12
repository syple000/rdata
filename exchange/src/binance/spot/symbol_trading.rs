use crate::binance::{
    errors::Result,
    spot::models::{Order, Trade},
};
use dashmap::{DashMap, DashSet};
use rust_decimal::Decimal;
use serde::Serialize;
use std::sync::{atomic::Ordering, Arc};
use tokio::sync::RwLock;

#[derive(Serialize, Debug)]
struct OrderStat {
    order_id: u64,
    want_price: Option<Decimal>, // 少数场景下会无数据，可以试着从depth获取
    order: Option<Order>,
    trades: Option<Vec<Trade>>,
}

impl OrderStat {
    fn need_fetch_order(&self) -> bool {
        self.order.is_none()
    }

    fn need_fetch_trades(&self) -> bool {
        let order = match &self.order {
            Some(v) => v,
            None => return false,
        };
        let mut qty = Decimal::new(0, 0);
        if let Some(trades) = &self.trades {
            for trade in trades {
                qty += trade.trade_quantity;
            }
        }
        qty < order.executed_qty
    }

    fn is_final(&self) -> bool {
        let order_terminate = match &self.order {
            Some(v) => v.order_status.is_final(),
            None => false,
        };
        if !order_terminate {
            return false;
        }

        !self.need_fetch_trades()
    }
}

// 来源1：websocket推送（订单/成交）
// 来源2：api获取在途订单（定期获取即可，仅极少情况需要依赖该方式补充数据）
// 来源3：api获取order为空的订单（定期执行即可，仅极少情况发生数据丢失）
// 来源4：api获取order成交比成交返回多的成交（定期执行即可，仅少数情况发生）
pub struct SymbolTrading {
    symbol: String,
    base_asset: String,
    quote_asset: String,

    on_order_stats: DashMap<u64, RwLock<OrderStat>>,
    final_orders: DashSet<u64>,

    // 数据库持久化
    db_tree: sled::Tree,
}

impl SymbolTrading {
    pub fn new(symbol: &str, base_asset: &str, quote_asset: &str, db: &sled::Db) -> Result<Self> {
        let db_tree = db
            .open_tree(format!("{}_symbol_trading", symbol))
            .map_err(|e| crate::binance::errors::BinanceError::ClientError {
                message: format!(
                    "Failed to open sled tree for symbol trading {}: {}",
                    symbol, e
                ),
            })?;
        Ok(Self {
            symbol: symbol.to_string(),
            base_asset: base_asset.to_string(),
            quote_asset: quote_asset.to_string(),
            on_order_stats: DashMap::new(),
            final_orders: DashSet::new(),
            db_tree,
        })
    }

    pub async fn update_by_want_price(&self, order_id: u64, want_price: Decimal) -> Result<()> {
        unimplemented!()
    }

    pub async fn update_by_order(&self, order: &Order) -> Result<()> {
        unimplemented!()
    }

    pub async fn update_by_trade(&self, trade: &Trade) -> Result<()> {
        unimplemented!()
    }
}
