use crate::binance::{
    errors::Result,
    spot::models::{Order, Trade},
};
use dashmap::{DashMap, DashSet};
use rust_decimal::Decimal;
use std::sync::Arc;

enum SymbolTradingStorageKey {
    KeyOfOrderIdWithClientOrderId(Option<String>),
    KeyOfWantPriceWithClientOrderId(Option<String>),
    KeyOfWantPriceWithExchangeOrderId(Option<u64>),
    KeyOfOrderWithExchangeOrderId(Option<u64>),
    KeyOfTradeWithExchangeOrderId(Option<u64>, Option<u64>), // (order_id, trade_id)

    KeyOfOnOrderWithClientOrderId(Option<String>),
}

impl SymbolTradingStorageKey {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            SymbolTradingStorageKey::KeyOfOrderIdWithClientOrderId(client_order_id) => {
                let mut key = Vec::with_capacity(16);
                key.extend_from_slice(b"coi:");
                if client_order_id.is_none() {
                    return key;
                }
                key.extend_from_slice(client_order_id.as_ref().unwrap().as_bytes());
                key
            }
            SymbolTradingStorageKey::KeyOfWantPriceWithClientOrderId(client_order_id) => {
                let mut key = Vec::with_capacity(16);
                key.extend_from_slice(b"cwp:");
                if client_order_id.is_none() {
                    return key;
                }
                key.extend_from_slice(client_order_id.as_ref().unwrap().as_bytes());
                key
            }
            SymbolTradingStorageKey::KeyOfWantPriceWithExchangeOrderId(order_id) => {
                let mut key = Vec::with_capacity(16);
                key.extend_from_slice(b"ewp:");
                if order_id.is_none() {
                    return key;
                }
                key.extend_from_slice(&order_id.unwrap().to_be_bytes());
                key
            }
            SymbolTradingStorageKey::KeyOfOrderWithExchangeOrderId(order_id) => {
                let mut key = Vec::with_capacity(16);
                key.extend_from_slice(b"eo:");
                if order_id.is_none() {
                    return key;
                }
                key.extend_from_slice(&order_id.unwrap().to_be_bytes());
                key
            }
            SymbolTradingStorageKey::KeyOfTradeWithExchangeOrderId(order_id, trade_id) => {
                let mut key = Vec::with_capacity(24);
                key.extend_from_slice(b"et:");
                if order_id.is_none() {
                    return key;
                }
                key.extend_from_slice(&order_id.unwrap().to_be_bytes());
                if trade_id.is_none() {
                    return key;
                }
                key.extend_from_slice(&trade_id.unwrap().to_be_bytes());
                key
            }
            SymbolTradingStorageKey::KeyOfOnOrderWithClientOrderId(client_order_id) => {
                let mut key = Vec::with_capacity(16);
                key.extend_from_slice(b"coo:");
                if client_order_id.is_none() {
                    return key;
                }
                key.extend_from_slice(client_order_id.as_ref().unwrap().as_bytes());
                key
            }
        }
    }
}

pub struct SymbolTrading {
    symbol: String,

    // 在途订单交易缓存
    client_order_id_want_price_map: DashMap<String, Decimal>,
    client_order_id_exchange_order_id_map: DashMap<String, u64>,
    orders: DashMap<u64, Arc<Order>>,
    trades: DashMap<u64, Vec<Arc<Trade>>>,

    // 已完成订单记录（从缓存中移除，仅数据库）
    final_client_orders: DashSet<String>,
    final_orders: DashSet<u64>,

    // 数据库持久化
    client_order_id_want_price_tree: sled::Tree,
    client_order_id_exchange_order_id_tree: sled::Tree,
    orders_tree: sled::Tree,
    trades_tree: sled::Tree,
    final_client_orders_tree: sled::Tree,
    final_orders_tree: sled::Tree,
    on_client_orders_tree: sled::Tree, // 恢复在途订单交易缓存用
}

impl SymbolTrading {
    pub fn new(symbol: &str, db: &sled::Db) -> Result<Self> {
        Ok(Self {
            symbol: symbol.to_string(),
            client_order_id_want_price_map: DashMap::new(),
            client_order_id_exchange_order_id_map: DashMap::new(),
            orders: DashMap::new(),
            trades: DashMap::new(),
            final_client_orders: DashSet::new(),
            final_orders: DashSet::new(),
            client_order_id_want_price_tree: Self::create_tree(
                symbol,
                db,
                "client_order_id_want_price",
            )?,
            client_order_id_exchange_order_id_tree: Self::create_tree(
                symbol,
                db,
                "client_order_id_exchange_order_id",
            )?,
            orders_tree: Self::create_tree(symbol, db, "orders")?,
            trades_tree: Self::create_tree(symbol, db, "trades")?,
            final_client_orders_tree: Self::create_tree(symbol, db, "final_client_orders")?,
            final_orders_tree: Self::create_tree(symbol, db, "final_orders")?,
            on_client_orders_tree: Self::create_tree(symbol, db, "on_client_orders")?,
        })
    }

    fn create_tree(symbol: &str, db: &sled::Db, suffix: &str) -> Result<sled::Tree> {
        db.open_tree(format!("{}_trading_{}", symbol, suffix))
            .map_err(|e| crate::binance::errors::BinanceError::ClientError {
                message: format!(
                    "Failed to open sled tree for symbol trading {} {}: {}",
                    symbol, suffix, e
                ),
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
