use crate::binance::{
    errors::Result,
    spot::models::{Account, Order, Trade},
};
use dashmap::{DashMap, DashSet};
use rust_decimal::Decimal;
use std::sync::{Arc, RwLock};

enum TradingStorageKey {
    KeyOfExchangeOrderIdWithClientOrderId(Option<String>),
    KeyOfWantPriceWithClientOrderId(Option<String>),
    KeyOfWantPriceWithExchangeOrderId(Option<u64>),
    KeyOfOrderWithExchangeOrderId(Option<u64>),
    KeyOfTradeWithExchangeOrderId(Option<u64>, Option<u64>), // (order_id, trade_id)
    KeyOfAccount,

    KeyOfOnOrderWithClientOrderId(Option<String>),
}

impl TradingStorageKey {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            TradingStorageKey::KeyOfExchangeOrderIdWithClientOrderId(client_order_id) => {
                let mut key = Vec::with_capacity(16);
                key.extend_from_slice(b"coi:");
                if client_order_id.is_none() {
                    return key;
                }
                key.extend_from_slice(client_order_id.as_ref().unwrap().as_bytes());
                key
            }
            TradingStorageKey::KeyOfWantPriceWithClientOrderId(client_order_id) => {
                let mut key = Vec::with_capacity(16);
                key.extend_from_slice(b"cwp:");
                if client_order_id.is_none() {
                    return key;
                }
                key.extend_from_slice(client_order_id.as_ref().unwrap().as_bytes());
                key
            }
            TradingStorageKey::KeyOfWantPriceWithExchangeOrderId(order_id) => {
                let mut key = Vec::with_capacity(16);
                key.extend_from_slice(b"ewp:");
                if order_id.is_none() {
                    return key;
                }
                key.extend_from_slice(&order_id.unwrap().to_be_bytes());
                key
            }
            TradingStorageKey::KeyOfOrderWithExchangeOrderId(order_id) => {
                let mut key = Vec::with_capacity(16);
                key.extend_from_slice(b"eo:");
                if order_id.is_none() {
                    return key;
                }
                key.extend_from_slice(&order_id.unwrap().to_be_bytes());
                key
            }
            TradingStorageKey::KeyOfTradeWithExchangeOrderId(order_id, trade_id) => {
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
            TradingStorageKey::KeyOfOnOrderWithClientOrderId(client_order_id) => {
                let mut key = Vec::with_capacity(16);
                key.extend_from_slice(b"coo:");
                if client_order_id.is_none() {
                    return key;
                }
                key.extend_from_slice(client_order_id.as_ref().unwrap().as_bytes());
                key
            }
            TradingStorageKey::KeyOfAccount => b"account".to_vec(),
        }
    }

    fn from_bytes(v: &[u8]) -> Result<Self> {
        if v.starts_with(b"coi:") {
            if v.len() == 4 {
                return Ok(TradingStorageKey::KeyOfExchangeOrderIdWithClientOrderId(
                    None,
                ));
            }
            let client_order_id = String::from_utf8(v[4..].to_vec()).unwrap();
            return Ok(TradingStorageKey::KeyOfExchangeOrderIdWithClientOrderId(
                Some(client_order_id),
            ));
        } else if v.starts_with(b"cwp:") {
            if v.len() == 4 {
                return Ok(TradingStorageKey::KeyOfWantPriceWithClientOrderId(None));
            }
            let client_order_id = String::from_utf8(v[4..].to_vec()).unwrap();
            return Ok(TradingStorageKey::KeyOfWantPriceWithClientOrderId(Some(
                client_order_id,
            )));
        } else if v.starts_with(b"ewp:") {
            if v.len() == 4 {
                return Ok(TradingStorageKey::KeyOfWantPriceWithExchangeOrderId(None));
            }
            let mut array = [0u8; 8];
            array.copy_from_slice(&v[4..12]);
            let order_id = u64::from_be_bytes(array);
            return Ok(TradingStorageKey::KeyOfWantPriceWithExchangeOrderId(Some(
                order_id,
            )));
        } else if v.starts_with(b"eo:") {
            if v.len() == 4 {
                return Ok(TradingStorageKey::KeyOfOrderWithExchangeOrderId(None));
            }
            let mut array = [0u8; 8];
            array.copy_from_slice(&v[4..12]);
            let order_id = u64::from_be_bytes(array);
            return Ok(TradingStorageKey::KeyOfOrderWithExchangeOrderId(Some(
                order_id,
            )));
        } else if v.starts_with(b"et:") {
            if v.len() == 4 {
                return Ok(TradingStorageKey::KeyOfTradeWithExchangeOrderId(None, None));
            } else if v.len() == 12 {
                let mut array = [0u8; 8];
                array.copy_from_slice(&v[4..12]);
                let order_id = u64::from_be_bytes(array);
                return Ok(TradingStorageKey::KeyOfTradeWithExchangeOrderId(
                    Some(order_id),
                    None,
                ));
            } else if v.len() == 20 {
                let mut array1 = [0u8; 8];
                array1.copy_from_slice(&v[4..12]);
                let order_id = u64::from_be_bytes(array1);
                let mut array2 = [0u8; 8];
                array2.copy_from_slice(&v[12..20]);
                let trade_id = u64::from_be_bytes(array2);
                return Ok(TradingStorageKey::KeyOfTradeWithExchangeOrderId(
                    Some(order_id),
                    Some(trade_id),
                ));
            }
        } else if v.starts_with(b"coo:") {
            if v.len() == 4 {
                return Ok(TradingStorageKey::KeyOfOnOrderWithClientOrderId(None));
            }
            let client_order_id = String::from_utf8(v[4..].to_vec()).unwrap();
            return Ok(TradingStorageKey::KeyOfOnOrderWithClientOrderId(Some(
                client_order_id,
            )));
        } else if v == b"account" {
            return Ok(TradingStorageKey::KeyOfAccount);
        }

        return Err(crate::binance::errors::BinanceError::ClientError {
            message: "Invalid TradingStorageKey bytes".to_string(),
        });
    }
}

pub struct Trading {
    // 在途订单状态缓存
    client_order_id_exchange_order_id_map: DashMap<String, u64>,
    client_order_id_want_price_map: DashMap<String, Decimal>,
    exchange_order_id_want_price_map: DashMap<u64, Decimal>,
    exchange_order_id_order_map: DashMap<u64, Arc<Order>>,
    exchange_order_id_trade_map: DashMap<u64, DashSet<Arc<Trade>>>,

    // 在途/结束订单ID缓存
    on_order_client_order_id_set: DashSet<String>,
    closed_order_exchange_order_id_set: DashSet<u64>,

    // 账户缓存
    account: RwLock<Option<Account>>,

    // 数据库持久化
    client_order_id_exchange_order_id_db: db::SledTreeProxy<u64>,
    client_order_id_want_price_db: db::SledTreeProxy<Decimal>,
    exchange_order_id_want_price_db: db::SledTreeProxy<Decimal>,
    exchange_order_id_order_db: db::SledTreeProxy<Order>,
    exchange_order_id_trade_id_trade_db: db::SledTreeProxy<Trade>,
    on_order_client_order_id_db: db::SledTreeProxy<String>,
    closed_order_exchange_order_id_db: db::SledTreeProxy<u64>,
    account_db: db::SledTreeProxy<Account>,
}

impl Trading {}
