use crate::binance::{
    errors::Result,
    spot::models::{Account, ExecutionReport, Order, OutboundAccountPosition, Trade},
};
use dashmap::DashMap;
use rust_decimal::Decimal;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

enum TradingStorageKey {
    KeyOfWantPriceWithClientOrderId(Option<String>),
    KeyOfOrderWithExchangeOrderId(Option<u64>),
    KeyOfTradeWithExchangeOrderId(Option<u64>, Option<u64>), // (order_id, trade_id)
    KeyOfAccount,
}

impl TradingStorageKey {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            TradingStorageKey::KeyOfWantPriceWithClientOrderId(client_order_id) => {
                let mut key = Vec::with_capacity(16);
                key.extend_from_slice(b"wp:");
                if client_order_id.is_none() {
                    return key;
                }
                key.extend_from_slice(client_order_id.as_ref().unwrap().as_bytes());
                key
            }
            TradingStorageKey::KeyOfOrderWithExchangeOrderId(order_id) => {
                let mut key = Vec::with_capacity(16);
                key.extend_from_slice(b"o:");
                if order_id.is_none() {
                    return key;
                }
                key.extend_from_slice(&order_id.unwrap().to_be_bytes());
                key
            }
            TradingStorageKey::KeyOfTradeWithExchangeOrderId(order_id, trade_id) => {
                let mut key = Vec::with_capacity(24);
                key.extend_from_slice(b"t:");
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
            TradingStorageKey::KeyOfAccount => b"account".to_vec(),
        }
    }

    fn from_bytes(v: &[u8]) -> Result<Self> {
        if v.starts_with(b"wp:") {
            if v.len() == 3 {
                return Ok(TradingStorageKey::KeyOfWantPriceWithClientOrderId(None));
            }
            let client_order_id = String::from_utf8(v[3..].to_vec()).unwrap();
            return Ok(TradingStorageKey::KeyOfWantPriceWithClientOrderId(Some(
                client_order_id,
            )));
        } else if v.starts_with(b"o:") {
            if v.len() == 2 {
                return Ok(TradingStorageKey::KeyOfOrderWithExchangeOrderId(None));
            }
            let mut array = [0u8; 8];
            if v.len() == 10 {
                array.copy_from_slice(&v[2..10]);
                let order_id = u64::from_be_bytes(array);
                return Ok(TradingStorageKey::KeyOfOrderWithExchangeOrderId(Some(
                    order_id,
                )));
            }
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: format!("Invalid TradingStorageKey order bytes length: {}", v.len()),
            });
        } else if v.starts_with(b"t:") {
            if v.len() == 2 {
                return Ok(TradingStorageKey::KeyOfTradeWithExchangeOrderId(None, None));
            } else if v.len() == 10 {
                let mut array = [0u8; 8];
                array.copy_from_slice(&v[2..10]);
                let order_id = u64::from_be_bytes(array);
                return Ok(TradingStorageKey::KeyOfTradeWithExchangeOrderId(
                    Some(order_id),
                    None,
                ));
            } else if v.len() == 18 {
                let mut array1 = [0u8; 8];
                array1.copy_from_slice(&v[2..10]);
                let order_id = u64::from_be_bytes(array1);
                let mut array2 = [0u8; 8];
                array2.copy_from_slice(&v[10..18]);
                let trade_id = u64::from_be_bytes(array2);
                return Ok(TradingStorageKey::KeyOfTradeWithExchangeOrderId(
                    Some(order_id),
                    Some(trade_id),
                ));
            }
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: format!("Invalid TradingStorageKey trade bytes length: {}", v.len()),
            });
        } else if v == b"account" {
            return Ok(TradingStorageKey::KeyOfAccount);
        }

        return Err(crate::binance::errors::BinanceError::ClientError {
            message: "Invalid TradingStorageKey bytes".to_string(),
        });
    }
}

pub struct Trading {
    client_order_id_want_price_map: DashMap<String, Decimal>,
    exchange_order_id_order_map: DashMap<u64, Arc<Order>>,
    exchange_order_id_trades_map: DashMap<u64, DashMap<u64, Arc<Trade>>>,
    account: RwLock<Option<Account>>,

    // 数据库持久化
    client_order_id_want_price_db: db::SledTreeProxy<Decimal>,
    exchange_order_id_order_db: db::SledTreeProxy<Order>,
    exchange_order_id_trades_db: db::SledTreeProxy<Trade>,
    account_db: db::SledTreeProxy<Account>,
}

impl Trading {
    pub fn new(
        db: &sled::Db,
        client_order_id_want_price_hook: Option<
            Arc<dyn db::SledTreeProxyHook<Item = Decimal> + Send + Sync>,
        >,
        exchange_order_id_order_hook: Option<
            Arc<dyn db::SledTreeProxyHook<Item = Order> + Send + Sync>,
        >,
        exchange_order_id_trades_hook: Option<
            Arc<dyn db::SledTreeProxyHook<Item = Trade> + Send + Sync>,
        >,
        account_hook: Option<Arc<dyn db::SledTreeProxyHook<Item = Account> + Send + Sync>>,
    ) -> Result<Self> {
        let client_order_id_want_price_db = db::SledTreeProxy::new(
            db,
            "trading_client_order_id_want_price",
            client_order_id_want_price_hook,
        )
        .map_err(|e| crate::binance::errors::BinanceError::ClientError {
            message: format!(
                "Failed to create trading client_order_id_want_price db: {}",
                e
            ),
        })?;

        let exchange_order_id_order_db = db::SledTreeProxy::new(
            db,
            "trading_exchange_order_id_order",
            exchange_order_id_order_hook,
        )
        .map_err(|e| crate::binance::errors::BinanceError::ClientError {
            message: format!("Failed to create trading exchange_order_id_order db: {}", e),
        })?;

        let exchange_order_id_trades_db = db::SledTreeProxy::new(
            db,
            "trading_exchange_order_id_trades",
            exchange_order_id_trades_hook,
        )
        .map_err(|e| crate::binance::errors::BinanceError::ClientError {
            message: format!(
                "Failed to create trading exchange_order_id_trades db: {}",
                e
            ),
        })?;

        let account_db =
            db::SledTreeProxy::new(db, "trading_account", account_hook).map_err(|e| {
                crate::binance::errors::BinanceError::ClientError {
                    message: format!("Failed to create trading account db: {}", e),
                }
            })?;

        // 从db中初始化内存数据
        let client_order_id_want_price_map = DashMap::new();
        let _ = client_order_id_want_price_db
            .scan_prefix(&TradingStorageKey::KeyOfWantPriceWithClientOrderId(None).to_bytes())
            .for_each(|key, val| {
                if let Ok(TradingStorageKey::KeyOfWantPriceWithClientOrderId(Some(
                    client_order_id,
                ))) = TradingStorageKey::from_bytes(&key)
                {
                    client_order_id_want_price_map.insert(client_order_id, val);
                }
            });

        let exchange_order_id_order_map = DashMap::new();
        let _ = exchange_order_id_order_db
            .scan_prefix(&TradingStorageKey::KeyOfOrderWithExchangeOrderId(None).to_bytes())
            .for_each(|key, val| {
                if let Ok(TradingStorageKey::KeyOfOrderWithExchangeOrderId(Some(order_id))) =
                    TradingStorageKey::from_bytes(&key)
                {
                    exchange_order_id_order_map.insert(order_id, Arc::new(val));
                }
            });

        let exchange_order_id_trades_map = DashMap::new();
        let _ = exchange_order_id_trades_db
            .scan_prefix(&TradingStorageKey::KeyOfTradeWithExchangeOrderId(None, None).to_bytes())
            .for_each(|key, val| {
                if let Ok(TradingStorageKey::KeyOfTradeWithExchangeOrderId(
                    Some(order_id),
                    Some(trade_id),
                )) = TradingStorageKey::from_bytes(&key)
                {
                    let trade_map = exchange_order_id_trades_map
                        .entry(order_id)
                        .or_insert_with(DashMap::new);
                    trade_map.insert(trade_id, Arc::new(val));
                }
            });

        let mut account = None;
        account_db
            .get(&TradingStorageKey::KeyOfAccount.to_bytes())
            .ok()
            .flatten()
            .map(|acc| {
                account = Some(acc);
            });

        Ok(Self {
            client_order_id_want_price_map: client_order_id_want_price_map,
            exchange_order_id_order_map: exchange_order_id_order_map,
            exchange_order_id_trades_map: exchange_order_id_trades_map,
            account: RwLock::new(account),
            client_order_id_want_price_db,
            exchange_order_id_order_db,
            exchange_order_id_trades_db,
            account_db,
        })
    }

    pub fn get_open_orders(&self) -> Vec<Arc<Order>> {
        self.exchange_order_id_order_map
            .iter()
            .filter_map(|entry| {
                let order = entry.value();
                if order.order_status.is_final() {
                    None
                } else {
                    Some(order.clone())
                }
            })
            .collect()
    }

    pub fn get_closed_orders(&self) -> Vec<Arc<Order>> {
        self.exchange_order_id_order_map
            .iter()
            .filter_map(|entry| {
                let order = entry.value();
                if order.order_status.is_final() {
                    Some(order.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_order_by_exchange_order_id(&self, order_id: u64) -> Option<Arc<Order>> {
        self.exchange_order_id_order_map
            .get(&order_id)
            .map(|entry| entry.value().clone())
    }

    pub fn get_trades_by_exchange_order_id(&self, order_id: u64) -> Option<Vec<Arc<Trade>>> {
        self.exchange_order_id_trades_map
            .get(&order_id)
            .map(|trade_map| {
                trade_map
                    .iter()
                    .map(|entry| entry.value().clone())
                    .collect()
            })
    }

    pub fn get_account(&self) -> Option<Account> {
        let account = self.account.read().unwrap();
        account.clone()
    }

    pub fn get_want_price_by_client_order_id(&self, client_order_id: &str) -> Option<Decimal> {
        self.client_order_id_want_price_map
            .get(client_order_id)
            .map(|entry| *entry.value())
    }

    pub fn get_want_price_by_exchange_order_id(&self, order_id: u64) -> Option<Decimal> {
        let order = self.get_order_by_exchange_order_id(order_id)?;
        self.get_want_price_by_client_order_id(&order.client_order_id)
    }

    pub fn update_want_price(&self, client_order_id: &str, want_price: Decimal) -> Result<()> {
        self.client_order_id_want_price_map
            .insert(client_order_id.to_string(), want_price);
        self.client_order_id_want_price_db
            .insert(
                &TradingStorageKey::KeyOfWantPriceWithClientOrderId(Some(
                    client_order_id.to_string(),
                ))
                .to_bytes(),
                &want_price,
            )
            .map_err(|e| crate::binance::errors::BinanceError::ClientError {
                message: format!("Failed to update want price db: {}", e),
            })?;
        Ok(())
    }

    pub fn update_order(&self, order: &Order) -> Result<()> {
        if self
            .exchange_order_id_order_map
            .contains_key(&order.order_id)
        {
            let existing_order = self
                .exchange_order_id_order_map
                .get(&order.order_id)
                .unwrap();
            if existing_order.update_time >= order.update_time {
                return Ok(());
            }
        }
        self.exchange_order_id_order_map
            .insert(order.order_id, Arc::new(order.clone()));
        self.exchange_order_id_order_db
            .insert(
                &TradingStorageKey::KeyOfOrderWithExchangeOrderId(Some(order.order_id)).to_bytes(),
                order,
            )
            .map_err(|e| crate::binance::errors::BinanceError::ClientError {
                message: format!("Failed to update order db: {}", e),
            })?;
        Ok(())
    }

    pub fn update_trade(&self, trade: &Trade) -> Result<()> {
        let trade_map = self
            .exchange_order_id_trades_map
            .entry(trade.order_id)
            .or_insert_with(DashMap::new);
        trade_map.insert(trade.trade_id, Arc::new(trade.clone()));
        self.exchange_order_id_trades_db
            .insert(
                &TradingStorageKey::KeyOfTradeWithExchangeOrderId(
                    Some(trade.order_id),
                    Some(trade.trade_id),
                )
                .to_bytes(),
                trade,
            )
            .map_err(|e| crate::binance::errors::BinanceError::ClientError {
                message: format!("Failed to update trade db: {}", e),
            })?;
        Ok(())
    }

    pub fn update_by_execution_report(&self, execution_report: &ExecutionReport) -> Result<()> {
        let order = execution_report.to_order();
        self.update_order(&order)?;

        if let Some(trade) = execution_report.to_trade() {
            self.update_trade(&trade)?;
        }

        Ok(())
    }

    pub fn update_account(&self, account: &Account) -> Result<()> {
        {
            let mut account_lock = self.account.write().unwrap();
            *account_lock = Some(account.clone());
        }
        self.account_db
            .insert(&TradingStorageKey::KeyOfAccount.to_bytes(), account)
            .map_err(|e| crate::binance::errors::BinanceError::ClientError {
                message: format!("Failed to update account db: {}", e),
            })?;
        Ok(())
    }

    pub fn update_by_outbound_account_position(
        &self,
        outbound_account_pos: &OutboundAccountPosition,
    ) -> Result<()> {
        let mut account_lock = self.account.write().unwrap();
        if account_lock.is_none() {
            return Err(crate::binance::errors::BinanceError::ClientError {
                message: "Account is None, cannot update by outbound account position".to_string(),
            });
        }
        let account = account_lock.as_mut().unwrap();
        if account.update_time >= outbound_account_pos.update_time {
            return Ok(());
        }

        account.update_time = outbound_account_pos.update_time;

        let mut assets: HashMap<_, _> = account
            .balances
            .iter()
            .map(|e| (e.asset.clone(), e.clone()))
            .collect();
        outbound_account_pos
            .balances
            .iter()
            .for_each(|new_balance| {
                assets.insert(new_balance.asset.clone(), new_balance.clone());
            });
        account.balances = assets.into_values().collect();

        Ok(())
    }
}
