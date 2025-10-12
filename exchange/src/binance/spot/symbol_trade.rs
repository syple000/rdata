// use crate::binance::{
//     errors::Result,
//     spot::models::{Order, Trade},
// };
// use dashmap::{DashMap, DashSet};
// use rust_decimal::Decimal;
// use serde::Serialize;
// use std::{
//     collections::HashMap,
//     sync::{
//         atomic::{AtomicU64, Ordering},
//         Arc,
//     },
// };
// use tokio::sync::RwLock;
//
// #[derive(Serialize, Debug)]
// struct OrderStat {
//     want_price: Decimal,
//     order: Order,
//     trades: Vec<Trade>,
// }
//
// pub struct SymbolTrade {
//     symbol: String,
//     base_asset: String,
//     quote_asset: String,
//
//     on_order_stats: DashMap<u64, RwLock<OrderStat>>,
//     final_orders: DashSet<u64>,
//
//     // 数据库持久化
//     db_tree: sled::Tree,
// }
//
// impl SymbolTrade {
//     pub fn new(symbol: &str) -> Self {
//         unimplemented!()
//     }
//
//     pub async fn clean(&self, till_ts: u64) -> Result<Vec<Arc<Order>>> {
//         if self.latest_clean_ts.load(Ordering::Relaxed) >= till_ts {
//             return Ok(vec![]);
//         }
//
//         let mut closed_orders = self.closed_orders.write().await;
//         self.latest_clean_ts.store(till_ts, Ordering::Release);
//
//         let mut clean_ids = vec![];
//         for (id, order) in closed_orders.iter() {
//             if order.update_time <= till_ts {
//                 clean_ids.push(*id);
//             }
//         }
//         let mut clean_orders = vec![];
//         for id in &clean_ids {
//             clean_orders.push(closed_orders.remove(id).unwrap());
//         }
//         return Ok(clean_orders);
//     }
//
//     pub async fn update(&self, order: &Order) -> Result<()> {
//         if order.symbol != self.symbol {
//             return Err(crate::binance::errors::BinanceError::ClientError {
//                 message: format!(
//                     "Order symbol {} does not match manager symbol {}",
//                     order.symbol, self.symbol
//                 ),
//             });
//         }
//         if order.order_status.is_final()
//             && order.update_time < self.latest_clean_ts.load(Ordering::Relaxed)
//         {
//             return Ok(());
//         }
//
//         if order.order_status.is_final() {
//             let mut open_orders = self.open_orders.write().await;
//             if open_orders.remove(&order.order_id).is_some() {
//                 // 在open order中，移动到close order中
//                 let mut closed_orders = self.closed_orders.write().await;
//                 closed_orders.insert(order.order_id, Arc::new(order.clone()));
//             } else {
//                 // 新增到close order中
//                 let mut closed_orders = self.closed_orders.write().await;
//                 closed_orders.insert(order.order_id, Arc::new(order.clone()));
//             }
//             return Ok(());
//         } else {
//             // 判断是否在close order中，如果在，说明是过时数据，不做处理
//             // 不应该清理最近的close order，因为可能会收到延时的update
//             let closed_orders = self.closed_orders.read().await;
//             if closed_orders.contains_key(&order.order_id) {
//                 return Ok(());
//             }
//             drop(closed_orders);
//             // 判断是否在open order中，如果在，判断update_time，如果更新，则更新；不在直接新增
//             let mut open_orders = self.open_orders.write().await;
//             if let Some(existing_order) = open_orders.get_mut(&order.order_id) {
//                 if order.update_time > existing_order.update_time {
//                     *existing_order = Arc::new(order.clone());
//                 }
//             } else {
//                 open_orders.insert(order.order_id, Arc::new(order.clone()));
//             }
//             return Ok(());
//         }
//     }
//
//     pub async fn get_open_orders(&self) -> Vec<Arc<Order>> {
//         let open_orders = self.open_orders.read().await;
//         open_orders.values().cloned().collect()
//     }
//
//     pub async fn get_closed_orders(&self) -> Vec<Arc<Order>> {
//         let closed_orders = self.closed_orders.read().await;
//         closed_orders.values().cloned().collect()
//     }
// }
