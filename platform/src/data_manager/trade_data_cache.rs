use crate::models::{Account, Order, UserTrade};
use std::collections::HashMap;

pub struct AccountCache {
    account: Account,
}

pub struct OrderUserTradeCache {
    orders: HashMap<String, Order>,
    trades: HashMap<String, UserTrade>,

    symbol_orders: HashMap<String, Vec<String>>,
    symbol_trades: HashMap<String, Vec<String>>,
    order_trades: HashMap<String, Vec<String>>,
}

pub struct TradeDataCache {}
