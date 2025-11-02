use crate::models::{DepthData, KlineData, KlineInterval, Order, Ticker24hr, Trade, UserTrade};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct KlineCache {
    data: Arc<RwLock<HashMap<String, HashMap<KlineInterval, VecDeque<KlineData>>>>>,
    max_count: usize,
}

impl KlineCache {
    pub fn new(max_count: usize) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            max_count,
        }
    }

    /// 添加 Kline 数据
    pub async fn add(&self, kline: KlineData) {
        let mut data = self.data.write().await;
        let symbol_map = data
            .entry(kline.symbol.clone())
            .or_insert_with(HashMap::new);
        let interval_deque = symbol_map
            .entry(kline.interval.clone())
            .or_insert_with(VecDeque::new);

        // 检查是否需要更新最后一条（未关闭的 K 线）
        if let Some(last) = interval_deque.back_mut() {
            if last.open_time == kline.open_time && !kline.is_closed {
                *last = kline;
                return;
            }
        }

        // 添加新的 K 线
        interval_deque.push_back(kline);

        // 保持最大数量限制
        while interval_deque.len() > self.max_count {
            interval_deque.pop_front();
        }
    }

    /// 批量添加 Kline 数据（用于初始化）
    pub async fn add_batch(&self, klines: Vec<KlineData>) {
        for kline in klines {
            self.add(kline).await;
        }
    }

    /// 获取指定数量的 Kline 数据
    pub async fn get(
        &self,
        symbol: &str,
        interval: &KlineInterval,
        limit: usize,
    ) -> Vec<KlineData> {
        let data = self.data.read().await;
        data.get(symbol)
            .and_then(|symbol_map| symbol_map.get(interval))
            .map(|deque| deque.iter().rev().take(limit).rev().cloned().collect())
            .unwrap_or_default()
    }

    /// 获取最后一个 open_time
    pub async fn get_last_open_time(&self, symbol: &str, interval: &KlineInterval) -> Option<u64> {
        let data = self.data.read().await;
        data.get(symbol)
            .and_then(|symbol_map| symbol_map.get(interval))
            .and_then(|deque| deque.back())
            .map(|kline| kline.open_time)
    }
}

/// Trade 数据缓存
#[derive(Debug, Clone)]
pub struct TradeCache {
    /// symbol -> VecDeque<Trade>
    data: Arc<RwLock<HashMap<String, VecDeque<Trade>>>>,
    max_count: usize,
}

impl TradeCache {
    pub fn new(max_count: usize) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            max_count,
        }
    }

    /// 添加 Trade 数据
    pub async fn add(&self, trade: Trade) {
        let mut data = self.data.write().await;
        let deque = data
            .entry(trade.symbol.clone())
            .or_insert_with(VecDeque::new);

        deque.push_back(trade);

        // 保持最大数量限制
        while deque.len() > self.max_count {
            deque.pop_front();
        }
    }

    /// 批量添加 Trade 数据（用于初始化）
    pub async fn add_batch(&self, trades: Vec<Trade>) {
        for trade in trades {
            self.add(trade).await;
        }
    }

    /// 获取指定数量的 Trade 数据
    pub async fn get(&self, symbol: &str, limit: usize) -> Vec<Trade> {
        let data = self.data.read().await;
        data.get(symbol)
            .map(|deque| deque.iter().rev().take(limit).rev().cloned().collect())
            .unwrap_or_default()
    }

    /// 获取最后一个 seq_id
    pub async fn get_last_seq_id(&self, symbol: &str) -> Option<u64> {
        let data = self.data.read().await;
        data.get(symbol)
            .and_then(|deque| deque.back())
            .map(|trade| trade.seq_id)
    }
}

/// Ticker 数据缓存（仅保留最新）
#[derive(Debug, Clone)]
pub struct TickerCache {
    /// symbol -> Ticker24hr
    data: Arc<RwLock<HashMap<String, Ticker24hr>>>,
}

impl TickerCache {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 更新 Ticker 数据
    pub async fn update(&self, ticker: Ticker24hr) {
        let mut data = self.data.write().await;
        data.insert(ticker.symbol.clone(), ticker);
    }

    /// 获取 Ticker 数据
    pub async fn get(&self, symbol: &str) -> Option<Ticker24hr> {
        let data = self.data.read().await;
        data.get(symbol).cloned()
    }

    /// 获取所有 Ticker 数据
    pub async fn get_all(&self) -> Vec<Ticker24hr> {
        let data = self.data.read().await;
        data.values().cloned().collect()
    }
}

/// Depth 数据缓存（仅保留最新）
#[derive(Debug, Clone)]
pub struct DepthCache {
    /// symbol -> DepthData
    data: Arc<RwLock<HashMap<String, DepthData>>>,
}

impl DepthCache {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 更新 Depth 数据
    pub async fn update(&self, depth: DepthData) {
        let mut data = self.data.write().await;
        data.insert(depth.symbol.clone(), depth);
    }

    /// 获取 Depth 数据
    pub async fn get(&self, symbol: &str) -> Option<DepthData> {
        let data = self.data.read().await;
        data.get(symbol).cloned()
    }
}

/// 订单缓存
#[derive(Debug, Clone)]
pub struct OrderCache {
    /// symbol -> VecDeque<Order>
    data: Arc<RwLock<HashMap<String, VecDeque<Order>>>>,
    max_count: usize,
    max_duration_ms: u64,
}

impl OrderCache {
    pub fn new(max_count: usize, max_duration_ms: u64) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            max_count,
            max_duration_ms,
        }
    }

    /// 添加或更新订单
    pub async fn upsert(&self, order: Order) {
        let mut data = self.data.write().await;
        let deque = data
            .entry(order.symbol.clone())
            .or_insert_with(VecDeque::new);

        // 查找是否已存在该订单
        if let Some(pos) = deque.iter().position(|o| o.order_id == order.order_id) {
            deque[pos] = order;
        } else {
            deque.push_back(order);

            // 保持最大数量限制
            while deque.len() > self.max_count {
                deque.pop_front();
            }
        }

        // 清理过期订单
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        deque.retain(|o| now - o.update_time < self.max_duration_ms);
    }

    /// 批量添加订单
    pub async fn add_batch(&self, orders: Vec<Order>) {
        for order in orders {
            self.upsert(order).await;
        }
    }

    /// 获取指定 symbol 的订单
    pub async fn get(&self, symbol: Option<&str>) -> Vec<Order> {
        let data = self.data.read().await;
        if let Some(sym) = symbol {
            data.get(sym)
                .map(|deque| deque.iter().cloned().collect())
                .unwrap_or_default()
        } else {
            data.values()
                .flat_map(|deque| deque.iter().cloned())
                .collect()
        }
    }

    /// 获取未完成的订单
    pub async fn get_open_orders(&self, symbol: Option<&str>) -> Vec<Order> {
        let orders = self.get(symbol).await;
        orders
            .into_iter()
            .filter(|o| {
                matches!(
                    o.order_status,
                    crate::models::OrderStatus::New
                        | crate::models::OrderStatus::PendingNew
                        | crate::models::OrderStatus::PartiallyFilled
                )
            })
            .collect()
    }
}

/// 用户成交缓存
#[derive(Debug, Clone)]
pub struct UserTradeCache {
    /// symbol -> VecDeque<UserTrade>
    data: Arc<RwLock<HashMap<String, VecDeque<UserTrade>>>>,
    max_count: usize,
    max_duration_ms: u64,
}

impl UserTradeCache {
    pub fn new(max_count: usize, max_duration_ms: u64) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            max_count,
            max_duration_ms,
        }
    }

    /// 添加用户成交
    pub async fn add(&self, user_trade: UserTrade) {
        let mut data = self.data.write().await;
        let deque = data
            .entry(user_trade.symbol.clone())
            .or_insert_with(VecDeque::new);

        deque.push_back(user_trade);

        // 保持最大数量限制
        while deque.len() > self.max_count {
            deque.pop_front();
        }

        // 清理过期成交
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        deque.retain(|t| now - t.timestamp < self.max_duration_ms);
    }

    /// 批量添加用户成交
    pub async fn add_batch(&self, user_trades: Vec<UserTrade>) {
        for user_trade in user_trades {
            self.add(user_trade).await;
        }
    }

    /// 获取指定 symbol 的用户成交
    pub async fn get(&self, symbol: Option<&str>, limit: usize) -> Vec<UserTrade> {
        let data = self.data.read().await;
        if let Some(sym) = symbol {
            data.get(sym)
                .map(|deque| deque.iter().rev().take(limit).rev().cloned().collect())
                .unwrap_or_default()
        } else {
            let mut all_trades: Vec<UserTrade> = data
                .values()
                .flat_map(|deque| deque.iter().cloned())
                .collect();
            all_trades.sort_by_key(|t| t.timestamp);
            all_trades.into_iter().rev().take(limit).rev().collect()
        }
    }
}
