use crate::{
    data_manager::{
        config::DataManagerConfig,
        data_cache::{OrderCache, UserTradeCache},
    },
    errors::Result,
    models::*,
    trade_provider::TradeProvider,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;

/// 账户数据管理器
/// 负责管理账户、订单、成交数据的缓存和同步
pub struct AccountDataManager {
    provider: Arc<dyn TradeProvider>,
    config: DataManagerConfig,

    // 数据缓存
    account: Arc<RwLock<Option<Account>>>,
    order_cache: OrderCache,
    user_trade_cache: UserTradeCache,

    // 广播通道（用于推送数据）
    order_tx: broadcast::Sender<Order>,
    user_trade_tx: broadcast::Sender<UserTrade>,
    account_update_tx: broadcast::Sender<AccountUpdate>,

    // 后台任务句柄
    tasks: Arc<RwLock<Vec<JoinHandle<()>>>>,
}

impl AccountDataManager {
    pub fn new(provider: Arc<dyn TradeProvider>, config: DataManagerConfig) -> Self {
        let (order_tx, _) = broadcast::channel(1000);
        let (user_trade_tx, _) = broadcast::channel(1000);
        let (account_update_tx, _) = broadcast::channel(100);

        Self {
            provider,
            account: Arc::new(RwLock::new(None)),
            order_cache: OrderCache::new(
                config.order_cache_config.max_count,
                config.order_cache_config.max_duration.as_millis() as u64,
            ),
            user_trade_cache: UserTradeCache::new(
                config.user_trade_cache_config.max_count,
                config.user_trade_cache_config.max_duration.as_millis() as u64,
            ),
            order_tx,
            user_trade_tx,
            account_update_tx,
            tasks: Arc::new(RwLock::new(Vec::new())),
            config,
        }
    }

    /// 初始化数据管理器
    /// 从 API 加载初始数据并启动推送监听任务
    pub async fn init(&mut self, symbols: Option<Vec<String>>) -> Result<()> {
        // 初始化账户数据
        match self.provider.get_account().await {
            Ok(account) => {
                *self.account.write().await = Some(account);
            }
            Err(e) => {
                log::warn!("Failed to initialize account data: {:?}", e);
            }
        }

        // 初始化订单数据
        if let Some(symbols) = &symbols {
            for symbol in symbols {
                let req = GetAllOrdersRequest {
                    symbol: symbol.clone(),
                    from_id: None,
                    limit: Some(self.config.order_cache_config.max_count as u32),
                    start_time: None,
                    end_time: None,
                };

                match self.provider.get_all_orders(req).await {
                    Ok(orders) => {
                        self.order_cache.add_batch(orders).await;
                    }
                    Err(e) => {
                        log::warn!("Failed to initialize order data for {}: {:?}", symbol, e);
                    }
                }
            }
        } else {
            // 如果没有指定 symbols，获取所有未完成订单
            let req = GetOpenOrdersRequest { symbol: None };
            match self.provider.get_open_orders(req).await {
                Ok(orders) => {
                    self.order_cache.add_batch(orders).await;
                }
                Err(e) => {
                    log::warn!("Failed to initialize open orders: {:?}", e);
                }
            }
        }

        // 初始化用户成交数据
        if let Some(symbols) = &symbols {
            for symbol in symbols {
                let req = GetUserTradesRequest {
                    symbol: symbol.clone(),
                    order_id: None,
                    from_id: None,
                    limit: Some(self.config.user_trade_cache_config.max_count as u32),
                    start_time: None,
                    end_time: None,
                };

                match self.provider.get_user_trades(req).await {
                    Ok(user_trades) => {
                        self.user_trade_cache.add_batch(user_trades).await;
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to initialize user trade data for {}: {:?}",
                            symbol,
                            e
                        );
                    }
                }
            }
        }

        // 启动推送监听任务
        self.start_stream_listeners().await;

        // 启动定期同步任务
        self.start_periodic_sync(symbols).await;

        Ok(())
    }

    /// 启动推送监听任务
    async fn start_stream_listeners(&self) {
        let mut tasks = self.tasks.write().await;

        // 订单推送监听
        let order_cache = self.order_cache.clone();
        let order_tx = self.order_tx.clone();
        let mut order_rx = self.provider.subscribe_order();
        tasks.push(tokio::spawn(async move {
            while let Ok(order) = order_rx.recv().await {
                order_cache.upsert(order.clone()).await;
                let _ = order_tx.send(order);
            }
        }));

        // 用户成交推送监听
        let user_trade_cache = self.user_trade_cache.clone();
        let user_trade_tx = self.user_trade_tx.clone();
        let mut user_trade_rx = self.provider.subscribe_user_trade();
        tasks.push(tokio::spawn(async move {
            while let Ok(user_trade) = user_trade_rx.recv().await {
                user_trade_cache.add(user_trade.clone()).await;
                let _ = user_trade_tx.send(user_trade);
            }
        }));

        // 账户更新推送监听
        let account = self.account.clone();
        let account_update_tx = self.account_update_tx.clone();
        let mut account_update_rx = self.provider.subscribe_account_update();
        tasks.push(tokio::spawn(async move {
            while let Ok(account_update) = account_update_rx.recv().await {
                // 更新账户数据
                let mut acc = account.write().await;
                if let Some(current_account) = acc.as_mut() {
                    current_account.balances = account_update.balances.clone();
                    current_account.timestamp = account_update.timestamp;
                } else {
                    *acc = Some(Account {
                        balances: account_update.balances.clone(),
                        timestamp: account_update.timestamp,
                    });
                }
                let _ = account_update_tx.send(account_update);
            }
        }));
    }

    /// 启动定期同步任务
    async fn start_periodic_sync(&self, symbols: Option<Vec<String>>) {
        let provider = self.provider.clone();
        let account = self.account.clone();
        let order_cache = self.order_cache.clone();
        let user_trade_cache = self.user_trade_cache.clone();
        let config = self.config.clone();

        let mut tasks = self.tasks.write().await;
        tasks.push(tokio::spawn(async move {
            let mut interval_timer =
                tokio::time::interval(config.sync_config.account_sync_interval);

            loop {
                interval_timer.tick().await;

                // 同步账户数据
                if let Ok(acc) = provider.get_account().await {
                    *account.write().await = Some(acc);
                }

                // 同步订单数据
                if let Some(ref symbols) = symbols {
                    for symbol in symbols {
                        let req = GetOpenOrdersRequest {
                            symbol: Some(symbol.clone()),
                        };
                        if let Ok(orders) = provider.get_open_orders(req).await {
                            order_cache.add_batch(orders).await;
                        }
                    }
                } else {
                    let req = GetOpenOrdersRequest { symbol: None };
                    if let Ok(orders) = provider.get_open_orders(req).await {
                        order_cache.add_batch(orders).await;
                    }
                }

                // 同步最新的用户成交
                if let Some(ref symbols) = symbols {
                    for symbol in symbols {
                        let req = GetUserTradesRequest {
                            symbol: symbol.clone(),
                            order_id: None,
                            from_id: None,
                            limit: Some(50), // 每次同步最近 50 条
                            start_time: None,
                            end_time: None,
                        };
                        if let Ok(user_trades) = provider.get_user_trades(req).await {
                            user_trade_cache.add_batch(user_trades).await;
                        }
                    }
                }
            }
        }));
    }

    /// 停止所有后台任务
    pub async fn shutdown(&self) {
        let mut tasks = self.tasks.write().await;
        for task in tasks.drain(..) {
            task.abort();
        }
    }

    /// 获取账户信息（仅从缓存）
    pub async fn get_account_cached(&self) -> Option<Account> {
        self.account.read().await.clone()
    }

    /// 获取订单（仅从缓存）
    pub async fn get_orders_cached(&self, symbol: Option<&str>) -> Vec<Order> {
        self.order_cache.get(symbol).await
    }

    /// 获取未完成订单（仅从缓存）
    pub async fn get_open_orders_cached(&self, symbol: Option<&str>) -> Vec<Order> {
        self.order_cache.get_open_orders(symbol).await
    }

    /// 获取用户成交（仅从缓存）
    pub async fn get_user_trades_cached(
        &self,
        symbol: Option<&str>,
        limit: usize,
    ) -> Vec<UserTrade> {
        self.user_trade_cache.get(symbol, limit).await
    }
}

// 实现 TradeProvider trait，优先从缓存获取数据
#[async_trait]
impl TradeProvider for AccountDataManager {
    async fn init(&mut self) -> Result<()> {
        // 由外部调用 AccountDataManager::init
        Ok(())
    }

    async fn place_order(&self, req: PlaceOrderRequest) -> Result<Order> {
        let order = self.provider.place_order(req).await?;
        self.order_cache.upsert(order.clone()).await;
        Ok(order)
    }

    async fn cancel_order(&self, req: CancelOrderRequest) -> Result<()> {
        self.provider.cancel_order(req).await
    }

    async fn get_order(&self, req: GetOrderRequest) -> Result<Order> {
        // 先从缓存查找
        let cached_orders = self.order_cache.get(Some(&req.symbol)).await;
        if let Some(ref order_id) = req.order_id {
            if let Some(order) = cached_orders.iter().find(|o| &o.order_id == order_id) {
                return Ok(order.clone());
            }
        } else if let Some(ref client_order_id) = req.orig_client_order_id {
            if let Some(order) = cached_orders
                .iter()
                .find(|o| &o.client_order_id == client_order_id)
            {
                return Ok(order.clone());
            }
        }

        // 缓存中没有，从 API 获取
        let order = self.provider.get_order(req).await?;
        self.order_cache.upsert(order.clone()).await;
        Ok(order)
    }

    async fn get_open_orders(&self, req: GetOpenOrdersRequest) -> Result<Vec<Order>> {
        // 从 API 获取最新数据
        let orders = self.provider.get_open_orders(req).await?;
        self.order_cache.add_batch(orders.clone()).await;
        Ok(orders)
    }

    async fn get_all_orders(&self, req: GetAllOrdersRequest) -> Result<Vec<Order>> {
        // 从 API 获取
        let orders = self.provider.get_all_orders(req).await?;
        self.order_cache.add_batch(orders.clone()).await;
        Ok(orders)
    }

    async fn get_user_trades(&self, req: GetUserTradesRequest) -> Result<Vec<UserTrade>> {
        let limit = req.limit.unwrap_or(500) as usize;

        // 先尝试从缓存获取
        let cached = self.user_trade_cache.get(Some(&req.symbol), limit).await;

        // 如果缓存足够，直接返回
        if cached.len() >= limit {
            return Ok(cached);
        }

        // 否则从 API 获取
        let user_trades = self.provider.get_user_trades(req).await?;
        self.user_trade_cache.add_batch(user_trades.clone()).await;
        Ok(user_trades)
    }

    async fn get_account(&self) -> Result<Account> {
        // 优先从缓存获取
        if let Some(account) = self.account.read().await.clone() {
            return Ok(account);
        }

        // 缓存中没有，从 API 获取
        let account = self.provider.get_account().await?;
        *self.account.write().await = Some(account.clone());
        Ok(account)
    }

    fn subscribe_order(&self) -> broadcast::Receiver<Order> {
        self.order_tx.subscribe()
    }

    fn subscribe_user_trade(&self) -> broadcast::Receiver<UserTrade> {
        self.user_trade_tx.subscribe()
    }

    fn subscribe_account_update(&self) -> broadcast::Receiver<AccountUpdate> {
        self.account_update_tx.subscribe()
    }
}
