use crate::{
    errors::Result,
    execution::{OrderValidator, PositionManager},
    models::*,
    strategy::PositionTargetSignal,
    trade_provider::TradeProvider,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// 执行引擎配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    /// 最大重试次数
    pub max_retries: u32,

    /// 重试间隔（毫秒）
    pub retry_interval_ms: u64,

    /// 订单超时时间（毫秒）
    pub order_timeout_ms: u64,

    /// 是否启用订单验证
    pub enable_validation: bool,

    /// 最小订单间隔（毫秒）
    pub min_order_interval_ms: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_interval_ms: 1000,
            order_timeout_ms: 30000,
            enable_validation: true,
            min_order_interval_ms: 100,
        }
    }
}

/// 执行引擎
/// 负责接收策略的目标持仓信号，执行交易直到达到目标持仓
pub struct ExecutionEngine {
    trade_provider: Arc<dyn TradeProvider>,
    position_manager: Arc<PositionManager>,
    order_validator: Arc<OrderValidator>,
    config: ExecutionConfig,

    /// 后台任务句柄
    tasks: Arc<RwLock<Vec<JoinHandle<()>>>>,

    /// 最后下单时间
    last_order_time: Arc<RwLock<std::collections::HashMap<String, u64>>>,
}

impl ExecutionEngine {
    pub fn new(trade_provider: Arc<dyn TradeProvider>, config: ExecutionConfig) -> Self {
        Self {
            trade_provider,
            position_manager: Arc::new(PositionManager::new()),
            order_validator: Arc::new(OrderValidator::new()),
            config,
            tasks: Arc::new(RwLock::new(Vec::new())),
            last_order_time: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// 初始化执行引擎
    pub async fn init(&self) -> Result<()> {
        // 获取并更新账户信息
        let account = self.trade_provider.get_account().await?;
        self.position_manager
            .update_balances(account.balances)
            .await;

        // 获取并更新交易所信息（用于订单验证）
        // 注意：这里需要通过 MarketProvider 获取，暂时省略
        // 如果需要验证，可以在外部传入 ExchangeInfo

        // 启动账户更新监听
        self.start_account_listener().await;

        // 启动用户成交监听
        self.start_user_trade_listener().await;

        Ok(())
    }

    /// 更新交易所信息（用于订单验证）
    pub async fn update_exchange_info(&self, info: ExchangeInfo) {
        self.order_validator.update_exchange_info(info).await;
    }

    /// 获取持仓管理器
    pub fn get_position_manager(&self) -> Arc<PositionManager> {
        self.position_manager.clone()
    }

    /// 执行目标持仓信号
    pub async fn execute_signal(&self, signal: PositionTargetSignal) -> Result<()> {
        for target in signal.targets {
            if let Err(e) = self.execute_target(&target).await {
                log::error!("Failed to execute target for {}: {:?}", target.symbol, e);
            }
        }
        Ok(())
    }

    /// 执行单个目标持仓
    async fn execute_target(&self, target: &crate::strategy::PositionTarget) -> Result<()> {
        // 计算需要交易的数量
        let diff = self
            .position_manager
            .calculate_diff(&target.symbol, target.target_quantity)
            .await;

        if diff.abs() < Decimal::new(1, 8) {
            // 差异很小，不需要交易
            return Ok(());
        }

        // 检查最小下单间隔
        self.check_order_interval(&target.symbol).await?;

        // 确定订单方向
        let side = if diff > Decimal::ZERO {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };

        let quantity = diff.abs();

        // 获取对应的资产余额用于验证
        let (base_asset, quote_asset) = self.parse_symbol(&target.symbol);
        let available_balance = match side {
            OrderSide::Buy => {
                self.position_manager
                    .get_available_balance(&quote_asset)
                    .await
            }
            OrderSide::Sell => {
                self.position_manager
                    .get_available_balance(&base_asset)
                    .await
            }
        };

        // 验证订单
        if self.config.enable_validation {
            self.order_validator
                .validate(
                    &target.symbol,
                    &side,
                    quantity,
                    target.target_price,
                    available_balance,
                )
                .await?;
        }

        // 下单
        let order_type = if target.target_price.is_some() {
            OrderType::Limit
        } else {
            OrderType::Market
        };

        let req = PlaceOrderRequest {
            symbol: target.symbol.clone(),
            side,
            r#type: order_type,
            time_in_force: Some(TimeInForce::Gtc),
            quantity: Some(quantity),
            price: target.target_price,
            new_client_order_id: None,
            stop_price: None,
            iceberg_qty: None,
        };

        // 带重试的下单
        let mut retries = 0;
        loop {
            match self.trade_provider.place_order(req.clone()).await {
                Ok(order) => {
                    log::info!(
                        "Order placed successfully: {} {:?} {} @ {:?}",
                        order.symbol,
                        order.order_side,
                        order.order_quantity,
                        order.order_price
                    );

                    // 更新最后下单时间
                    self.update_last_order_time(&target.symbol).await;

                    return Ok(());
                }
                Err(e) => {
                    retries += 1;
                    if retries >= self.config.max_retries {
                        return Err(e);
                    }
                    log::warn!(
                        "Failed to place order (attempt {}/{}): {:?}",
                        retries,
                        self.config.max_retries,
                        e
                    );
                    tokio::time::sleep(Duration::from_millis(self.config.retry_interval_ms)).await;
                }
            }
        }
    }

    /// 检查最小下单间隔
    async fn check_order_interval(&self, symbol: &str) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let last_time = self.last_order_time.read().await;
        if let Some(&last) = last_time.get(symbol) {
            let elapsed = now - last;
            if elapsed < self.config.min_order_interval_ms {
                return Err(crate::errors::PlatformError::ExecutionError {
                    message: format!(
                        "Order interval too short: {} ms (minimum {} ms)",
                        elapsed, self.config.min_order_interval_ms
                    ),
                });
            }
        }

        Ok(())
    }

    /// 更新最后下单时间
    async fn update_last_order_time(&self, symbol: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut last_time = self.last_order_time.write().await;
        last_time.insert(symbol.to_string(), now);
    }

    /// 解析交易对（简单实现，实际应该从 ExchangeInfo 获取）
    fn parse_symbol(&self, symbol: &str) -> (String, String) {
        // 简单假设交易对格式为 BASEUSDT
        if symbol.ends_with("USDT") {
            let base = symbol.trim_end_matches("USDT");
            (base.to_string(), "USDT".to_string())
        } else if symbol.ends_with("BTC") {
            let base = symbol.trim_end_matches("BTC");
            (base.to_string(), "BTC".to_string())
        } else {
            // 默认假设最后3个字符是quote asset
            let len = symbol.len();
            if len > 3 {
                (symbol[..len - 3].to_string(), symbol[len - 3..].to_string())
            } else {
                (symbol.to_string(), "USDT".to_string())
            }
        }
    }

    /// 启动账户更新监听
    async fn start_account_listener(&self) {
        let position_manager = self.position_manager.clone();
        let mut account_rx = self.trade_provider.subscribe_account_update();

        let mut tasks = self.tasks.write().await;
        tasks.push(tokio::spawn(async move {
            while let Ok(account_update) = account_rx.recv().await {
                position_manager
                    .update_balances(account_update.balances)
                    .await;
            }
        }));
    }

    /// 启动用户成交监听
    async fn start_user_trade_listener(&self) {
        let position_manager = self.position_manager.clone();
        let mut user_trade_rx = self.trade_provider.subscribe_user_trade();

        let mut tasks = self.tasks.write().await;
        tasks.push(tokio::spawn(async move {
            while let Ok(user_trade) = user_trade_rx.recv().await {
                let trade_qty = match user_trade.order_side {
                    OrderSide::Buy => user_trade.trade_quantity,
                    OrderSide::Sell => -user_trade.trade_quantity,
                };
                position_manager
                    .update_position(&user_trade.symbol, trade_qty, user_trade.trade_price)
                    .await;
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
}
