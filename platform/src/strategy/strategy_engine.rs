use crate::{
    errors::Result,
    factor::FactorManager,
    strategy::{PositionTargetSignal, Strategy, StrategyConfig},
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;

/// 策略引擎
/// 负责管理多个策略的生命周期、数据流转和信号分发
pub struct StrategyEngine {
    /// 策略集合
    strategies: Arc<RwLock<HashMap<String, Arc<RwLock<dyn Strategy>>>>>,

    /// 因子管理器
    factor_manager: Arc<FactorManager>,

    /// 目标持仓信号广播通道
    signal_tx: broadcast::Sender<PositionTargetSignal>,

    /// 后台任务句柄
    tasks: Arc<RwLock<Vec<JoinHandle<()>>>>,
}

impl StrategyEngine {
    pub fn new(factor_manager: Arc<FactorManager>) -> Self {
        let (signal_tx, _) = broadcast::channel(1000);

        Self {
            strategies: Arc::new(RwLock::new(HashMap::new())),
            factor_manager,
            signal_tx,
            tasks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// 注册策略
    pub async fn register_strategy(
        &self,
        strategy: Arc<RwLock<dyn Strategy>>,
        config: StrategyConfig,
    ) -> Result<()> {
        let name = strategy.read().await.name();
        let mut strategies = self.strategies.write().await;

        if strategies.contains_key(&name) {
            return Err(crate::errors::PlatformError::StrategyError {
                message: format!("Strategy '{}' already registered", name),
            });
        }

        strategies.insert(name.clone(), strategy.clone());

        if config.enabled {
            // 启动策略监听任务
            self.start_strategy_listeners(name.clone(), strategy.clone(), config)
                .await;
        }

        Ok(())
    }

    /// 注销策略
    pub async fn unregister_strategy(&self, name: &str) -> Result<()> {
        let mut strategies = self.strategies.write().await;

        if strategies.remove(name).is_none() {
            return Err(crate::errors::PlatformError::StrategyError {
                message: format!("Strategy '{}' not found", name),
            });
        }

        Ok(())
    }

    /// 获取策略
    pub async fn get_strategy(&self, name: &str) -> Option<Arc<RwLock<dyn Strategy>>> {
        let strategies = self.strategies.read().await;
        strategies.get(name).cloned()
    }

    /// 获取所有策略名称
    pub async fn get_strategy_names(&self) -> Vec<String> {
        let strategies = self.strategies.read().await;
        strategies.keys().cloned().collect()
    }

    /// 订阅所有策略的目标持仓信号
    pub fn subscribe_signals(&self) -> broadcast::Receiver<PositionTargetSignal> {
        self.signal_tx.subscribe()
    }

    /// 初始化所有策略
    pub async fn init_all(&self) -> Result<()> {
        let strategies = self.strategies.read().await;

        for (name, strategy) in strategies.iter() {
            let mut strategy_guard = strategy.write().await;
            if let Err(e) = strategy_guard.init().await {
                log::error!("Failed to initialize strategy '{}': {:?}", name, e);
            }
        }

        Ok(())
    }

    /// 启动策略监听任务
    async fn start_strategy_listeners(
        &self,
        name: String,
        strategy: Arc<RwLock<dyn Strategy>>,
        config: StrategyConfig,
    ) {
        let mut tasks = self.tasks.write().await;

        // 监听因子值更新
        if !config.factor_names.is_empty() {
            let strategy_clone = strategy.clone();
            let name_clone = name.clone();
            let mut value_rx = self.factor_manager.subscribe_values();
            let factor_names = config.factor_names.clone();

            tasks.push(tokio::spawn(async move {
                while let Ok(value) = value_rx.recv().await {
                    // 只处理配置中指定的因子
                    if factor_names.contains(&value.factor_name) {
                        let mut strategy_guard = strategy_clone.write().await;
                        if let Err(e) = strategy_guard.on_factor_value(value).await {
                            log::error!("Strategy '{}' error on factor value: {:?}", name_clone, e);
                        }
                    }
                }
            }));
        }

        // 监听因子事件
        if !config.factor_names.is_empty() {
            let strategy_clone = strategy.clone();
            let name_clone = name.clone();
            let mut event_rx = self.factor_manager.subscribe_events();
            let factor_names = config.factor_names.clone();

            tasks.push(tokio::spawn(async move {
                while let Ok(event) = event_rx.recv().await {
                    // 只处理配置中指定的因子
                    if factor_names.contains(&event.factor_name) {
                        let mut strategy_guard = strategy_clone.write().await;
                        if let Err(e) = strategy_guard.on_factor_event(event).await {
                            log::error!("Strategy '{}' error on factor event: {:?}", name_clone, e);
                        }
                    }
                }
            }));
        }

        // 监听策略信号
        let signal_tx = self.signal_tx.clone();
        let strategy_clone = strategy.clone();
        let mut signal_rx = strategy_clone.read().await.subscribe_signals();
        tasks.push(tokio::spawn(async move {
            while let Ok(signal) = signal_rx.recv().await {
                let _ = signal_tx.send(signal);
            }
        }));

        // 定期计算目标持仓
        let strategy_clone = strategy.clone();
        let name_clone = name.clone();
        let interval = Duration::from_millis(config.calculate_interval_ms);
        tasks.push(tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                let mut strategy_guard = strategy_clone.write().await;
                if let Err(e) = strategy_guard.calculate_targets().await {
                    log::error!(
                        "Strategy '{}' error calculating targets: {:?}",
                        name_clone,
                        e
                    );
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
}
