use crate::{
    errors::Result,
    factor::{Factor, FactorEvent, FactorValue},
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;

/// 因子管理器
/// 负责管理多个因子的生命周期、数据流转和事件分发
pub struct FactorManager {
    /// 因子集合
    factors: Arc<RwLock<HashMap<String, Arc<RwLock<dyn Factor>>>>>,

    /// 因子值广播通道
    value_tx: broadcast::Sender<FactorValue>,

    /// 因子事件广播通道
    event_tx: broadcast::Sender<FactorEvent>,

    /// 后台任务句柄
    tasks: Arc<RwLock<Vec<JoinHandle<()>>>>,
}

impl FactorManager {
    pub fn new() -> Self {
        let (value_tx, _) = broadcast::channel(1000);
        let (event_tx, _) = broadcast::channel(1000);

        Self {
            factors: Arc::new(RwLock::new(HashMap::new())),
            value_tx,
            event_tx,
            tasks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// 注册因子
    pub async fn register_factor(&self, factor: Arc<RwLock<dyn Factor>>) -> Result<()> {
        let name = factor.read().await.name();
        let mut factors = self.factors.write().await;

        if factors.contains_key(&name) {
            return Err(crate::errors::PlatformError::FactorError {
                message: format!("Factor '{}' already registered", name),
            });
        }

        factors.insert(name.clone(), factor.clone());

        // 启动因子值监听任务
        self.start_factor_listeners(name, factor).await;

        Ok(())
    }

    /// 注销因子
    pub async fn unregister_factor(&self, name: &str) -> Result<()> {
        let mut factors = self.factors.write().await;

        if factors.remove(name).is_none() {
            return Err(crate::errors::PlatformError::FactorError {
                message: format!("Factor '{}' not found", name),
            });
        }

        Ok(())
    }

    /// 获取因子
    pub async fn get_factor(&self, name: &str) -> Option<Arc<RwLock<dyn Factor>>> {
        let factors = self.factors.read().await;
        factors.get(name).cloned()
    }

    /// 获取所有因子名称
    pub async fn get_factor_names(&self) -> Vec<String> {
        let factors = self.factors.read().await;
        factors.keys().cloned().collect()
    }

    /// 获取指定因子的指定标的的值
    pub async fn get_factor_value(&self, factor_name: &str, symbol: &str) -> Option<FactorValue> {
        let factors = self.factors.read().await;
        if let Some(factor) = factors.get(factor_name) {
            factor.read().await.get_value(symbol).await
        } else {
            None
        }
    }

    /// 获取指定标的的所有因子值
    pub async fn get_all_factor_values(&self, symbol: &str) -> Vec<FactorValue> {
        let factors = self.factors.read().await;
        let mut values = Vec::new();

        for factor in factors.values() {
            if let Some(value) = factor.read().await.get_value(symbol).await {
                values.push(value);
            }
        }

        values
    }

    /// 获取所有因子的所有值
    pub async fn get_all_values(&self) -> HashMap<String, Vec<FactorValue>> {
        let factors = self.factors.read().await;
        let mut result = HashMap::new();

        for (name, factor) in factors.iter() {
            let values = factor.read().await.get_all_values().await;
            result.insert(name.clone(), values);
        }

        result
    }

    /// 订阅所有因子值更新
    pub fn subscribe_values(&self) -> broadcast::Receiver<FactorValue> {
        self.value_tx.subscribe()
    }

    /// 订阅所有因子事件
    pub fn subscribe_events(&self) -> broadcast::Receiver<FactorEvent> {
        self.event_tx.subscribe()
    }

    /// 初始化所有因子
    pub async fn init_all(&self) -> Result<()> {
        let factors = self.factors.read().await;

        for (name, factor) in factors.iter() {
            let mut factor_guard = factor.write().await;
            if let Err(e) = factor_guard.init().await {
                log::error!("Failed to initialize factor '{}': {:?}", name, e);
            }
        }

        Ok(())
    }

    /// 更新所有因子
    pub async fn update_all(&self) -> Result<()> {
        let factors = self.factors.read().await;

        for (name, factor) in factors.iter() {
            let mut factor_guard = factor.write().await;
            if let Err(e) = factor_guard.update().await {
                log::error!("Failed to update factor '{}': {:?}", name, e);
            }
        }

        Ok(())
    }

    /// 启动因子监听任务
    async fn start_factor_listeners(&self, _name: String, factor: Arc<RwLock<dyn Factor>>) {
        let mut tasks = self.tasks.write().await;

        // 监听因子值更新
        let value_tx = self.value_tx.clone();
        let factor_clone = factor.clone();
        let mut value_rx = factor_clone.read().await.subscribe_value();
        tasks.push(tokio::spawn(async move {
            while let Ok(value) = value_rx.recv().await {
                let _ = value_tx.send(value);
            }
        }));

        // 监听因子事件
        let event_tx = self.event_tx.clone();
        let factor_clone = factor.clone();
        let mut event_rx = factor_clone.read().await.subscribe_event();
        tasks.push(tokio::spawn(async move {
            while let Ok(event) = event_rx.recv().await {
                let _ = event_tx.send(event);
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

impl Default for FactorManager {
    fn default() -> Self {
        Self::new()
    }
}
