use crate::{
    errors::Result,
    factor::{FactorEvent, FactorValue},
    strategy::PositionTargetSignal,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::broadcast;

/// 策略 Trait
/// 定义策略的基本接口
#[async_trait]
pub trait Strategy: Send + Sync {
    /// 获取策略名称
    fn name(&self) -> String;

    /// 获取策略描述
    fn description(&self) -> String;

    /// 初始化策略
    async fn init(&mut self) -> Result<()>;

    /// 当因子值更新时调用
    async fn on_factor_value(&mut self, value: FactorValue) -> Result<()>;

    /// 当因子事件触发时调用
    async fn on_factor_event(&mut self, event: FactorEvent) -> Result<()>;

    /// 定期计算目标持仓
    async fn calculate_targets(&mut self) -> Result<PositionTargetSignal>;

    /// 订阅目标持仓信号
    fn subscribe_signals(&self) -> broadcast::Receiver<PositionTargetSignal>;

    /// 设置策略参数
    async fn set_parameter(&mut self, key: &str, value: &str) -> Result<()>;

    /// 获取策略参数
    async fn get_parameter(&self, key: &str) -> Option<String>;

    /// 获取策略状态
    async fn get_status(&self) -> StrategyStatus;
}

/// 策略状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StrategyStatus {
    /// 未初始化
    Uninitialized,

    /// 运行中
    Running,

    /// 暂停
    Paused,

    /// 停止
    Stopped,

    /// 错误
    Error(String),
}

/// 策略配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    /// 策略名称
    pub name: String,

    /// 是否启用
    pub enabled: bool,

    /// 计算间隔（毫秒）
    pub calculate_interval_ms: u64,

    /// 监听的因子列表
    pub factor_names: Vec<String>,

    /// 参数
    pub parameters: HashMap<String, String>,
}

impl StrategyConfig {
    pub fn new(name: String) -> Self {
        Self {
            name,
            enabled: true,
            calculate_interval_ms: 5000, // 默认 5 秒
            factor_names: Vec::new(),
            parameters: HashMap::new(),
        }
    }

    pub fn with_factor(mut self, factor_name: String) -> Self {
        self.factor_names.push(factor_name);
        self
    }

    pub fn with_factors(mut self, factor_names: Vec<String>) -> Self {
        self.factor_names = factor_names;
        self
    }

    pub fn with_parameter(mut self, key: String, value: String) -> Self {
        self.parameters.insert(key, value);
        self
    }

    pub fn with_interval(mut self, interval_ms: u64) -> Self {
        self.calculate_interval_ms = interval_ms;
        self
    }

    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}
