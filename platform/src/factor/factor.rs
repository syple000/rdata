use crate::errors::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::broadcast;

/// 因子值
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorValue {
    /// 标的符号
    pub symbol: String,

    /// 因子名称
    pub factor_name: String,

    /// 因子值
    pub value: Decimal,

    /// 时间戳
    pub timestamp: u64,

    /// 附加信息
    pub metadata: HashMap<String, String>,
}

/// 因子事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorEvent {
    /// 标的符号
    pub symbol: String,

    /// 因子名称
    pub factor_name: String,

    /// 事件类型
    pub event_type: FactorEventType,

    /// 因子值
    pub value: Decimal,

    /// 时间戳
    pub timestamp: u64,

    /// 事件描述
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FactorEventType {
    /// 因子超过阈值
    ThresholdExceeded,

    /// 因子低于阈值
    ThresholdBelow,

    /// 因子方向改变
    DirectionChanged,

    /// 因子信号触发
    SignalTriggered,

    /// 自定义事件
    Custom(String),
}

/// 因子 Trait
/// 定义因子的基本接口
#[async_trait]
pub trait Factor: Send + Sync {
    /// 获取因子名称
    fn name(&self) -> String;

    /// 获取因子描述
    fn description(&self) -> String;

    /// 初始化因子
    async fn init(&mut self) -> Result<()>;

    /// 更新因子（当收到新数据时调用）
    async fn update(&mut self) -> Result<()>;

    /// 获取指定标的的因子值
    async fn get_value(&self, symbol: &str) -> Option<FactorValue>;

    /// 获取所有标的的因子值
    async fn get_all_values(&self) -> Vec<FactorValue>;

    /// 订阅因子值更新
    fn subscribe_value(&self) -> broadcast::Receiver<FactorValue>;

    /// 订阅因子事件
    fn subscribe_event(&self) -> broadcast::Receiver<FactorEvent>;

    /// 设置因子参数
    async fn set_parameter(&mut self, key: &str, value: &str) -> Result<()>;

    /// 获取因子参数
    async fn get_parameter(&self, key: &str) -> Option<String>;
}

/// 因子配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorConfig {
    /// 因子名称
    pub name: String,

    /// 是否启用
    pub enabled: bool,

    /// 更新间隔（毫秒）
    pub update_interval_ms: u64,

    /// 参数
    pub parameters: HashMap<String, String>,
}

impl FactorConfig {
    pub fn new(name: String) -> Self {
        Self {
            name,
            enabled: true,
            update_interval_ms: 1000, // 默认 1 秒
            parameters: HashMap::new(),
        }
    }

    pub fn with_parameter(mut self, key: String, value: String) -> Self {
        self.parameters.insert(key, value);
        self
    }

    pub fn with_interval(mut self, interval_ms: u64) -> Self {
        self.update_interval_ms = interval_ms;
        self
    }

    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}
