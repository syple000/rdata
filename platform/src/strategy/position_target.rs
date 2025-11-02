use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// 目标持仓
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionTarget {
    /// 标的符号
    pub symbol: String,

    /// 目标数量（正数表示做多，负数表示做空，0 表示清仓）
    pub target_quantity: Decimal,

    /// 目标价格（如果指定，使用限价单；否则使用市价单）
    pub target_price: Option<Decimal>,

    /// 时间戳
    pub timestamp: u64,

    /// 策略名称
    pub strategy_name: String,

    /// 优先级（数值越大优先级越高）
    pub priority: i32,

    /// 备注
    pub note: String,
}

impl PositionTarget {
    pub fn new(symbol: String, target_quantity: Decimal, strategy_name: String) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            symbol,
            target_quantity,
            target_price: None,
            timestamp,
            strategy_name,
            priority: 0,
            note: String::new(),
        }
    }

    pub fn with_price(mut self, price: Decimal) -> Self {
        self.target_price = Some(price);
        self
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_note(mut self, note: String) -> Self {
        self.note = note;
        self
    }
}

/// 目标持仓信号
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionTargetSignal {
    /// 多个标的的目标持仓
    pub targets: Vec<PositionTarget>,

    /// 时间戳
    pub timestamp: u64,

    /// 策略名称
    pub strategy_name: String,
}

impl PositionTargetSignal {
    pub fn new(strategy_name: String) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            targets: Vec::new(),
            timestamp,
            strategy_name,
        }
    }

    pub fn add_target(mut self, target: PositionTarget) -> Self {
        self.targets.push(target);
        self
    }

    pub fn with_targets(mut self, targets: Vec<PositionTarget>) -> Self {
        self.targets = targets;
        self
    }
}
