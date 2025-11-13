use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StrategyOutput {
    symbol: String,
    target_pos: Decimal,
    confidence: f64, // 置信度
    comment: Option<String>,
}
