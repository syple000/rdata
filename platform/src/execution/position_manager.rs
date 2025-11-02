use crate::models::Balance;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 持仓
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    /// 标的符号
    pub symbol: String,

    /// 持仓数量（正数表示做多，负数表示做空）
    pub quantity: Decimal,

    /// 平均成本
    pub avg_price: Decimal,

    /// 最后更新时间
    pub last_update: u64,
}

impl Position {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            quantity: Decimal::ZERO,
            avg_price: Decimal::ZERO,
            last_update: 0,
        }
    }

    /// 更新持仓
    pub fn update(&mut self, trade_qty: Decimal, trade_price: Decimal) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if self.quantity.is_zero() {
            // 新开仓
            self.quantity = trade_qty;
            self.avg_price = trade_price;
        } else if (self.quantity > Decimal::ZERO && trade_qty > Decimal::ZERO)
            || (self.quantity < Decimal::ZERO && trade_qty < Decimal::ZERO)
        {
            // 加仓
            let total_cost = self.quantity * self.avg_price + trade_qty * trade_price;
            self.quantity += trade_qty;
            self.avg_price = total_cost / self.quantity;
        } else {
            // 减仓或反向
            self.quantity += trade_qty;
            if self.quantity.is_zero() {
                self.avg_price = Decimal::ZERO;
            } else if (self.quantity > Decimal::ZERO && trade_qty < Decimal::ZERO)
                || (self.quantity < Decimal::ZERO && trade_qty > Decimal::ZERO)
            {
                // 反向开仓，重新计算成本
                self.avg_price = trade_price;
            }
        }

        self.last_update = now;
    }
}

/// 持仓管理器
pub struct PositionManager {
    /// 持仓映射 symbol -> Position
    positions: Arc<RwLock<HashMap<String, Position>>>,

    /// 账户余额
    balances: Arc<RwLock<HashMap<String, Balance>>>,
}

impl PositionManager {
    pub fn new() -> Self {
        Self {
            positions: Arc::new(RwLock::new(HashMap::new())),
            balances: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 更新持仓
    pub async fn update_position(&self, symbol: &str, trade_qty: Decimal, trade_price: Decimal) {
        let mut positions = self.positions.write().await;
        let position = positions
            .entry(symbol.to_string())
            .or_insert_with(|| Position::new(symbol.to_string()));
        position.update(trade_qty, trade_price);
    }

    /// 获取持仓
    pub async fn get_position(&self, symbol: &str) -> Option<Position> {
        let positions = self.positions.read().await;
        positions.get(symbol).cloned()
    }

    /// 获取所有持仓
    pub async fn get_all_positions(&self) -> Vec<Position> {
        let positions = self.positions.read().await;
        positions.values().cloned().collect()
    }

    /// 更新账户余额
    pub async fn update_balances(&self, balances: Vec<Balance>) {
        let mut balance_map = self.balances.write().await;
        balance_map.clear();
        for balance in balances {
            balance_map.insert(balance.asset.clone(), balance);
        }
    }

    /// 获取资产余额
    pub async fn get_balance(&self, asset: &str) -> Option<Balance> {
        let balances = self.balances.read().await;
        balances.get(asset).cloned()
    }

    /// 获取可用余额
    pub async fn get_available_balance(&self, asset: &str) -> Decimal {
        let balances = self.balances.read().await;
        balances.get(asset).map(|b| b.free).unwrap_or(Decimal::ZERO)
    }

    /// 计算目标持仓与当前持仓的差异
    pub async fn calculate_diff(&self, symbol: &str, target_qty: Decimal) -> Decimal {
        let current_qty = self
            .get_position(symbol)
            .await
            .map(|p| p.quantity)
            .unwrap_or(Decimal::ZERO);
        target_qty - current_qty
    }
}

impl Default for PositionManager {
    fn default() -> Self {
        Self::new()
    }
}
