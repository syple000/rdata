use crate::{
    errors::{PlatformError, Result},
    models::{ExchangeInfo, OrderSide, SymbolInfo},
};
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 订单验证器
/// 负责验证订单的合法性
pub struct OrderValidator {
    exchange_info: Arc<RwLock<Option<ExchangeInfo>>>,
}

impl OrderValidator {
    pub fn new() -> Self {
        Self {
            exchange_info: Arc::new(RwLock::new(None)),
        }
    }

    /// 更新交易所信息
    pub async fn update_exchange_info(&self, info: ExchangeInfo) {
        *self.exchange_info.write().await = Some(info);
    }

    /// 验证订单
    pub async fn validate(
        &self,
        symbol: &str,
        side: &OrderSide,
        quantity: Decimal,
        price: Option<Decimal>,
        available_balance: Decimal,
    ) -> Result<()> {
        let exchange_info = self.exchange_info.read().await;

        if exchange_info.is_none() {
            return Err(PlatformError::ExecutionError {
                message: "Exchange info not available".to_string(),
            });
        }

        let info = exchange_info.as_ref().unwrap();
        let symbol_info = info
            .symbols
            .iter()
            .find(|s| s.symbol == symbol)
            .ok_or_else(|| PlatformError::ExecutionError {
                message: format!("Symbol '{}' not found", symbol),
            })?;

        // 验证交易对状态
        if symbol_info.status != crate::models::SymbolStatus::Trading {
            return Err(PlatformError::ExecutionError {
                message: format!("Symbol '{}' is not trading", symbol),
            });
        }

        // 验证数量
        self.validate_quantity(symbol_info, quantity)?;

        // 验证价格
        if let Some(p) = price {
            self.validate_price(symbol_info, p)?;
        }

        // 验证余额
        self.validate_balance(symbol_info, side, quantity, price, available_balance)?;

        Ok(())
    }

    /// 验证数量
    fn validate_quantity(&self, symbol_info: &SymbolInfo, quantity: Decimal) -> Result<()> {
        if let Some(min_qty) = symbol_info.min_quantity {
            if quantity < min_qty {
                return Err(PlatformError::ExecutionError {
                    message: format!("Quantity {} is below minimum {}", quantity, min_qty),
                });
            }
        }

        if let Some(max_qty) = symbol_info.max_quantity {
            if quantity > max_qty {
                return Err(PlatformError::ExecutionError {
                    message: format!("Quantity {} exceeds maximum {}", quantity, max_qty),
                });
            }
        }

        if let Some(step_size) = symbol_info.quantity_step_size {
            if !step_size.is_zero() {
                let remainder = quantity % step_size;
                if !remainder.is_zero() {
                    return Err(PlatformError::ExecutionError {
                        message: format!(
                            "Quantity {} does not match step size {}",
                            quantity, step_size
                        ),
                    });
                }
            }
        }

        Ok(())
    }

    /// 验证价格
    fn validate_price(&self, symbol_info: &SymbolInfo, price: Decimal) -> Result<()> {
        if let Some(min_price) = symbol_info.min_price {
            if price < min_price {
                return Err(PlatformError::ExecutionError {
                    message: format!("Price {} is below minimum {}", price, min_price),
                });
            }
        }

        if let Some(max_price) = symbol_info.max_price {
            if price > max_price {
                return Err(PlatformError::ExecutionError {
                    message: format!("Price {} exceeds maximum {}", price, max_price),
                });
            }
        }

        if let Some(tick_size) = symbol_info.price_tick_size {
            if !tick_size.is_zero() {
                let remainder = price % tick_size;
                if !remainder.is_zero() {
                    return Err(PlatformError::ExecutionError {
                        message: format!("Price {} does not match tick size {}", price, tick_size),
                    });
                }
            }
        }

        Ok(())
    }

    /// 验证余额
    fn validate_balance(
        &self,
        symbol_info: &SymbolInfo,
        side: &OrderSide,
        quantity: Decimal,
        price: Option<Decimal>,
        available_balance: Decimal,
    ) -> Result<()> {
        match side {
            OrderSide::Buy => {
                // 买入需要计算所需的报价资产数量
                let required = if let Some(p) = price {
                    quantity * p
                } else {
                    // 市价单，无法精确计算，这里简单检查是否有余额
                    if available_balance.is_zero() {
                        return Err(PlatformError::ExecutionError {
                            message: "Insufficient balance for market buy order".to_string(),
                        });
                    }
                    return Ok(());
                };

                if let Some(min_notional) = symbol_info.min_notional {
                    if required < min_notional {
                        return Err(PlatformError::ExecutionError {
                            message: format!(
                                "Order value {} is below minimum notional {}",
                                required, min_notional
                            ),
                        });
                    }
                }

                if required > available_balance {
                    return Err(PlatformError::ExecutionError {
                        message: format!(
                            "Insufficient balance: required {}, available {}",
                            required, available_balance
                        ),
                    });
                }
            }
            OrderSide::Sell => {
                // 卖出需要检查基础资产余额
                if quantity > available_balance {
                    return Err(PlatformError::ExecutionError {
                        message: format!(
                            "Insufficient balance: required {}, available {}",
                            quantity, available_balance
                        ),
                    });
                }
            }
        }

        Ok(())
    }
}

impl Default for OrderValidator {
    fn default() -> Self {
        Self::new()
    }
}
