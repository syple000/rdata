use crate::{
    data_manager::local_data_manager::LocalMarketDataManager, errors::Result, models::MarketType,
};
use async_trait::async_trait;

/// 具体的因子实现这个 trait，可以基于 kline、trade、depth 等任意数据计算
/// 返回 (因子值, 行情时间戳)
#[async_trait]
pub trait FactorCalculator {
    async fn calculate(
        &self,
        manager: &LocalMarketDataManager,
        market_type: &MarketType,
        symbol: &str,
    ) -> Result<(f64, u64)>;
}

/// 价格提供者 trait，允许用户自定义价格获取逻辑
/// 返回 (价格, 行情时间戳)
#[async_trait]
pub trait PriceProvider {
    async fn get_price(
        &self,
        manager: &LocalMarketDataManager,
        market_type: &MarketType,
        symbol: &str,
    ) -> Result<(f64, u64)>;
}
