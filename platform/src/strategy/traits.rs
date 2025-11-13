use std::sync::Arc;

use crate::{
    data_manager::{MarketDataManager, TradeDataManager},
    models::StrategyOutput,
};
use async_trait::async_trait;

#[async_trait]
pub trait Strategy: Send + Sync {
    fn get_name(&self) -> &str;

    async fn generate_signals(
        &self,
        market_data: Arc<dyn MarketDataManager>,
        trade_data: Arc<dyn TradeDataManager>,
    ) -> Vec<StrategyOutput>;
}
