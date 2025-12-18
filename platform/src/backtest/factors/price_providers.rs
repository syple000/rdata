use crate::{
    backtest::factors::traits::PriceProvider,
    data_manager::{local_data_manager::LocalMarketDataManager, MarketDataManager},
    errors::Result,
    models::{KlineData, KlineInterval, MarketType, Trade},
};
use async_trait::async_trait;

pub struct KlineClosePriceProvider {
    interval: KlineInterval,
}

impl KlineClosePriceProvider {
    pub fn new(interval: KlineInterval) -> Self {
        Self { interval }
    }
}

#[async_trait]
impl PriceProvider for KlineClosePriceProvider {
    async fn get_price(
        &self,
        manager: &LocalMarketDataManager,
        market_type: &MarketType,
        symbol: &str,
    ) -> Result<(f64, u64)> {
        let klines: Vec<KlineData> = manager
            .get_klines(market_type, &symbol.to_string(), &self.interval, Some(1))
            .await?;
        if klines.is_empty() {
            return Err(crate::errors::PlatformError::PlatformError {
                message: format!(
                    "No klines found for symbol {} at interval {:?}",
                    symbol, self.interval
                ),
            });
        }
        let last_kline = klines.last().unwrap();
        let price = last_kline
            .close
            .to_string()
            .parse::<f64>()
            .unwrap_or(f64::NAN);
        let market_timestamp = last_kline.open_time;

        Ok((price, market_timestamp))
    }
}

pub struct TradePriceProvider;

impl TradePriceProvider {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl PriceProvider for TradePriceProvider {
    async fn get_price(
        &self,
        manager: &LocalMarketDataManager,
        market_type: &MarketType,
        symbol: &str,
    ) -> Result<(f64, u64)> {
        let trades: Vec<Trade> = manager
            .get_trades(market_type, &symbol.to_string(), Some(1))
            .await?;
        if trades.is_empty() {
            log::warn!(
                "No trades found for symbol {} when getting trade price",
                symbol
            );
            return Err(crate::errors::PlatformError::PlatformError {
                message: format!("No trades found for symbol {}", symbol),
            });
        }
        let last_trade = trades.last().unwrap();
        let price = last_trade
            .price
            .to_string()
            .parse::<f64>()
            .unwrap_or(f64::NAN);
        let market_timestamp = last_trade.timestamp;

        Ok((price, market_timestamp))
    }
}
