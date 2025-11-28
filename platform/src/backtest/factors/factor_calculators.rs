use crate::{
    backtest::factors::traits::FactorCalculator,
    data_manager::{local_data_manager::LocalMarketDataManager, MarketDataManager},
    errors::Result,
    factors::{calc_kline_factors, calc_trade_factors},
    models::{KlineData, KlineInterval, MarketType, Trade},
};
use async_trait::async_trait;

pub enum KlineFactorType {
    PriceReturn,
    TrendStrength,
    PriceVolatility,
    PriceRange,
    PricePosition,
    AvgVolume,
    VolumeVolatility,
    VolumeTrend,
    OBV,
    PriceVolumeCorrelation,
    AvgIntradayRange,
    AvgBodyRatio,
}

impl KlineFactorType {
    pub fn from_str(factor_name: &str) -> Option<Self> {
        match factor_name {
            "PriceReturn" => Some(KlineFactorType::PriceReturn),
            "TrendStrength" => Some(KlineFactorType::TrendStrength),
            "PriceVolatility" => Some(KlineFactorType::PriceVolatility),
            "PriceRange" => Some(KlineFactorType::PriceRange),
            "PricePosition" => Some(KlineFactorType::PricePosition),
            "AvgVolume" => Some(KlineFactorType::AvgVolume),
            "VolumeVolatility" => Some(KlineFactorType::VolumeVolatility),
            "VolumeTrend" => Some(KlineFactorType::VolumeTrend),
            "OBV" => Some(KlineFactorType::OBV),
            "PriceVolumeCorrelation" => Some(KlineFactorType::PriceVolumeCorrelation),
            "AvgIntradayRange" => Some(KlineFactorType::AvgIntradayRange),
            "AvgBodyRatio" => Some(KlineFactorType::AvgBodyRatio),
            _ => None,
        }
    }
}

pub struct KlineFactorCalculators {
    pub factor_type: KlineFactorType,
    pub interval: KlineInterval,
    pub window_size: usize,
}

impl KlineFactorCalculators {
    pub fn new(factor_type: KlineFactorType, interval: KlineInterval, window_size: usize) -> Self {
        Self {
            factor_type,
            interval,
            window_size,
        }
    }
}

#[async_trait]
impl FactorCalculator for KlineFactorCalculators {
    async fn calculate(
        &self,
        manager: &LocalMarketDataManager,
        market_type: &MarketType,
        symbol: &str,
    ) -> Result<(f64, u64)> {
        let klines: Vec<KlineData> = manager
            .get_klines(
                market_type,
                &symbol.to_string(),
                &self.interval,
                Some(self.window_size),
            )
            .await?;
        if klines.len() < self.window_size {
            log::warn!(
                "Not enough klines for factor calculation: have {}, need {}",
                klines.len(),
                self.window_size
            );
            return Err(crate::errors::PlatformError::FactorError {
                message: format!(
                    "Not enough klines for factor calculation: have {}, need {}",
                    klines.len(),
                    self.window_size
                ),
            });
        }
        let start_open_time = klines.first().unwrap().open_time;
        let end_open_time = klines.last().unwrap().open_time;
        let close_time = klines.last().unwrap().close_time;
        if (end_open_time - start_open_time) / self.interval.to_millis()
            != self.window_size as u64 - 1
        {
            log::warn!(
                "Klines are not continuous for factor calculation: start {}, end {}, expected window size {}",
                start_open_time,
                end_open_time,
                self.window_size
            );
            return Err(crate::errors::PlatformError::FactorError {
                message: format!(
                    "Klines are not continuous for factor calculation: start {}, end {}, expected window size {}",
                    start_open_time,
                    end_open_time,
                    self.window_size
                ),
            });
        }

        let factors = calc_kline_factors(&klines)?;
        let factor_value = match self.factor_type {
            KlineFactorType::PriceReturn => factors.price_return,
            KlineFactorType::TrendStrength => factors.trend_strength,
            KlineFactorType::PriceVolatility => factors.price_volatility,
            KlineFactorType::PriceRange => factors.price_range,
            KlineFactorType::PricePosition => factors.price_position,
            KlineFactorType::AvgVolume => factors.avg_volume,
            KlineFactorType::VolumeVolatility => factors.volume_volatility,
            KlineFactorType::VolumeTrend => factors.volume_trend,
            KlineFactorType::OBV => factors.obv,
            KlineFactorType::PriceVolumeCorrelation => factors.price_volume_correlation,
            KlineFactorType::AvgIntradayRange => factors.avg_intraday_range,
            KlineFactorType::AvgBodyRatio => factors.avg_body_ratio,
        };
        Ok((factor_value, close_time))
    }
}

pub enum TradeFactorType {
    PriceReturn,
    TrendStrength,
    PriceVolatility,
    PriceRange,
    PriceAcceleration,
    PricePosition,
    AvgVol,
    VolVolatility,
    VolSkew,
    LargeTradeRatio,
    VolTrend,
    BuyCount,
    SellCount,
    TradeImbalance,
    BuyVol,
    SellVol,
    VolImbalance,
    NetBuyRatio,
    AvgTradeSizeRatio,
    Vwap,
    PriceVwapDeviation,
    VwapSlope,
    OBV,
    PriceVolumeCorrelation,
    TradeFrequency,
    AvgTradeInterval,
    TradeIntervalStd,
}

impl TradeFactorType {
    pub fn from_str(factor_name: &str) -> Option<Self> {
        match factor_name {
            "PriceReturn" => Some(TradeFactorType::PriceReturn),
            "TrendStrength" => Some(TradeFactorType::TrendStrength),
            "PriceVolatility" => Some(TradeFactorType::PriceVolatility),
            "PriceRange" => Some(TradeFactorType::PriceRange),
            "PriceAcceleration" => Some(TradeFactorType::PriceAcceleration),
            "PricePosition" => Some(TradeFactorType::PricePosition),
            "AvgVol" => Some(TradeFactorType::AvgVol),
            "VolVolatility" => Some(TradeFactorType::VolVolatility),
            "VolSkew" => Some(TradeFactorType::VolSkew),
            "LargeTradeRatio" => Some(TradeFactorType::LargeTradeRatio),
            "VolTrend" => Some(TradeFactorType::VolTrend),
            "BuyCount" => Some(TradeFactorType::BuyCount),
            "SellCount" => Some(TradeFactorType::SellCount),
            "TradeImbalance" => Some(TradeFactorType::TradeImbalance),
            "BuyVol" => Some(TradeFactorType::BuyVol),
            "SellVol" => Some(TradeFactorType::SellVol),
            "VolImbalance" => Some(TradeFactorType::VolImbalance),
            "NetBuyRatio" => Some(TradeFactorType::NetBuyRatio),
            "AvgTradeSizeRatio" => Some(TradeFactorType::AvgTradeSizeRatio),
            "Vwap" => Some(TradeFactorType::Vwap),
            "PriceVwapDeviation" => Some(TradeFactorType::PriceVwapDeviation),
            "VwapSlope" => Some(TradeFactorType::VwapSlope),
            "OBV" => Some(TradeFactorType::OBV),
            "PriceVolumeCorrelation" => Some(TradeFactorType::PriceVolumeCorrelation),
            "TradeFrequency" => Some(TradeFactorType::TradeFrequency),
            "AvgTradeInterval" => Some(TradeFactorType::AvgTradeInterval),
            "TradeIntervalStd" => Some(TradeFactorType::TradeIntervalStd),
            _ => None,
        }
    }
}

pub struct TradeFactorCalculators {
    pub factor_type: TradeFactorType,
    pub window_size: usize,
}

impl TradeFactorCalculators {
    pub fn new(factor_type: TradeFactorType, window_size: usize) -> Self {
        Self {
            factor_type,
            window_size,
        }
    }
}

#[async_trait]
impl FactorCalculator for TradeFactorCalculators {
    async fn calculate(
        &self,
        manager: &LocalMarketDataManager,
        market_type: &MarketType,
        symbol: &str,
    ) -> Result<(f64, u64)> {
        let trades: Vec<Trade> = manager
            .get_trades(market_type, &symbol.to_string(), Some(self.window_size))
            .await?;
        if trades.len() < self.window_size {
            log::warn!(
                "Not enough trades for factor calculation: have {}, need {}",
                trades.len(),
                self.window_size
            );
            return Err(crate::errors::PlatformError::FactorError {
                message: format!(
                    "Not enough trades for factor calculation: have {}, need {}",
                    trades.len(),
                    self.window_size
                ),
            });
        }
        let start_id = trades.first().unwrap().seq_id;
        let end_id = trades.last().unwrap().seq_id;
        if (end_id - start_id) / (self.window_size as u64 - 1) > 60_000 {
            log::warn!(
                "Trades are not continuous for factor calculation: start {}, end {}, expected window size {}",
                start_id,
                end_id,
                self.window_size
            );
            return Err(crate::errors::PlatformError::FactorError {
                message: format!(
                    "Trades are not continuous for factor calculation: start {}, end {}, expected window size {}",
                    start_id,
                    end_id,
                    self.window_size
                ),
            });
        }
        let factors = calc_trade_factors(&trades)?;
        let factor_value = match self.factor_type {
            TradeFactorType::PriceReturn => factors.price_return,
            TradeFactorType::TrendStrength => factors.trend_strength,
            TradeFactorType::PriceVolatility => factors.price_volatility,
            TradeFactorType::PriceRange => factors.price_range,
            TradeFactorType::PriceAcceleration => factors.price_acceleration,
            TradeFactorType::PricePosition => factors.price_position,
            TradeFactorType::AvgVol => factors.avg_vol,
            TradeFactorType::VolVolatility => factors.vol_volatility,
            TradeFactorType::VolSkew => factors.vol_skew,
            TradeFactorType::LargeTradeRatio => factors.large_trade_ratio,
            TradeFactorType::VolTrend => factors.vol_trend,
            TradeFactorType::BuyCount => factors.buy_count as f64,
            TradeFactorType::SellCount => factors.sell_count as f64,
            TradeFactorType::TradeImbalance => factors.trade_imbalance,
            TradeFactorType::BuyVol => factors.buy_vol,
            TradeFactorType::SellVol => factors.sell_vol,
            TradeFactorType::VolImbalance => factors.vol_imbalance,
            TradeFactorType::NetBuyRatio => factors.net_buy_ratio,
            TradeFactorType::AvgTradeSizeRatio => factors.avg_trade_size_ratio,
            TradeFactorType::Vwap => factors.vwap,
            TradeFactorType::PriceVwapDeviation => factors.price_vwap_deviation,
            TradeFactorType::VwapSlope => factors.vwap_slope,
            TradeFactorType::OBV => factors.obv,
            TradeFactorType::PriceVolumeCorrelation => factors.price_volume_correlation,
            TradeFactorType::TradeFrequency => factors.trade_frequency,
            TradeFactorType::AvgTradeInterval => factors.avg_trade_interval,
            TradeFactorType::TradeIntervalStd => factors.trade_interval_std,
        };
        let market_timestamp = trades.last().unwrap().timestamp;
        Ok((factor_value, market_timestamp))
    }
}
