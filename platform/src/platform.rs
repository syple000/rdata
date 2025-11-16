use crate::{
    config::PlatformConfig,
    data_manager::{
        market_data::MarketData, trade_data::TradeData, MarketDataManager, TradeDataManager,
    },
    errors::Result,
    market_provider::{BinanceSpotMarketProvider, MarketProvider},
    models::MarketType,
    trade_provider::{BinanceSpotTradeProvider, TradeProvider},
};
use std::{collections::HashMap, sync::Arc};

pub struct Platform {
    config: Arc<PlatformConfig>,

    market_providers: Option<Arc<HashMap<MarketType, Arc<dyn MarketProvider>>>>,
    trade_providers: Option<Arc<HashMap<MarketType, Arc<dyn TradeProvider>>>>,

    market_data_manager: Option<Arc<dyn MarketDataManager>>,
    trade_data_manager: Option<Arc<dyn TradeDataManager>>,
}

impl Platform {
    pub fn new(config: Arc<PlatformConfig>) -> Result<Self> {
        Ok(Self {
            config,
            market_providers: None,
            trade_providers: None,
            market_data_manager: None,
            trade_data_manager: None,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        let market_types: Vec<MarketType> = self.config.markets.clone();

        let mut market_providers: HashMap<MarketType, Arc<dyn MarketProvider>> = HashMap::new();
        let mut trade_providers: HashMap<MarketType, Arc<dyn TradeProvider>> = HashMap::new();
        for market_type in market_types {
            match market_type {
                MarketType::BinanceSpot => {
                    let mut market_provider = BinanceSpotMarketProvider::new(
                        self.config.configs[&MarketType::BinanceSpot].clone(),
                        self.config.proxy.clone(),
                    )?;
                    market_provider.init().await?;
                    market_providers.insert(
                        MarketType::BinanceSpot,
                        Arc::new(market_provider) as Arc<dyn MarketProvider>,
                    );

                    let mut trade_provider = BinanceSpotTradeProvider::new(
                        self.config.configs[&MarketType::BinanceSpot].clone(),
                        self.config.proxy.clone(),
                    )?;
                    trade_provider.init().await?;
                    trade_providers.insert(
                        MarketType::BinanceSpot,
                        Arc::new(trade_provider) as Arc<dyn TradeProvider>,
                    );
                }
            }
        }
        self.market_providers = Some(Arc::new(market_providers));
        self.trade_providers = Some(Arc::new(trade_providers));

        let market_data_manager = MarketData::new(
            self.config.clone(),
            self.market_providers.as_ref().unwrap().clone(),
        )?;
        market_data_manager.init().await?;

        let trade_data_manager = TradeData::new(
            self.config.clone(),
            self.trade_providers.as_ref().unwrap().clone(),
        )?;
        trade_data_manager.init().await?;

        self.market_data_manager = Some(Arc::new(market_data_manager));
        self.trade_data_manager = Some(Arc::new(trade_data_manager));

        Ok(())
    }
}
