use crate::{
    config::Config,
    data_manager::{
        market_data::MarketData, trade_data::TradeData, MarketDataManager, TradeDataManager,
    },
    errors::{PlatformError, Result},
    market_provider::{BinanceSpotMarketProvider, MarketProvider},
    models::MarketType,
    trade_provider::{BinanceSpotTradeProvider, TradeProvider},
};
use db::sqlite::SQLiteDB;
use std::{collections::HashMap, sync::Arc};

pub struct Platform {
    config: Arc<Config>,

    market_providers: Option<Arc<HashMap<MarketType, Arc<dyn MarketProvider>>>>,
    trade_providers: Option<Arc<HashMap<MarketType, Arc<dyn TradeProvider>>>>,

    market_data_manager: Option<Arc<dyn MarketDataManager>>,
    trade_data_manager: Option<Arc<dyn TradeDataManager>>,

    db: Option<Arc<SQLiteDB>>,
}

impl Platform {
    pub fn new(config: Arc<Config>) -> Result<Self> {
        Ok(Self {
            config,
            market_providers: None,
            trade_providers: None,
            market_data_manager: None,
            trade_data_manager: None,
            db: None,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        let market_types: Vec<MarketType> =
            self.config
                .get("markets")
                .map_err(|e| PlatformError::ConfigError {
                    message: format!("Failed to get market types: {}", e),
                })?;

        let mut market_providers: HashMap<MarketType, Arc<dyn MarketProvider>> = HashMap::new();
        let mut trade_providers: HashMap<MarketType, Arc<dyn TradeProvider>> = HashMap::new();
        for market_type in market_types {
            match market_type {
                MarketType::BinanceSpot => {
                    let mut market_provider = BinanceSpotMarketProvider::new(self.config.clone())?;
                    market_provider.init().await?;
                    market_providers.insert(
                        MarketType::BinanceSpot,
                        Arc::new(market_provider) as Arc<dyn MarketProvider>,
                    );

                    let mut trade_provider = BinanceSpotTradeProvider::new(self.config.clone())?;
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

        let db_path: String =
            self.config
                .get("db_path")
                .map_err(|e| PlatformError::ConfigError {
                    message: format!("db_path not found: {}", e),
                })?;
        self.db = Some(Arc::new(SQLiteDB::new(&db_path).map_err(|e| {
            PlatformError::PlatformError {
                message: format!("connect db: {} err: {}", db_path, e),
            }
        })?));

        Ok(())
    }
}
