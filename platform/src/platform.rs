use crate::{
    config::Config,
    data_manager::{
        market_data::MarketData, trade_data::TradeData, MarketDataManager, TradeDataManager,
    },
    errors::{PlatformError, Result},
    market_provider::{BinanceSpotMarketProvider, MarketProvider},
    models::{MarketType, StrategyOutput},
    trade_provider::{BinanceSpotTradeProvider, TradeProvider},
};
use db::sqlite::SQLiteDB;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct Platform {
    config: Arc<Config>,

    market_providers: Option<Arc<HashMap<MarketType, Arc<dyn MarketProvider>>>>,
    trade_providers: Option<Arc<HashMap<MarketType, Arc<dyn TradeProvider>>>>,

    market_data_manager: Option<Arc<dyn MarketDataManager>>,
    trade_data_manager: Option<Arc<dyn TradeDataManager>>,

    db: Option<Arc<SQLiteDB>>,
    strategy_output_sender: Option<mpsc::Sender<StrategyOutput>>,
    strategy_output_receiver: Option<mpsc::Receiver<StrategyOutput>>,

    shutdown_token: CancellationToken,
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
            strategy_output_sender: None,
            strategy_output_receiver: None,
            shutdown_token: CancellationToken::new(),
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

        let (sender, receiver) = mpsc::channel(100);
        self.strategy_output_sender = Some(sender);
        self.strategy_output_receiver = Some(receiver);

        // 读取策略，并在协程中启动（当前只考虑单策略）
        // 多策略扩展依赖trade data注册维护虚拟账户，也需要平台执行检测冲突
        // 暂时规避复杂度，做单策略

        // 轮询策略输出通道，写入数据库，风控，执行交易

        Ok(())
    }

    fn _create_strategy_output_table(&self) -> Result<()> {
        // 创建策略输出数据表
        Ok(())
    }

    pub async fn run_strategy() -> Result<()> {
        // 根据策略ID，读取策略配置，启动策略协程

        Ok(())
    }
}
