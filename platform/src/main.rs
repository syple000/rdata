use std::{collections::HashMap, sync::Arc};

use platform::{
    config::{Config, PlatformConfig},
    market_dump::market_dump,
    market_provider::{BinanceSpotMarketProvider, MarketProvider},
    models::MarketType,
};

#[tokio::main]
pub async fn main() {
    let config = Config::from_toml("platform_conf.toml").unwrap();
    let platform_config = PlatformConfig::from_config(config).unwrap();
    let mut symbols: HashMap<MarketType, Vec<String>> = HashMap::new();
    let mut market_providers: HashMap<MarketType, Arc<dyn MarketProvider>> = HashMap::new();
    for market_type in platform_config.markets.iter() {
        let market_config = platform_config.configs[market_type].clone();
        let mut market_provider =
            BinanceSpotMarketProvider::new(market_config.clone(), platform_config.proxy.clone())
                .unwrap();
        market_provider.init().await.unwrap();
        symbols.insert(
            market_type.clone(),
            market_config.subscribed_symbols.clone(),
        );
        market_providers.insert(market_type.clone(), Arc::new(market_provider));
    }
    market_dump(symbols, market_providers, &platform_config.db_path)
        .await
        .unwrap();
}
