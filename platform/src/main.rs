use std::{collections::HashMap, fs::OpenOptions, io::Write, sync::Arc};

use env_logger::Env;
use platform::{
    config::{Config, PlatformConfig},
    market_dump::market_dump,
    market_provider::{BinanceSpotMarketProvider, MarketProvider},
    models::MarketType,
};

#[tokio::main]
pub async fn main() {
    let log_file_path = "platform.log";
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file_path)
        .expect("failed to open log file");

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format(move |buf, record| {
            let ts = buf.timestamp();
            writeln!(buf, "{} [{}] - {}", ts, record.level(), record.args())
        })
        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .init();

    log::info!("platform starting, logging to {}", log_file_path);

    let config = Config::from_toml("conf/platform_conf.toml").unwrap();
    let platform_config = PlatformConfig::from_config(config).unwrap();
    let mut symbols: HashMap<MarketType, Vec<String>> = HashMap::new();
    let mut market_providers: HashMap<MarketType, Arc<dyn MarketProvider>> = HashMap::new();
    for market_type in platform_config.markets.iter() {
        let market_config = platform_config.configs[market_type].clone();
        let mut market_provider =
            BinanceSpotMarketProvider::new(market_config.clone(), platform_config.proxy.clone())
                .unwrap();
        market_provider.init().await.unwrap();

        log::info!(
            "init market provider: {:?}, symbols: {:?}",
            market_type,
            market_config.subscribed_symbols
        );

        symbols.insert(
            market_type.clone(),
            market_config.subscribed_symbols.clone(),
        );
        market_providers.insert(market_type.clone(), Arc::new(market_provider));
    }
    if let Err(e) = market_dump(symbols, market_providers, &platform_config.db_path).await {
        log::error!("market_dump error: {:?}", e);
    } else {
        log::info!("market_dump finished successfully");
    }
}
