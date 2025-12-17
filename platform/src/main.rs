use std::{collections::HashMap, fs::OpenOptions, io::Write, sync::Arc};

use db::sqlite::SQLiteDB;
use env_logger::Env;
use platform::{
    backtest::factors::{
        factor_backtest::FactorBacktester,
        factor_calculators::{
            KlineFactorCalculators, KlineFactorType, TradeFactorCalculators, TradeFactorType,
        },
        price_providers::{KlineClosePriceProvider, TradePriceProvider},
        traits::{FactorCalculator, PriceProvider},
    },
    config::{Config, PlatformConfig},
    data_manager::{
        local_data_manager::{Clock, LocalMarketDataManager},
        MarketDataManager,
    },
    errors::PlatformError,
    market_dump::market_dump,
    market_provider::{BinanceSpotMarketProvider, MarketProvider},
    models::{KlineInterval, MarketType},
};

async fn market_dump_main(conf: &str) {
    let config = Config::from_toml(conf).unwrap();
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

async fn db_migration_main(conf: &str, args: &HashMap<String, String>) {
    let config = Config::from_toml(conf).unwrap();
    let platform_config = PlatformConfig::from_config(config).unwrap();
    let db = Arc::new(
        SQLiteDB::new(&platform_config.db_path)
            .map_err(|e| PlatformError::PlatformError {
                message: format!("Failed to open database: {}", e),
            })
            .expect("init db failed"),
    );
    let target_db_path = args
        .get("target_db_path")
        .map(String::as_str)
        .expect("target_db_path not found");
    let table = args
        .get("table")
        .map(String::as_str)
        .expect("table not found");
    // 1. attach
    db.execute_update(
        &format!("ATTACH DATABASE '{}' AS target_db;", target_db_path),
        &[],
    )
    .expect("attach database failed");
    // 2. copy
    db.begin_transaction().expect("begin transaction failed");
    db.execute_update(
        &format!(
            "CREATE TABLE target_db.{} AS SELECT * FROM {};",
            table, table
        ),
        &[],
    )
    .expect("copy table failed");
    db.execute_update(&format!("DROP TABLE {};", table), &[])
        .expect("drop table failed");
    db.commit_transaction().expect("commit transaction failed");
    // 3. detach and vacuum
    db.execute_update("DETACH DATABASE target_db;", &[])
        .expect("detach database failed");
    db.execute_update("VACUUM;", &[]).expect("vacuum failed");
}

async fn factor_backtest_main(conf: &str, args: &HashMap<String, String>) {
    let config = Config::from_toml(conf).unwrap();
    let platform_config = Arc::new(PlatformConfig::from_config(config).unwrap());

    let db = Arc::new(
        SQLiteDB::new(&platform_config.db_path)
            .map_err(|e| PlatformError::PlatformError {
                message: format!("Failed to open database: {}", e),
            })
            .expect("init db failed"),
    );

    let from_ts = args
        .get("from_ts")
        .and_then(|s| s.parse::<u64>().ok())
        .expect("from_ts not found");
    let to_ts = args
        .get("to_ts")
        .and_then(|s| s.parse::<u64>().ok())
        .expect("to_ts not found");
    let data_type = args.get("data_type").expect("data_type not found");
    let (calculator, price_provider) = match data_type.as_str() {
        "kline" => {
            let factor_type = args.get("factor_type").expect("factor_type not found");
            let factor_type =
                KlineFactorType::from_str(factor_type).expect("invalid kline_factor_type");
            let interval = args.get("interval").expect("interval not found");
            let interval = KlineInterval::from_str(interval).expect("invalid kline interval");
            let window_size = args
                .get("window_size")
                .and_then(|s| s.parse::<usize>().ok())
                .expect("window_size not found");
            let calculator =
                KlineFactorCalculators::new(factor_type, interval.clone(), window_size);
            let price_provider = KlineClosePriceProvider::new(interval.clone());
            (
                Arc::new(calculator) as Arc<dyn FactorCalculator>,
                Arc::new(price_provider) as Arc<dyn PriceProvider>,
            )
        }
        "trade" => {
            let factor_type = args.get("factor_type").expect("factor_type not found");
            let factor_type =
                TradeFactorType::from_str(factor_type).expect("invalid kline_factor_type");
            let window_size = args
                .get("window_size")
                .and_then(|s| s.parse::<usize>().ok())
                .expect("window_size not found");
            let calcutor = TradeFactorCalculators::new(factor_type, window_size);
            let price_providers = TradePriceProvider::new();
            (
                Arc::new(calcutor) as Arc<dyn FactorCalculator>,
                Arc::new(price_providers) as Arc<dyn PriceProvider>,
            )
        }
        _ => panic!("unsupported data_type"),
    };

    let clock = Arc::new(Clock::new(from_ts));
    let local_market_mgr = LocalMarketDataManager::new(platform_config, clock.clone(), db, 10000)
        .expect("new local data manager failed");
    local_market_mgr
        .init()
        .await
        .expect("init local data manager failed");
    let local_data_manager = Arc::new(local_market_mgr);

    let market_type = args
        .get("market_type")
        .and_then(|s| MarketType::from_str(s))
        .expect("market_type not found");
    let symbol = args.get("symbol").expect("symbol not found");
    let step_ms = args
        .get("step_ms")
        .and_then(|s| s.parse::<u64>().ok())
        .expect("step_ms not found");
    let forward_steps = args
        .get("forward_steps")
        .and_then(|s| s.parse::<usize>().ok())
        .expect("forward_steps not found");

    let factor_backtest = FactorBacktester::new(local_data_manager.clone(), clock.clone());
    let factor_records = factor_backtest
        .run_test(
            calculator.as_ref(),
            price_provider.as_ref(),
            market_type,
            symbol,
            from_ts,
            to_ts,
            step_ms,
            forward_steps,
        )
        .await
        .expect("run_test failed");
    let ic = factor_backtest.calculate_ic(&factor_records);
    let (ic_mean, ic_ir) = factor_backtest.calculate_ic_ir(&factor_records);
    log::info!(
        "factor backtest finished, records: {}, IC: {:.6}, IC Mean: {:.6}, IC IR: {:.6}",
        factor_records.len(),
        ic,
        ic_mean,
        ic_ir
    );
}

fn parse_args() -> HashMap<String, String> {
    let mut args_map = HashMap::new();
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i].starts_with("--") && i + 1 < args.len() {
            let key = args[i][2..].to_string();
            let value = args[i + 1].to_string();
            args_map.insert(key, value);
            i += 2;
        } else {
            i += 1;
        }
    }
    args_map
}

fn init_log(command: &str) {
    let log_file_path = format!("platform_{}.log", command);
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file_path)
        .expect("failed to open log file");

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format(move |buf, record| {
            let ts = buf.timestamp();
            writeln!(buf, "{} [{}] - {}", ts, record.level(), record.args())
        })
        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .init();

    log::info!("platform starting, logging to {}", log_file_path);
}

#[tokio::main]
pub async fn main() {
    let args: HashMap<String, String> = parse_args();

    match args.get("command").map(String::as_str) {
        Some("market_dump") => {
            init_log("market_dump");
            let conf = args
                .get("config")
                .map(String::as_str)
                .unwrap_or("conf/platform_conf.toml");
            market_dump_main(conf).await;
        }
        Some("database_migration") => {
            init_log("database_migration");
            let conf = args
                .get("config")
                .map(String::as_str)
                .unwrap_or("conf/platform_conf.toml");
            db_migration_main(conf, &args).await;
        }
        Some("factor_backtest") => {
            init_log("factor_backtest");
            let conf = args
                .get("config")
                .map(String::as_str)
                .unwrap_or("conf/platform_conf.toml");
            factor_backtest_main(conf, &args).await;
        }
        _ => {
            log::error!("invalid or missing command argument");
        }
    }
}
