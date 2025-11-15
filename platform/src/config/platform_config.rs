use crate::config::Config;
use crate::models::KlineInterval;
use crate::{
    errors::{PlatformError, Result},
    models::MarketType,
};
use rate_limiter::RateLimiter;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};

#[derive(Clone, Serialize, Deserialize)]
pub struct Proxy {
    pub url: String,
}

fn default_cache_capacity() -> usize {
    1000
}

fn default_market_refresh_interval_secs() -> u64 {
    300
}

fn default_trade_refresh_interval_secs() -> u64 {
    300
}

fn default_channel_capacity() -> usize {
    5000
}

fn default_api_timeout_milli_secs() -> u64 {
    30000
}

fn default_reconnect_interval_milli_secs() -> u64 {
    5000
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MarketConfig {
    #[serde(default = "default_cache_capacity")]
    pub cache_capacity: usize, // 行情缓存容量

    #[serde(default = "default_market_refresh_interval_secs")]
    pub market_refresh_interval_secs: u64, // 行情刷新间隔（秒）
    #[serde(default = "default_trade_refresh_interval_secs")]
    pub trade_refresh_interval_secs: u64, // 交易数据刷新间隔（秒）

    pub api_base_url: String,
    pub stream_base_url: String,
    pub stream_api_base_url: String,

    pub api_key: String,
    pub secret_key: String,

    pub subscribed_symbols: Vec<String>,
    pub subscribed_kline_intervals: Vec<KlineInterval>,

    // rate_limiter全局配置，在config中完成初始化
    pub api_rate_limits: Option<Vec<(u64, u64)>>,
    pub stream_rate_limits: Option<Vec<(u64, u64)>>,
    pub stream_api_rate_limits: Option<Vec<(u64, u64)>>,

    #[serde(skip)]
    pub api_rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    #[serde(skip)]
    pub stream_rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    #[serde(skip)]
    pub stream_api_rate_limiters: Option<Arc<Vec<RateLimiter>>>,

    #[serde(default = "default_channel_capacity")]
    pub kline_event_channel_capacity: usize,
    #[serde(default = "default_channel_capacity")]
    pub trade_event_channel_capacity: usize,
    #[serde(default = "default_channel_capacity")]
    pub depth_event_channel_capacity: usize,
    #[serde(default = "default_channel_capacity")]
    pub ticker_event_channel_capacity: usize,
    #[serde(default = "default_channel_capacity")]
    pub depth_cache_channel_capacity: usize,

    #[serde(default = "default_channel_capacity")]
    pub order_event_channel_capacity: usize,
    #[serde(default = "default_channel_capacity")]
    pub user_trade_event_channel_capacity: usize,
    #[serde(default = "default_channel_capacity")]
    pub account_event_channel_capacity: usize,

    #[serde(default = "default_api_timeout_milli_secs")]
    pub api_timeout_milli_secs: u64,
    #[serde(default = "default_reconnect_interval_milli_secs")]
    pub stream_reconnect_interval_milli_secs: u64,
    #[serde(default = "default_reconnect_interval_milli_secs")]
    pub stream_api_reconnect_interval_milli_secs: u64,
}

pub struct PlatformConfig {
    pub markets: Vec<MarketType>,
    pub proxy: Option<Proxy>,
    pub db_path: String,
    pub configs: HashMap<MarketType, Arc<MarketConfig>>,
}

impl PlatformConfig {
    pub fn from_config(config: Config) -> Result<Self> {
        let markets: Vec<MarketType> =
            config
                .get("markets")
                .map_err(|e| PlatformError::ConfigError {
                    message: format!("get markets err: {}", e),
                })?;
        let proxy: Option<Proxy> = config.get("proxy").unwrap_or(None);
        let db_path: String = config
            .get("db_path")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("get db_path err: {}", e),
            })?;
        let mut configs: HashMap<MarketType, Arc<MarketConfig>> = HashMap::new();
        for market_type in &markets {
            let mut market_config: MarketConfig =
                config
                    .get(market_type.as_str())
                    .map_err(|e| PlatformError::ConfigError {
                        message: format!("get market_config for {:?} err: {}", market_type, e),
                    })?;
            market_config.api_rate_limiters = match &market_config.api_rate_limits {
                Some(limits) => Some(Arc::new(
                    limits
                        .iter()
                        .map(|(duration, max_weight)| {
                            RateLimiter::new(Duration::from_millis(*duration), *max_weight)
                        })
                        .collect(),
                )),
                None => None,
            };
            market_config.stream_rate_limiters = match &market_config.stream_rate_limits {
                Some(limits) => Some(Arc::new(
                    limits
                        .iter()
                        .map(|(duration, max_weight)| {
                            RateLimiter::new(Duration::from_millis(*duration), *max_weight)
                        })
                        .collect(),
                )),
                None => None,
            };
            market_config.stream_api_rate_limiters = match &market_config.stream_api_rate_limits {
                Some(limits) => Some(Arc::new(
                    limits
                        .iter()
                        .map(|(duration, max_weight)| {
                            RateLimiter::new(Duration::from_millis(*duration), *max_weight)
                        })
                        .collect(),
                )),
                None => None,
            };
            configs.insert(market_type.clone(), Arc::new(market_config));
        }
        Ok(Self {
            markets,
            proxy,
            db_path,
            configs,
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_platform_config() {
        let config_content = r#"
    {
        "markets": ["binance_spot"],
        "db_path": "test_db_path",
        "binance_spot": {
            "cache_capacity": 100,
            "market_refresh_interval_secs": 30,
            "trade_refresh_interval_secs": 5,
            "api_base_url": "https://testnet.binance.vision",
            "stream_base_url": "wss://stream.binance.com:9443/stream",
            "stream_api_base_url": "wss://ws-api.testnet.binance.vision/ws-api/v3",
            "api_key": "GMh8WTFiTiRPpbt1EFwYaDEunKN9gJy9qgRyYF8irvSYCdgjYcIaACDeyfKFOMcq",
            "secret_key": "NgIxnbabjf6cTnPYZpyVDAP7UoVNm3wzhJcLh89FYWSA5SkXJlCZD0yDCQcA4R33",
            "subscribed_symbols": ["BTCUSDT", "ETHUSDT"],
            "subscribed_kline_intervals": ["1m", "5m"],
            "api_rate_limits": [[1000, 500], [60000, 5000]],
            "stream_rate_limits": [[1000, 500]],
            "stream_api_rate_limits": [[1000, 500]],
            "kline_event_channel_capacity": 5000,
            "trade_event_channel_capacity": 5000,
            "depth_event_channel_capacity": 5000,
            "ticker_event_channel_capacity": 5000,
            "depth_cache_channel_capacity": 5000,
            "order_event_channel_capacity": 5000,
            "user_trade_event_channel_capacity": 5000,
            "account_event_channel_capacity": 5000,
            "stream_reconnect_interval_milli_secs": 3000,
            "stream_api_reconnect_interval_milli_secs": 3000,
            "api_timeout_milli_secs": 30000
        },
        "proxy": {
            "url": "socks5://127.0.0.1:10808"
        }
    }
    "#;
        let mut config_file = NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut config_file, config_content.as_bytes()).unwrap();
        let config_path = config_file.path().to_str().unwrap();
        let config = Config::from_json(config_path).unwrap();

        let platform_config = PlatformConfig::from_config(config);
        assert!(platform_config.is_ok());
    }
}
