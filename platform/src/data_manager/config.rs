use std::time::Duration;

/// 数据管理器配置
#[derive(Debug, Clone)]
pub struct DataManagerConfig {
    /// Kline 数据缓存配置
    pub kline_cache_config: KlineCacheConfig,

    /// Trade 数据缓存配置
    pub trade_cache_config: TradeCacheConfig,

    /// 订单缓存配置
    pub order_cache_config: OrderCacheConfig,

    /// 用户成交缓存配置
    pub user_trade_cache_config: UserTradeCacheConfig,

    /// 同步配置
    pub sync_config: SyncConfig,
}

#[derive(Debug, Clone)]
pub struct KlineCacheConfig {
    /// 每个 symbol 和 interval 组合保留的最大条数
    pub max_count: usize,
}

#[derive(Debug, Clone)]
pub struct TradeCacheConfig {
    /// 每个 symbol 保留的最大条数
    pub max_count: usize,
}

#[derive(Debug, Clone)]
pub struct OrderCacheConfig {
    /// 保留的最大订单数
    pub max_count: usize,

    /// 保留的最大时间范围
    pub max_duration: Duration,
}

#[derive(Debug, Clone)]
pub struct UserTradeCacheConfig {
    /// 保留的最大成交数
    pub max_count: usize,

    /// 保留的最大时间范围
    pub max_duration: Duration,
}

#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// 行情数据间断多久后触发 API 同步
    pub market_gap_threshold: Duration,

    /// Kline 跳跃时间阈值
    pub kline_jump_threshold: Duration,

    /// Trade 跳跃时间阈值
    pub trade_jump_threshold: Duration,

    /// 账户数据主动同步间隔
    pub account_sync_interval: Duration,
}

impl Default for DataManagerConfig {
    fn default() -> Self {
        Self {
            kline_cache_config: KlineCacheConfig { max_count: 1000 },
            trade_cache_config: TradeCacheConfig { max_count: 500 },
            order_cache_config: OrderCacheConfig {
                max_count: 100,
                max_duration: Duration::from_secs(24 * 3600), // 24 hours
            },
            user_trade_cache_config: UserTradeCacheConfig {
                max_count: 500,
                max_duration: Duration::from_secs(7 * 24 * 3600), // 7 days
            },
            sync_config: SyncConfig {
                market_gap_threshold: Duration::from_secs(60), // 1 minute
                kline_jump_threshold: Duration::from_secs(300), // 5 minutes
                trade_jump_threshold: Duration::from_secs(300), // 5 minutes
                account_sync_interval: Duration::from_secs(60), // 1 minute
            },
        }
    }
}
