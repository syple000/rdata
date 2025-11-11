use crate::{
    errors::Result,
    models::{
        Account, AccountUpdate, CancelOrderRequest, DepthData, KlineData, KlineInterval,
        MarketType, Order, PlaceOrderRequest, Ticker24hr, Trade, UserTrade,
    },
};
use async_trait::async_trait;

#[async_trait]
pub trait MarketDataManager: Send + Sync {
    async fn init(&self) -> Result<()>;

    /// 添加 K线数据
    async fn add_kline(
        &self,
        market_type: &MarketType,
        kline: KlineData,
    ) -> Result<Option<KlineData>>;

    /// 获取 K线数据
    async fn get_klines(
        &self,
        market_type: &MarketType,
        symbol: &String,
        interval: &KlineInterval,
        limit: Option<usize>,
    ) -> Result<Vec<KlineData>>;

    /// 添加交易数据
    async fn add_trade(&self, market_type: &MarketType, trade: Trade) -> Result<Option<Trade>>;

    /// 获取交易数据
    async fn get_trades(
        &self,
        market_type: &MarketType,
        symbol: &String,
        limit: Option<usize>,
    ) -> Result<Vec<Trade>>;

    /// 添加深度数据
    async fn add_depth(&self, market_type: &MarketType, depth: DepthData) -> Result<()>;

    /// 获取深度数据
    async fn get_depth(
        &self,
        market_type: &MarketType,
        symbol: &String,
    ) -> Result<Option<DepthData>>;

    /// 添加 Ticker 数据
    async fn add_ticker(&self, market_type: &MarketType, ticker: Ticker24hr) -> Result<()>;

    /// 获取 Ticker 数据
    async fn get_ticker(
        &self,
        market_type: &MarketType,
        symbol: &String,
    ) -> Result<Option<Ticker24hr>>;
}

#[async_trait]
pub trait TradeDataManager: Send + Sync {
    async fn init(&self) -> Result<()>;

    /// 更新订单信息
    async fn update_order(&self, market_type: &MarketType, order: Order) -> Result<()>;

    /// 更新用户交易信息
    async fn update_user_trade(&self, market_type: &MarketType, trade: UserTrade) -> Result<()>;

    /// 更新账户信息
    async fn update_account(&self, market_type: &MarketType, account: Account) -> Result<()>;

    /// 更新账户变更信息
    async fn update_account_update(
        &self,
        market_type: &MarketType,
        account_update: AccountUpdate,
    ) -> Result<()>;

    /// 获取账户信息
    async fn get_account(&self, market_type: &MarketType) -> Result<Option<Account>>;

    /// 获取所有在途订单
    async fn get_open_orders(&self, market_type: &MarketType) -> Result<Vec<Order>>;

    /// 获取用户交易记录
    async fn get_user_trades(&self, market_type: &MarketType) -> Result<Vec<UserTrade>>;

    /// 根据订单ID获取交易记录
    async fn get_user_trades_by_order(
        &self,
        market_type: &MarketType,
        order_id: &str,
    ) -> Result<Vec<UserTrade>>;

    /// 下单
    async fn place_order(&self, market_type: &MarketType, req: PlaceOrderRequest) -> Result<Order>;

    /// 撤单
    async fn cancel_order(&self, market_type: &MarketType, req: CancelOrderRequest) -> Result<()>;

    /// 从数据库获取账户信息
    async fn get_account_from_db(&self, market_type: &MarketType) -> Result<Option<Account>>;

    /// 从数据库获取订单列表
    async fn get_orders_from_db(
        &self,
        market_type: &MarketType,
        symbol: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Order>>;

    /// 从数据库获取用户交易记录
    async fn get_user_trades_from_db(
        &self,
        market_type: &MarketType,
        symbol: &str,
        limit: Option<usize>,
    ) -> Result<Vec<UserTrade>>;

    /// 从数据库获取最后同步时间戳
    async fn get_last_sync_ts_from_db(&self, market_type: &MarketType) -> Result<Option<u64>>;

    /// 从数据库根据订单ID获取订单
    async fn get_order_by_id_from_db(
        &self,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<Order>>;

    /// 从数据库获取在途订单
    async fn get_open_orders_from_db(&self, market_type: &MarketType) -> Result<Vec<Order>>;

    /// 从数据库根据订单ID获取用户交易记录
    async fn get_user_trades_by_order_id_from_db(
        &self,
        market_type: &MarketType,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<UserTrade>>;
}
