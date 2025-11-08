use crate::{
    config::Config,
    errors::{PlatformError, Result},
    models::{
        Account, AccountUpdate, CancelOrderRequest, GetAllOrdersRequest, GetOpenOrdersRequest,
        GetOrderRequest, GetUserTradesRequest, Order, PlaceOrderRequest, UserTrade,
    },
    trade_provider::TradeProvider,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use exchange::binance::spot::{
    requests::{self},
    trade_api::TradeApi,
    trade_stream::TradeStream,
};
use log::error;
use rate_limiter::RateLimiter;
use std::{sync::Arc, time::Duration};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use ws::WsError;

pub struct BinanceSpotTradeProvider {
    config: Arc<Config>,
    api_rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    stream_rate_limiters: Option<Arc<Vec<RateLimiter>>>,

    trade_api: Option<Arc<TradeApi>>,
    trade_stream: Option<Arc<ArcSwap<TradeStream>>>,

    order_sender: broadcast::Sender<Order>,
    order_receiver: broadcast::Receiver<Order>,
    user_trade_sender: broadcast::Sender<UserTrade>,
    user_trade_receiver: broadcast::Receiver<UserTrade>,
    account_update_sender: broadcast::Sender<AccountUpdate>,
    account_update_receiver: broadcast::Receiver<AccountUpdate>,

    shutdown_token: CancellationToken,
}

impl BinanceSpotTradeProvider {
    pub fn new(config: Arc<Config>) -> Result<Self> {
        let api_rate_limits: Option<Vec<(u64, u64)>> =
            config.get("binance.spot.api_rate_limits").ok();
        let api_rate_limiters = api_rate_limits.map(|limits| {
            Arc::new(
                limits
                    .into_iter()
                    .map(|(duration, limit)| {
                        RateLimiter::new(Duration::from_millis(duration), limit)
                    })
                    .collect::<Vec<_>>(),
            )
        });

        let stream_rate_limits: Option<Vec<(u64, u64)>> =
            config.get("binance.spot.stream_api_rate_limits").ok();
        let stream_rate_limiters = stream_rate_limits.map(|limits| {
            Arc::new(
                limits
                    .into_iter()
                    .map(|(duration, limit)| {
                        RateLimiter::new(Duration::from_millis(duration), limit)
                    })
                    .collect::<Vec<_>>(),
            )
        });

        let order_chan_cap = config
            .get("binance.spot.order_event_channel_capacity")
            .unwrap_or(5000);
        let user_trade_chan_cap = config
            .get("binance.spot.user_trade_event_channel_capacity")
            .unwrap_or(5000);
        let account_chan_cap = config
            .get("binance.spot.account_event_channel_capacity")
            .unwrap_or(5000);

        let (order_sender, order_receiver) = broadcast::channel(order_chan_cap);
        let (user_trade_sender, user_trade_receiver) = broadcast::channel(user_trade_chan_cap);
        let (account_sender, account_receiver) = broadcast::channel(account_chan_cap);

        Ok(Self {
            config,
            api_rate_limiters,
            stream_rate_limiters,
            trade_api: None,
            trade_stream: None,
            order_sender,
            order_receiver,
            user_trade_sender,
            user_trade_receiver,
            account_update_sender: account_sender,
            account_update_receiver: account_receiver,
            shutdown_token: CancellationToken::new(),
        })
    }
}

fn create_trade_api(
    config: Arc<Config>,
    rate_limiters: Option<Arc<Vec<RateLimiter>>>,
) -> Result<TradeApi> {
    let base_url: String =
        config
            .get("binance.spot.api_base_url")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get api_base_url: {}", e),
            })?;
    let proxy_url: Option<String> = config.get("proxy.url").ok();
    let timeout_milli_secs: u64 =
        config
            .get("binance.spot.api_timeout_milli_secs")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get api_timeout_milli_secs: {}", e),
            })?;
    let api_key: String =
        config
            .get("binance.spot.api_key")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get api_key: {}", e),
            })?;
    let secret_key: String =
        config
            .get("binance.spot.secret_key")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get secret_key: {}", e),
            })?;

    let mut trade_api = TradeApi::new(
        base_url,
        proxy_url,
        rate_limiters,
        api_key,
        secret_key,
        timeout_milli_secs,
    );
    trade_api
        .init()
        .map_err(|e| PlatformError::TradeProviderError {
            message: format!("Failed to init trade_api: {}", e),
        })?;

    Ok(trade_api)
}

async fn create_trade_stream(
    config: Arc<Config>,
    rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    order_sender: broadcast::Sender<Order>,
    user_trade_sender: broadcast::Sender<UserTrade>,
    account_update_sender: broadcast::Sender<AccountUpdate>,
) -> Result<TradeStream> {
    let stream_api_base_url: String =
        config
            .get("binance.spot.stream_api_base_url")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get stream_api_base_url: {}", e),
            })?;
    let proxy_url: Option<String> = config.get("proxy.url").ok();
    let api_key: String =
        config
            .get("binance.spot.api_key")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get api_key: {}", e),
            })?;
    let secret_key: String =
        config
            .get("binance.spot.secret_key")
            .map_err(|e| PlatformError::ConfigError {
                message: format!("Failed to get secret_key: {}", e),
            })?;

    let mut trade_stream = TradeStream::new(
        stream_api_base_url,
        proxy_url,
        rate_limiters,
        api_key,
        secret_key,
    );

    trade_stream.register_execution_report_callback(move |execution_report| {
        let order_sender = order_sender.clone();
        let user_trade_sender = user_trade_sender.clone();
        Box::pin(async move {
            let order = execution_report.to_order().into();
            let _ = order_sender.send(order).map_err(|e| WsError::HandleError {
                message: format!("Failed to send order event: {}", e),
            })?;

            if let Some(user_trade) = execution_report.to_trade() {
                let user_trade = user_trade.into();
                let _ = user_trade_sender
                    .send(user_trade)
                    .map_err(|e| WsError::HandleError {
                        message: format!("Failed to send user trade event: {}", e),
                    })?;
            }

            Ok(())
        })
    });

    trade_stream.register_outbound_account_position_callback(move |update| {
        let account_sender = account_update_sender.clone();
        Box::pin(async move {
            account_sender
                .send(update.into())
                .map_err(|e| WsError::HandleError {
                    message: format!("Failed to send account position update: {}", e),
                })?;
            Ok(())
        })
    });

    let _ = trade_stream
        .init()
        .await
        .map_err(|e| PlatformError::TradeProviderError {
            message: format!("Failed to init trade_stream: {}", e),
        })?;

    Ok(trade_stream)
}

#[async_trait]
impl TradeProvider for BinanceSpotTradeProvider {
    async fn init(&mut self) -> Result<()> {
        let trade_api = Arc::new(create_trade_api(
            self.config.clone(),
            self.api_rate_limiters.clone(),
        )?);

        let trade_stream = create_trade_stream(
            self.config.clone(),
            self.stream_rate_limiters.clone(),
            self.order_sender.clone(),
            self.user_trade_sender.clone(),
            self.account_update_sender.clone(),
        )
        .await?;

        self.trade_api = Some(trade_api);
        self.trade_stream = Some(Arc::new(ArcSwap::from_pointee(trade_stream)));

        let shutdown_token = self.shutdown_token.clone();
        let trade_stream = self.trade_stream.as_ref().unwrap().clone();
        let config = self.config.clone();
        let stream_rate_limiters = self.stream_rate_limiters.clone();
        let order_sender = self.order_sender.clone();
        let user_trade_sender = self.user_trade_sender.clone();
        let account_update_sender = self.account_update_sender.clone();
        tokio::spawn(async move {
            let retry_interval = config
                .get::<u64>("binance.spot.stream_reconnect_interval_milli_secs")
                .unwrap_or(3000);
            let mut latest_retry_ts = 0u64;
            loop {
                let stream_shutdown_token = trade_stream
                    .load_full()
                    .get_ws_shutdown_token()
                    .await
                    .unwrap();
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        break;
                    },
                    _ = stream_shutdown_token.cancelled() => {
                        let now = time::get_current_milli_timestamp();
                        if now - latest_retry_ts < retry_interval {
                            tokio::time::sleep(Duration::from_millis(retry_interval - (now - latest_retry_ts))).await;
                        }
                        latest_retry_ts = now;
                        let new_stream = create_trade_stream(
                            config.clone(),
                            stream_rate_limiters.clone(),
                            order_sender.clone(),
                            user_trade_sender.clone(),
                            account_update_sender.clone(),
                        ).await;
                        match new_stream {
                            Ok(stream) => {
                                trade_stream.store(Arc::new(stream));
                            },
                            Err(e) => {
                                error!("Failed to recreate trade stream: {}", e);
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn place_order(&self, req: PlaceOrderRequest) -> Result<Order> {
        // 优先从stream下单，如果stream的状态不可用，回退到API下单
        let (stream, ok) = match &self.trade_stream {
            None => (None, false),
            Some(stream_arc) => {
                let stream = stream_arc.load_full();
                if let Some(token) = stream.get_ws_shutdown_token().await {
                    if token.is_cancelled() {
                        (None, false)
                    } else {
                        (Some(stream), true)
                    }
                } else {
                    (None, false)
                }
            }
        };

        if ok {
            match stream.unwrap().place_order(req.into()).await {
                Ok(response) => Ok(response.into()),
                Err(e) => Err(PlatformError::TradeProviderError {
                    message: format!("Failed to place order via stream: {}", e),
                }),
            }
        } else {
            match &self.trade_api {
                None => Err(PlatformError::TradeProviderError {
                    message: "Trade API not initialized".to_string(),
                }),
                Some(api) => api
                    .place_order(req.into())
                    .await
                    .map(|o| o.into())
                    .map_err(|e| PlatformError::TradeProviderError {
                        message: format!("Failed to place order via API: {}", e),
                    }),
            }
        }
    }

    async fn cancel_order(&self, req: CancelOrderRequest) -> Result<()> {
        let (stream, ok) = match &self.trade_stream {
            None => (None, false),
            Some(stream_arc) => {
                let stream = stream_arc.load_full();
                if let Some(token) = stream.get_ws_shutdown_token().await {
                    if token.is_cancelled() {
                        (None, false)
                    } else {
                        (Some(stream), true)
                    }
                } else {
                    (None, false)
                }
            }
        };

        if ok {
            match stream.unwrap().cancel_order(req.into()).await {
                Ok(response) => Ok(response.into()),
                Err(e) => Err(PlatformError::TradeProviderError {
                    message: format!("Failed to place order via stream: {}", e),
                }),
            }
        } else {
            match &self.trade_api {
                None => Err(PlatformError::TradeProviderError {
                    message: "Trade API not initialized".to_string(),
                }),
                Some(api) => api
                    .cancel_order(req.into())
                    .await
                    .map(|o| o.into())
                    .map_err(|e| PlatformError::TradeProviderError {
                        message: format!("Failed to place order via API: {}", e),
                    }),
            }
        }
    }

    async fn get_order(&self, req: GetOrderRequest) -> Result<Order> {
        let api = self
            .trade_api
            .as_ref()
            .ok_or(PlatformError::TradeProviderError {
                message: "Trade API not initialized".to_string(),
            })?;

        let order =
            api.get_order(req.into())
                .await
                .map_err(|e| PlatformError::TradeProviderError {
                    message: format!("Failed to get order: {}", e),
                })?;

        Ok(order.into())
    }

    async fn get_open_orders(&self, req: GetOpenOrdersRequest) -> Result<Vec<Order>> {
        let api = self
            .trade_api
            .as_ref()
            .ok_or(PlatformError::TradeProviderError {
                message: "Trade API not initialized".to_string(),
            })?;

        let orders = api.get_open_orders(req.into()).await.map_err(|e| {
            PlatformError::TradeProviderError {
                message: format!("Failed to get open orders: {}", e),
            }
        })?;

        Ok(orders.into_iter().map(|o| o.into()).collect())
    }

    async fn get_all_orders(&self, req: GetAllOrdersRequest) -> Result<Vec<Order>> {
        let api = self
            .trade_api
            .as_ref()
            .ok_or(PlatformError::TradeProviderError {
                message: "Trade API not initialized".to_string(),
            })?;

        let orders = api.get_all_orders(req.into()).await.map_err(|e| {
            PlatformError::TradeProviderError {
                message: format!("Failed to get all orders: {}", e),
            }
        })?;

        Ok(orders.into_iter().map(|o| o.into()).collect())
    }

    async fn get_user_trades(&self, req: GetUserTradesRequest) -> Result<Vec<UserTrade>> {
        let api = self
            .trade_api
            .as_ref()
            .ok_or(PlatformError::TradeProviderError {
                message: "Trade API not initialized".to_string(),
            })?;

        let trades =
            api.get_trades(req.into())
                .await
                .map_err(|e| PlatformError::TradeProviderError {
                    message: format!("Failed to get user trades: {}", e),
                })?;

        Ok(trades.into_iter().map(|t| t.into()).collect())
    }

    async fn get_account(&self) -> Result<Account> {
        let api = self
            .trade_api
            .as_ref()
            .ok_or(PlatformError::TradeProviderError {
                message: "Trade API not initialized".to_string(),
            })?;

        let account = api
            .get_account(requests::GetAccountRequest {})
            .await
            .map_err(|e| PlatformError::TradeProviderError {
                message: format!("Failed to get account: {}", e),
            })?;

        Ok(account.into())
    }

    fn subscribe_order(&self) -> broadcast::Receiver<Order> {
        self.order_receiver.resubscribe()
    }

    fn subscribe_user_trade(&self) -> broadcast::Receiver<UserTrade> {
        self.user_trade_receiver.resubscribe()
    }

    fn subscribe_account_update(&self) -> broadcast::Receiver<AccountUpdate> {
        self.account_update_receiver.resubscribe()
    }
}

impl Drop for BinanceSpotTradeProvider {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
    }
}
