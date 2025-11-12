use crate::binance::{
    errors::{BinanceError, Result},
    spot::{
        models::{ExecutionReport, OrderType, OutboundAccountPosition, TimeInForce},
        parser::{AccountUpdateRaw, CancelOrderStreamRaw, PlaceOrderStreamRaw},
        requests::{CancelOrderRequest, PlaceOrderRequest},
        responses::{CancelOrderResponse, PlaceOrderResponse},
    },
    utils::{encode_params, hmac_sha256, sort_params},
};
use log::error;
use rand::{distr::Alphanumeric, Rng};
use rate_limiter::RateLimiter;
use serde::Deserialize;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use tokio_util::sync::CancellationToken;
use ws::{RecvMsg, SendMsg};

type Fut = Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>;
pub struct TradeStream {
    url: String,
    proxy_url: Option<String>,
    rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    api_key: String,
    secret_key: String,

    execution_report_cb: Option<Arc<dyn Fn(ExecutionReport) -> Fut + Send + Sync + 'static>>,
    outbound_account_position_cb:
        Option<Arc<dyn Fn(OutboundAccountPosition) -> Fut + Send + Sync + 'static>>,

    client: Option<ws::Client>,
}

impl TradeStream {
    pub fn new(
        url: String,
        proxy_url: Option<String>,
        rate_limiters: Option<Arc<Vec<RateLimiter>>>,
        api_key: String,
        secret_key: String,
    ) -> Self {
        TradeStream {
            url,
            proxy_url,
            rate_limiters,
            api_key,
            secret_key,
            execution_report_cb: None,
            outbound_account_position_cb: None,
            client: None,
        }
    }

    pub fn register_execution_report_callback<F>(&mut self, cb: F)
    where
        F: Fn(ExecutionReport) -> Fut + Send + Sync + 'static,
    {
        self.execution_report_cb = Some(Arc::new(cb));
    }

    pub fn register_outbound_account_position_callback<F>(&mut self, cb: F)
    where
        F: Fn(OutboundAccountPosition) -> Fut + Send + Sync + 'static,
    {
        self.outbound_account_position_cb = Some(Arc::new(cb));
    }

    // 完成ws连接并订阅
    pub async fn init(&mut self) -> Result<CancellationToken> {
        let execution_report_cb = self.execution_report_cb.clone();
        let outbound_account_position_cb = self.outbound_account_position_cb.clone();
        let mut config = ws::Config::default(
            self.url.clone(),
            Arc::new(Self::calc_recv_msg_id),
            Arc::new(move |msg: RecvMsg| {
                let execution_report_cb = execution_report_cb.clone();
                let outbound_account_position_cb = outbound_account_position_cb.clone();
                Box::pin(async move {
                    Self::handle(msg, execution_report_cb, outbound_account_position_cb).await
                })
            }),
        );
        config.proxy_url = self.proxy_url.clone();
        config.rate_limiters = self.rate_limiters.clone();

        let mut ws_client = ws::Client::new(config).map_err(|e| {
            error!("WebSocket client error: {:?}", e);
            BinanceError::ExternalError(Box::new(e))
        })?;

        ws_client.connect().await.map_err(|e| {
            error!("WebSocket connect error: {:?}", e);
            BinanceError::NetworkError {
                message: e.to_string(),
            }
        })?;

        let msg_id = Self::rand_id();
        let mut params = vec![];
        self.sign_params(&mut params);
        let content = serde_json::to_string(&serde_json::json!({
            "id": msg_id,
            "method": "userDataStream.subscribe.signature",
            "params": params.into_iter().collect::<HashMap<_, _>>(),
        }))
        .unwrap();
        let recv_msg = ws_client
            .call(SendMsg::Text {
                msg_id: Some(msg_id),
                content,
                weight: None,
            })
            .await
            .map_err(|e| {
                error!("WebSocket send subscribe message error: {:?}", e);
                BinanceError::NetworkError {
                    message: e.to_string(),
                }
            })?;
        #[derive(Debug, Deserialize)]
        struct Resp {
            id: String,
            status: i32,
            result: Option<HashMap<String, serde_json::Value>>,
        }
        match recv_msg {
            RecvMsg::Text { msg_id: _, content } => {
                let resp = serde_json::from_str::<Resp>(&content).map_err(|e| {
                    error!("Parse subscribe response error: {:?}", e);
                    BinanceError::ParseResultError {
                        message: e.to_string(),
                    }
                })?;
                if resp.status != 200 {
                    return Err(BinanceError::NetworkError {
                        message: format!("Subscribe failed: {:?}", resp),
                    });
                }
            }
            _ => {
                return Err(BinanceError::NetworkError {
                    message: format!("Unexpected subscribe response: {:?}", recv_msg),
                });
            }
        }

        let shutdown_token = ws_client.get_shutdown_token();
        self.client = Some(ws_client);

        Ok(shutdown_token)
    }

    pub async fn get_ws_shutdown_token(&self) -> Option<CancellationToken> {
        match &self.client {
            None => None,
            Some(client) => Some(client.get_shutdown_token()),
        }
    }

    // 支持websocket下单/撤单等时间延迟敏感操作
    // 普通查询可以走API接口
    pub async fn place_order(&self, req: PlaceOrderRequest) -> Result<PlaceOrderResponse> {
        let client = match &self.client {
            Some(c) => c,
            None => {
                return Err(BinanceError::NetworkError {
                    message: "WebSocket client is not connected".to_string(),
                })
            }
        };

        match &req.r#type {
            OrderType::Limit => {
                if req.time_in_force.is_none() || req.price.is_none() || req.quantity.is_none() {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "time_in_force, price, quantity are required for LIMIT order"
                            .to_string(),
                    });
                }
            }
            OrderType::Market => {
                if req.quantity.is_none() {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "quantity is required for MARKET order".to_string(),
                    });
                }
            }
            OrderType::StopLoss => {
                if req.quantity.is_none() || req.stop_price.is_none() {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "quantity, stop_price are required for STOP_LOSS order"
                            .to_string(),
                    });
                }
            }
            OrderType::StopLossLimit => {
                if req.time_in_force.is_none()
                    || req.quantity.is_none()
                    || req.price.is_none()
                    || req.stop_price.is_none()
                {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "time_in_force, quantity, price, stop_price are required for STOP_LOSS_LIMIT order"
                            .to_string(),
                    });
                }
            }
            OrderType::TakeProfit => {
                if req.quantity.is_none() || req.stop_price.is_none() {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "quantity, stop_price are required for TAKE_PROFIT order"
                            .to_string(),
                    });
                }
            }
            OrderType::TakeProfitLimit => {
                if req.time_in_force.is_none()
                    || req.quantity.is_none()
                    || req.price.is_none()
                    || req.stop_price.is_none()
                {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "time_in_force, quantity, price, stop_price are required for TAKE_PROFIT_LIMIT order"
                            .to_string(),
                    });
                }
            }
            OrderType::LimitMaker => {
                if req.quantity.is_none() || req.price.is_none() {
                    return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                        message: "quantity, price are required for LIMIT_MAKER order".to_string(),
                    });
                }
            }
        }
        if req.iceberg_qty.is_some() {
            if !matches!(req.r#type, OrderType::Limit | OrderType::LimitMaker) {
                return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                    message: "iceberg_qty is only valid for LIMIT and LIMIT_MAKER orders"
                        .to_string(),
                });
            }
            if req.time_in_force.is_none()
                || !matches!(req.time_in_force.as_ref().unwrap(), TimeInForce::Gtc)
            {
                return Err(crate::binance::errors::BinanceError::ParametersInvalid {
                    message: "time_in_force must be GTC when iceberg_qty is set".to_string(),
                });
            }
        }

        let mut params = vec![
            ("symbol", req.symbol.clone()),
            ("side", req.side.as_str().to_string()),
            ("type", req.r#type.as_str().to_string()),
            ("newOrderRespType", "ACK".to_string()),
        ];
        if req.time_in_force.is_some() {
            params.push((
                "timeInForce",
                req.time_in_force.as_ref().unwrap().as_str().to_string(),
            ));
        }
        if req.quantity.is_some() {
            params.push(("quantity", req.quantity.unwrap().to_string()));
        }
        if req.price.is_some() {
            params.push(("price", req.price.unwrap().to_string()));
        }
        if req.new_client_order_id.is_some() {
            params.push((
                "newClientOrderId",
                req.new_client_order_id.as_ref().unwrap().to_string(),
            ));
        }
        if req.stop_price.is_some() {
            params.push(("stopPrice", req.stop_price.unwrap().to_string()));
        }
        if req.iceberg_qty.is_some() {
            params.push(("icebergQty", req.iceberg_qty.unwrap().to_string()));
        }

        self.sign_params(&mut params);

        let msg_id = Self::rand_id();
        let content = serde_json::to_string(&serde_json::json!(
            {
                "id": msg_id.clone(),
                "method": "order.place",
                "params": params.into_iter().collect::<HashMap<_, _>>(),
            }
        ))
        .unwrap();

        let recv_msg = client
            .call(SendMsg::Text {
                msg_id: Some(msg_id),
                content,
                weight: Some(1),
            })
            .await
            .map_err(|e| {
                error!("WebSocket send place order message error: {:?}", e);
                BinanceError::NetworkError {
                    message: e.to_string(),
                }
            })?;

        match recv_msg {
            RecvMsg::Text { msg_id: _, content } => {
                let resp = serde_json::from_str::<PlaceOrderStreamRaw>(&content).map_err(|e| {
                    error!("Parse place order response error: {:?}", e);
                    BinanceError::ParseResultError {
                        message: e.to_string(),
                    }
                })?;
                if resp.status != 200 {
                    return Err(BinanceError::NetworkError {
                        message: format!("Place order failed: {:?}", content),
                    });
                }
                if resp.result.is_none() {
                    return Err(BinanceError::NetworkError {
                        message: format!("Place order failed: {:?}", content),
                    });
                }
                Ok((req, resp.result.unwrap()).into())
            }
            _ => Err(BinanceError::NetworkError {
                message: format!("Unexpected place order response: {:?}", recv_msg),
            }),
        }
    }

    pub async fn cancel_order(&self, req: CancelOrderRequest) -> Result<CancelOrderResponse> {
        let client = match &self.client {
            Some(c) => c,
            None => {
                return Err(BinanceError::NetworkError {
                    message: "WebSocket client is not connected".to_string(),
                })
            }
        };

        // Validate parameters: either order_id or orig_client_order_id must be provided
        if req.order_id.is_none() && req.orig_client_order_id.is_none() {
            return Err(BinanceError::ParametersInvalid {
                message: "Either orderId or origClientOrderId must be provided".to_string(),
            });
        }

        let mut params = vec![("symbol", req.symbol.clone())];

        if let Some(order_id) = req.order_id {
            params.push(("orderId", order_id.to_string()));
        }

        if let Some(orig_client_order_id) = &req.orig_client_order_id {
            params.push(("origClientOrderId", orig_client_order_id.clone()));
        }

        if let Some(new_client_order_id) = &req.new_client_order_id {
            params.push(("newClientOrderId", new_client_order_id.clone()));
        }

        self.sign_params(&mut params);

        let msg_id = Self::rand_id();
        let content = serde_json::to_string(&serde_json::json!(
            {
                "id": msg_id.clone(),
                "method": "order.cancel",
                "params": params.into_iter().collect::<HashMap<_, _>>(),
            }
        ))
        .unwrap();

        let recv_msg = client
            .call(SendMsg::Text {
                msg_id: Some(msg_id),
                content,
                weight: Some(1),
            })
            .await
            .map_err(|e| {
                error!("WebSocket send cancel order message error: {:?}", e);
                BinanceError::NetworkError {
                    message: e.to_string(),
                }
            })?;

        match recv_msg {
            RecvMsg::Text { msg_id: _, content } => {
                let resp = serde_json::from_str::<CancelOrderStreamRaw>(&content).map_err(|e| {
                    error!("Parse cancel order response error: {:?}", e);
                    BinanceError::ParseResultError {
                        message: e.to_string(),
                    }
                })?;
                if resp.status != 200 {
                    return Err(BinanceError::NetworkError {
                        message: format!("Cancel order failed: {:?}", content),
                    });
                }
                Ok(())
            }
            _ => Err(BinanceError::NetworkError {
                message: format!("Unexpected cancel order response: {:?}", recv_msg),
            }),
        }
    }

    fn rand_id() -> String {
        rand::rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect()
    }

    fn calc_recv_msg_id(msg: &str) -> Option<String> {
        #[derive(Deserialize)]
        struct ID {
            id: String,
        }
        let id = serde_json::from_str::<ID>(msg).ok();
        if id.is_none() {
            return None;
        }
        let id = id.unwrap();
        Some(id.id)
    }

    async fn handle(
        msg: RecvMsg,
        execution_report_cb: Option<Arc<dyn Fn(ExecutionReport) -> Fut + Send + Sync + 'static>>,
        outbound_account_position_cb: Option<
            Arc<dyn Fn(OutboundAccountPosition) -> Fut + Send + Sync + 'static>,
        >,
    ) -> ws::Result<()> {
        let text = match msg {
            RecvMsg::Text { msg_id: _, content } => content,
            RecvMsg::Binary { msg_id: _, data } => std::str::from_utf8(&data)
                .map_err(|e| ws::WsError::HandleError {
                    message: e.to_string(),
                })?
                .to_string(),
            _ => {
                error!("Unsupported message: {:?}", msg);
                return Ok(());
            }
        };

        #[derive(Deserialize)]
        struct StreamMsg {
            event: AccountUpdateRaw,
        }

        let account_update = serde_json::from_str::<StreamMsg>(&text)
            .map_err(|e| ws::WsError::HandleError {
                message: format!("Parse message error: {:?}, msg: {}", e, text),
            })?
            .event;

        match account_update {
            AccountUpdateRaw::Unknown => {
                error!("unknown account update message: {}", text);
                return Ok(());
            }
            AccountUpdateRaw::OutboundAccountPosition { .. } => {
                if outbound_account_position_cb.is_none() {
                    return Ok(());
                }
                let outbound_account_position = account_update
                    .into_outbound_account_position()
                    .map_err(|e| ws::WsError::HandleError {
                        message: format!("Convert to OutboundAccountPosition error: {:?}", e),
                    })?;
                outbound_account_position_cb.unwrap()(outbound_account_position).await?;
                return Ok(());
            }
            AccountUpdateRaw::ExecutionReport { .. } => {
                if execution_report_cb.is_none() {
                    return Ok(());
                }
                let execution_report = account_update.into_execution_report().map_err(|e| {
                    ws::WsError::HandleError {
                        message: format!("Convert to ExecutionReport error: {:?}", e),
                    }
                })?;
                execution_report_cb.unwrap()(execution_report).await?;
                return Ok(());
            }
        }
    }

    fn sign_params(&self, params: &mut Vec<(&str, String)>) {
        params.push(("apiKey", self.api_key.clone()));
        params.push(("timestamp", time::get_current_milli_timestamp().to_string()));
        params.push(("recvWindow", "5000".to_string()));

        sort_params(params);

        // 添加签名
        let signature = hmac_sha256(&self.secret_key, encode_params(&params).as_str());
        params.push(("signature", signature));
    }
}
