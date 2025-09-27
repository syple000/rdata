use log::{error, info};
use rand::{distr::Alphanumeric, Rng};
use rate_limiter::RateLimiter;
use serde::Deserialize;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use tokio_util::sync::CancellationToken;
use ws::{RecvMsg, SendMsg};

use crate::binance::{
    errors::{BinanceError, Result},
    spot::models::{AccountUpdateData, ExecutionReport},
    utils::{encode_params, hmac_sha256, sort_params},
};

type Fut = Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>;
pub struct TradeStream {
    url: String,
    proxy_url: Option<String>,
    rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    api_key: String,
    secret_key: String,

    execution_report_cb: Option<Arc<dyn Fn(ExecutionReport) -> Fut + Send + Sync + 'static>>,
    account_update_cb: Option<Arc<dyn Fn(AccountUpdateData) -> Fut + Send + Sync + 'static>>,

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
            account_update_cb: None,
            client: None,
        }
    }

    pub fn register_execution_report_callback<F>(&mut self, cb: F)
    where
        F: Fn(ExecutionReport) -> Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        self.execution_report_cb = Some(Arc::new(cb));
    }

    pub fn register_outbound_account_position_callback<F>(&mut self, cb: F)
    where
        F: Fn(AccountUpdateData) -> Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        self.account_update_cb = Some(Arc::new(cb));
    }

    // 完成ws连接并订阅
    pub async fn init(&mut self) -> Result<CancellationToken> {
        let execution_report_cb = self.execution_report_cb.clone();
        let account_update_cb = self.account_update_cb.clone();
        let mut config = ws::Config::default(
            self.url.clone(),
            Arc::new(Self::calc_recv_msg_id),
            Arc::new(move |msg: RecvMsg| {
                let execution_report_cb = execution_report_cb.clone();
                let account_update_cb = account_update_cb.clone();
                Box::pin(
                    async move { Self::handle(msg, execution_report_cb, account_update_cb).await },
                )
            }),
        );
        config.proxy_url = self.proxy_url.clone();
        config.rate_limiters = self.rate_limiters.clone();

        let ws_client = ws::Client::new(config).map_err(|e| {
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
        let mut params = vec![
            ("apiKey", self.api_key.clone()),
            ("timestamp", time::get_current_milli_timestamp().to_string()),
            ("recvWindow", "5000".to_string()),
        ];
        sort_params(&mut params);
        let signature = hmac_sha256(&self.secret_key, encode_params(&params).as_ref());
        params.push(("signature", signature));
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
            result: HashMap<String, serde_json::Value>,
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

        let shutdown_token = ws_client.get_shutdown_token().await.unwrap();
        self.client = Some(ws_client);

        Ok(shutdown_token)
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
        _execution_report_cb: Option<Arc<dyn Fn(ExecutionReport) -> Fut + Send + Sync + 'static>>,
        _account_update_cb: Option<Arc<dyn Fn(AccountUpdateData) -> Fut + Send + Sync + 'static>>,
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

        info!("handle msg: {}", text);
        Ok(())
    }
}
