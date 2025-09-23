use super::super::errors::*;
use super::super::utils::*;
use super::models::market::*;
use super::parser::*;
use super::requests::market::*;
use super::responses::market::*;
use log::error;
use log::info;
use rand::distr::Alphanumeric;
use rand::Rng;
use rate_limiter::RateLimiter;
use serde::Deserialize;
use std::char;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use ws::RecvMsg;
use ws::SendMsg;

pub struct Market {
    client: reqwest::Client,
    base_url: String,
    rate_limiters: Option<Arc<Vec<RateLimiter>>>,
}

impl Market {
    pub fn new(base_url: String, rate_limiters: Option<Arc<Vec<RateLimiter>>>) -> Self {
        Market {
            client: reqwest::Client::new(),
            base_url,
            rate_limiters: rate_limiters,
        }
    }

    pub async fn get_klines(&self, req: GetKlinesRequest) -> Result<GetKlinesResponse> {
        let mut start_time = req.start_time.unwrap_or(0);
        let end_time = req.end_time.unwrap_or(0);
        let batch_size = req.limit.unwrap_or(1000) as u32;

        if start_time > 0 && start_time >= end_time {
            return Err(BinanceError::ParametersInvalid {
                message: format!(
                    "start_time: {} must be less than end_time: {}",
                    start_time, end_time
                ),
            });
        }

        let mut klines = Vec::<KlineData>::new();
        loop {
            info!(
                "get kline with parameters start_time: {}, end_time: {}, limit: {}",
                start_time, end_time, batch_size
            );
            let mut params = vec![
                ("symbol", req.symbol.clone()),
                ("interval", req.interval.as_str().to_string()),
                ("limit", batch_size.to_string()),
            ];
            if start_time > 0 {
                params.push(("startTime", start_time.to_string()));
                params.push(("endTime", end_time.to_string()));
            }
            sort_params(&mut params);

            if let Some(rate_limiters) = &self.rate_limiters {
                for rl in rate_limiters.iter() {
                    _ = rl.wait(2).await;
                }
            }

            let text = self
                .client
                .get(format!("{}/api/v3/klines", self.base_url))
                .query(&params)
                .send()
                .await
                .map_err(|e| {
                    error!("Network error: {:?}", e);
                    BinanceError::NetworkError {
                        message: e.to_string(),
                    }
                })?
                .text()
                .await
                .map_err(|e| {
                    error!("Network error: {:?}", e);
                    BinanceError::NetworkError {
                        message: e.to_string(),
                    }
                })?;

            let mut batch_klines = parse_klines(req.symbol.clone(), &text).map_err(|e| {
                error!("Parse result: {:?} error: {:?}", text, e);
                BinanceError::ParseResultError {
                    message: e.to_string(),
                }
            })?;
            batch_klines.sort_by(|a, b| a.open_time.cmp(&b.open_time));

            if start_time > 0 {
                let mut index = batch_klines.len();
                for (i, kline) in batch_klines.iter().enumerate() {
                    if kline.open_time > end_time {
                        index = i;
                        break;
                    }
                }
                batch_klines.truncate(index);
            }

            klines.extend_from_slice(&batch_klines);

            if batch_klines.len() < batch_size as usize {
                break;
            }
            if start_time == 0 {
                break;
            }
            start_time = batch_klines.last().unwrap().close_time + 1;
        }

        Ok(GetKlinesResponse { klines: klines })
    }
}

#[derive(Clone)]
enum MarketStreamType {
    UpdateDepth,
    AggTrade,
}

#[derive(Clone)]
struct SubDetail {
    stream_type: MarketStreamType,
    symbol: String,
}

pub struct MarketStream {
    url: String,
    rate_limiters: Option<Arc<Vec<RateLimiter>>>,

    // 订阅 & 回调
    sub_details: HashMap<String, SubDetail>,
    update_depth_cb: Option<
        Arc<
            dyn Fn(DepthUpdate) -> Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>
                + Send
                + Sync
                + 'static,
        >,
    >,

    // ws客户端
    client: Mutex<Option<ws::Client>>,
}

impl MarketStream {
    // 订阅/注册回调等请在初始化前完成
    pub fn new(url: String, rate_limiters: Option<Arc<Vec<RateLimiter>>>) -> Self {
        MarketStream {
            url,
            rate_limiters,
            sub_details: HashMap::new(),
            update_depth_cb: None,
            client: Mutex::new(None),
        }
    }

    pub fn subscribe_depth_update(&mut self, symbol: &str) {
        self.sub_details.insert(
            format!("{}@depth@100ms", symbol.to_lowercase()),
            SubDetail {
                stream_type: MarketStreamType::UpdateDepth,
                symbol: symbol.to_string(),
            },
        );
    }

    pub fn register_depth_update_callback<F>(&mut self, cb: F)
    where
        F: Fn(DepthUpdate) -> Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        self.update_depth_cb = Some(Arc::new(cb));
    }

    // 完成ws连接并订阅
    pub async fn init(&self) -> Result<()> {
        let sub_details = Arc::new(self.sub_details.clone());
        let update_depth_cb = self.update_depth_cb.clone();
        let mut config = ws::Config::default(
            self.url.clone(),
            Arc::new(Self::calc_recv_msg_id),
            Arc::new(move |msg: RecvMsg| {
                let sub_details = sub_details.clone();
                let update_depth_cb = update_depth_cb.clone();
                Box::pin(async move { Self::handle(msg, sub_details, update_depth_cb).await })
            }),
        );
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

        // 发生订阅消息
        let msg_id = Self::rand_id();
        let recv_msg = ws_client
            .call(SendMsg::Text {
                msg_id: Some(msg_id.clone()),
                content: serde_json::to_string(&serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": self.sub_details.keys().collect::<Vec<&String>>(),
                    "id": msg_id,
                }))
                .unwrap(),
                weight: None,
            })
            .await
            .map_err(|e| {
                error!("WebSocket send subscribe message error: {:?}", e);
                BinanceError::NetworkError {
                    message: e.to_string(),
                }
            });
        if recv_msg.is_err() {
            return Err(BinanceError::NetworkError {
                message: format!("send subscribe message failed: {:?}", recv_msg.err()),
            });
        }

        let mut client_guard = self.client.lock().await;
        *client_guard = Some(ws_client);

        Ok(())
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
        sub_details: Arc<HashMap<String, SubDetail>>,
        update_depth_cb: Option<
            Arc<
                dyn Fn(DepthUpdate) -> Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
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

        info!("Received message: {}", text);

        #[derive(Deserialize)]
        struct StreamMsg<'a> {
            stream: String,
            #[serde(borrow)]
            data: Box<&'a serde_json::value::RawValue>,
        }
        let stream_msg =
            serde_json::from_str::<StreamMsg>(&text).map_err(|e| ws::WsError::HandleError {
                message: e.to_string(),
            })?;
        if !sub_details.contains_key(stream_msg.stream.as_str()) {
            error!("Unknown stream: {}", stream_msg.stream);
            return Err(ws::WsError::HandleError {
                message: format!("Unknown stream: {}", stream_msg.stream),
            });
        }
        let sub_detail = sub_details.get(stream_msg.stream.as_str()).unwrap();
        match sub_detail.stream_type {
            MarketStreamType::UpdateDepth => {
                let depth_update =
                    parse_depth_update(sub_detail.symbol.clone(), stream_msg.data.get()).map_err(
                        |e| ws::WsError::HandleError {
                            message: e.to_string(),
                        },
                    )?;
                if let Some(cb) = update_depth_cb {
                    cb(depth_update)
                        .await
                        .map_err(|e| ws::WsError::HandleError {
                            message: e.to_string(),
                        })?;
                }
            }
            _ => {}
        }
        return Ok(());
    }
}
