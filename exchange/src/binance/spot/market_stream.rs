use super::super::errors::*;
use super::models::market::*;
use super::parser::*;
use crate::binance::spot::models::KlineInterval;
use log::error;
use log::info;
use rand::distr::Alphanumeric;
use rand::Rng;
use rate_limiter::RateLimiter;
use serde::Deserialize;
use std::char;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use ws::RecvMsg;
use ws::SendMsg;

#[derive(Clone)]
enum MarketStreamType {
    UpdateDepth,
    AggTrade,
    Kline,
    Ticker,
}

#[derive(Clone)]
struct SubDetail {
    stream_type: MarketStreamType,
    symbol: String,
}

type Fut = Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>;
pub struct MarketStream {
    url: String,
    proxy_url: Option<String>,
    rate_limiters: Option<Arc<Vec<RateLimiter>>>,

    // 订阅 & 回调
    sub_details: HashMap<String, SubDetail>,
    update_depth_cb: Option<Arc<dyn Fn(DepthUpdate) -> Fut + Send + Sync + 'static>>,
    agg_trade_cb: Option<Arc<dyn Fn(AggTrade) -> Fut + Send + Sync + 'static>>,
    kline_cb: Option<Arc<dyn Fn(KlineData) -> Fut + Send + Sync + 'static>>,
    ticker_cb: Option<Arc<dyn Fn(Ticker24hr) -> Fut + Send + Sync + 'static>>,

    // ws客户端
    client: Option<ws::Client>,
}

impl MarketStream {
    // 订阅/注册回调等请在初始化前完成
    pub fn new(
        url: String,
        proxy_url: Option<String>,
        rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    ) -> Self {
        MarketStream {
            url,
            proxy_url,
            rate_limiters,
            sub_details: HashMap::new(),
            update_depth_cb: None,
            agg_trade_cb: None,
            kline_cb: None,
            ticker_cb: None,
            client: None,
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

    pub fn subscribe_agg_trade(&mut self, symbol: &str) {
        self.sub_details.insert(
            format!("{}@aggTrade", symbol.to_lowercase()),
            SubDetail {
                stream_type: MarketStreamType::AggTrade,
                symbol: symbol.to_string(),
            },
        );
    }

    pub fn subscribe_kline(&mut self, symbol: &str, interval: &KlineInterval) {
        self.sub_details.insert(
            format!("{}@kline_{}", symbol.to_lowercase(), interval.as_str()),
            SubDetail {
                stream_type: MarketStreamType::Kline,
                symbol: symbol.to_string(),
            },
        );
    }

    pub fn subscribe_ticker(&mut self, symbol: &str) {
        self.sub_details.insert(
            format!("{}@ticker", symbol.to_lowercase()),
            SubDetail {
                stream_type: MarketStreamType::Ticker,
                symbol: symbol.to_string(),
            },
        );
    }

    pub fn register_depth_update_callback<F>(&mut self, cb: F)
    where
        F: Fn(DepthUpdate) -> Fut + Send + Sync + 'static,
    {
        self.update_depth_cb = Some(Arc::new(cb));
    }

    pub fn register_agg_trade_callback<F>(&mut self, cb: F)
    where
        F: Fn(AggTrade) -> Fut + Send + Sync + 'static,
    {
        self.agg_trade_cb = Some(Arc::new(cb));
    }

    pub fn register_kline_callback<F>(&mut self, cb: F)
    where
        F: Fn(KlineData) -> Fut + Send + Sync + 'static,
    {
        self.kline_cb = Some(Arc::new(cb));
    }

    pub fn register_ticker_callback<F>(&mut self, cb: F)
    where
        F: Fn(Ticker24hr) -> Fut + Send + Sync + 'static,
    {
        self.ticker_cb = Some(Arc::new(cb));
    }

    // 完成ws连接并订阅
    pub async fn init(&mut self) -> Result<CancellationToken> {
        let sub_details = Arc::new(self.sub_details.clone());
        let update_depth_cb = self.update_depth_cb.clone();
        let agg_trade_cb = self.agg_trade_cb.clone();
        let kline_cb = self.kline_cb.clone();
        let ticker_cb = self.ticker_cb.clone();
        let mut config = ws::Config::default(
            self.url.clone(),
            Arc::new(Self::calc_recv_msg_id),
            Arc::new(move |msg: RecvMsg| {
                let sub_details = sub_details.clone();
                let update_depth_cb = update_depth_cb.clone();
                let agg_trade_cb = agg_trade_cb.clone();
                let kline_cb = kline_cb.clone();
                let ticker_cb = ticker_cb.clone();
                Box::pin(async move {
                    Self::handle(
                        msg,
                        sub_details,
                        update_depth_cb,
                        agg_trade_cb,
                        kline_cb,
                        ticker_cb,
                    )
                    .await
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

        let shutdown_token = ws_client.get_shutdown_token();
        self.client = Some(ws_client);

        Ok(shutdown_token)
    }

    pub fn get_ws_shutdown_token(&self) -> Option<CancellationToken> {
        match &self.client {
            None => None,
            Some(client) => Some(client.get_shutdown_token()),
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
        sub_details: Arc<HashMap<String, SubDetail>>,
        update_depth_cb: Option<
            Arc<
                dyn Fn(DepthUpdate) -> Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        >,
        agg_trade_cb: Option<
            Arc<
                dyn Fn(AggTrade) -> Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        >,
        kline_cb: Option<
            Arc<
                dyn Fn(KlineData) -> Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        >,
        ticker_cb: Option<
            Arc<
                dyn Fn(Ticker24hr) -> Pin<Box<dyn Future<Output = ws::Result<()>> + Send>>
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
            MarketStreamType::AggTrade => {
                let agg_trade = parse_agg_trade_stream(stream_msg.data.get()).map_err(|e| {
                    ws::WsError::HandleError {
                        message: e.to_string(),
                    }
                })?;
                if let Some(cb) = agg_trade_cb {
                    cb(agg_trade).await.map_err(|e| ws::WsError::HandleError {
                        message: e.to_string(),
                    })?;
                }
            }
            MarketStreamType::Kline => {
                let kline = parse_kline_stream(stream_msg.data.get()).map_err(|e| {
                    ws::WsError::HandleError {
                        message: e.to_string(),
                    }
                })?;
                if let Some(cb) = kline_cb {
                    cb(kline).await.map_err(|e| ws::WsError::HandleError {
                        message: e.to_string(),
                    })?;
                }
            }
            MarketStreamType::Ticker => {
                let ticker = parse_ticker_stream(stream_msg.data.get()).map_err(|e| {
                    ws::WsError::HandleError {
                        message: e.to_string(),
                    }
                })?;
                if let Some(cb) = ticker_cb {
                    cb(ticker).await.map_err(|e| ws::WsError::HandleError {
                        message: e.to_string(),
                    })?;
                }
            }
        }
        return Ok(());
    }
}
