use futures_util::SinkExt;
use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use log::{self, error, info};
use rate_limiter::RateLimiter;
use scopeguard::defer;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::{
    net::TcpStream,
    sync::mpsc::{Receiver, Sender, channel},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub enum SendMsg {
    Text {
        msg_id: Option<String>,
        content: String,
        weight: Option<u64>,
    },
    Binary {
        msg_id: Option<String>,
        data: Vec<u8>,
        weight: Option<u64>,
    },
    Ping {
        data: Vec<u8>,
        weight: Option<u64>,
    },
    Pong {
        data: Vec<u8>,
        weight: Option<u64>,
    },
    Close {
        code: Option<u16>,
        reason: Option<String>,
        weight: Option<u64>,
    },
}

impl SendMsg {
    pub fn msg_id(&self) -> Option<String> {
        match self {
            SendMsg::Text { msg_id, .. } => msg_id.clone(),
            SendMsg::Binary { msg_id, .. } => msg_id.clone(),
            SendMsg::Ping { .. } => None,
            SendMsg::Pong { .. } => None,
            SendMsg::Close { .. } => None,
        }
    }

    pub fn weight(&self) -> Option<u64> {
        match self {
            SendMsg::Text { weight, .. } => *weight,
            SendMsg::Binary { weight, .. } => *weight,
            SendMsg::Ping { weight, .. } => *weight,
            SendMsg::Pong { weight, .. } => *weight,
            SendMsg::Close { weight, .. } => *weight,
        }
    }

    pub fn to_websocket_message(self) -> Message {
        match self {
            SendMsg::Text { content, .. } => Message::Text(content.into()),
            SendMsg::Binary { data, .. } => Message::Binary(data.into()),
            SendMsg::Ping { data, .. } => Message::Ping(data.into()),
            SendMsg::Pong { data, .. } => Message::Pong(data.into()),
            SendMsg::Close { code, reason, .. } => {
                let close_frame = if let Some(code) = code {
                    Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                        code:
                            tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(
                                code,
                            ),
                        reason: reason.unwrap_or_default().into(),
                    })
                } else {
                    None
                };
                Message::Close(close_frame)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum RecvMsg {
    Text {
        msg_id: Option<String>,
        content: String,
    },
    Binary {
        msg_id: Option<String>,
        data: Vec<u8>,
    },
    Ping {
        data: Vec<u8>,
    },
    Pong {
        data: Vec<u8>,
    },
    Close {
        code: Option<u16>,
        reason: Option<String>,
    },
}

impl RecvMsg {
    pub fn msg_id(&self) -> Option<String> {
        match self {
            RecvMsg::Text { msg_id, .. } => msg_id.clone(),
            RecvMsg::Binary { msg_id, .. } => msg_id.clone(),
            RecvMsg::Ping { .. } => None,
            RecvMsg::Pong { .. } => None,
            RecvMsg::Close { .. } => None,
        }
    }

    pub fn from_websocket_message(
        msg: Message,
        calc_msg_id: &dyn Fn(&str) -> Option<String>,
    ) -> Option<Self> {
        match msg {
            Message::Text(content) => {
                let content_str = content.to_string();
                let msg_id = calc_msg_id(&content_str);
                Some(RecvMsg::Text {
                    msg_id,
                    content: content_str,
                })
            }
            Message::Binary(data) => {
                let data_vec = data.to_vec();
                let mut msg_id = None;
                if let Ok(data_str) = std::str::from_utf8(&data_vec) {
                    msg_id = calc_msg_id(data_str);
                }
                Some(RecvMsg::Binary {
                    msg_id: msg_id,
                    data: data_vec,
                })
            }
            Message::Ping(data) => Some(RecvMsg::Ping {
                data: data.to_vec(),
            }),
            Message::Pong(data) => Some(RecvMsg::Pong {
                data: data.to_vec(),
            }),
            Message::Close(close_frame) => {
                let (code, reason) = if let Some(frame) = close_frame {
                    (Some(frame.code.into()), Some(frame.reason.to_string()))
                } else {
                    (None, None)
                };
                Some(RecvMsg::Close { code, reason })
            }
            _ => None, // Frame messages are handled internally by tungstenite
        }
    }
}

pub struct Config {
    pub url: String,
    pub send_buf_size: usize,
    pub rate_limiters: Arc<Option<Vec<RateLimiter>>>,
    pub calc_recv_msg_id: Arc<dyn Fn(&str) -> Option<String> + Send + Sync>,
    pub handle: Arc<dyn Fn(RecvMsg) -> Result<(), String> + Send + Sync>,
    pub connect_timeout: Option<Duration>,
    pub call_timeout: Option<Duration>,
    pub heartbeat_interval: Option<Duration>,
}

impl Config {
    // 默认配置。如果需要修改配置，可以直接修改返回的结构体字段
    pub fn default(
        url: String,
        calc_recv_msg_id: Arc<dyn Fn(&str) -> Option<String> + Send + Sync>,
        handle: Arc<dyn Fn(RecvMsg) -> Result<(), String> + Send + Sync>,
    ) -> Self {
        Self {
            url,
            send_buf_size: 1024,
            rate_limiters: Arc::new(None),
            calc_recv_msg_id,
            handle,
            connect_timeout: Some(Duration::from_millis(500)),
            call_timeout: Some(Duration::from_millis(500)),
            heartbeat_interval: Some(Duration::from_secs(30)),
        }
    }
}

pub struct Client {
    config: Config,
    send_tx: Mutex<Option<Sender<SendMsg>>>,
    shutdown_token: Mutex<Option<CancellationToken>>,
    sync_call_chs: Arc<Mutex<HashMap<String, Sender<RecvMsg>>>>,
    join_handles: Mutex<Vec<JoinHandle<Result<(), String>>>>,
}

impl Client {
    pub fn new(config: Config) -> Self {
        Client {
            config,
            send_tx: Mutex::new(None),
            shutdown_token: Mutex::new(None),
            sync_call_chs: Arc::new(Mutex::new(HashMap::new())),
            join_handles: Mutex::new(Vec::new()),
        }
    }

    // 外部监控WSClient信号
    pub async fn get_shutdown_token(&self) -> Option<CancellationToken> {
        let token_guard = self.shutdown_token.lock().await;
        token_guard.clone()
    }

    pub async fn connect(&self) -> Result<(), String> {
        let connect_timeout = self
            .config
            .connect_timeout
            .unwrap_or(Duration::from_millis(500));
        let result = tokio::time::timeout(connect_timeout, connect_async(&self.config.url)).await;
        let (ws_stream, _) = match result {
            Ok(Ok((stream, resp))) => {
                info!("Connected to {}, response: {:?}", &self.config.url, resp);
                (stream, resp)
            }
            Ok(Err(e)) => {
                error!("WebSocket connect error: {}", e);
                return Err(e.to_string());
            }
            Err(e) => {
                error!("WebSocket connect timeout: {}", e);
                return Err("connect timeout".to_string());
            }
        };
        let (sender, receiver) = ws_stream.split();

        let shutdown_token = CancellationToken::new();
        {
            let mut token_guard = self.shutdown_token.lock().await;
            *token_guard = Some(shutdown_token.clone());
        }

        let rate_limiters = self.config.rate_limiters.clone();

        let (send_tx, send_rx) = channel::<SendMsg>(self.config.send_buf_size);
        {
            let mut send_tx_guard = self.send_tx.lock().await;
            *send_tx_guard = Some(send_tx.clone());
        }

        let shutdown_token1 = shutdown_token.clone();
        let send_loop_handle = tokio::spawn(async move {
            Self::send_loop(sender, send_rx, rate_limiters, shutdown_token1).await
        });

        let calc_recv_msg_id = self.config.calc_recv_msg_id.clone();
        let sync_call_chs = self.sync_call_chs.clone();
        let handle = self.config.handle.clone();

        let shutdown_token2 = shutdown_token.clone();
        let send_tx1 = send_tx.clone();
        let recv_loop_handle = tokio::spawn(async move {
            Self::recv_loop(
                receiver,
                calc_recv_msg_id,
                sync_call_chs,
                handle,
                send_tx1,
                shutdown_token2,
            )
            .await
        });

        let mut heartbeat_handle = None;
        let shutdown_token3 = shutdown_token.clone();
        let send_tx2 = send_tx.clone();
        if let Some(interval) = self.config.heartbeat_interval {
            heartbeat_handle = Some(tokio::spawn(async move {
                Self::heartbeat(send_tx2, interval, shutdown_token3).await
            }));
        }

        {
            let mut join_handles_guard = self.join_handles.lock().await;
            join_handles_guard.push(send_loop_handle);
            join_handles_guard.push(recv_loop_handle);
            if let Some(handle) = heartbeat_handle {
                join_handles_guard.push(handle);
            }
        }

        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), String> {
        {
            let token_guard = self.shutdown_token.lock().await;
            if let Some(token) = &*token_guard {
                token.cancel();
            }
        }
        {
            let mut join_handles_guard = self.join_handles.lock().await;
            for handle in join_handles_guard.drain(..) {
                if let Err(e) = handle.await {
                    error!("Task join error: {}", e);
                }
            }
        }
        Ok(())
    }

    pub async fn send(&self, msg: SendMsg) -> Result<(), String> {
        let send_tx = self.send_tx.lock().await;
        if let Some(send_tx) = send_tx.as_ref() {
            send_tx.send(msg).await.map_err(|e| e.to_string())
        } else {
            Err("send channel not initialized".to_string())
        }
    }

    pub async fn call(&self, msg: SendMsg) -> Result<RecvMsg, String> {
        if msg.msg_id().is_none() {
            return Err("msg_id is required for call".to_string());
        }
        let msg_id = msg.msg_id().unwrap();

        let (resp_tx, mut resp_rx) = channel::<RecvMsg>(1);

        let shutdown_token: CancellationToken;
        {
            let shutdown_token_guard = self.shutdown_token.lock().await;
            if shutdown_token_guard.is_none() {
                return Err("client is not connected".to_string());
            }
            shutdown_token = shutdown_token_guard.as_ref().unwrap().clone();
        }

        {
            let send_tx = self.send_tx.lock().await;
            if let Some(send_tx) = send_tx.as_ref() {
                let mut sync_call_chs_guard = self.sync_call_chs.lock().await;
                sync_call_chs_guard.insert(msg_id.clone(), resp_tx);
                if let Err(e) = send_tx.send(msg).await {
                    sync_call_chs_guard.remove(&msg_id);
                    return Err(format!("{:?}", e));
                }
            } else {
                return Err("send channel not initialized".to_string());
            }
        }

        let timeout = self
            .config
            .call_timeout
            .unwrap_or(Duration::from_millis(500));
        tokio::select! {
            _ = tokio::time::sleep(timeout) => {
                let mut sync_call_chs_guard = self.sync_call_chs.lock().await;
                sync_call_chs_guard.remove(&msg_id);
                return Err("call timeout".to_string());
            }
            resp = resp_rx.recv() => {
                if let Some(resp) = resp {
                    return Ok(resp);
                } else {
                    return Err("response channel closed".to_string());
                }
            }
            _ = shutdown_token.cancelled() => {
                let mut sync_call_chs_guard = self.sync_call_chs.lock().await;
                sync_call_chs_guard.remove(&msg_id);
                return Err("client is shutting down".to_string());
            }
        }
    }

    async fn recv_loop(
        mut receiver: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        calc_recv_msg_id: Arc<dyn Fn(&str) -> Option<String> + Send + Sync>,
        sync_call_chs: Arc<Mutex<HashMap<String, Sender<RecvMsg>>>>,
        handle: Arc<dyn Fn(RecvMsg) -> Result<(), String> + Send + Sync>,
        send_tx: Sender<SendMsg>,
        shutdown_token: CancellationToken,
    ) -> Result<(), String> {
        defer!(
            shutdown_token.cancel();
        );
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    return Err("shutdown".to_string());
                }
                msg = receiver.next() => {
                    if let None = msg {
                        return Err("connection closed".to_string());
                    }
                    let msg = msg.unwrap();
                    if let Err(e) = msg {
                        error!("WebSocket receive error: {}", e);
                        return Err(e.to_string());
                    }
                    let msg = msg.unwrap();
                    if let Some(recv_msg) = RecvMsg::from_websocket_message(msg, calc_recv_msg_id.as_ref()) {
                        match &recv_msg {
                            RecvMsg::Text { .. } | RecvMsg::Binary { .. } => {
                                let mut ch: Option<Sender<RecvMsg>> = None;
                                {
                                    let mut sync_call_chs_guard = sync_call_chs.lock().await;
                                    if let Some(msg_id) = recv_msg.msg_id() {
                                        ch = sync_call_chs_guard.remove(&msg_id);
                                    }
                                }
                                if let Some(ch) = ch {
                                    if let Err(e) = ch.send(recv_msg).await {
                                        error!("Failed to send message to sync call channel: {}", e);
                                    }
                                    continue;
                                }
                                if let Err(e) = handle(recv_msg) {
                                    error!("Failed to handle message: {}", e);
                                }
                            },
                            RecvMsg::Ping { data } => {
                                let data = std::str::from_utf8(&data);
                                if let Err(e) = data {
                                    error!("Invalid Ping data: {}", e);
                                } else {
                                    let data_str = data.unwrap();
                                    info!("Received Ping: {}", data_str);
                                    let pong_msg = SendMsg::Pong {
                                        data: data_str.as_bytes().to_vec(),
                                        weight: None
                                    };
                                    _ = send_tx.send(pong_msg).await;
                                }
                            },
                            RecvMsg::Pong { data } => {
                                let data = std::str::from_utf8(&data);
                                if let Err(e) = data {
                                    error!("Invalid Pong data: {}", e);
                                } else {
                                    let data_str = data.unwrap();
                                    info!("Received Pong: {}", data_str);
                                }
                            },
                            RecvMsg::Close { code, reason } => {
                                info!("WebSocket connection closed: code={:?}, reason={:?}", code, reason);
                                return Err("connection closed".to_string());
                            }
                        }
                    } else {
                        error!("Unsupported WebSocket message type");
                        return Err("unsupported message type".to_string());
                    }
                }
            }
        }
    }

    async fn send_loop(
        mut sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        mut send_rx: Receiver<SendMsg>,
        rate_limiters: Arc<Option<Vec<RateLimiter>>>,
        shutdown_token: CancellationToken,
    ) -> Result<(), String> {
        defer!(
            shutdown_token.cancel();
        );
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    send_rx.close();
                    return Err("shutdown".to_string());
                }
                msg = send_rx.recv() => {
                    if let Some(msg) = msg {
                        let weight = msg.weight().unwrap_or(1);
                        if let Some(limiters) = rate_limiters.as_ref() {
                            for limiter in limiters.iter() {
                                limiter.wait(weight).await.map_err(|e| e.to_string())?;
                            }
                        }
                        if let Err(e) = sender.send(msg.to_websocket_message()).await {
                            error!("WebSocket send error: {}", e);
                        }
                    } else {
                        send_rx.close();
                        return Err("send channel closed".to_string());
                    }
                }
            }
        }
    }

    async fn heartbeat(
        send_tx: Sender<SendMsg>,
        interval: Duration,
        shutdown_token: CancellationToken,
    ) -> Result<(), String> {
        defer!(
            shutdown_token.cancel();
        );
        let mut interval = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    return Err("shutdown".to_string());
                }
                _ = interval.tick() => {
                    let now_ts = time::get_current_nano_timestamp() / 1000000;
                    let heartbeat_msg = SendMsg::Ping {
                        data: now_ts.to_string().as_bytes().to_vec(),
                        weight: None,
                    };
                    if let Err(e) = send_tx.send(heartbeat_msg).await {
                        error!("Failed to send heartbeat: {}", e);
                    }
                }
            }
        }
    }
}
