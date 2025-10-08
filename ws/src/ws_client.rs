use crate::{Result, WsError};
use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use futures_util::SinkExt;
use log::{self, error, info};
use rate_limiter::RateLimiter;
use scopeguard::defer;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_socks::tcp::Socks5Stream;
use tokio_tungstenite::{client_async, connect_async, tungstenite::Message, WebSocketStream};
use tokio_util::sync::CancellationToken;
use url::Url;

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
    pub proxy_url: Option<String>,
    pub send_buf_size: usize,
    pub rate_limiters: Option<Arc<Vec<RateLimiter>>>,
    pub calc_recv_msg_id: Arc<dyn Fn(&str) -> Option<String> + Send + Sync>,
    pub handle:
        Arc<dyn Fn(RecvMsg) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>,
    pub connect_timeout: Duration,
    pub call_timeout: Duration,
    pub heartbeat_interval: Duration,
}

impl Config {
    // 默认配置。如果需要修改配置，可以直接修改返回的结构体字段
    pub fn default(
        url: String,
        calc_recv_msg_id: Arc<dyn Fn(&str) -> Option<String> + Send + Sync>,
        handle: Arc<
            dyn Fn(RecvMsg) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync,
        >,
    ) -> Self {
        Self {
            url,
            proxy_url: None,
            send_buf_size: 1024,
            rate_limiters: None,
            calc_recv_msg_id,
            handle,
            connect_timeout: Duration::from_millis(10000),
            call_timeout: Duration::from_millis(10000),
            heartbeat_interval: Duration::from_secs(30),
        }
    }
}

pub struct Client {
    config: Config,
    send_tx: Option<Sender<SendMsg>>,
    shutdown_token: CancellationToken,
    sync_call_chs: Arc<RwLock<HashMap<String, Sender<RecvMsg>>>>,
    join_handles: Vec<JoinHandle<Result<()>>>,
}

impl Client {
    pub fn new(config: Config) -> Result<Self> {
        if config.url.is_empty() {
            return Err(WsError::invalid_url(config.url));
        }
        if config.connect_timeout.is_zero() {
            return Err(WsError::invalid_timeout("connect_timeout".to_string()));
        }
        if config.call_timeout.is_zero() {
            return Err(WsError::invalid_timeout("call_timeout".to_string()));
        }
        if config.heartbeat_interval.is_zero() {
            return Err(WsError::invalid_heartbeat_interval());
        }
        if config.send_buf_size == 0 {
            return Err(WsError::invalid_send_buf_size());
        }
        Ok(Client {
            config,
            send_tx: None,
            shutdown_token: CancellationToken::new(),
            sync_call_chs: Arc::new(RwLock::new(HashMap::new())),
            join_handles: Vec::new(),
        })
    }

    // 外部监控WSClient信号
    pub async fn get_shutdown_token(&self) -> CancellationToken {
        return self.shutdown_token.clone();
    }

    pub async fn connect(&mut self) -> Result<()> {
        let connect_timeout = self.config.connect_timeout;
        if let Some(proxy_url_str) = self.config.proxy_url.as_ref() {
            let proxy_url = Url::parse(proxy_url_str)
                .map_err(|_| WsError::invalid_proxy_url(proxy_url_str.clone()))?;
            let proxy_host = proxy_url
                .host_str()
                .ok_or_else(|| WsError::invalid_proxy_url(proxy_url_str.clone()))?;
            let proxy_port = proxy_url
                .port()
                .ok_or_else(|| WsError::invalid_proxy_url(proxy_url_str.clone()))?;

            let url = Url::parse(&self.config.url)
                .map_err(|_| WsError::invalid_url(self.config.url.clone()))?;
            let host = url
                .host_str()
                .ok_or_else(|| WsError::invalid_url(self.config.url.clone()))?;
            let port = url
                .port()
                .unwrap_or_else(|| if url.scheme() == "wss" { 443 } else { 80 });

            let proxy_stream = tokio::time::timeout(
                connect_timeout,
                Socks5Stream::connect((proxy_host, proxy_port), (host, port)),
            )
            .await
            .map_err(|_| WsError::connection_timeout(proxy_url_str.clone(), connect_timeout))?
            .map_err(|e| WsError::connection_failed(proxy_url_str.clone(), e))?;

            if url.scheme() == "wss" {
                let root_cert_store = rustls::RootCertStore {
                    roots: webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect(),
                };
                let config = Arc::new(
                    tokio_rustls::rustls::ClientConfig::builder()
                        .with_root_certificates(root_cert_store)
                        .with_no_client_auth(),
                );
                let connector = tokio_rustls::TlsConnector::from(config);
                let domain = rustls::pki_types::ServerName::try_from(host.to_string())
                    .map_err(|_| WsError::invalid_url(self.config.url.clone()))?;
                let tls_stream = connector
                    .connect(domain, proxy_stream)
                    .await
                    .map_err(|e| WsError::connection_failed(self.config.url.clone(), e))?;
                let (ws_stream, _) = client_async(&self.config.url, tls_stream)
                    .await
                    .map_err(|e| WsError::connection_failed(self.config.url.clone(), e))?;
                self.initialize_stream(ws_stream).await
            } else {
                let (ws_stream, _) = client_async(&self.config.url, proxy_stream)
                    .await
                    .map_err(|e| WsError::connection_failed(self.config.url.clone(), e))?;
                self.initialize_stream(ws_stream).await
            }
        } else {
            let (ws_stream, _) =
                tokio::time::timeout(connect_timeout, connect_async(&self.config.url))
                    .await
                    .map_err(|_| {
                        WsError::connection_timeout(self.config.url.clone(), connect_timeout)
                    })?
                    .map_err(|e| WsError::connection_failed(self.config.url.clone(), e))?;
            self.initialize_stream(ws_stream).await
        }
    }

    async fn initialize_stream<S>(&mut self, ws_stream: WebSocketStream<S>) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (sender, receiver) = ws_stream.split();

        let rate_limiters = self.config.rate_limiters.clone();

        let (send_tx, send_rx) = channel::<SendMsg>(self.config.send_buf_size);
        self.send_tx = Some(send_tx.clone());

        let shutdown_token1 = self.shutdown_token.clone();
        let send_loop_handle = tokio::spawn(async move {
            Self::send_loop(sender, send_rx, rate_limiters, shutdown_token1).await
        });

        let calc_recv_msg_id = self.config.calc_recv_msg_id.clone();
        let sync_call_chs = self.sync_call_chs.clone();
        let handle = self.config.handle.clone();
        let shutdown_token2 = self.shutdown_token.clone();
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

        let hearbeat_interval = self.config.heartbeat_interval.clone();
        let shutdown_token3 = self.shutdown_token.clone();
        let send_tx2 = send_tx.clone();
        let heartbeat_handle = tokio::spawn(async move {
            Self::heartbeat(send_tx2, hearbeat_interval, shutdown_token3).await
        });

        self.join_handles.push(send_loop_handle);
        self.join_handles.push(recv_loop_handle);
        self.join_handles.push(heartbeat_handle);

        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<()> {
        self.shutdown_token.cancel();

        for handle in self.join_handles.drain(..) {
            if let Err(e) = handle.await {
                error!("Task join error: {}", e);
            }
        }

        Ok(())
    }

    pub async fn send(&self, msg: SendMsg) -> Result<()> {
        let send_tx = match self.send_tx.as_ref() {
            Some(tx) => tx,
            None => return Err(WsError::disconnected()),
        };
        send_tx
            .send(msg)
            .await
            .map_err(|e| WsError::channel_closed("send_tx".to_string(), e.to_string()))
    }

    pub async fn call(&self, msg: SendMsg) -> Result<RecvMsg> {
        if msg.msg_id().is_none() {
            return Err(WsError::message_id_required());
        }
        let msg_id = msg.msg_id().unwrap();

        let (resp_tx, mut resp_rx) = channel::<RecvMsg>(1);

        {
            let send_tx = match self.send_tx.as_ref() {
                Some(tx) => tx,
                None => return Err(WsError::disconnected()),
            };
            let mut sync_call_chs_guard = self.sync_call_chs.write().await;
            if sync_call_chs_guard.contains_key(&msg_id) {
                return Err(WsError::duplicated_message_id(msg_id));
            }
            sync_call_chs_guard.insert(msg_id.clone(), resp_tx);
            if let Err(e) = send_tx.send(msg).await {
                sync_call_chs_guard.remove(&msg_id);
                return Err(WsError::channel_closed(
                    "send_tx".to_string(),
                    e.to_string(),
                ));
            }
        }

        let timeout = self.config.call_timeout.clone();
        tokio::select! {
            _ = tokio::time::sleep(timeout) => {
                let mut sync_call_chs_guard = self.sync_call_chs.write().await;
                sync_call_chs_guard.remove(&msg_id);
                return Err(WsError::call_timeout(msg_id.clone(), timeout));
            }
            resp = resp_rx.recv() => {
                let mut sync_call_chs_guard = self.sync_call_chs.write().await;
                sync_call_chs_guard.remove(&msg_id);
                return Ok(resp.unwrap());
            }
            _ = self.shutdown_token.cancelled() => {
                let mut sync_call_chs_guard = self.sync_call_chs.write().await;
                sync_call_chs_guard.remove(&msg_id);
                return Err(WsError::disconnected());
            }
        }
    }

    async fn recv_loop<S>(
        mut receiver: SplitStream<WebSocketStream<S>>,
        calc_recv_msg_id: Arc<dyn Fn(&str) -> Option<String> + Send + Sync>,
        sync_call_chs: Arc<RwLock<HashMap<String, Sender<RecvMsg>>>>,
        handle: Arc<
            dyn Fn(RecvMsg) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync,
        >,
        send_tx: Sender<SendMsg>,
        shutdown_token: CancellationToken,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        defer!(
            shutdown_token.cancel();
        );
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    return Err(WsError::disconnected());
                }
                msg = receiver.next() => {
                    if let None = msg {
                        return Err(WsError::receive_failed(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "WebSocket stream ended unexpectedly"
                        )));
                    }
                    let msg = msg.unwrap();
                    if let Err(e) = msg {
                        error!("WebSocket receive error: {}", e);
                        return Err(WsError::receive_failed(e));
                    }
                    let msg = msg.unwrap();
                    if let Some(recv_msg) = RecvMsg::from_websocket_message(msg, calc_recv_msg_id.as_ref()) {
                        match &recv_msg {
                            RecvMsg::Text { .. } | RecvMsg::Binary { .. } => {
                                let mut ch: Option<Sender<RecvMsg>> = None;
                                if let Some(msg_id) = recv_msg.msg_id() {
                                    let sync_call_chs_guard = sync_call_chs.read().await;
                                    ch = sync_call_chs_guard.get(&msg_id).cloned();
                                }
                                if let Some(ch) = ch {
                                    if let Err(e) = ch.send(recv_msg).await {
                                        error!("failed to send message to sync call channel: {}", e);
                                    }
                                } else {
                                    if let Err(e) = handle(recv_msg).await {
                                        error!("failed to handle message: {}", e);
                                    }
                                }
                            },
                            RecvMsg::Ping { data } => {
                                let data_str = std::str::from_utf8(&data);
                                info!("Received Ping: {}", data_str.unwrap_or_default());
                                let pong_msg = SendMsg::Pong {
                                    data: data.clone(),
                                    weight: None
                                };
                                if let Err(e) = send_tx.send(pong_msg).await {
                                    error!("failed to send Pong: {}", e);
                                    return Err(WsError::channel_closed("send_tx".to_string(), e.to_string()));
                                }
                            },
                            RecvMsg::Pong { data } => {
                                let data_str = std::str::from_utf8(&data);
                                info!("Received Pong: {}", data_str.unwrap_or_default());
                            },
                            RecvMsg::Close { code, reason } => {
                                info!("WebSocket connection closed: code={:?}, reason={:?}", code, reason);
                                return Err(WsError::connection_closed(code.unwrap_or(0), reason.clone().unwrap_or_default()));
                            }
                        }
                    } else {
                        error!("Unsupported WebSocket message type");
                        return Err(WsError::transport("unsupported message type"));
                    }
                }
            }
        }
    }

    async fn send_loop<S>(
        mut sender: SplitSink<WebSocketStream<S>, Message>,
        mut send_rx: Receiver<SendMsg>,
        rate_limiters: Option<Arc<Vec<RateLimiter>>>,
        shutdown_token: CancellationToken,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        defer!(
            shutdown_token.cancel();
        );
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    send_rx.close();
                    return Err(WsError::disconnected());
                }
                msg = send_rx.recv() => {
                    if let Some(msg) = msg {
                        let weight = msg.weight().unwrap_or(1);
                        if let Some(limiters) = rate_limiters.as_ref() {
                            for limiter in limiters.iter() {
                                limiter.wait(weight).await.map_err(|e| WsError::External(e.into()))?;
                            }
                        }
                        if let Err(e) = sender.send(msg.to_websocket_message()).await {
                            error!("WebSocket send error: {}", e);
                            return Err(WsError::send_failed("websocket message".to_string(), e));
                        }
                    } else {
                        send_rx.close();
                        return Err(WsError::channel_closed("send_rx".to_string(), "send channel closed".to_string()));
                    }
                }
            }
        }
    }

    async fn heartbeat(
        send_tx: Sender<SendMsg>,
        interval: Duration,
        shutdown_token: CancellationToken,
    ) -> Result<()> {
        defer!(
            shutdown_token.cancel();
        );
        let mut interval = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    return Err(WsError::disconnected());
                }
                _ = interval.tick() => {
                    let now_ts = time::get_current_nano_timestamp() / 1000000;
                    let heartbeat_msg = SendMsg::Ping {
                        data: now_ts.to_string().as_bytes().to_vec(),
                        weight: None,
                    };
                    if let Err(e) = send_tx.send(heartbeat_msg).await {
                        error!("Failed to send heartbeat: {}", e);
                        return Err(WsError::channel_closed("send_tx".to_string(), e.to_string()));
                    }
                }
            }
        }
    }
}
