#[cfg(test)]
use crate::error::WsError;
use crate::ws_client::*;
use futures_util::{SinkExt, StreamExt};
use rate_limiter::RateLimiter;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{timeout, Instant};
use tokio_tungstenite::{accept_async, tungstenite::protocol::Message};

// 模拟WebSocket服务器
struct MockWebSocketServer {
    addr: String,
    received_messages: Arc<Mutex<Vec<String>>>,
    should_respond: Arc<AtomicBool>,
    should_echo: Arc<AtomicBool>,
    connection_count: Arc<AtomicU32>,
    shutdown: Arc<AtomicBool>,
}

impl MockWebSocketServer {
    async fn new(port: u16) -> Self {
        let addr = format!("127.0.0.1:{}", port);
        Self {
            addr: addr.clone(),
            received_messages: Arc::new(Mutex::new(Vec::new())),
            should_respond: Arc::new(AtomicBool::new(false)),
            should_echo: Arc::new(AtomicBool::new(true)),
            connection_count: Arc::new(AtomicU32::new(0)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn start(&self) -> tokio::task::JoinHandle<()> {
        let listener = TcpListener::bind(&self.addr).await.unwrap();
        let received_messages = self.received_messages.clone();
        let should_respond = self.should_respond.clone();
        let should_echo = self.should_echo.clone();
        let connection_count = self.connection_count.clone();
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            while !shutdown.load(Ordering::Relaxed) {
                match timeout(Duration::from_millis(100), listener.accept()).await {
                    Ok(Ok((stream, _))) => {
                        connection_count.fetch_add(1, Ordering::Relaxed);
                        let received_messages = received_messages.clone();
                        let should_respond = should_respond.clone();
                        let should_echo = should_echo.clone();
                        let shutdown = shutdown.clone();

                        tokio::spawn(async move {
                            if let Err(e) = Self::handle_connection(
                                stream,
                                received_messages,
                                should_respond,
                                should_echo,
                                shutdown,
                            )
                            .await
                            {
                                eprintln!("Connection handling error: {}", e);
                            }
                        });
                    }
                    Ok(Err(_)) | Err(_) => continue,
                }
            }
        })
    }

    async fn handle_connection(
        stream: TcpStream,
        received_messages: Arc<Mutex<Vec<String>>>,
        should_respond: Arc<AtomicBool>,
        should_echo: Arc<AtomicBool>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ws_stream = accept_async(stream).await?;
        let (mut sender, mut receiver) = ws_stream.split();

        while !shutdown.load(Ordering::Relaxed) {
            match timeout(Duration::from_millis(100), receiver.next()).await {
                Ok(Some(Ok(msg))) => match msg {
                    Message::Text(text) => {
                        {
                            let mut messages = received_messages.lock().await;
                            messages.push(text.to_string());
                        }

                        // 处理JSON消息
                        if let Ok(json_msg) = serde_json::from_str::<Value>(&text) {
                            if let Some(msg_id) = json_msg.get("id").and_then(|v| v.as_str()) {
                                if should_respond.load(Ordering::Relaxed) {
                                    let response = json!({
                                        "id": msg_id,
                                        "result": "success",
                                        "data": format!("Response to {}", msg_id)
                                    });
                                    let _ = sender
                                        .send(Message::Text(response.to_string().into()))
                                        .await;
                                }
                            } else if should_echo.load(Ordering::Relaxed) {
                                let _ = sender.send(Message::Text(text.into())).await;
                            }
                        } else if should_echo.load(Ordering::Relaxed) {
                            let _ = sender.send(Message::Text(text.into())).await;
                        }
                    }
                    Message::Ping(data) => {
                        let _ = sender.send(Message::Pong(data)).await;
                    }
                    Message::Close(_) => break,
                    _ => {}
                },
                Ok(Some(Err(_))) | Ok(None) => break,
                Err(_) => continue,
            }
        }
        Ok(())
    }

    fn get_url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    async fn get_received_messages(&self) -> Vec<String> {
        self.received_messages.lock().await.clone()
    }

    fn set_should_respond(&self, should_respond: bool) {
        self.should_respond.store(should_respond, Ordering::Relaxed);
    }

    fn set_should_echo(&self, should_echo: bool) {
        self.should_echo.store(should_echo, Ordering::Relaxed);
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

// 测试用的消息ID计算函数
fn calc_test_msg_id(msg: &str) -> Option<String> {
    if let Ok(json_msg) = serde_json::from_str::<Value>(msg) {
        json_msg
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    } else {
        None
    }
}

// 测试用的消息处理函数
fn create_test_handler() -> (
    Arc<dyn Fn(RecvMsg) -> Result<(), WsError> + Send + Sync>,
    Arc<Mutex<Vec<RecvMsg>>>,
) {
    let received_messages = Arc::new(Mutex::new(Vec::new()));
    let received_messages_clone = received_messages.clone();

    let handler = Arc::new(move |msg: RecvMsg| -> Result<(), WsError> {
        let received_messages = received_messages_clone.clone();
        tokio::spawn(async move {
            let mut messages = received_messages.lock().await;
            messages.push(msg);
        });
        Ok(())
    });

    (handler, received_messages)
}

#[tokio::test]
async fn test_basic_connect_and_disconnect() {
    let server = MockWebSocketServer::new(8081).await;
    let _server_handle = server.start().await;

    // 等待服务器启动
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (handler, _) = create_test_handler();
    let config = Config {
        url: server.get_url(),
        send_buf_size: 100,
        rate_limiters: Arc::new(None),
        calc_recv_msg_id: Arc::new(calc_test_msg_id),
        handle: handler,
        connect_timeout: Duration::from_millis(1000),
        call_timeout: Duration::from_millis(1000),
        heartbeat_interval: Duration::from_millis(1000),
    };

    let client = Client::new(config).unwrap();

    // 测试连接
    let result = client.connect().await;
    assert!(result.is_ok(), "Failed to connect: {:?}", result);

    // 验证连接状态
    let token = client.get_shutdown_token().await;
    assert!(token.is_some(), "Shutdown token should be available");

    // 测试断开连接
    let result = client.disconnect().await;
    assert!(result.is_ok(), "Failed to disconnect: {:?}", result);

    server.shutdown();
}

#[tokio::test]
async fn test_send_message() {
    let server = MockWebSocketServer::new(8082).await;
    server.set_should_echo(true);
    let _server_handle = server.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (handler, received_messages) = create_test_handler();
    let config = Config {
        url: server.get_url(),
        send_buf_size: 100,
        rate_limiters: Arc::new(None),
        calc_recv_msg_id: Arc::new(calc_test_msg_id),
        handle: handler,
        connect_timeout: Duration::from_millis(1000),
        call_timeout: Duration::from_millis(1000),
        heartbeat_interval: Duration::from_millis(1000),
    };

    let client = Client::new(config).unwrap();
    client.connect().await.unwrap();

    // 发送消息
    let test_message = "Hello WebSocket!";
    let send_msg = SendMsg::Text {
        msg_id: None,
        content: test_message.to_string(),
        weight: None,
    };

    let result = client.send(send_msg).await;
    assert!(result.is_ok(), "Failed to send message: {:?}", result);

    // 等待消息被处理
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 验证服务器收到消息
    let server_messages = server.get_received_messages().await;
    assert!(!server_messages.is_empty(), "Server should receive message");
    assert_eq!(server_messages[0], test_message);

    // 验证客户端收到回显消息
    let client_messages = received_messages.lock().await;
    assert!(
        !client_messages.is_empty(),
        "Client should receive echoed message"
    );
    match &client_messages[0] {
        RecvMsg::Text { content, .. } => assert_eq!(content, test_message),
        _ => panic!("Expected text message, got {:?}", client_messages[0]),
    }

    client.disconnect().await.unwrap();
    server.shutdown();
}

#[tokio::test]
async fn test_synchronous_call() {
    let server = MockWebSocketServer::new(8083).await;
    server.set_should_respond(true);
    server.set_should_echo(false);
    let _server_handle = server.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (handler, _) = create_test_handler();
    let config = Config {
        url: server.get_url(),
        send_buf_size: 100,
        rate_limiters: Arc::new(None),
        calc_recv_msg_id: Arc::new(calc_test_msg_id),
        handle: handler,
        connect_timeout: Duration::from_millis(1000),
        call_timeout: Duration::from_millis(2000),
        heartbeat_interval: Duration::from_millis(1000),
    };

    let client = Client::new(config).unwrap();
    client.connect().await.unwrap();

    // 发送同步调用
    let request = json!({
        "id": "test-123",
        "method": "ping",
        "params": {}
    });

    let send_msg = SendMsg::Text {
        msg_id: Some("test-123".to_string()),
        content: request.to_string(),
        weight: None,
    };

    let result = client.call(send_msg).await;
    assert!(result.is_ok(), "Call should succeed: {:?}", result);

    let response = result.unwrap();
    assert_eq!(response.msg_id(), Some("test-123".to_string()));

    // 验证响应内容
    match response {
        RecvMsg::Text { content, .. } => {
            let response_json: Value = serde_json::from_str(&content).unwrap();
            assert_eq!(response_json["id"], "test-123");
            assert_eq!(response_json["result"], "success");
        }
        _ => panic!("Expected text response"),
    }

    client.disconnect().await.unwrap();
    server.shutdown();
}

#[tokio::test]
async fn test_call_timeout() {
    let server = MockWebSocketServer::new(8084).await;
    server.set_should_respond(false); // 不响应消息
    let _server_handle = server.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (handler, _) = create_test_handler();
    let config = Config {
        url: server.get_url(),
        send_buf_size: 100,
        rate_limiters: Arc::new(None),
        calc_recv_msg_id: Arc::new(calc_test_msg_id),
        handle: handler,
        connect_timeout: Duration::from_millis(1000),
        call_timeout: Duration::from_millis(500),
        heartbeat_interval: Duration::from_millis(1000),
    };

    let client = Client::new(config).unwrap();
    client.connect().await.unwrap();

    let request = json!({
        "id": "timeout-test",
        "method": "ping"
    });

    let send_msg = SendMsg::Text {
        msg_id: Some("timeout-test".to_string()),
        content: request.to_string(),
        weight: None,
    };

    let start = Instant::now();
    let result = client.call(send_msg).await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "Call should timeout");
    assert!(elapsed >= Duration::from_millis(500));
    assert!(elapsed < Duration::from_millis(700)); // 允许一些误差
    assert!(matches!(result.unwrap_err(), WsError::Client { .. }));

    client.disconnect().await.unwrap();
    server.shutdown();
}

#[tokio::test]
async fn test_call_without_msg_id() {
    let server = MockWebSocketServer::new(8085).await;
    let _server_handle = server.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (handler, _) = create_test_handler();
    let config = Config {
        url: server.get_url(),
        send_buf_size: 100,
        rate_limiters: Arc::new(None),
        calc_recv_msg_id: Arc::new(calc_test_msg_id),
        handle: handler,
        connect_timeout: Duration::from_millis(1000),
        call_timeout: Duration::from_millis(1000),
        heartbeat_interval: Duration::from_millis(1000),
    };

    let client = Client::new(config).unwrap();
    client.connect().await.unwrap();

    let send_msg = SendMsg::Text {
        msg_id: None, // 没有消息ID
        content: "test message".to_string(),
        weight: None,
    };

    let result = client.call(send_msg).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), WsError::Client { .. }));

    client.disconnect().await.unwrap();
    server.shutdown();
}

#[tokio::test]
async fn test_rate_limiter() {
    let server = MockWebSocketServer::new(8086).await;
    server.set_should_respond(true);
    server.set_should_echo(true);
    let _server_handle = server.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 创建一个宽松的限流器，主要验证功能而不是严格的时间控制
    let rate_limiter = RateLimiter::new(Duration::from_millis(500), 10);
    let rate_limiters = vec![rate_limiter];

    let (handler, _) = create_test_handler();
    let config = Config {
        url: server.get_url(),
        send_buf_size: 100,
        rate_limiters: Arc::new(Some(rate_limiters)),
        calc_recv_msg_id: Arc::new(calc_test_msg_id),
        handle: handler,
        connect_timeout: Duration::from_millis(1000),
        call_timeout: Duration::from_millis(1000),
        heartbeat_interval: Duration::from_millis(1000),
    };

    let client = Client::new(config).unwrap();
    client.connect().await.unwrap();

    let ts1 = time::get_current_nano_timestamp();
    for i in 0..3 {
        let msg = SendMsg::Text {
            msg_id: None,
            content: format!("message{}", i),
            weight: Some(1),
        };
        let result = client.send(msg).await;
        assert!(result.is_ok(), "Message {} should be sent successfully", i);
    }
    let ts2 = time::get_current_nano_timestamp();
    assert!(
        (ts2 - ts1) / 1000000 < 100,
        "Messages should be sent without significant delay"
    );

    let msg = SendMsg::Text {
        msg_id: Some("1".to_string()),
        content: "{\"id\": \"1\"}".to_string(),
        weight: Some(8),
    };
    let result = client.call(msg).await;
    assert!(
        result.is_ok(),
        "Heavy message should be sent successfully: {:?}",
        result.unwrap_err(),
    );

    let ts3 = time::get_current_nano_timestamp();
    assert!(
        (ts3 - ts2) / 1000000 > 400,
        "Heavy message should be rate limited"
    );
    assert!(
        (ts3 - ts2) / 1000000 < 600,
        "Heavy message should not be excessively delayed"
    );

    // 等待一下确保消息被处理
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 验证服务器收到了消息
    let server_messages = server.get_received_messages().await;
    assert!(
        server_messages.len() >= 3,
        "Server should receive at least 3 messages"
    );

    client.disconnect().await.unwrap();
    server.shutdown();
}

#[tokio::test]
async fn test_ping_pong_handling() {
    let server = MockWebSocketServer::new(8087).await;
    let _server_handle = server.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (handler, _received_messages) = create_test_handler();
    let config = Config {
        url: server.get_url(),
        send_buf_size: 100,
        rate_limiters: Arc::new(None),
        calc_recv_msg_id: Arc::new(calc_test_msg_id),
        handle: handler,
        connect_timeout: Duration::from_millis(1000),
        call_timeout: Duration::from_millis(1000),
        heartbeat_interval: Duration::from_millis(100),
    };

    let client = Client::new(config).unwrap();
    client.connect().await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    client.disconnect().await.unwrap();
    server.shutdown();
}

#[tokio::test]
async fn test_connection_error() {
    let (handler, _) = create_test_handler();
    let config = Config {
        url: "ws://127.0.0.1:9999".to_string(), // 不存在的服务器
        send_buf_size: 100,
        rate_limiters: Arc::new(None),
        calc_recv_msg_id: Arc::new(calc_test_msg_id),
        handle: handler,
        connect_timeout: Duration::from_millis(1000),
        call_timeout: Duration::from_millis(1000),
        heartbeat_interval: Duration::from_millis(1000),
    };

    let client = Client::new(config).unwrap();
    let result = client.connect().await;
    assert!(
        result.is_err(),
        "Should fail to connect to non-existent server"
    );
}

#[tokio::test]
async fn test_send_before_connect() {
    let (handler, _) = create_test_handler();
    let config = Config {
        url: "ws://127.0.0.1:8088".to_string(),
        send_buf_size: 100,
        rate_limiters: Arc::new(None),
        calc_recv_msg_id: Arc::new(calc_test_msg_id),
        handle: handler,
        connect_timeout: Duration::from_millis(1000),
        call_timeout: Duration::from_millis(1000),
        heartbeat_interval: Duration::from_millis(1000),
    };

    let client = Client::new(config).unwrap();

    let send_msg = SendMsg::Text {
        msg_id: None,
        content: "test".to_string(),
        weight: None,
    };

    let result = client.send(send_msg).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), WsError::Client { .. }));
}

#[tokio::test]
async fn test_call_before_connect() {
    let (handler, _) = create_test_handler();
    let config = Config {
        url: "ws://127.0.0.1:8089".to_string(),
        send_buf_size: 100,
        rate_limiters: Arc::new(None),
        calc_recv_msg_id: Arc::new(calc_test_msg_id),
        handle: handler,
        connect_timeout: Duration::from_millis(1000),
        call_timeout: Duration::from_millis(1000),
        heartbeat_interval: Duration::from_millis(1000),
    };

    let client = Client::new(config).unwrap();

    let send_msg = SendMsg::Text {
        msg_id: Some("test".to_string()),
        content: "test".to_string(),
        weight: None,
    };

    let result = client.call(send_msg).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), WsError::Client { .. }));
}

#[tokio::test]
async fn test_multiple_concurrent_calls() {
    let server = MockWebSocketServer::new(8090).await;
    server.set_should_respond(true);
    server.set_should_echo(false);
    let _server_handle = server.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (handler, _) = create_test_handler();
    let config = Config {
        url: server.get_url(),
        send_buf_size: 100,
        rate_limiters: Arc::new(None),
        calc_recv_msg_id: Arc::new(calc_test_msg_id),
        handle: handler,
        connect_timeout: Duration::from_millis(1000),
        call_timeout: Duration::from_millis(2000),
        heartbeat_interval: Duration::from_millis(1000),
    };

    let client = Arc::new(Client::new(config).unwrap());
    client.connect().await.unwrap();

    let mut handles = Vec::new();

    // 启动多个并发调用
    for i in 0..5 {
        let client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let request = json!({
                "id": format!("concurrent-{}", i),
                "method": "test",
                "params": {"index": i}
            });

            let send_msg = SendMsg::Text {
                msg_id: Some(format!("concurrent-{}", i)),
                content: request.to_string(),
                weight: None,
            };

            client_clone.call(send_msg).await
        });
        handles.push(handle);
    }

    // 等待所有调用完成
    let mut success_count = 0;
    for handle in handles {
        if let Ok(Ok(_)) = handle.await {
            success_count += 1;
        }
    }

    assert_eq!(success_count, 5, "All concurrent calls should succeed");

    client.disconnect().await.unwrap();
    server.shutdown();
}
