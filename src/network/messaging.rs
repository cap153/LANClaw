use crate::models::{HandshakeMessage, TextMessage};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;

/// 消息通道：网络收到消息 → router 处理
pub type MessageSender = mpsc::UnboundedSender<(TextMessage, std::net::SocketAddr)>;
pub type MessageReceiver = mpsc::UnboundedReceiver<(TextMessage, std::net::SocketAddr)>;

pub fn message_channel() -> (MessageSender, MessageReceiver) {
    mpsc::unbounded_channel()
}

/// 向一个 peer 发送文本消息（首选 WebSocket，回退 TCP）
/// `min_timestamp` 可选：回复消息时传入原始消息的 timestamp，
/// 确保回复的时间戳 ≥ 原始消息时间戳，避免因时钟偏差导致排序错乱
pub async fn send_text_message(
    peer_addr: &str,
    from_id: String,
    from_name: String,
    content: String,
    min_timestamp: Option<u64>,
) -> Result<(), String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let timestamp = min_timestamp.map_or(now, |mt| mt.max(now));

    let message = TextMessage {
        msg_type: "text".to_string(),
        from_id,
        from_name,
        content,
        timestamp,
    };

    let json = serde_json::to_string(&message).map_err(|e| format!("序列化失败: {}", e))?;

    // 尝试 WebSocket
    let ws_url = format!("ws://{}/ws", peer_addr);
    match tokio_tungstenite::connect_async(&ws_url).await {
        Ok((mut ws_stream, _)) => {
            use tokio_tungstenite::tungstenite::Message as WsMessage;
            ws_stream
                .send(WsMessage::Text(json))
                .await
                .map_err(|e| format!("WebSocket 发送失败: {}", e))?;
            let _ = ws_stream.close(None).await;
            Ok(())
        }
        Err(e) => {
            // 回退 TCP
            eprintln!("[MSG] WS 连接失败 ({}), 尝试 TCP", e);
            send_via_tcp(peer_addr, &message).await
        }
    }
}

/// 通过原始 TCP 发送（LANChat 桌面端兼容）
async fn send_via_tcp(peer_addr: &str, message: &TextMessage) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;
    let mut stream = tokio::net::TcpStream::connect(peer_addr)
        .await
        .map_err(|e| format!("TCP 连接失败: {}", e))?;

    let json = serde_json::to_string(message).map_err(|e| format!("序列化失败: {}", e))?;
    let len = json.len() as u32;

    let mut buf = Vec::with_capacity(4 + json.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(json.as_bytes());

    stream
        .write_all(&buf)
        .await
        .map_err(|e| format!("TCP 发送失败: {}", e))?;

    Ok(())
}

/// 发送文件消息通知给 peer（告知对方有文件可下载）
#[allow(dead_code)]
pub async fn send_file_notification(
    peer_addr: &str,
    from_id: String,
    from_name: String,
    file_name: String,
    file_size: u64,
) -> Result<(), String> {
    // 文件消息使用 LANChat 格式：构造一个 file 类型的文本消息
    let content = format!("[文件] {} ({} 字节)", file_name, file_size);
    send_text_message(peer_addr, from_id, from_name, content, None).await
}

// ─── axum WebSocket Handler ────────────────────────────────────────────────

/// WebSocket 连接处理（axum 的 ws 回调）
/// 接收 LANChat 发来的消息，通过 channel 转发给 router
pub async fn handle_ws_connection(
    socket: WebSocket,
    tx: MessageSender,
    peer_addr: std::net::SocketAddr,
) {
    let (_sender, mut receiver) = socket.split();
    tracing::debug!("[WS] 新连接: {}", peer_addr);

    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                // 尝试解析为 TextMessage
                if let Ok(tm) = serde_json::from_str::<TextMessage>(&text) {
                    // 消息内容由 [RPC] prompt 日志覆盖
                    let _ = tx.send((tm, peer_addr));
                } else if let Ok(_hm) =
                    serde_json::from_str::<HandshakeMessage>(&text)
                {
                    tracing::debug!("[WS] 收到握手消息 (忽略)");
                } else {
                    tracing::warn!("[WS] 无法解析的消息: {}", text.chars().take(100).collect::<String>());
                }
            }
            Ok(Message::Close(_)) => {
                tracing::debug!("[WS] 连接关闭: {}", peer_addr);
                break;
            }
            Err(e) => {
                tracing::error!("[WS] 错误: {}", e);
                break;
            }
            _ => {}
        }
    }
}
