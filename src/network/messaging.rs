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

/// 流式发送：通过同一个 WebSocket 逐 chunk 发送回复内容
/// 收到 `chunks` 关闭时自动标记最后一条为 `stream_final: true`
pub async fn send_stream_chunks(
    peer_addr: &str,
    from_id: String,
    from_name: String,
    mut chunks: mpsc::Receiver<String>,
    min_timestamp: u64,
) -> Result<(), String> {
    let ws_url = format!("ws://{}/ws", peer_addr);
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .map_err(|e| format!("WS 连接失败: {}", e))?;

    let stream_id = uuid::Uuid::new_v4().to_string();
    let mut pending: Option<String> = None;
    // 所有 chunk 使用相同时间戳（紧跟在用户消息之后），防止 seq 累加导致时间戳偏大
    let ts = min_timestamp.saturating_add(1);

    use tokio_tungstenite::tungstenite::Message as WsMessage;

    loop {
        let chunk = chunks.recv().await;
        match chunk {
            Some(text) => {
                // 发送前一段（非 final）
                if let Some(prev) = pending.take() {
                    let json = stream_json(&from_id, &from_name, &prev, ts, &stream_id, false);
                    ws_stream.send(WsMessage::Text(json.into())).await.map_err(|e| e.to_string())?;
                }
                pending = Some(text);
            }
            None => {
                // 通道关闭 → 最后一段标记 final
                if let Some(last) = pending.take() {
                    let json = stream_json(&from_id, &from_name, &last, ts, &stream_id, true);
                    ws_stream.send(WsMessage::Text(json.into())).await.map_err(|e| e.to_string())?;
                }
                break;
            }
        }
    }

    let _ = ws_stream.close(None).await;
    Ok(())
}

fn stream_json(from_id: &str, from_name: &str, content: &str, timestamp: u64, stream_id: &str, is_final: bool) -> String {
    serde_json::json!({
        "msg_type": "text",
        "from_id": from_id,
        "from_name": from_name,
        "content": content,
        "timestamp": timestamp,
        "stream_id": stream_id,
        "stream_final": is_final,
    }).to_string()
}

/// 发送文件消息通知给 peer（告知对方有文件可下载）"}]
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
