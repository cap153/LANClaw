use crate::models::{HandshakeMessage, StreamChunk, TextMessage};
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
            eprintln!("[MSG] WS 连接失败 ({}), 尝试 TCP", e);
            send_via_tcp(peer_addr, &message).await
        }
    }
}

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

/// 流式发送。
/// - thinking/text 的每个 delta 都实时发 WS（保证流式效果）
/// - final_segments 只积累每个 phase 的最终值（用于 DB 保存的完整摘要）
/// - stream_final: true 只出现在 segs_content 消息上（确保只存一条）
pub async fn send_stream_chunks(
    peer_addr: &str,
    from_id: String,
    from_name: String,
    mut chunks: mpsc::Receiver<StreamChunk>,
    min_timestamp: u64,
) -> Result<(), String> {
    let ws_url = format!("ws://{}/ws", peer_addr);
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .map_err(|e| format!("WS 连接失败: {}", e))?;

    let stream_id = uuid::Uuid::new_v4().to_string();
    let ts = min_timestamp.saturating_add(1);

    use tokio_tungstenite::tungstenite::Message as WsMessage;

    // 积累最终段落（每个连续 phase 只产生一段）
    let mut final_segments: Vec<StreamChunk> = Vec::new();
    let mut pending_thinking: Option<String> = None;
    let mut pending_text: Option<String> = None;

    /// 将 pending_thinking 刷入 final_segments
    fn flush_thinking(segments: &mut Vec<StreamChunk>, buf: &mut Option<String>) {
        if let Some(content) = buf.take() {
            segments.push(StreamChunk::Text { content, is_thinking: true });
        }
    }

    /// 将 pending_text 刷入 final_segments
    fn flush_text_final(segments: &mut Vec<StreamChunk>, buf: &mut Option<String>) {
        if let Some(content) = buf.take() {
            segments.push(StreamChunk::Text { content, is_thinking: false });
        }
    }

    loop {
        let chunk = chunks.recv().await;
        match chunk {
            Some(StreamChunk::Text { content, is_thinking }) if is_thinking => {
                // 实时 WS（流式）
                let json = stream_chunk_json(&from_id, &from_name, &content, ts, &stream_id, false, true);
                let _ = ws_stream.send(WsMessage::Text(json.into())).await;
                // 积累最终值
                pending_thinking = Some(content);
            }
            Some(StreamChunk::Text { content, is_thinking: false }) => {
                // 实时 WS
                let json = stream_chunk_json(&from_id, &from_name, &content, ts, &stream_id, false, false);
                let _ = ws_stream.send(WsMessage::Text(json.into())).await;
                // 如果之前有 thinking 段，结束它
                flush_thinking(&mut final_segments, &mut pending_thinking);
                // 积累最终 text
                pending_text = Some(content);
            }
            Some(StreamChunk::ToolCall { name, args }) => {
                // 结束当前 thinking / text phase
                flush_thinking(&mut final_segments, &mut pending_thinking);
                flush_text_final(&mut final_segments, &mut pending_text);
                // tool_call 实时 WS
                let json = serde_json::json!({
                    "msg_type": "tool_call",
                    "from_id": &from_id,
                    "from_name": &from_name,
                    "tool_name": &name,
                    "tool_args": &args,
                    "timestamp": ts,
                    "stream_id": &stream_id,
                    "stream_final": false,
                }).to_string();
                let _ = ws_stream.send(WsMessage::Text(json.into())).await;
                final_segments.push(StreamChunk::ToolCall { name, args });
            }
            Some(StreamChunk::ToolResult { output, is_error }) => {
                flush_thinking(&mut final_segments, &mut pending_thinking);
                flush_text_final(&mut final_segments, &mut pending_text);
                let json = serde_json::json!({
                    "msg_type": "tool_result",
                    "from_id": &from_id,
                    "from_name": &from_name,
                    "tool_output": &output,
                    "is_error": is_error,
                    "timestamp": ts,
                    "stream_id": &stream_id,
                    "stream_final": false,
                }).to_string();
                let _ = ws_stream.send(WsMessage::Text(json.into())).await;
                final_segments.push(StreamChunk::ToolResult { output, is_error });
            }
            Some(other) => {
                final_segments.push(other);
            }
            None => {
                // 通道关闭：结束最后一段 thinking/text，然后只发 segs_content（stream_final: true）
                flush_thinking(&mut final_segments, &mut pending_thinking);
                flush_text_final(&mut final_segments, &mut pending_text);

                // 发送完整段落 JSON（stream_final: true，只有这一条会被存 DB）
                let segs: Vec<serde_json::Value> = final_segments.iter().map(|s| match s {
                    StreamChunk::Text { content, is_thinking } => {
                        serde_json::json!({"type": if *is_thinking { "thinking" } else { "text" }, "content": content})
                    }
                    StreamChunk::ToolCall { name, args } => {
                        serde_json::json!({"type": "tool_call", "name": name, "args": args})
                    }
                    StreamChunk::ToolResult { output, is_error } => {
                        serde_json::json!({"type": "tool_result", "output": output, "is_error": is_error})
                    }
                }).collect();

                let summary_json = serde_json::json!({
                    "msg_type": "text",
                    "from_id": &from_id,
                    "from_name": &from_name,
                    "content": serde_json::json!({"v": 2, "segments": segs}).to_string(),
                    "timestamp": ts,
                    "stream_id": &stream_id,
                    "stream_final": true,
                    "is_thinking": false,
                    "segs_content": true,
                }).to_string();
                let _ = ws_stream.send(WsMessage::Text(summary_json.into())).await;
                break;
            }
        }
    }

    let _ = ws_stream.close(None).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}

fn stream_chunk_json(from_id: &str, from_name: &str, content: &str, timestamp: u64, stream_id: &str, is_final: bool, is_thinking: bool) -> String {
    serde_json::json!({
        "msg_type": "text",
        "from_id": from_id,
        "from_name": from_name,
        "content": content,
        "timestamp": timestamp,
        "stream_id": stream_id,
        "stream_final": is_final,
        "is_thinking": is_thinking,
    }).to_string()
}

#[allow(dead_code)]
pub async fn send_file_notification(
    peer_addr: &str,
    from_id: String,
    from_name: String,
    file_name: String,
    file_size: u64,
) -> Result<(), String> {
    let content = format!("[文件] {} ({} 字节)", file_name, file_size);
    send_text_message(peer_addr, from_id, from_name, content, None).await
}

// ─── axum WebSocket Handler ────────────────────────────────────────────────

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
                if let Ok(tm) = serde_json::from_str::<TextMessage>(&text) {
                    let _ = tx.send((tm, peer_addr));
                } else if let Ok(_hm) =
                    serde_json::from_str::<HandshakeMessage>(&text)
                {
                    tracing::debug!("[WS] 收到握手消息 (忽略)");
                } else if let Some(msg_type) =
                    serde_json::from_str::<serde_json::Value>(&text).ok()
                        .and_then(|v| v.get("msg_type").and_then(|t| t.as_str()).map(|s| s.to_string()))
                {
                    match msg_type.as_str() {
                        "file_offer" | "file_request" | "file_accept" | "file_not_found"
                        | "file_status_update" | "file_download_progress" | "start_upload" => {
                            tracing::trace!("[WS] 收到文件协议消息 (忽略): {}", msg_type);
                        }
                        _ => {
                            tracing::debug!("[WS] 未知消息类型: {}", msg_type);
                        }
                    }
                } else {
                    tracing::debug!("[WS] 无法解析的消息: {}", text.chars().take(100).collect::<String>());
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
