use axum::extract::{DefaultBodyLimit, Multipart, Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::{Json, Router};
use futures_util::SinkExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use crate::config;
use crate::models::FileCompleteEvent;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::io::ReaderStream;

/// 应用层共享状态
#[derive(Clone)]
pub struct AppState {
    pub msg_tx: crate::network::messaging::MessageSender,
    pub file_complete_tx: mpsc::UnboundedSender<FileCompleteEvent>,
}

/// 构建文件路由
pub fn file_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/upload", axum::routing::post(upload_handler))
        .route_layer(DefaultBodyLimit::disable())
        .route("/api/download/{file_id}", axum::routing::get(download_handler))
}

/// 分块上传中的临时文件索引（sender_id → .downloading 路径）
static PENDING_CHUNKS: OnceLock<Mutex<HashMap<String, PendingChunkFile>>> = OnceLock::new();

struct PendingChunkFile {
    /// 完整后的最终文件名
    final_name: String,
}

fn pending_chunks() -> &'static Mutex<HashMap<String, PendingChunkFile>> {
    PENDING_CHUNKS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 上传文件处理（兼容 LANChat 分块协议）
async fn upload_handler(
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let mut file_name = String::new();
    let mut file_size: u64 = 0;
    let mut chunk_index: usize = 0;
    let mut chunk_total: usize = 0;
    let mut chunk_data: Option<Vec<u8>> = None;
    let mut sender_id = String::new();

    loop {
        let mut field = match multipart.next_field().await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => {
                eprintln!("[File] 解析 multipart 失败: {}", e);
                return (StatusCode::BAD_REQUEST, format!("multipart 解析错误: {}", e)).into_response();
            }
        };
        let field_name = match field.name() {
            Some(n) => n.to_string(),
            None => continue,
        };
        match field_name.as_str() {
            "peer_id" => {
                sender_id = field.text().await.unwrap_or_default();
            }
            "file_name" => {
                file_name = field.text().await.unwrap_or_default();
            }
            "file_size" => {
                file_size = field.text().await.unwrap_or_default().parse().unwrap_or(0);
            }
            "chunk_index" => {
                chunk_index = field.text().await.unwrap_or_default().parse().unwrap_or(0);
            }
            "chunk_total" => {
                chunk_total = field.text().await.unwrap_or_default().parse().unwrap_or(0);
            }
            "chunk" => {
                let mut data = Vec::new();
                loop {
                    match field.chunk().await {
                        Ok(Some(chunk)) => data.extend_from_slice(&chunk),
                        Ok(None) => break,
                        Err(e) => {
                            eprintln!("[File] 读取分块数据失败: {}", e);
                            return (StatusCode::INTERNAL_SERVER_ERROR, format!("读取分块失败: {}", e)).into_response();
                        }
                    }
                }
                chunk_data = Some(data);
            }
            _ => {
                // 消耗未识别字段的数据以推进 multipart 流
                while let Ok(Some(_)) = field.chunk().await {}
            }
        }
    }

    let chunk_data = match chunk_data {
        Some(d) => d,
        None => return (StatusCode::BAD_REQUEST, "缺少 chunk 数据").into_response(),
    };

    if chunk_total <= 1 && file_size > 0 && (chunk_data.len() as u64) < file_size {
        tracing::warn!(
            "[File] 分块数据不完整: 收到 {} 字节, 期望 {} 字节 (单分块传输)",
            chunk_data.len(), file_size
        );
        // 不返回错误，继续写入已收到的部分
    }
    let files_dir = config::files_dir();

    // ── 第一块：秒传检查 + 确定写入路径 ──
    if chunk_index == 0 {
        let candidate = files_dir.join(&file_name);
        let downloading_path = files_dir.join(format!("{}.downloading", file_name));

        if candidate.exists() && !downloading_path.exists() {
            if let Ok(existing_size) = tokio::fs::metadata(&candidate).await.map(|m| m.len()) {
                if existing_size == file_size && file_size > 0 {
                    tracing::info!(
                        "[File] ✓ 秒传命中: {:?} (大小相同: {} 字节)",
                        candidate, file_size
                    );
                    let _ = state.file_complete_tx.send(FileCompleteEvent {
                        sender_id: sender_id.clone(),
                        file_path: candidate.clone(),
                        file_name: file_name.clone(),
                    });
                    return Json(serde_json::json!({
                        "status": "already_exists",
                        "file_name": file_name,
                        "file_size": file_size,
                        "message": "文件已存在且完整，秒传成功",
                    }))
                    .into_response();
                }
            }
        }

        // 始终用原始 file_name 作为写入目标（多分块写入同一文件）
        let pending_path = files_dir.join(format!("{}.downloading", file_name));
        let _ = tokio::fs::remove_file(&pending_path).await;
        pending_chunks().lock().unwrap().insert(
            sender_id.clone(),
            PendingChunkFile {
                final_name: file_name.clone(),
            },
        );
    }

    // ── 查询当前传输的最终文件名 ──
    let final_name = {
        let map = pending_chunks().lock().unwrap();
        match map.get(&sender_id) {
            Some(info) => info.final_name.clone(),
            None => file_name.clone(),
        }
    };

    let temp_path = files_dir.join(format!("{}.downloading", final_name));
    let final_path = files_dir.join(&final_name);

    // 追加写入
    let file = match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&temp_path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[File] 打开文件失败: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("IO 错误: {}", e),
            )
                .into_response();
        }
    };

    let mut writer = tokio::io::BufWriter::new(file);
    if let Err(e) = writer.write_all(&chunk_data).await {
        eprintln!("[File] 写入失败: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, "写入失败").into_response();
    }
    writer.flush().await.unwrap_or_default();

    // 检查是否所有块都写完了
    let temp_size = tokio::fs::metadata(&temp_path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);

    if temp_size >= file_size && file_size > 0 {
        // 清理 pending 记录
        pending_chunks().lock().unwrap().remove(&sender_id);

        // 若最终文件已存在，重命名避让
        let target_path = if final_path.exists() {
            let (stem, ext) = split_file_stem_ext(&final_name);
            let mut i = 1usize;
            loop {
                let renamed = format!("{}({}){}", stem, i, ext);
                let p = files_dir.join(&renamed);
                if !p.exists() {
                    tracing::info!("[File] 目标已存在，重命名为: {}", renamed);
                    break p;
                }
                i += 1;
                if i > 9999 {
                    let ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    break files_dir.join(format!("{}_{}{}", stem, ts, ext));
                }
            }
        } else {
            final_path.clone()
        };

        match tokio::fs::rename(&temp_path, &target_path).await {
            Ok(_) => {
                let size_str = if file_size >= 1024 * 1024 {
                    format!("{:.1} MB", file_size as f64 / 1048576.0)
                } else if file_size >= 1024 {
                    format!("{:.1} KB", file_size as f64 / 1024.0)
                } else {
                    format!("{} B", file_size)
                };
                tracing::info!(
                    "[File] ✓ 文件接收完成: {} ({}) 来自 {}",
                    target_path.file_name().and_then(|s| s.to_str()).unwrap_or(&final_name),
                    size_str,
                    sender_id
                );
                let _ = state.file_complete_tx.send(FileCompleteEvent {
                    sender_id: sender_id.clone(),
                    file_path: target_path.clone(),
                    file_name: target_path.file_name().and_then(|s| s.to_str()).unwrap_or(&final_name).to_string(),
                });
            }
            Err(e) => {
                eprintln!("[File] 重命名失败: {}", e);
            }
        }
    }

    Json(serde_json::json!({
        "status": "success",
        "file_name": final_name,
        "file_size": file_size,
        "chunk_index": chunk_index,
        "chunk_total": chunk_total,
    }))
    .into_response()
}

fn split_file_stem_ext(name: &str) -> (String, String) {
    let p = std::path::Path::new(name);
    let s = p
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(name)
        .to_string();
    let e = p
        .extension()
        .and_then(|s| s.to_str())
        .map(|e| format!(".{}", e))
        .unwrap_or_default();
    (s, e)
}

/// 文件下载
async fn download_handler(Path(file_id): Path<String>) -> impl IntoResponse {
    let files_dir = config::files_dir();
    let path = files_dir.join(&file_id);

    match tokio::fs::read(&path).await {
        Ok(data) => {
            let content_type: String = if file_id.ends_with(".jpg") || file_id.ends_with(".jpeg") {
                "image/jpeg".into()
            } else if file_id.ends_with(".png") {
                "image/png".into()
            } else if file_id.ends_with(".gif") {
                "image/gif".into()
            } else if file_id.ends_with(".webp") {
                "image/webp".into()
            } else if file_id.ends_with(".svg") {
                "image/svg+xml".into()
            } else {
                mime_guess::from_path(&file_id)
                    .first_or_octet_stream()
                    .to_string()
            };

            ([(header::CONTENT_TYPE, content_type.as_str())], data).into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "文件不存在").into_response(),
    }
}

// ─── 向 LANChat/LANClaw 用户发送文件 ───────────────────────────────────────

/// 根据文件大小和设备内存计算最优分块大小（零拷贝友好）
/// `receiver_memory_mb` = 接收方可用内存（MB），0 表示未知
fn calculate_optimal_chunk_size(file_size: u64, receiver_memory_mb: u64) -> u64 {
    // 基础分块策略（匹配 LANChat web 端的 baseChunkSize 逻辑）
    let base_size = if file_size < 100 * 1024 * 1024 {
        100 * 1024 * 1024
    } else if file_size < 500 * 1024 * 1024 {
        200 * 1024 * 1024
    } else if file_size < 1024 * 1024 * 1024 {
        300 * 1024 * 1024
    } else if file_size < 5 * 1024 * 1024 * 1024 {
        400 * 1024 * 1024
    } else {
        500 * 1024 * 1024
    };

    // 根据发送方可用内存限制（Linux 读取 /proc/meminfo）
    let mut sender_limit = u64::MAX;
    #[cfg(target_os = "linux")]
    if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
        for line in content.lines() {
            if let Some(rest) = line.strip_prefix("MemAvailable:") {
                if let Ok(kb) = rest.trim().trim_end_matches(" kB").parse::<u64>() {
                    sender_limit = (kb * 1024) * 80 / 100; // 可用内存的 80%
                    break;
                }
            }
        }
    };

    // 根据接收方可用内存限制（LANChat 同款公式）
    let receiver_limit = if receiver_memory_mb > 0 {
        std::cmp::max(50 * 1024 * 1024, receiver_memory_mb as u64 * 1024 * 1024 / 4)
    } else {
        u64::MAX
    };

    base_size.min(sender_limit).min(receiver_limit)
}

/// 向目标用户发送文件（通过接收端的 upload API）
pub async fn send_file_to_peer(
    peer_addr: &str,
    my_id: &str,
    user_name: &str,
    file_path: &std::path::Path,
    sender_msg_id: i64,
    receiver_memory_mb: u64,
) -> Result<(), String> {
    let file_name = file_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("file");
    let file_size = tokio::fs::metadata(file_path)
        .await
        .map_err(|e| format!("读取文件属性失败: {}", e))?
        .len();

    // ── 发送 file_offer 通知，让接收端创建消息记录 ──
    let offer = serde_json::json!({
        "msg_type": "file_offer",
        "from_id": my_id,
        "from_name": user_name,
        "file_name": file_name,
        "file_size": file_size,
        "sender_msg_id": sender_msg_id,
    });
    let ws_url = format!("ws://{}/ws", peer_addr);
    if let Ok((mut ws_stream, _)) = tokio_tungstenite::connect_async(&ws_url).await {
        use tokio_tungstenite::tungstenite::Message as WsMessage;
        let _ = ws_stream.send(WsMessage::Text(offer.to_string())).await;
        let _ = ws_stream.close(None).await;
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|e| format!("创建 HTTP 客户端失败: {}", e))?;

    let upload_url = format!("http://{}/api/upload", peer_addr);
    let chunk_size = calculate_optimal_chunk_size(file_size, receiver_memory_mb);
    let total_chunks = (file_size + chunk_size - 1) / chunk_size;

    tracing::info!(
        "[File] 发送 {} ({} 字节)，分块大小 {} MB，共 {} 块 (接收方内存: {} MB)",
        file_name,
        file_size,
        chunk_size / (1024 * 1024),
        total_chunks,
        receiver_memory_mb,
    );

    // 打开文件句柄（后续每块用 try_clone 获取独立句柄做零拷贝流式读取）
    let src_file = tokio::fs::File::open(file_path)
        .await
        .map_err(|e| format!("打开文件失败: {}", e))?;

    let start_time = std::time::Instant::now();
    let mut total_sent: u64 = 0;

    for chunk_index in 0..total_chunks {
        let offset = chunk_index * chunk_size;
        let this_chunk_size = chunk_size.min(file_size - offset);

        // 计算速度（LANChat web 端同款公式：offset / (1024*1024) / elapsed）
        let speed_mb_s = if chunk_index > 0 {
            let elapsed = start_time.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                (total_sent as f64 / (1024.0 * 1024.0)) / elapsed
            } else {
                0.0
            }
        } else {
            0.0
        };

        // 为该块创建独立文件句柄并 seek 到对应偏移（零拷贝：ReaderStream 直接从内核读取）
        let mut chunk_handle = src_file
            .try_clone()
            .await
            .map_err(|e| format!("克隆文件句柄失败: {}", e))?;
        chunk_handle
            .seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|e| format!("文件 seek 失败: {}", e))?;

        // 限制只读取 this_chunk_size 字节
        let bounded = chunk_handle.take(this_chunk_size);
        let stream = ReaderStream::new(bounded);
        let body = reqwest::Body::wrap_stream(stream);

        let form = reqwest::multipart::Form::new()
            .text("peer_id", my_id.to_string())
            .text("file_name", file_name.to_string())
            .text("file_size", file_size.to_string())
            .text("chunk_index", chunk_index.to_string())
            .text("chunk_total", total_chunks.to_string())
            .text("sender_msg_id", sender_msg_id.to_string())
            .text("speed_mb_s", format!("{:.1}", speed_mb_s))
            .part(
                "chunk",
                reqwest::multipart::Part::stream_with_length(body, this_chunk_size)
                    .mime_str("application/octet-stream")
                    .unwrap(),
            );

        let resp = client
            .post(&upload_url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| format!("上传分块 {} 失败: {}", chunk_index + 1, e))?;

        if !resp.status().is_success() {
            return Err(format!("HTTP {} (分块 {})", resp.status(), chunk_index + 1));
        }

        total_sent += this_chunk_size;

        // 第一块后检查秒传命中
        if chunk_index == 0 {
            if let Ok(resp_data) = resp.json::<serde_json::Value>().await {
                if resp_data
                    .get("status")
                    .and_then(|s| s.as_str())
                    == Some("already_exists")
                {
                    tracing::info!(
                        "[File] ✓ 接收端已有完整文件，秒传完成: {}",
                        file_name
                    );
                    return Ok(());
                }
            }
        }

        if (chunk_index + 1) % 5 == 0 || chunk_index + 1 == total_chunks {
            let elapsed = start_time.elapsed().as_secs_f64();
            let avg_speed = if elapsed > 0.0 {
                (total_sent as f64 / (1024.0 * 1024.0)) / elapsed
            } else {
                0.0
            };
            tracing::info!(
                "[File] 发送 {}: {}/{} 块 ({:.1} MB/s)",
                file_name,
                chunk_index + 1,
                total_chunks,
                avg_speed,
            );
        }
    }

    let total_elapsed = start_time.elapsed().as_secs_f64();
    let avg_speed = if total_elapsed > 0.0 {
        (file_size as f64 / (1024.0 * 1024.0)) / total_elapsed
    } else {
        0.0
    };
    tracing::info!(
        "[File] ✓ 文件发送完成: {} ({:.1} MB/s)",
        file_name,
        avg_speed,
    );
    Ok(())
}
