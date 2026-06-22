use axum::extract::{Multipart, Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::{Json, Router};
use std::sync::Arc;

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
        .route("/api/download/{file_id}", axum::routing::get(download_handler))
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

    while let Some(mut field) = multipart.next_field().await.ok().flatten() {
        let field_name = field.name().map(|s| s.to_string()).unwrap_or_default();
        match field_name.as_str() {
            "peer_id" => sender_id = field.text().await.unwrap_or_default(),
            "file_name" => file_name = field.text().await.unwrap_or_default(),
            "file_size" => {
                file_size = field.text().await.unwrap_or_default().parse().unwrap_or(0)
            }
            "chunk_index" => {
                chunk_index = field.text().await.unwrap_or_default().parse().unwrap_or(0)
            }
            "chunk_total" => {
                chunk_total = field.text().await.unwrap_or_default().parse().unwrap_or(0)
            }
            "chunk" => {
                let mut data = Vec::new();
                while let Ok(Some(chunk)) = field.chunk().await {
                    data.extend_from_slice(&chunk);
                }
                chunk_data = Some(data);
            }
            _ => {}
        }
    }

    if chunk_data.is_none() {
        return (StatusCode::BAD_REQUEST, "缺少 chunk 数据").into_response();
    }

    let chunk_data = chunk_data.unwrap();
    let files_dir = config::files_dir();

    // ── 第一块：智能命名协商（含秒传/重命名） ───────────────────────────
    let final_name: String;

    if chunk_index == 0 {
        // 拆分主文件名和扩展名
        let (stem, ext) = {
            let p = std::path::Path::new(&file_name);
            let s = p
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or(&file_name)
                .to_string();
            let e = p
                .extension()
                .and_then(|s| s.to_str())
                .map(|e| format!(".{}", e))
                .unwrap_or_default();
            (s, e)
        };

        let candidate = files_dir.join(&file_name);
        let downloading_path = files_dir.join(format!("{}.downloading", file_name));

        if candidate.exists() && !downloading_path.exists() {
            // 目标文件完整，比较大小
            let existing_size = tokio::fs::metadata(&candidate)
                .await
                .map(|m| m.len())
                .unwrap_or(0);

            if existing_size == file_size && file_size > 0 {
                // 大小完全相同 → 秒传
                tracing::info!(
                    "[File] ✓ 秒传命中: {:?} (大小相同: {} 字节)",
                    candidate, file_size
                );
                // 通知发送端停止，同时触发文件处理事件（引用已有文件）
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
            } else {
                // 大小不同 → 冲突，需要重命名
                tracing::info!(
                    "[File] 同名文件大小不同 (已有: {}, 新: {})，触发重命名",
                    existing_size, file_size
                );
            }
        }

        // 找一个不冲突的文件名（原名冲突或 .downloading 残留）
        let mut resolved_name = file_name.clone();
        if candidate.exists() || downloading_path.exists() {
            let mut i = 1usize;
            loop {
                let candidate_name = format!("{}({}){}", stem, i, ext);
                let cp = files_dir.join(&candidate_name);
                let dp = files_dir.join(format!("{}.downloading", candidate_name));
                if !cp.exists() && !dp.exists() {
                    resolved_name = candidate_name;
                    tracing::info!("[File] 重命名为: {}", resolved_name);
                    break;
                }
                i += 1;
                if i > 9999 {
                    resolved_name =
                        format!("{}_{}{}", stem, chrono::Utc::now().timestamp(), ext);
                    break;
                }
            }
        }

        final_name = resolved_name;
    } else {
        final_name = file_name.clone();
    }

    let temp_path = files_dir.join(format!("{}.downloading", final_name));
    let final_path = files_dir.join(&final_name);

    // 第一块时清理旧临时文件
    if chunk_index == 0 {
        let _ = tokio::fs::remove_file(&temp_path).await;
    }

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
        match tokio::fs::rename(&temp_path, &final_path).await {
            Ok(_) => {
                let size_str = if file_size >= 1024 * 1024 {
                    format!("{:.1} MB", file_size as f64 / 1048576.0)
                } else if file_size >= 1024 {
                    format!("{:.1} KB", file_size as f64 / 1024.0)
                } else {
                    format!("{} B", file_size)
                };
                tracing::info!(
                    "[File] 文件接收完成: {} ({}) 来自 {}",
                    final_name,
                    size_str,
                    sender_id
                );
                // 自动触发 pi 处理
                let _ = state.file_complete_tx.send(FileCompleteEvent {
                    sender_id: sender_id.clone(),
                    file_path: final_path.clone(),
                    file_name: final_name.clone(),
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
fn calculate_optimal_chunk_size(file_size: u64) -> u64 {
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

    // 尝试读取系统可用内存（Linux）作为上限
    #[cfg(target_os = "linux")]
    if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
        for line in content.lines() {
            if let Some(rest) = line.strip_prefix("MemAvailable:") {
                if let Ok(kb) = rest.trim().trim_end_matches(" kB").parse::<u64>() {
                    let max_chunk = (kb * 1024) * 80 / 100; // 可用内存的 80%
                    return base_size.min(max_chunk);
                }
            }
        }
    }

    base_size
}

/// 向目标用户发送文件（通过接收端的 upload API）
pub async fn send_file_to_peer(
    peer_addr: &str,
    my_id: &str,
    file_path: &std::path::Path,
) -> Result<(), String> {
    let file_name = file_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("file");
    let file_size = tokio::fs::metadata(file_path)
        .await
        .map_err(|e| format!("读取文件属性失败: {}", e))?
        .len();

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|e| format!("创建 HTTP 客户端失败: {}", e))?;

    let upload_url = format!("http://{}/api/upload", peer_addr);
    let chunk_size = calculate_optimal_chunk_size(file_size);
    let total_chunks = (file_size + chunk_size - 1) / chunk_size;

    tracing::info!(
        "[File] 发送 {} ({} 字节)，分块大小 {} MB，共 {} 块",
        file_name,
        file_size,
        chunk_size / (1024 * 1024),
        total_chunks,
    );

    // 打开文件句柄（后续每块用 try_clone 获取独立句柄做零拷贝流式读取）
    let src_file = tokio::fs::File::open(file_path)
        .await
        .map_err(|e| format!("打开文件失败: {}", e))?;

    for chunk_index in 0..total_chunks {
        let offset = chunk_index * chunk_size;
        let this_chunk_size = chunk_size.min(file_size - offset);

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
            tracing::info!(
                "[File] 发送 {}: {}/{} 块",
                file_name,
                chunk_index + 1,
                total_chunks
            );
        }
    }

    tracing::info!("[File] 文件发送完成: {}", file_name);
    Ok(())
}
