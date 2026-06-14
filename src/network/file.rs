use axum::extract::{Multipart, Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::{Json, Router};
use std::path::PathBuf;
use std::sync::Arc;

use crate::config;

/// 应用层共享状态
#[derive(Clone)]
pub struct AppState {
    pub msg_tx: crate::network::messaging::MessageSender,
}

/// 构建文件路由
pub fn file_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/upload", axum::routing::post(upload_handler))
        .route("/api/download/{file_id}", axum::routing::get(download_handler))
}

/// 上传文件处理（兼容 LANChat 分块协议）
async fn upload_handler(
    State(_state): State<Arc<AppState>>,
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

    // 第一块时确定最终文件名（处理重名冲突）
    let final_name = if chunk_index == 0 {
        resolve_file_name(&files_dir, &file_name)
    } else {
        file_name.clone()
    };

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

    use tokio::io::AsyncWriteExt;
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

/// 重名文件自动重命名
fn resolve_file_name(dir: &PathBuf, name: &str) -> String {
    let candidate = dir.join(name);
    if !candidate.exists() {
        return name.to_string();
    }

    let stem = std::path::Path::new(name)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(name);
    let ext = std::path::Path::new(name)
        .extension()
        .and_then(|s| s.to_str())
        .map(|e| format!(".{}", e))
        .unwrap_or_default();

    for i in 1..1000 {
        let new_name = format!("{}({}){}", stem, i, ext);
        if !dir.join(&new_name).exists() {
            return new_name;
        }
    }
    format!("{}_{}{}", stem, chrono::Utc::now().timestamp(), ext)
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

// ─── 向 LANChat 用户发送文件 ───────────────────────────────────────────────

/// 向目标用户发送文件（通过 LANChat 的 upload API）
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
    let chunk_size = 50 * 1024 * 1024;
    let total_chunks = (file_size + chunk_size - 1) / chunk_size;

    let mut file = tokio::fs::File::open(file_path)
        .await
        .map_err(|e| format!("打开文件失败: {}", e))?;

    for chunk_index in 0..total_chunks {
        let mut buf = vec![0u8; chunk_size as usize];
        use tokio::io::AsyncReadExt;
        let n = file.read(&mut buf).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        buf.truncate(n);

        let form = reqwest::multipart::Form::new()
            .text("peer_id", my_id.to_string())
            .text("file_name", file_name.to_string())
            .text("file_size", file_size.to_string())
            .text("chunk_index", chunk_index.to_string())
            .text("chunk_total", total_chunks.to_string())
            .part(
                "chunk",
                reqwest::multipart::Part::bytes(buf)
                    .mime_str("application/octet-stream")
                    .unwrap(),
            );

        let resp = client
            .post(&upload_url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| format!("上传分块失败: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("HTTP {}", resp.status()));
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
