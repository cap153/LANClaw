use crate::models::{PeerMap, TextMessage};
use crate::network::messaging;
use crate::rpc_client::RpcClient;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

#[allow(dead_code)]
pub struct BotConfig {
    pub name: String,
    pub model: String,
    pub thinking: String,
    pub bot_id: String,
    pub rpc: Arc<RpcClient>,
}

/// 消息路由器
pub async fn handle_message(
    msg: TextMessage,
    _peer_addr: SocketAddr,
    peers: PeerMap,
    config: &BotConfig,
) {
    // 忽略自己发出的消息
    if msg.from_id == config.bot_id {
        return;
    }

    let content = msg.content.trim().to_string();
    let from_id = msg.from_id.clone();
    let from_name = msg.from_name.clone();

    tracing::info!(
        "[Router] 来自 {} ({}): {}",
        from_name,
        from_id.chars().take(8).collect::<String>(),
        content.chars().take(60).collect::<String>()
    );

    // ─── /new 命令 ──────────────────────────────────────────────────
    if content == "/new" {
        match config.rpc.reset_session(&from_id).await {
            Ok(_) => {
                let reply = "🗑️ Session 已重置，开始全新对话。发送任意消息开始。";
                send_to_peer(&peers, &from_id, &config.name, reply, config).await;
            }
            Err(e) => {
                let reply = format!("❌ 重置失败: {}", e);
                send_to_peer(&peers, &from_id, &config.name, &reply, config).await;
            }
        }
        return;
    }

    // ─── 文件通知处理 ──────────────────────────────────────────────
    let is_file_notification = content.starts_with("[文件]");

    if is_file_notification {
        let files_dir = crate::config::files_dir();
        let latest_file = find_latest_file(&files_dir);

        if let Some(file_path) = latest_file {
            let file_name = file_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();

            tracing::info!("[Router] 新文件: {}", file_name);

            let is_image = matches!(
                file_path.extension().and_then(|s| s.to_str()),
                Some("jpg" | "jpeg" | "png" | "gif" | "webp" | "bmp" | "svg")
            );

            let prompt = if is_image {
                format!("分析这张图片的内容，描述你看到的一切。文件名: {}", file_name)
            } else {
                format!("阅读这个文件 ({}), 总结其内容。如果代码则分析代码。", file_name)
            };

            let result = config.rpc.prompt(&from_id, &prompt, &[file_path]).await;

            match result {
                Ok(pi_result) => {
                    send_to_peer(&peers, &from_id, &config.name, &pi_result.text, config).await;
                    for f in &pi_result.files {
                        send_file_to_user(&peers, &from_id, f, config).await;
                    }
                }
                Err(e) => {
                    let reply = format!("❌ 分析失败: {}", e);
                    send_to_peer(&peers, &from_id, &config.name, &reply, config).await;
                }
            }
        } else {
            let reply = "📁 已收到文件通知，但未找到文件数据。请稍后再试。";
            send_to_peer(&peers, &from_id, &config.name, reply, config).await;
        }
        return;
    }

    // ─── 普通文本 → 交给 pi ────────────────────────────────────
    let result = config.rpc.prompt(&from_id, &content, &[]).await;

    match result {
        Ok(pi_result) => {
            // 发送文本回复
            if !pi_result.text.is_empty() {
                send_to_peer(&peers, &from_id, &config.name, &pi_result.text, config).await;
            }

            // 发送生成的文件
            for file_path in &pi_result.files {
                send_file_to_user(&peers, &from_id, file_path, config).await;
            }
        }
        Err(e) => {
            let reply = format!("❌ pi 调用失败: {}", e);
            send_to_peer(&peers, &from_id, &config.name, &reply, config).await;
        }
    }
}

/// 通过 peer 地址发送文本消息
async fn send_to_peer(
    peers: &PeerMap,
    target_id: &str,
    bot_name: &str,
    content: &str,
    config: &BotConfig,
) {
    let addr = {
        let map = peers.read().await;
        map.get(target_id).map(|p| p.addr.clone())
    };

    match addr {
        Some(addr) => {
            if let Err(e) = messaging::send_text_message(
                &addr,
                config.bot_id.clone(),
                bot_name.to_string(),
                content.to_string(),
            )
            .await
            {
                tracing::error!("[Router] 发送失败: {}", e);
            }
        }
        None => {
            tracing::warn!("[Router] 用户 {} 不在线或未知", target_id);
        }
    }
}

/// 向用户发送文件
async fn send_file_to_user(
    peers: &PeerMap,
    target_id: &str,
    file_path: &PathBuf,
    config: &BotConfig,
) {
    if !file_path.exists() {
        return;
    }

    let addr = {
        let map = peers.read().await;
        map.get(target_id).map(|p| p.addr.clone())
    };

    if let Some(ref addr) = addr {
        tracing::info!("[Router] 发送文件 {} 给 {}", file_path.display(), target_id);
        if let Err(e) =
            crate::network::file::send_file_to_peer(addr, &config.bot_id, file_path).await
        {
            tracing::error!("[Router] 发送文件失败: {}", e);
        }
    }
}

/// 查找 files 目录中最新的文件
fn find_latest_file(dir: &std::path::Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(dir).ok()?;
    entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && !p.extension()
                    .and_then(|s| s.to_str())
                    .map_or(false, |ext| ext != "downloading")
        })
        .max_by_key(|p| {
            std::fs::metadata(p)
                .and_then(|m| m.modified())
                .ok()
        })
}
