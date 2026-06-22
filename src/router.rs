use crate::models::{PeerMap, StreamChunk, TextMessage};
use crate::network::messaging;
use crate::rpc_client::RpcClient;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

pub struct BotConfig {
    pub name: String,
    pub bot_id: String,
    pub rpc: Arc<RpcClient>,
    /// 模型切换中标志（防止重复点击）
    pub switching_model: AtomicBool,
    /// 用户 `!` 命令积累（user_id → 命令输出文本列表）
    pub pending_bash: Arc<Mutex<HashMap<String, Vec<String>>>>,
    /// 当前运行中的 bash 取消令牌（user_id → token）
    pub bash_tokens: Arc<Mutex<HashMap<String, CancellationToken>>>,
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

    // 路由信息由 [RPC] prompt 日志覆盖

    // ─── /new 命令 ──────────────────────────────────────────────────
    if content == "/new" {
        // 先强制打断卡住的 RPC（不经过 rpc_mutex，直接杀子进程）
        config.rpc.kill_child().await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // 清空积累 + 取消运行中的 bash
        {
            let mut map = config.pending_bash.lock().await;
            map.remove(&from_id);
        }
        cancel_user_bash(config, &from_id).await;

        // 子进程已被杀，先重启再重置 session
        let cfg = crate::config::Config::load();
        match config.rpc.restart(&cfg.model, &cfg.thinking).await {
            Ok(_) => {
                match config.rpc.reset_session(&from_id).await {
                    Ok(_) => {
                        let reply = "🗑️ Session 已重置，开始全新对话。发送任意消息开始。";
                        send_to_peer(&peers, &from_id, &config.name, reply, config, Some(msg.timestamp + 1)).await;
                    }
                    Err(e) => {
                        let reply = format!("❌ 重置失败: {}", e);
                        send_to_peer(&peers, &from_id, &config.name, &reply, config, Some(msg.timestamp + 1)).await;
                    }
                }
            }
            Err(e) => {
                let reply = format!("❌ 重启 pi 失败: {}", e);
                send_to_peer(&peers, &from_id, &config.name, &reply, config, Some(msg.timestamp + 1)).await;
            }
        }
        return;
    }

    // ─── /model 命令 ───────────────────────────────────────────────
    if content == "/model" {
        // 先强制打断卡住的 RPC
        config.rpc.kill_child().await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        cancel_user_bash(config, &from_id).await;

        // 子进程已被杀，先重启再查询模型
        let cfg = crate::config::Config::load();
        match config.rpc.restart(&cfg.model, &cfg.thinking).await {
            Ok(_) => {
                // 获取当前模型信息
                let current_model = config.rpc.get_current_model().await.ok().flatten();
                let current_line = match &current_model {
                    Some(m) => {
                        let name = if !m.name.is_empty() { &m.name } else { &m.id };
                        format!("🟢 当前模型: {} ({})", name, m.provider)
                    }
                    None => "🟢 当前模型: pi 默认".to_string(),
                };

                match config.rpc.get_available_models().await {
                    Ok(models) => {
                        if models.is_empty() {
                            let reply = format!("{}\n\n⚠️ pi 配置中没有找到可用模型。请先在 pi 中配置至少一个模型。", current_line);
                            send_to_peer(&peers, &from_id, &config.name, &reply, config, Some(msg.timestamp + 1)).await;
                            return;
                        }
                        // 构建 [MODEL_LIST] 消息，第一行显示当前模型，第二行是 JSON 列表
                        let list_json = serde_json::to_string(&models).unwrap_or_default();
                        let reply = format!("[MODEL_LIST]\n{}\n{}", current_line, list_json);
                        send_to_peer(&peers, &from_id, &config.name, &reply, config, Some(msg.timestamp + 1)).await;
                    }
                    Err(e) => {
                        let reply = format!("{}\n\n❌ 查询模型列表失败: {}", current_line, e);
                        send_to_peer(&peers, &from_id, &config.name, &reply, config, Some(msg.timestamp + 1)).await;
                    }
                }
            }
            Err(e) => {
                let reply = format!("❌ 重启 pi 失败: {}", e);
                send_to_peer(&peers, &from_id, &config.name, &reply, config, Some(msg.timestamp + 1)).await;
            }
        }
        return;
    }

    // ─── /model select <provider> <modelId> ────────────────────────
    if content.starts_with("/model select ") {
        // 先强制打断卡住的 RPC
        config.rpc.kill_child().await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        cancel_user_bash(config, &from_id).await;
        // 检查是否正在切换
        if config.switching_model.load(Ordering::Acquire) {
            let reply = "⏳ 正在切换模型中，请稍候...";
            send_to_peer(&peers, &from_id, &config.name, reply, config, Some(msg.timestamp + 1)).await;
            return;
        }

        let selector = content.trim_start_matches("/model select ").trim().to_string();
        let parts: Vec<&str> = selector.splitn(2, ' ').collect();
        if parts.len() < 2 {
            let reply = "⚠️ 用法: /model select <provider> <modelId>";
            send_to_peer(&peers, &from_id, &config.name, &reply, config, Some(msg.timestamp + 1)).await;
            return;
        }
        let _provider = parts[0].trim();
        let model_id = parts[1].trim();

        config.switching_model.store(true, Ordering::Release);

        // 更新配置文件
        let mut cfg = crate::config::Config::load();
        cfg.update_model(model_id);

        // 重启 pi 子进程
        let thinking = cfg.thinking.clone();
        match config.rpc.restart(model_id, &thinking).await {
            Ok(_) => {
                let reply = format!("✅ 已切换到模型: {}，pi 子进程已重启", model_id);
                send_to_peer(&peers, &from_id, &config.name, &reply, config, Some(msg.timestamp + 1)).await;
            }
            Err(e) => {
                let reply = format!("❌ 模型切换失败: {}", e);
                send_to_peer(&peers, &from_id, &config.name, &reply, config, Some(msg.timestamp + 1)).await;
            }
        }
        config.switching_model.store(false, Ordering::Release);
        return;
    }

    // ─── ! / !! 命令执行 ──────────────────────────────────────────
    if content.starts_with("!!") || content.starts_with("!") {
        let is_silent = content.starts_with("!!");
        let cmd = if is_silent {
            content.trim_start_matches("!!").trim()
        } else {
            content.trim_start_matches("!").trim()
        };

        if cmd.is_empty() {
            let reply = "⚠️ 请在 `!` 或 `!!` 后输入要执行的命令";
            send_to_peer(&peers, &from_id, &config.name, reply, config, Some(msg.timestamp + 1)).await;
            return;
        }

        // 执行 bash
        let token = {
            let mut map = config.bash_tokens.lock().await;
            // 取消旧的 bash
            if let Some(old) = map.remove(&from_id) {
                old.cancel();
            }
            let token = CancellationToken::new();
            map.insert(from_id.clone(), token.clone());
            token
        };
        let result = execute_bash(cmd, token).await;

        // 构建 tool_result JSON segments（复用 LANChat tool_result 渲染）
        let segments_json = format_tool_result(cmd, &result.output, result.is_error);
        send_to_peer(&peers, &from_id, &config.name, &segments_json, config, Some(msg.timestamp + 1)).await;

        // 积累 ! 命令（!! 不积累）
        if !is_silent {
            let mut map = config.pending_bash.lock().await;
            map.entry(from_id.clone()).or_default().push(result.context_line);
        }
        return;
    }

    // ─── 普通文本 → 流式回复 ────────────────────────────────────
    // 取消运行中的 bash 后处理
    cancel_user_bash(config, &from_id).await;
    // 检查是否有积累的 ! 命令
    let final_content = {
        let mut map = config.pending_bash.lock().await;
        match map.remove(&from_id) {
            Some(cmds) if !cmds.is_empty() => {
                let prefix = cmds.join("\n---\n");
                format!("{}\n\n{}", prefix, content)
            }
            _ => content.clone(),
        }
    };

    let (chunk_tx, chunk_rx) = mpsc::channel::<StreamChunk>(8);
    let stream_handle = send_to_peer_stream(&peers, &from_id, &config.name, chunk_rx, config.bot_id.clone(), msg.timestamp).await;

    if let Some(handle) = stream_handle {
        let result = config.rpc.prompt_stream(&from_id, &final_content, &[], chunk_tx).await;
        match result {
            Ok(_pi_result) => {
                let _ = handle.await;
            }
            Err(e) => {
                let reply = format!("❌ pi 调用失败: {}", e);
                send_to_peer(&peers, &from_id, &config.name, &reply, config, None).await;
            }
        }
    } else {
        // WS 连不上，回退到非流式
        let result = config.rpc.prompt(&from_id, &final_content, &[]).await;
        match result {
            Ok(pi_result) => {
                if !pi_result.text.is_empty() {
                    send_to_peer(&peers, &from_id, &config.name, &pi_result.text, config, Some(msg.timestamp)).await;
                }
            }
            Err(e) => {
                let reply = format!("❌ pi 调用失败: {}", e);
                send_to_peer(&peers, &from_id, &config.name, &reply, config, None).await;
            }
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
    min_timestamp: Option<u64>,
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
                min_timestamp,
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

/// 通过 peer 地址流式发送文本消息
/// 返回 JoinHandle，失败时（WS 连不上）返回 None
async fn send_to_peer_stream(
    peers: &PeerMap,
    target_id: &str,
    bot_name: &str,
    chunk_rx: mpsc::Receiver<StreamChunk>,
    bot_id: String,
    min_timestamp: u64,
) -> Option<tokio::task::JoinHandle<()>> {
    let addr = {
        let map = peers.read().await;
        map.get(target_id).map(|p| p.addr.clone())
    };

    match addr {
        Some(addr) => {
            let bot_name = bot_name.to_string();
            Some(tokio::spawn(async move {
                if let Err(e) = messaging::send_stream_chunks(
                    &addr,
                    bot_id,
                    bot_name,
                    chunk_rx,
                    min_timestamp,
                )
                .await
                {
                    tracing::error!("[Router] 流式发送失败: {}", e);
                }
            }))
        }
        None => {
            tracing::warn!("[Router] 用户 {} 不在线或未知", target_id);
            None
        }
    }
}

// ─── ! / !! 命令辅助函数 ────────────────────────────────────────────

struct BashResult {
    output: String,
    is_error: bool,
    /// 包含 "$ command\noutput" 格式的上下文文本
    context_line: String,
}

/// 执行 bash 命令，返回输出和是否失败
async fn execute_bash(command: &str, cancel: CancellationToken) -> BashResult {
    let mut child = match tokio::process::Command::new("sh")
        .arg("-c")
        .arg(command)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            let line = format!("命令启动失败: {}", e);
            return BashResult {
                output: line.clone(),
                is_error: true,
                context_line: format!("$ {}\n{}", command, line),
            };
        }
    };

    // 先取走管道句柄，等待结束后再读取
    let mut child_stdout = child.stdout.take();
    let mut child_stderr = child.stderr.take();

    // 等待进程结束或被取消
    let exit_status: Option<std::process::ExitStatus> = {
        let cancel = cancel.clone();
        tokio::select! {
            result = child.wait() => {
                result.ok()
            }
            _ = cancel.cancelled() => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                None
            }
        }
    };

    // 读取输出
    let mut combined = String::new();
    if let Some(ref mut out) = child_stdout {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let _ = out.read_to_end(&mut buf).await;
        if !buf.is_empty() {
            combined.push_str(&String::from_utf8_lossy(&buf));
        }
    }
    if let Some(ref mut err) = child_stderr {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let _ = err.read_to_end(&mut buf).await;
        if !buf.is_empty() {
            if !combined.is_empty() {
                combined.push('\n');
            }
            combined.push_str(&String::from_utf8_lossy(&buf));
        }
    }

    match exit_status {
        None => {
            let truncated = truncate_output(&combined);
            let msg = format!("命令被取消。此前输出（{} 字节）：\n{}\n", combined.len(), truncated);
            BashResult {
                output: msg.clone(),
                is_error: true,
                context_line: format!("$ {}\n{}", command, combined),
            }
        }
        Some(status) => {
            let is_error = !status.success();
            let display_output = truncate_output(&combined);
            let context_line = format!("$ {}\n{}", command, combined);
            BashResult {
                output: display_output,
                is_error,
                context_line,
            }
        }
    }
}

/// 截断大输出（10KB）
fn truncate_output(output: &str) -> String {
    const MAX_LEN: usize = 10 * 1024;
    if output.len() > MAX_LEN {
        format!(
            "{}...\n(output truncated, {} bytes total)",
            &output[..MAX_LEN],
            output.len()
        )
    } else {
        output.to_string()
    }
}

/// 取消用户的运行中 bash
async fn cancel_user_bash(config: &BotConfig, user_id: &str) {
    let mut map = config.bash_tokens.lock().await;
    if let Some(token) = map.remove(user_id) {
        token.cancel();
    }
}

/// 将命令输出格式化为 segments JSON（复用 LANChat tool_result 渲染）
fn format_tool_result(command: &str, output: &str, is_error: bool) -> String {
    let segments = serde_json::json!({
        "v": 2,
        "segments": [{
            "type": "tool_result",
            "output": format!("$ {}\n{}", command, output),
            "is_error": is_error,
        }]
    });
    segments.to_string()
}

