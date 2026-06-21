use crate::config;
use crate::models::{PiResult, StreamChunk};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, Mutex};

// ─── JSONL 协议类型 ─────────────────────────────────────────────────────────

// ─── JSONL 协议类型 ─────────────────────────────────────────────────────────

#[derive(Debug)]
enum RpcEvent {
    Response { success: bool, data: Option<Value>, error: Option<String> },
    AgentEnd,
    TextDelta { text: String },
    ThinkingDelta { text: String },
    ToolCallEnd { tool_name: String, tool_args: String },
    ToolExecutionEnd { output: String, is_error: bool },
    Other { event_type: String },
}

impl RpcEvent {
    fn event_type(&self) -> &str {
        match self {
            RpcEvent::Response { .. } => "response",
            RpcEvent::AgentEnd => "agent_end",
            RpcEvent::TextDelta { .. } => "text_delta",
            RpcEvent::ThinkingDelta { .. } => "thinking_delta",
            RpcEvent::ToolCallEnd { .. } => "toolcall_end",
            RpcEvent::ToolExecutionEnd { .. } => "tool_execution_end",
            RpcEvent::Other { event_type } => event_type,
        }
    }

    fn is_agent_end(&self) -> bool {
        matches!(self, RpcEvent::AgentEnd)
    }

    fn try_into_response(self) -> Option<(bool, Option<Value>, Option<String>)> {
        match self {
            RpcEvent::Response { success, data, error } => Some((success, data, error)),
            _ => None,
        }
    }}

// ─── RPC 子进程管理 ─────────────────────────────────────────────────────────

pub struct RpcClient {
    stdin_tx: Mutex<mpsc::Sender<String>>,
    event_rx: Mutex<mpsc::Receiver<RpcEvent>>,
    rpc_mutex: Mutex<()>,
    child: Mutex<tokio::process::Child>,
}

impl RpcClient {
    /// 启动 pi --mode rpc 子进程，开始监听事件流
    pub async fn spawn(model: &str, thinking: &str) -> Result<Arc<Self>, String> {
        let (_, log_cmd) = Self::build_rpc_command(model, thinking);
        tracing::info!("[RPC] 启动:\n    {}", log_cmd);

        let (stdin_tx, event_rx, child) = Self::spawn_inner(model, thinking).await?;

        let client = Arc::new(Self {
            stdin_tx: Mutex::new(stdin_tx),
            event_rx: Mutex::new(event_rx),
            rpc_mutex: Mutex::new(()),
            child: Mutex::new(child),
        });

        // 等待 RPC 启动就绪
        let state_cmd = serde_json::json!({ "type": "get_state" });
        match client.send_and_wait(&state_cmd).await {
            Ok(_) => tracing::info!("[RPC] 子进程就绪"),
            Err(e) => tracing::warn!("[RPC] 启动验证异常: {}", e),
        }

        Ok(client)
    }

    /// 提取子进程创建逻辑，供 spawn 和 restart 共用
    async fn spawn_inner(model: &str, thinking: &str) -> Result<(mpsc::Sender<String>, mpsc::Receiver<RpcEvent>, tokio::process::Child), String> {
        let (mut cmd, _) = Self::build_rpc_command(model, thinking);

        let mut child = cmd
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("启动 pi --mode rpc 失败: {}", e))?;

        let stdin = child.stdin.take().expect("stdin 必须存在");
        let stdout = child.stdout.take().expect("stdout 必须存在");
        let stderr = child.stderr.take().expect("stderr 必须存在");

        let (stdin_tx, mut stdin_rx) = mpsc::channel::<String>(16);
        let (event_tx, event_rx) = mpsc::channel::<RpcEvent>(256);

        // stdin 写入任务
        let mut stdin_stream = stdin;
        tokio::spawn(async move {
            while let Some(line) = stdin_rx.recv().await {
                if stdin_stream.write_all(line.as_bytes()).await.is_err()
                    || stdin_stream.write_all(b"\n").await.is_err()
                    || stdin_stream.flush().await.is_err()
                {
                    break;
                }
            }
        });

        // stdout 读取任务
        let reader = BufReader::new(stdout);
        tokio::spawn(async move {
            Self::read_loop(reader, event_tx).await;
        });

        // stderr 读取任务（日志输出，防止管道阻塞）
        tokio::spawn(async move {
            let mut stderr_reader = BufReader::new(stderr);
            let mut line = String::new();
            loop {
                line.clear();
                match stderr_reader.read_line(&mut line).await {
                    Ok(0) => break,
                    Ok(_) => {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() {
                            tracing::warn!("[RPC stderr] {}", trimmed);
                        }
                    }
                    Err(e) => {
                        tracing::error!("[RPC] stderr 读取错误: {}", e);
                        break;
                    }
                }
            }
        });

        Ok((stdin_tx, event_rx, child))
    }

    // ─── 构建启动参数 ─────────────────────────────────────────────────────

    fn build_rpc_command(model: &str, thinking: &str) -> (Command, String) {
        let session_dir = config::sessions_dir();

        let mut cmd = Command::new("pi");
        cmd.arg("--mode").arg("rpc");
        cmd.arg("--no-context-files");
        cmd.arg("--session-dir").arg(&session_dir);

        let mut log_parts = vec![
            "pi".to_string(),
            "--mode rpc".to_string(),
            "--no-context-files".to_string(),
            format!("--session-dir {}", session_dir.display()),
        ];

        // 注入身份认知到 system prompt
        let identity = concat!(
            "你是一个 LANChat 聊天机器人。\n",
            "每条消息前有 [user_id:...]（仅作参考，回复中不要包含）。\n",
            "回复直接返回给用户。\n",
            "\n",
            "[定时任务] 用户说提醒/定时/每天/每周等意图时创建：\n",
            "  lanclaw task add <时间> --reply \"文本\" --exec \"命令\" --user-id <用户ID>\n",
            "  --reply/--exec 可组合，结果自动发给创建者。\n",
            "  时间格式: 30s, 30min, 2h, every:10s, daily:HH:MM, weekly:day:HH:MM,\n",
            "    monthly:DD:HH:MM, monthly:last:HH:MM, yearly:MM-DD:HH:MM\n",
            "  管理: lanclaw task list / cancel <ID> / logs <ID>\n",
            "\n",
            "[发送文件] 用户要求发送文件时：\n",
            "  lanclaw send-file <文件路径> --user-id <用户ID>\n",
            "\n",
            "复杂重复任务（需判断分析的）用：\n",
            "  lanclaw task add daily:09:00 --exec 'pi --print \"任务描述\"' --user-id <用户ID>\n",
        );
        cmd.arg("--append-system-prompt").arg(identity);
        log_parts.push(format!("--append-system-prompt ({} bytes)", identity.len()));

        if !model.is_empty() {
            cmd.arg("--model").arg(model);
            log_parts.push(format!("--model {}", model));
        }

        // 始终传递 thinking 级别（即使 off 也要显式传，否则 pi 用默认 high）
        if !thinking.is_empty() {
            cmd.arg("--thinking").arg(thinking);
            log_parts.push(format!("--thinking {}", thinking));
        }

        (cmd, log_parts.join(" \\\n    "))
    }

    // ─── 核心 API ─────────────────────────────────────────────────────────

    /// 给指定用户发送 prompt，等待完整回复
    pub async fn prompt(
        &self,
        user_id: &str,
        message: &str,
        files: &[PathBuf],
    ) -> Result<PiResult, String> {
        let _lock = self.rpc_mutex.lock().await;

        // 1. 切换到用户的 session（文件不存在时 pi 自动创建）
        let session_path = config::sessions_dir().join(format!("{}.jsonl", user_id));
        let session_str = session_path.to_string_lossy().to_string();
        let switch_cmd = serde_json::json!({
            "type": "switch_session",
            "sessionPath": session_str,
        });
        self.send_and_wait(&switch_cmd).await?;

        // 切换后重设 thinking level（旧 session 可能存了 high）
        let think_cmd = serde_json::json!({
            "type": "set_thinking_level",
            "level": "off",
        });
        let _ = self.send_and_wait(&think_cmd).await;

        // 2. 在消息前注入用户身份（让 pi 知道 --user-id 该填什么）
        let user_tag = format!("[user_id:{}] ", user_id);
        let tagged_message = format!("{}{}", user_tag, message);

        // 2. 构建 prompt 命令（带文件）
        let prompt_cmd = if !files.is_empty() {
            let images: Vec<Value> = files
                .iter()
                .filter(|f| f.exists())
                .map(|f| {
                    let data = std::fs::read(f).unwrap_or_default();
                    let mime = mime_guess::from_path(f)
                        .first_or_octet_stream()
                        .to_string();
                    serde_json::json!({
                        "type": "image",
                        "data": base64_encode(&data),
                        "mimeType": mime,
                    })
                })
                .collect();

            if images.is_empty() {
                serde_json::json!({ "type": "prompt", "message": &tagged_message })
            } else {
                serde_json::json!({
                    "type": "prompt",
                    "message": &tagged_message,
                    "images": images,
                })
            }
        } else {
            serde_json::json!({ "type": "prompt", "message": &tagged_message })
        };

        // 3. 发送 prompt 并等 agent_end（空回复时自动重试 2 次）
        tracing::info!(
            "[RPC] >>> user={} msg={}",
            user_id.chars().take(8).collect::<String>(),
            serde_json::to_string(&prompt_cmd).unwrap_or_default()
        );

        let text = self.prompt_with_retry(&prompt_cmd, 2).await?;

        if text.is_empty() {
            tracing::warn!("[RPC] 回复为空");
        } else {
            tracing::info!(
                "[RPC] 回复 ({} chars): {}",
                text.len(),
                text.chars().take(80).collect::<String>()
            );
        }

        // 去掉回复中可能泄露的 [user_id:...] 标记
        let text = strip_user_id_tag(&text);

        Ok(PiResult {
            text,
        })
    }

    /// 获取 pi 配置中所有可用模型
    pub async fn get_available_models(&self) -> Result<Vec<crate::models::ModelInfo>, String> {
        let cmd = serde_json::json!({ "type": "get_available_models" });
        let data = self.send_and_wait(&cmd).await?;
        let models: Vec<crate::models::ModelInfo> =
            serde_json::from_value(data.get("models").cloned().unwrap_or(serde_json::json!([])))
                .map_err(|e| format!("解析模型列表失败: {}", e))?;
        Ok(models)
    }

    /// 获取当前模型信息
    pub async fn get_current_model(&self) -> Result<Option<crate::models::ModelInfo>, String> {
        let cmd = serde_json::json!({ "type": "get_state" });
        let data = self.send_and_wait(&cmd).await?;
        match data.get("model") {
            Some(model_val) if !model_val.is_null() => {
                let model: crate::models::ModelInfo =
                    serde_json::from_value(model_val.clone())
                        .map_err(|e| format!("解析当前模型失败: {}", e))?;
                Ok(Some(model))
            }
            _ => Ok(None),
        }
    }

    /// 重启 pi 子进程（杀掉旧进程，用新模型重新 spawn）
    pub async fn restart(&self, model: &str, thinking: &str) -> Result<(), String> {
        let _lock = self.rpc_mutex.lock().await;
        tracing::info!("[RPC] 重启 pi 子进程: model={}", model);

        // 先创建新进程（成功后再杀旧进程，减少宕机时间）
        let (new_stdin_tx, new_event_rx, new_child) = Self::spawn_inner(model, thinking).await?;

        // 换入新通道和新进程
        let old_stdin = std::mem::replace(&mut *self.stdin_tx.lock().await, new_stdin_tx);
        let old_event_rx = std::mem::replace(&mut *self.event_rx.lock().await, new_event_rx);
        let old_child = std::mem::replace(&mut *self.child.lock().await, new_child);

        // 验证新进程就绪
        let state_cmd = serde_json::json!({ "type": "get_state" });
        match self.send_and_wait(&state_cmd).await {
            Ok(data) => {
                // 成功，丢弃旧进程（kill_on_drop 自动杀死）
                drop(old_child);
                drop(old_stdin);
                drop(old_event_rx);
                // 打印新模型信息
                let new_model_id = data.get("model")
                    .and_then(|m| m.get("id"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("unknown");
                tracing::info!("[RPC] 重启成功，当前模型: {}", new_model_id);
                Ok(())
            }
            Err(e) => {
                // 回滚：恢复旧进程
                tracing::error!("[RPC] 新进程验证失败，回滚: {}", e);
                let _ = std::mem::replace(&mut *self.stdin_tx.lock().await, old_stdin);
                let _ = std::mem::replace(&mut *self.event_rx.lock().await, old_event_rx);
                let _ = std::mem::replace(&mut *self.child.lock().await, old_child);
                Err(format!("新 pi 子进程未就绪: {}", e))
            }
        }
    }

    /// 重置用户 session
    pub async fn reset_session(&self, user_id: &str) -> Result<(), String> {
        let _lock = self.rpc_mutex.lock().await;

        // 删除 session 文件，然后让 pi 切换到一个全新的空 session
        let session_path = config::sessions_dir().join(format!("{}.jsonl", user_id));
        if session_path.exists() {
            std::fs::remove_file(&session_path)
                .map_err(|e| format!("删除 session 文件失败: {}", e))?;
        }

        let session_str = session_path.to_string_lossy().to_string();
        let switch_cmd = serde_json::json!({
            "type": "switch_session",
            "sessionPath": session_str,
        });
        self.send_and_wait(&switch_cmd).await?;

        tracing::info!("[RPC] Session 已重置: {}", user_id);
        Ok(())
    }

    // ─── 内部：发送命令 + 等 response ────────────────────────────────────

    async fn send_and_wait(&self, cmd: &Value) -> Result<Value, String> {
        let cmd_str = serde_json::to_string(cmd).map_err(|e| format!("序列化失败: {}", e))?;
        self.stdin_tx
            .lock()
            .await
            .send(cmd_str)
            .await
            .map_err(|e| format!("发送命令失败: {}", e))?;

        let mut event_rx = self.event_rx.lock().await;
        loop {
            let event = event_rx
                .recv()
                .await
                .ok_or_else(|| "RPC 事件通道关闭".to_string())?;

            let event_type = event.event_type().to_string();

            if let Some((success, data, error)) = event.try_into_response() {
                if success {
                    return Ok(data.unwrap_or(serde_json::json!(null)));
                } else {
                    return Err(error.unwrap_or_else(|| "RPC 命令失败".to_string()));
                }
            }
            tracing::trace!("[RPC] send_and_wait 跳过: {}", event_type);
        }
    }

    /// 发送 prompt 并等待回复，空回复时自动重试
    async fn prompt_with_retry(&self, cmd: &Value, max_retries: u32) -> Result<String, String> {
        for attempt in 0..=max_retries {
            if attempt > 0 {
                tracing::warn!("[RPC] 空回复，第 {} 次重试...", attempt);
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }

            self.send_and_wait(cmd).await?;
            let text = self.wait_for_text(None).await?;

            if !text.is_empty() {
                return Ok(text);
            }
        }
        Ok(String::new())
    }

    /// 流式 prompt：通过 chunk_tx 逐段发送累积文本/思考/工具事件
    /// 保持互斥锁直到回复完成
    pub async fn prompt_stream(
        self: &Arc<Self>,
        user_id: &str,
        message: &str,
        files: &[PathBuf],
        chunk_tx: mpsc::Sender<StreamChunk>,
    ) -> Result<PiResult, String> {
        let _lock = self.rpc_mutex.lock().await;

        // 1. 切换 session
        let session_path = config::sessions_dir().join(format!("{}.jsonl", user_id));
        let session_str = session_path.to_string_lossy().to_string();
        let switch_cmd = serde_json::json!({ "type": "switch_session", "sessionPath": session_str });
        self.send_and_wait(&switch_cmd).await?;

        // 重设 thinking level
        let think_cmd = serde_json::json!({ "type": "set_thinking_level", "level": "off" });
        let _ = self.send_and_wait(&think_cmd).await;

        // 2. 注入用户身份
        let user_tag = format!("[user_id:{}] ", user_id);
        let tagged_message = format!("{}{}", user_tag, message);

        // 构建 prompt 命令
        let prompt_cmd = if !files.is_empty() {
            let images: Vec<Value> = files
                .iter()
                .filter(|f| f.exists())
                .map(|f| {
                    let data = std::fs::read(f).unwrap_or_default();
                    let mime = mime_guess::from_path(f).first_or_octet_stream().to_string();
                    serde_json::json!({
                        "type": "image", "data": base64_encode(&data), "mimeType": mime,
                    })
                })
                .collect();
            if images.is_empty() {
                serde_json::json!({ "type": "prompt", "message": &tagged_message })
            } else {
                serde_json::json!({ "type": "prompt", "message": &tagged_message, "images": images })
            }
        } else {
            serde_json::json!({ "type": "prompt", "message": &tagged_message })
        };

        tracing::info!(
            "[RPC] >>> user={} msg={}",
            user_id.chars().take(8).collect::<String>(),
            serde_json::to_string(&prompt_cmd).unwrap_or_default()
        );

        // 3. 发送 prompt，流式收集
        self.send_and_wait(&prompt_cmd).await?;
        let text = self.wait_for_text(Some(chunk_tx)).await?;

        let text = strip_user_id_tag(&text);

        if text.is_empty() {
            tracing::warn!("[RPC] 回复为空");
        } else {
            tracing::info!("[RPC] 回复 ({} chars): {}", text.len(), text.chars().take(80).collect::<String>());
        }

        Ok(PiResult { text })
    }

    /// 等待 agent_end，同时收集 text_delta / thinking_delta / tool 事件
    /// 如果传了 chunk_tx，每个 delta 后把累积文本发过去（流式）
    async fn wait_for_text(&self, chunk_tx: Option<mpsc::Sender<StreamChunk>>) -> Result<String, String> {
        let mut event_rx = self.event_rx.lock().await;
        let mut thinking_buf = String::new();
        let mut response_buf = String::new();

        loop {
            let event = event_rx
                .recv()
                .await
                .ok_or_else(|| "RPC 事件通道关闭".to_string())?;

            if event.is_agent_end() {
                return Ok(response_buf);
            }

            match event {
                RpcEvent::TextDelta { text } => {
                    response_buf.push_str(&text);
                    if let Some(ref tx) = chunk_tx {
                        let _ = tx
                            .send(StreamChunk::Text {
                                content: response_buf.clone(),
                                is_thinking: false,
                            })
                            .await;
                    }
                }
                RpcEvent::ThinkingDelta { text } => {
                    thinking_buf.push_str(&text);
                    if let Some(ref tx) = chunk_tx {
                        let _ = tx
                            .send(StreamChunk::Text {
                                content: thinking_buf.clone(),
                                is_thinking: true,
                            })
                            .await;
                    }
                }
                RpcEvent::ToolCallEnd { tool_name, tool_args } => {
                    // 重置 response 段和 thinking 段——后续 text/thinking 作为新的独立段落
                    response_buf.clear();
                    thinking_buf.clear();
                    if let Some(ref tx) = chunk_tx {
                        let _ = tx
                            .send(StreamChunk::ToolCall {
                                name: tool_name,
                                args: tool_args,
                            })
                            .await;
                    }
                }
                RpcEvent::ToolExecutionEnd { output, is_error } => {
                    if let Some(ref tx) = chunk_tx {
                        let _ = tx
                            .send(StreamChunk::ToolResult { output, is_error })
                            .await;
                    }
                }
                _ => {
                    // 忽略其他事件
                    tracing::trace!("[RPC] wait_for_text 跳过事件");
                }
            }
        }
    }

    // ─── 内部：stdout 读取循环 ────────────────────────────────────────────

    async fn read_loop<R: tokio::io::AsyncRead + Unpin>(
        mut reader: BufReader<R>,
        event_tx: mpsc::Sender<RpcEvent>,
    ) {
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    match serde_json::from_str::<Value>(trimmed) {
                        Ok(val) => {
                            let event = Self::parse_event(&val);
                            if event_tx.send(event).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "[RPC] JSON 解析失败: {} | {}",
                                e,
                                trimmed.chars().take(100).collect::<String>()
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("[RPC] stdout 读取错误: {}", e);
                    break;
                }
            }
        }
    }

    fn parse_event(val: &Value) -> RpcEvent {
        let event_type = val.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match event_type {
            "response" => RpcEvent::Response {
                success: val.get("success").and_then(|s| s.as_bool()).unwrap_or(false),
                data: val.get("data").cloned(),
                error: val.get("error").and_then(|e| e.as_str()).map(String::from),
            },
            "agent_end" => RpcEvent::AgentEnd,
            "message_update" => {
                let msg_type = val
                    .get("assistantMessageEvent")
                    .and_then(|e| e.get("type"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("");
                match msg_type {
                    "text_delta" => {
                        let delta = val
                            .get("assistantMessageEvent")
                            .and_then(|e| e.get("delta"))
                            .and_then(|d| d.as_str())
                            .unwrap_or("")
                            .to_string();
                        RpcEvent::TextDelta { text: delta }
                    }
                    "thinking_delta" => {
                        let delta = val
                            .get("assistantMessageEvent")
                            .and_then(|e| e.get("delta"))
                            .and_then(|d| d.as_str())
                            .unwrap_or("")
                            .to_string();
                        RpcEvent::ThinkingDelta { text: delta }
                    }
                    "toolcall_end" => {
                        let tool_call = val
                            .get("assistantMessageEvent")
                            .and_then(|e| e.get("toolCall"));
                        let tool_name = tool_call
                            .and_then(|tc| tc.get("name"))
                            .and_then(|n| n.as_str())
                            .unwrap_or("")
                            .to_string();
                        let tool_args = tool_call
                            .and_then(|tc| tc.get("arguments"))
                            .map(|a| {
                                if let Some(s) = a.as_str() {
                                    s.to_string()
                                } else {
                                    a.to_string()
                                }
                            })
                            .unwrap_or_default();
                        RpcEvent::ToolCallEnd { tool_name, tool_args }
                    }
                    _ => RpcEvent::Other { event_type: event_type.to_string() },
                }
            }
            "tool_execution_end" => {
                let result = val.get("result");
                let output = result
                    .and_then(|r| r.get("content"))
                    .and_then(|c| c.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|first| first.get("text"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string();
                let is_error = val.get("isError").and_then(|e| e.as_bool()).unwrap_or(false);
                RpcEvent::ToolExecutionEnd { output, is_error }
            }
            _ => RpcEvent::Other {
                event_type: event_type.to_string(),
            },
        }
    }
}

// ─── 工具函数 ───────────────────────────────────────────────────────────────

fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = Vec::with_capacity(data.len() * 4 / 3 + 4);
    for chunk in data.chunks(3) {
        match chunk.len() {
            3 => {
                let b0 = chunk[0] as u32;
                let b1 = chunk[1] as u32;
                let b2 = chunk[2] as u32;
                let triple = (b0 << 16) | (b1 << 8) | b2;
                result.push(CHARS[((triple >> 18) & 0x3F) as usize]);
                result.push(CHARS[((triple >> 12) & 0x3F) as usize]);
                result.push(CHARS[((triple >> 6) & 0x3F) as usize]);
                result.push(CHARS[(triple & 0x3F) as usize]);
            }
            2 => {
                let b0 = chunk[0] as u32;
                let b1 = chunk[1] as u32;
                let triple = (b0 << 16) | (b1 << 8);
                result.push(CHARS[((triple >> 18) & 0x3F) as usize]);
                result.push(CHARS[((triple >> 12) & 0x3F) as usize]);
                result.push(CHARS[((triple >> 6) & 0x3F) as usize]);
                result.push(b'=');
            }
            1 => {
                let b0 = chunk[0] as u32;
                let triple = b0 << 16;
                result.push(CHARS[((triple >> 18) & 0x3F) as usize]);
                result.push(CHARS[((triple >> 12) & 0x3F) as usize]);
                result.push(b'=');
                result.push(b'=');
            }
            _ => {}
        }
    }
    String::from_utf8(result).unwrap_or_default()
}

fn strip_user_id_tag(text: &str) -> String {
    let text = text.trim();
    if let Some(rest) = text.strip_prefix("[user_id:") {
        if let Some(end) = rest.find(']') {
            return rest[end + 1..].trim().to_string();
        }
    }
    text.to_string()
}
