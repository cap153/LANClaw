use crate::config;
use crate::models::PiResult;
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, Mutex};

// ─── JSONL 协议类型 ─────────────────────────────────────────────────────────

#[derive(Debug)]
enum RpcEvent {
    Response { success: bool, data: Option<Value>, error: Option<String> },
    AgentEnd,
    Other { event_type: String },
}

impl RpcEvent {
    fn event_type(&self) -> &str {
        match self {
            RpcEvent::Response { .. } => "response",
            RpcEvent::AgentEnd => "agent_end",
            RpcEvent::Other { event_type } => event_type,
        }
    }

    fn is_agent_end(&self) -> bool {
        matches!(self, RpcEvent::AgentEnd { .. })
    }

    fn try_into_response(self) -> Option<(bool, Option<Value>, Option<String>)> {
        match self {
            RpcEvent::Response { success, data, error } => Some((success, data, error)),
            _ => None,
        }
    }
}

// ─── RPC 子进程管理 ─────────────────────────────────────────────────────────

pub struct RpcClient {
    stdin_tx: mpsc::Sender<String>,
    event_rx: Mutex<mpsc::Receiver<RpcEvent>>,
    rpc_mutex: Mutex<()>,
}

impl RpcClient {
    /// 启动 pi --mode rpc 子进程，开始监听事件流
    pub async fn spawn(model: &str, thinking: &str) -> Result<Arc<Self>, String> {
        let (mut cmd, log_cmd) = Self::build_rpc_command(model, thinking);

        tracing::info!("[RPC] 启动:\n    {}", log_cmd);

        let mut child = cmd
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("启动 pi --mode rpc 失败: {}", e))?;

        let stdin = child.stdin.take().expect("stdin 必须存在");
        let stdout = child.stdout.take().expect("stdout 必须存在");

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

        let client = Arc::new(Self {
            stdin_tx,
            event_rx: Mutex::new(event_rx),
            rpc_mutex: Mutex::new(()),
        });

        // 等待 RPC 启动就绪
        let state_cmd = serde_json::json!({ "type": "get_state" });
        match client.send_and_wait(&state_cmd).await {
            Ok(_) => tracing::info!("[RPC] 子进程就绪"),
            Err(e) => tracing::warn!("[RPC] 启动验证异常: {}", e),
        }

        Ok(client)
    }

    // ─── 构建启动参数 ─────────────────────────────────────────────────────

    fn build_rpc_command(model: &str, thinking: &str) -> (Command, String) {
        let skill_path = config::skill_path();
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

        if skill_path.exists() {
            cmd.arg("--skill").arg(&skill_path);
            log_parts.push(format!("--skill {}", skill_path.display()));
        }

        if !model.is_empty() {
            cmd.arg("--model").arg(model);
            log_parts.push(format!("--model {}", model));
        }

        if !thinking.is_empty() && thinking != "off" {
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

        // 1. 切换到用户的 session
        let session_path = config::sessions_dir().join(format!("{}.jsonl", user_id));
        if session_path.exists() {
            let session_str = session_path.to_string_lossy().to_string();
            let cmd = serde_json::json!({
                "type": "switch_session",
                "sessionPath": session_str,
            });
            self.send_and_wait(&cmd).await?;
        }

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
                serde_json::json!({ "type": "prompt", "message": message })
            } else {
                serde_json::json!({
                    "type": "prompt",
                    "message": message,
                    "images": images,
                })
            }
        } else {
            serde_json::json!({ "type": "prompt", "message": message })
        };

        tracing::info!(
            "[RPC] prompt: user={} files={} msg={}",
            user_id.chars().take(8).collect::<String>(),
            files.len(),
            message.chars().take(60).collect::<String>()
        );

        // 3. 发送 prompt 并等 agent_end
        self.send_and_wait(&prompt_cmd).await?;
        self.wait_for(|e| e.is_agent_end()).await?;

        // 4. 获取最终文本
        let text_cmd = serde_json::json!({ "type": "get_last_assistant_text" });
        let resp = self.send_and_wait(&text_cmd).await?;
        let text = resp
            .get("data")
            .and_then(|d| d.get("text"))
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_string();

        let generated_files = scan_generated_files();

        tracing::info!(
            "[RPC] 回复: {}",
            text.chars().take(120).collect::<String>()
        );

        Ok(PiResult {
            text,
            files: generated_files,
        })
    }

    /// 重置用户 session
    pub async fn reset_session(&self, user_id: &str) -> Result<(), String> {
        let _lock = self.rpc_mutex.lock().await;

        let session_path = config::sessions_dir().join(format!("{}.jsonl", user_id));
        if session_path.exists() {
            let session_str = session_path.to_string_lossy().to_string();
            let switch_cmd = serde_json::json!({
                "type": "switch_session",
                "sessionPath": session_str,
            });
            self.send_and_wait(&switch_cmd).await?;
        }

        let new_session_cmd = serde_json::json!({ "type": "new_session" });
        self.send_and_wait(&new_session_cmd).await?;

        tracing::info!("[RPC] Session 已重置: {}", user_id);
        Ok(())
    }

    // ─── 内部：发送命令 + 等 response ────────────────────────────────────

    async fn send_and_wait(&self, cmd: &Value) -> Result<Value, String> {
        let cmd_str = serde_json::to_string(cmd).map_err(|e| format!("序列化失败: {}", e))?;
        self.stdin_tx
            .send(cmd_str)
            .await
            .map_err(|e| format!("发送命令失败: {}", e))?;

        let mut event_rx = self.event_rx.lock().await;
        loop {
            let event = event_rx
                .recv()
                .await
                .ok_or_else(|| "RPC 事件通道关闭".to_string())?;

            if let Some((success, data, error)) = event.try_into_response() {
                if success {
                    return Ok(data.unwrap_or(serde_json::json!(null)));
                } else {
                    return Err(error.unwrap_or_else(|| "RPC 命令失败".to_string()));
                }
            }
        }
    }

    /// 等待满足条件的事件（忽略其他事件）
    async fn wait_for<F>(&self, mut pred: F) -> Result<(), String>
    where
        F: FnMut(&RpcEvent) -> bool,
    {
        let mut event_rx = self.event_rx.lock().await;
        loop {
            let event = event_rx
                .recv()
                .await
                .ok_or_else(|| "RPC 事件通道关闭".to_string())?;

            if pred(&event) {
                return Ok(());
            }

            if event.event_type() == "extension_error" {
                tracing::warn!("[RPC] extension_error 事件");
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

fn scan_generated_files() -> Vec<PathBuf> {
    let out_dir = config::files_out_dir();
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&out_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                files.push(path);
            }
        }
    }
    files
}

pub fn clean_files_out() -> std::io::Result<()> {
    let out_dir = config::files_out_dir();
    if out_dir.exists() {
        std::fs::remove_dir_all(&out_dir)?;
    }
    std::fs::create_dir_all(&out_dir)
}
