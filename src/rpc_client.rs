use crate::config;
use crate::models::PiResult;
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, Mutex};

// ─── JSONL 协议类型 ─────────────────────────────────────────────────────────

#[derive(Debug)]
enum RpcEvent {
    Response {
        id: Option<String>,
        success: bool,
        command: String,
        data: Option<Value>,
        error: Option<String>,
    },
    AgentEnd {
        messages: Vec<Value>,
    },
    MessageUpdate {
        event: Option<Value>,
    },
    TextDelta {
        delta: String,
    },
    Other {
        event_type: String,
    },
}

// ─── RPC 子进程管理 ─────────────────────────────────────────────────────────

pub struct RpcClient {
    /// 发送命令到 pi 的 stdin
    stdin_tx: mpsc::Sender<String>,
    /// 接收事件的 channel（由后台读取任务推送）
    event_rx: Mutex<mpsc::Receiver<ParsedEvent>>,
    /// 串行化所有 RPC 操作（同一时间只能处理一个用户的消息）
    rpc_mutex: Mutex<()>,
    /// 子进程句柄
    child: Mutex<Option<Child>>,
}

/// 解析后的事件（带 ID 对应）
#[derive(Debug)]
struct ParsedEvent {
    raw: RpcEvent,
}

// ─── 构建启动参数 ───────────────────────────────────────────────────────────

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

    // Skill 文件
    if skill_path.exists() {
        cmd.arg("--skill").arg(&skill_path);
        log_parts.push(format!("--skill {}", skill_path.display()));
    }

    // Model
    if !model.is_empty() {
        cmd.arg("--model").arg(model);
        log_parts.push(format!("--model {}", model));
    }

    // Thinking
    if !thinking.is_empty() && thinking != "off" {
        cmd.arg("--thinking").arg(thinking);
        log_parts.push(format!("--thinking {}", thinking));
    }

    // 禁止 stdin 编辑和 TUI
    cmd.env("PI_NO_INTERACTIVE", "1");

    (cmd, log_parts.join(" \\\n    "))
}

impl RpcClient {
    /// 启动 pi --mode rpc 子进程，开始监听事件流
    pub async fn spawn(model: &str, thinking: &str) -> Result<Arc<Self>, String> {
        let (mut cmd, log_cmd) = build_rpc_command(model, thinking);

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

        let (stdin_tx, stdin_rx) = mpsc::channel::<String>(16);
        let (event_tx, event_rx) = mpsc::channel::<ParsedEvent>(256);

        // 启动 stdin 写入任务
        let mut stdin_stream = stdin;
        tokio::spawn(async move {
            let mut stdin_rx = stdin_rx;
            while let Some(line) = stdin_rx.recv().await {
                if let Err(e) = stdin_stream.write_all(line.as_bytes()).await {
                    tracing::error!("[RPC] stdin 写入失败: {}", e);
                    break;
                }
                if let Err(e) = stdin_stream.write_all(b"\n").await {
                    tracing::error!("[RPC] stdin flush 失败: {}", e);
                    break;
                }
                if let Err(e) = stdin_stream.flush().await {
                    tracing::error!("[RPC] stdin flush 失败: {}", e);
                    break;
                }
            }
        });

        // 启动 stdout 读取任务
        let reader = BufReader::new(stdout);
        tokio::spawn(async move {
            Self::read_loop(reader, event_tx).await;
        });

        let client = Arc::new(Self {
            stdin_tx,
            event_rx: Mutex::new(event_rx),
            rpc_mutex: Mutex::new(()),
            child: Mutex::new(Some(child)),
        });

        // 等待 RPC 启动就绪
        let state_cmd = serde_json::json!({ "type": "get_state" });
        match client.send_and_wait_for_response(&state_cmd).await {
            Ok(_resp) => {
                tracing::info!("[RPC] 子进程就绪");
            }
            Err(e) => {
                tracing::warn!("[RPC] 启动验证响应异常: {}", e);
            }
        }

        Ok(client)
    }

    // ─── 核心 API ─────────────────────────────────────────────────────────

    /// 给指定用户发送 prompt，等待完整回复
    ///
    /// 串行化执行：同一时间只处理一个 RPC 请求
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
            self.send_and_wait_for_response(&cmd).await?;
        }
        // 如果 session 不存在，直接 prompt（pi 会自动创建）

        // 2. 构建 prompt 命令（带文件）
        let prompt_value = if !files.is_empty() {
            let images: Vec<Value> = files
                .iter()
                .filter(|f| f.exists())
                .map(|f| {
                    // 读取文件为 base64
                    let data = std::fs::read(f).unwrap_or_default();
                    let mime = mime_guess::from_path(f)
                        .first_or_octet_stream()
                        .to_string();
                    let b64 = base64_encode(&data);
                    serde_json::json!({
                        "type": "image",
                        "data": b64,
                        "mimeType": mime,
                    })
                })
                .collect();

            if images.is_empty() {
                serde_json::json!({
                    "type": "prompt",
                    "message": message,
                })
            } else {
                serde_json::json!({
                    "type": "prompt",
                    "message": message,
                    "images": images,
                })
            }
        } else {
            serde_json::json!({
                "type": "prompt",
                "message": message,
            })
        };

        tracing::info!(
            "[RPC] prompt: user={} files={} msg={}",
            user_id.chars().take(8).collect::<String>(),
            files.len(),
            message.chars().take(60).collect::<String>()
        );

        // 3. 发送 prompt
        self.send_and_wait_for_response(&prompt_value).await?;

        // 4. 等待 agent_end
        self.wait_for_agent_end().await?;

        // 5. 获取最终文本
        let text_cmd = serde_json::json!({
            "type": "get_last_assistant_text",
        });
        let resp = self.send_and_wait_for_response(&text_cmd).await?;

        // 提取 data.text
        let text = resp
            .get("data")
            .and_then(|d| d.get("text"))
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_string();

        // 检查 files_out 目录是否有新文件
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
            // 切换到用户 session，然后 new_session
            let session_str = session_path.to_string_lossy().to_string();
            let switch_cmd = serde_json::json!({
                "type": "switch_session",
                "sessionPath": session_str,
            });
            self.send_and_wait_for_response(&switch_cmd).await?;
        }

        let new_session_cmd = serde_json::json!({
            "type": "new_session",
        });
        self.send_and_wait_for_response(&new_session_cmd).await?;

        tracing::info!("[RPC] Session 已重置: {}", user_id);
        Ok(())
    }

    // ─── 内部：发送命令 + 等待 response ───────────────────────────────────

    /// 发送 JSON 命令并等待对应的 response（按 id 对应）
    async fn send_and_wait_for_response(&self, cmd: &Value) -> Result<Value, String> {
        let cmd_str = serde_json::to_string(cmd).map_err(|e| format!("序列化失败: {}", e))?;
        self.stdin_tx
            .send(cmd_str)
            .await
            .map_err(|e| format!("发送命令失败: {}", e))?;

        // 等待 response（可能有其他事件混入，需要跳过）
        let mut event_rx = self.event_rx.lock().await;
        loop {
            let event = event_rx
                .recv()
                .await
                .ok_or_else(|| "RPC 事件通道关闭".to_string())?;

            match event.raw {
                RpcEvent::Response {
                    success, data, error, ..
                } => {
                    if success {
                        return Ok(data.unwrap_or(serde_json::json!(null)));
                    } else {
                        return Err(error.unwrap_or_else(|| "RPC 命令失败".to_string()));
                    }
                }
                RpcEvent::Other { event_type } => {
                    tracing::debug!("[RPC] 跳过事件: {}", event_type);
                }
                _ => {
                    tracing::debug!("[RPC] 跳过非 response 事件");
                }
            }
        }
    }

    /// 等待 agent_end 事件（期间所有其他事件跳过）
    async fn wait_for_agent_end(&self) -> Result<(), String> {
        let mut event_rx = self.event_rx.lock().await;
        loop {
            let event = event_rx
                .recv()
                .await
                .ok_or_else(|| "RPC 事件通道关闭".to_string())?;

            match event.raw {
                RpcEvent::AgentEnd { .. } => {
                    return Ok(());
                }
                RpcEvent::Other { event_type } => {
                    if event_type == "extension_error" {
                        tracing::warn!("[RPC] extension_error 事件");
                    }
                }
                _ => {}
            }
        }
    }

    // ─── 内部：stdout 读取循环 ────────────────────────────────────────────

    /// 从 stdout 读取 JSONL 行，解析后分发到 event_tx
    async fn read_loop<R: tokio::io::AsyncRead + Unpin>(
        mut reader: BufReader<R>,
        event_tx: mpsc::Sender<ParsedEvent>,
    ) {
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => {
                    tracing::debug!("[RPC] stdout EOF");
                    break;
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    match serde_json::from_str::<Value>(trimmed) {
                        Ok(val) => {
                            let parsed = Self::parse_event(&val);
                            if event_tx.send(parsed).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "[RPC] JSON 解析失败: {} | 原文: {}",
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

    /// 解析 JSON 事件为 ParsedEvent
    fn parse_event(val: &Value) -> ParsedEvent {
        let event_type = val.get("type").and_then(|t| t.as_str()).unwrap_or("");

        let raw = match event_type {
            "response" => RpcEvent::Response {
                id: val.get("id").and_then(|i| i.as_str()).map(String::from),
                success: val.get("success").and_then(|s| s.as_bool()).unwrap_or(false),
                command: val
                    .get("command")
                    .and_then(|c| c.as_str())
                    .unwrap_or("")
                    .to_string(),
                data: val.get("data").cloned(),
                error: val.get("error").and_then(|e| e.as_str()).map(String::from),
            },

            "agent_end" => RpcEvent::AgentEnd {
                messages: val
                    .get("messages")
                    .and_then(|m| m.as_array())
                    .cloned()
                    .unwrap_or_default(),
            },

            "message_update" => {
                // 提取 text_delta
                let delta_text = val
                    .get("assistantMessageEvent")
                    .and_then(|e| e.get("type"))
                    .and_then(|t| t.as_str())
                    .and_then(|t| {
                        if t == "text_delta" {
                            val.get("assistantMessageEvent")
                                .and_then(|e| e.get("delta"))
                                .and_then(|d| d.as_str())
                        } else {
                            None
                        }
                    });

                if let Some(delta) = delta_text {
                    RpcEvent::TextDelta {
                        delta: delta.to_string(),
                    }
                } else {
                    RpcEvent::MessageUpdate {
                        event: val.get("assistantMessageEvent").cloned(),
                    }
                }
            }

            _ => RpcEvent::Other {
                event_type: event_type.to_string(),
            },
        };

        ParsedEvent { raw }
    }

    /// 检查子进程是否还活着
    pub async fn is_alive(&self) -> bool {
        let mut child = self.child.lock().await;
        match child.as_mut() {
            Some(c) => matches!(c.try_wait(), Ok(None)),
            None => false,
        }
    }
}

// ─── 工具函数 ───────────────────────────────────────────────────────────────

/// 简单的 base64 编码（不需要外部依赖）
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

/// 扫描 files_out 目录中的文件
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

/// 清理 files_out 目录
pub fn clean_files_out() -> std::io::Result<()> {
    let out_dir = config::files_out_dir();
    if out_dir.exists() {
        std::fs::remove_dir_all(&out_dir)?;
    }
    std::fs::create_dir_all(&out_dir)
}
