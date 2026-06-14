use crate::config;
use crate::models::PiResult;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};
use tokio::process::Command;
use tokio::sync::Mutex as TokioMutex;

/// Per-user session locks (防止同一用户并发调用 pi)
fn session_locks() -> &'static Mutex<HashMap<String, Arc<TokioMutex<()>>>> {
    static LOCKS: LazyLock<Mutex<HashMap<String, Arc<TokioMutex<()>>>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));
    &LOCKS
}

async fn get_user_lock(user_id: &str) -> Arc<TokioMutex<()>> {
    let mut locks = session_locks().lock().unwrap();
    locks
        .entry(user_id.to_string())
        .or_insert_with(|| Arc::new(TokioMutex::new(())))
        .clone()
}

/// 构建 pi 命令行参数，返回 (cmd, 参数字符串用于日志)
fn build_pi_command(
    user_id: &str,
    model: &str,
    thinking: &str,
    files: &[PathBuf],
    message: &str,
) -> (Command, String) {
    let session_path = config::sessions_dir().join(format!("{}.jsonl", user_id));
    let skill_path = config::skill_path();

    let mut cmd = Command::new("pi");
    let mut log_parts = vec!["pi".to_string()];

    cmd.arg("-p");
    log_parts.push("-p".to_string());

    cmd.arg("--session").arg(&session_path);
    log_parts.push(format!("--session {}", session_path.display()));

    // 仅当显式指定了 model 时才传 --model
    if !model.is_empty() {
        cmd.arg("--model").arg(model);
        log_parts.push(format!("--model {}", model));
    }

    // 仅当显式指定了 thinking 且不是默认 "off" 时才传 --thinking
    if !thinking.is_empty() && thinking != "off" {
        cmd.arg("--thinking").arg(thinking);
        log_parts.push(format!("--thinking {}", thinking));
    }

    cmd.arg("--no-context-files");
    log_parts.push("--no-context-files".to_string());

    // 加载技能文件（如果存在）
    if skill_path.exists() {
        cmd.arg("--skill").arg(&skill_path);
        log_parts.push(format!("--skill {}", skill_path.display()));
    }

    // 附加文件
    for file in files {
        if file.exists() {
            cmd.arg("@").arg(file);
            log_parts.push(format!("@{} (file)", file.display()));
        }
    }

    cmd.arg(message);
    log_parts.push(format!("message={}", message.chars().take(60).collect::<String>()));

    let log_cmd = log_parts.join(" \\\n    ");
    (cmd, log_cmd)
}

/// 向 pi 发送 prompt，返回回复
///
/// - `user_id`: 用户 UUID，用作 session 文件名
/// - `message`: prompt 文本
/// - `model`: pi 模型名（如 sonnet, haiku）
/// - `files`: 附件文件路径列表（图片/文档，会通过 `@path` 传入）
pub async fn query_pi(
    user_id: &str,
    message: &str,
    model: &str,
    thinking: &str,
    files: &[PathBuf],
) -> Result<PiResult, String> {
    // 获取 per-user 锁，防止同一用户并发调用 pi 导致 session 损坏
    let user_lock = get_user_lock(user_id).await;
    let _lock = user_lock.lock().await;

    let (mut cmd, log_cmd) = build_pi_command(user_id, model, thinking, files, message);

    tracing::info!("[Pi] 执行:\n    {}", log_cmd);

    let output = cmd
        .output()
        .await
        .map_err(|e| format!("启动 pi 失败: {}", e))?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();

    if !output.status.success() {
        let err_msg = if !stderr.is_empty() {
            stderr
        } else {
            stdout.clone()
        };
        tracing::error!("[Pi] 失败 (exit={}): {}", output.status.code().unwrap_or(-1), err_msg);
        return Err(format!("pi 错误: {}", err_msg));
    }

    if stdout.is_empty() {
        tracing::warn!("[Pi] 返回空回复");
    } else {
        tracing::info!(
            "[Pi] 回复: {}",
            stdout.chars().take(120).collect::<String>()
        );
    }

    // 检查 files_out 目录是否有新文件（pi 通过 bash 创建的）
    let generated_files = scan_generated_files();

    Ok(PiResult {
        text: stdout,
        files: generated_files,
    })
}

/// 重置用户 session（删除 session 文件）
pub fn reset_session(user_id: &str) -> Result<(), String> {
    let session_path = config::sessions_dir().join(format!("{}.jsonl", user_id));
    if session_path.exists() {
        std::fs::remove_file(&session_path)
            .map_err(|e| format!("删除 session 失败: {}", e))?;
        tracing::info!("[Pi] Session 已重置: {}", user_id);
    }
    Ok(())
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
