use crate::config;
use crate::models::{TaskAction, TaskLog, TaskSchedule, TaskStore, TimerTask};
use chrono::Local;
use fs2::FileExt;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::sync::Arc;

/// 加载任务存储（带文件锁）
fn load_tasks() -> Result<TaskStore, String> {
    let path = config::tasks_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .map_err(|e| format!("打开 tasks.json 失败: {}", e))?;

    file.lock_shared().map_err(|e| format!("加锁失败: {}", e))?;

    let mut content = String::new();
    file.read_to_string(&mut content).unwrap_or_default();

    let store: TaskStore = if content.trim().is_empty() {
        TaskStore { tasks: Vec::new() }
    } else {
        serde_json::from_str(&content).unwrap_or_else(|e| {
            eprintln!("[Scheduler] tasks.json 解析失败: {}，使用空列表", e);
            TaskStore { tasks: Vec::new() }
        })
    };

    let _ = file.unlock();
    Ok(store)
}

/// 保存任务存储（带文件锁）
fn save_tasks(store: &TaskStore) -> Result<(), String> {
    let path = config::tasks_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .map_err(|e| format!("打开 tasks.json 失败: {}", e))?;

    file.lock_exclusive().map_err(|e| format!("加锁失败: {}", e))?;
    let content = serde_json::to_string_pretty(store).map_err(|e| format!("序列化失败: {}", e))?;
    writeln!(&file, "{}", content).map_err(|e| format!("写入失败: {}", e))?;
    let _ = file.unlock();
    Ok(())
}

// ─── 辅助：动作列表转摘要文本 ──────────────────────────────────────────────

fn actions_summary(actions: &[TaskAction], max_chars: usize) -> String {
    let mut parts: Vec<String> = Vec::new();
    for a in actions {
        match a {
            TaskAction::Reply { message } => {
                parts.push(format!("回复: {}", message.chars().take(40).collect::<String>()));
            }
            TaskAction::Exec { command } => {
                parts.push(format!("执行: {}", command.chars().take(40).collect::<String>()));
            }
            TaskAction::SendFile { path } => {
                let name = std::path::Path::new(path)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or(path);
                parts.push(format!("发文件: {}", name));
            }
        }
    }
    let joined = parts.join(" | ");
    if joined.len() > max_chars {
        format!("{}...", joined.chars().take(max_chars).collect::<String>())
    } else {
        joined
    }
}

// ─── CLI 子命令 ──────────────────────────────────────────────────────────────

/// 添加定时任务
pub fn add_task(
    when: &str,
    actions: &[TaskAction],
    creator_id: &str,
    creator_name: &str,
) -> Result<String, String> {
    if actions.is_empty() {
        return Err("至少需要一个动作".to_string());
    }
    let mut store = load_tasks()?;
    let schedule = parse_when(when)?;
    let id = uuid::Uuid::new_v4().to_string();
    let now = Local::now().timestamp() as u64;

    let task = TimerTask {
        id: id.clone(),
        creator_id: creator_id.to_string(),
        creator_name: creator_name.to_string(),
        schedule,
        actions: actions.to_vec(),
        created_at: now,
        status: "pending".to_string(),
        logs: Vec::new(),
    };

    store.tasks.push(task);
    save_tasks(&store)?;
    Ok(id)
}

fn parse_when(when: &str) -> Result<TaskSchedule, String> {
    if let Some(time) = when.strip_prefix("daily:") {
        let parts: Vec<&str> = time.split(':').collect();
        if parts.len() == 2 {
            let h: u32 = parts[0].parse().map_err(|_| "无效的小时")?;
            let m: u32 = parts[1].parse().map_err(|_| "无效的分钟")?;
            if h < 24 && m < 60 {
                return Ok(TaskSchedule::Daily { time: format!("{:02}:{:02}", h, m) });
            }
        }
        return Err("格式: daily:HH:MM".to_string());
    }
    if let Some(rest) = when.strip_prefix("every:") {
        let secs = parse_duration(rest)?;
        if secs < 1 {
            return Err("间隔至少 1 秒".to_string());
        }
        return Ok(TaskSchedule::Every { interval_secs: secs });
    }
    if let Some(rest) = when.strip_prefix("weekly:") {
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() == 3 {
            let day = parts[0].to_lowercase();
            let valid_days = ["mon","tue","wed","thu","fri","sat","sun"];
            if valid_days.contains(&day.as_str()) {
                let h: u32 = parts[1].parse().map_err(|_| "无效的小时")?;
                let m: u32 = parts[2].parse().map_err(|_| "无效的分钟")?;
                if h < 24 && m < 60 {
                    return Ok(TaskSchedule::Weekly { day, time: format!("{:02}:{:02}", h, m) });
                }
            }
            return Err("格式: weekly:day:HH:MM".to_string());
        }
    }
    if let Some(n) = when.strip_suffix('s').or_else(|| when.strip_suffix("秒")) {
        let secs: f64 = n.parse().map_err(|_| "无效的时间")?;
        let execute_at = Local::now().timestamp() as u64 + secs as u64;
        return Ok(TaskSchedule::Once { execute_at });
    }
    if let Some(n) = when.strip_suffix("min") {
        let minutes: f64 = n.parse().map_err(|_| "无效的时间")?;
        let execute_at = Local::now().timestamp() as u64 + (minutes * 60.0) as u64;
        return Ok(TaskSchedule::Once { execute_at });
    }
    if let Some(n) = when.strip_suffix('h') {
        let hours: f64 = n.parse().map_err(|_| "无效的时间")?;
        let execute_at = Local::now().timestamp() as u64 + (hours * 3600.0) as u64;
        return Ok(TaskSchedule::Once { execute_at });
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(when, "%Y-%m-%dT%H:%M") {
        let execute_at = dt.and_utc().timestamp() as u64;
        if execute_at <= Local::now().timestamp() as u64 {
            return Err("指定的时间已过去".to_string());
        }
        return Ok(TaskSchedule::Once { execute_at });
    }
    Err("无法解析时间，支持的格式: 30s, 30min, 2h, every:10s, daily:HH:MM, weekly:day:HH:MM, 2026-06-15T09:00".to_string())
}

/// 解析间隔时长
fn parse_duration(s: &str) -> Result<u64, String> {
    if let Some(n) = s.strip_suffix('s') {
        Ok(n.parse::<f64>().map_err(|_| "无效时间")? as u64)
    } else if let Some(n) = s.strip_suffix("min") {
        Ok((n.parse::<f64>().map_err(|_| "无效时间")? * 60.0) as u64)
    } else if let Some(n) = s.strip_suffix('h') {
        Ok((n.parse::<f64>().map_err(|_| "无效时间")? * 3600.0) as u64)
    } else {
        // 纯数字默认秒
        s.parse::<u64>().map_err(|_| "格式: every:10s / every:5min / every:2h".to_string())
    }
}

/// 列出所有任务
pub fn list_tasks() -> Result<String, String> {
    let store = load_tasks()?;
    if store.tasks.is_empty() {
        return Ok("📋 暂无定时任务".to_string());
    }

    let mut output = String::from("📋 定时任务列表:\n\n");
    for task in &store.tasks {
        let schedule_str = match &task.schedule {
            TaskSchedule::Once { execute_at } => {
                let dt = chrono::DateTime::from_timestamp(*execute_at as i64, 0)
                    .map(|d| d.format("%Y-%m-%d %H:%M").to_string())
                    .unwrap_or_else(|| execute_at.to_string());
                format!("单次 @ {}", dt)
            }
            TaskSchedule::Daily { time } => format!("每天 {}", time),
            TaskSchedule::Weekly { day, time } => format!("每周{} {}", day, time),
            TaskSchedule::Every { interval_secs } => {
                if *interval_secs < 60 {
                    format!("每 {} 秒", interval_secs)
                } else if *interval_secs < 3600 {
                    format!("每 {} 分钟", interval_secs / 60)
                } else {
                    format!("每 {} 小时", interval_secs / 3600)
                }
            }
        };

        let status_icon = match task.status.as_str() {
            "pending" => "⏳", "completed" => "✅", "cancelled" => "❌", _ => "❓",
        };

        let log_count = task.logs.len();
        let summary = actions_summary(&task.actions, 100);

        output.push_str(&format!(
            "  {} [{}] {}\n    创建者: {}\n    状态: {} | 执行 {} 次\n    动作: {}\n\n",
            status_icon,
            task.id.chars().take(8).collect::<String>(),
            schedule_str,
            task.creator_name,
            task.status, log_count, summary,
        ));
    }
    Ok(output)
}

/// 取消任务
pub fn cancel_task(task_id: &str) -> Result<String, String> {
    let mut store = load_tasks()?;
    let idx = store.tasks.iter().position(|t| {
        (t.id == task_id || t.id.starts_with(task_id)) && t.status == "pending"
    });
    match idx {
        Some(i) => {
            let summary = actions_summary(&store.tasks[i].actions, 60);
            store.tasks[i].status = "cancelled".to_string();
            save_tasks(&store)?;
            Ok(format!("✅ 任务已取消: {}", summary))
        }
        None => {
            let exists = store.tasks.iter().any(|t| t.id == task_id || t.id.starts_with(task_id));
            if exists { Err("任务状态不是 pending，无法取消".to_string()) }
            else { Err("未找到该任务".to_string()) }
        }
    }
}

/// 查看任务日志
pub fn task_logs(task_id: &str) -> Result<String, String> {
    let store = load_tasks()?;
    let task = store.tasks.iter().find(|t| t.id == task_id || t.id.starts_with(task_id));
    match task {
        Some(t) => {
            let summary = actions_summary(&t.actions, 40);
            if t.logs.is_empty() {
                Ok(format!("📋 任务「{}」暂无执行记录", summary))
            } else {
                let mut output = format!("📋 任务「{}」执行记录 (共 {} 次):\n\n", summary, t.logs.len());
                for (i, log) in t.logs.iter().enumerate() {
                    let dt = chrono::DateTime::from_timestamp(log.executed_at as i64, 0)
                        .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| log.executed_at.to_string());
                    output.push_str(&format!(
                        "  #{}\n  时间: {}\n  耗时: {}s\n  结果: {}\n\n",
                        i + 1, dt, log.duration_secs,
                        log.result.chars().take(200).collect::<String>()
                    ));
                }
                Ok(output)
            }
        }
        None => Err("未找到该任务".to_string()),
    }
}

// ─── 后台调度器 ──────────────────────────────────────────────────────────────

/// 启动调度器后台循环
pub async fn start_scheduler(
    send_fn: Arc<dyn Fn(String, String) + Send + Sync + 'static>,
    peers: crate::models::PeerMap,
    bot_id: String,
    bot_name: String,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

    // 快速启动：前 3 秒每秒检查一次
    let mut fast_tick = 0u32;
    loop {
        if fast_tick < 3 {
            interval.tick().await;
            fast_tick += 1;
        } else {
            interval.tick().await;
        }
        if let Err(e) = tick(&send_fn, &peers, &bot_id, &bot_name).await {
            eprintln!("[Scheduler] tick 错误: {}", e);
        }
    }
}

async fn tick(
    send_fn: &Arc<dyn Fn(String, String) + Send + Sync + 'static>,
    peers: &crate::models::PeerMap,
    bot_id: &str,
    _bot_name: &str,
) -> Result<(), String> {
    let now = Local::now().timestamp() as u64;

    let due_tasks: Vec<(String, TimerTask)> = match load_tasks() {
        Ok(store) => store.tasks.into_iter()
            .filter(|t| t.status == "pending")
            .filter_map(|t| t.next_run().map(|nr| (nr, t)))
            .filter(|(nr, _)| *nr <= now)
            .map(|(_, t)| (t.id.clone(), t))
            .collect(),
        Err(e) => { eprintln!("[Scheduler] 加载任务失败: {}", e); return Ok(()); }
    };

    for (task_id, task_info) in &due_tasks {
        tracing::info!("[Scheduler] 执行: {}", actions_summary(&task_info.actions, 60));
        let start = std::time::Instant::now();

        let mut output_parts: Vec<String> = Vec::new();

        for action in &task_info.actions {
            match action {
                TaskAction::Reply { message } => {
                    output_parts.push(message.clone());
                }
                TaskAction::Exec { command } => {
                    match tokio::process::Command::new("sh")
                        .arg("-c")
                        .arg(command)
                        .output()
                        .await
                    {
                        Ok(out) => {
                            let out_text = String::from_utf8_lossy(&out.stdout).trim().to_string();
                            let err_text = String::from_utf8_lossy(&out.stderr).trim().to_string();
                            let has_out = !out_text.is_empty();
                            let has_err = !err_text.is_empty();
                            if has_out {
                                output_parts.push(out_text);
                            }
                            if has_err {
                                output_parts.push(format!("[stderr]\n{}", err_text));
                            }
                            if !has_out && !has_err {
                                output_parts.push(format!("(命令执行成功，exit={})", out.status.code().unwrap_or(-1)));
                            }
                        }
                        Err(e) => {
                            output_parts.push(format!("命令执行失败: {}", e));
                        }
                    }
                }
                TaskAction::SendFile { path } => {
                    let file_path = std::path::PathBuf::from(path);
                    let file_name = file_path.file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("file")
                        .to_string();

                    // 查找用户地址并发文件
                    let addr = {
                        let map = peers.read().await;
                        map.get(&task_info.creator_id).map(|p| p.addr.clone())
                    };

                    match addr {
                        Some(peer_addr) if file_path.exists() => {
                            match crate::network::file::send_file_to_peer(&peer_addr, bot_id, &file_path).await {
                                Ok(_) => output_parts.push(format!("📎 已发送文件: {}", file_name)),
                                Err(e) => output_parts.push(format!("📎 发送文件失败: {}", e)),
                            }
                        }
                        Some(_) => output_parts.push(format!("📎 文件不存在: {}", file_name)),
                        None => output_parts.push("📎 用户不在线，无法发送文件".to_string()),
                    }
                }
            }
        }

        let duration = start.elapsed().as_secs();
        let result_text = output_parts.join("\n\n");

        // 更新任务状态和日志
        match load_tasks() {
            Ok(mut store) => {
                // 先找到任务（判断类型，获取 creator_id）
                let (is_once, creator_id) = store.tasks.iter().find(|t| t.id == *task_id)
                    .map(|t| (matches!(t.schedule, TaskSchedule::Once { .. }), t.creator_id.clone()))
                    .unwrap_or((false, String::new()));

                // 记日志（所有类型任务）
                if let Some(t) = store.tasks.iter_mut().find(|t| t.id == *task_id) {
                    t.logs.push(TaskLog {
                        executed_at: now,
                        result: result_text.clone(),
                        duration_secs: duration,
                    });
                }

                // 单次任务：发送通知后删除；重复任务：仅保存日志
                if is_once {
                    let msg = format!("⏰ 定时任务完成\n\n{}", result_text);
                    send_fn(creator_id, msg);
                    store.tasks.retain(|t| t.id != *task_id);
                }

                let _ = save_tasks(&store);
            }
            Err(e) => eprintln!("[Scheduler] 加载任务失败: {}", e),
        }
    }

    Ok(())
}
