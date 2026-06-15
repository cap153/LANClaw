use crate::config;
use crate::models::{TaskLog, TaskSchedule, TaskStore, TimerTask};
use chrono::Local;
use fs2::FileExt;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::sync::Arc;

/// 加载任务存储（带文件锁）
fn load_tasks() -> Result<TaskStore, String> {
    let path = config::tasks_path();
    // 确保父目录存在
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .map_err(|e| format!("打开 tasks.json 失败: {}", e))?;

    // 尝试加共享锁（读取）
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

    // 确保父目录存在
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .map_err(|e| format!("打开 tasks.json 失败: {}", e))?;

    // 加排他锁
    file.lock_exclusive().map_err(|e| format!("加锁失败: {}", e))?;

    let content =
        serde_json::to_string_pretty(store).map_err(|e| format!("序列化失败: {}", e))?;
    writeln!(&file, "{}", content).map_err(|e| format!("写入失败: {}", e))?;

    let _ = file.unlock();
    Ok(())
}

// ─── CLI 子命令 ──────────────────────────────────────────────────────────────

/// 添加定时任务
pub fn add_task(
    when: &str,
    prompt: &str,
    creator_id: &str,
    creator_name: &str,
    model: &str,
    thinking: &str,
) -> Result<String, String> {
    let mut store = load_tasks()?;

    let schedule = parse_when(when)?;
    let id = uuid::Uuid::new_v4().to_string();
    let now = Local::now().timestamp() as u64;

    let task = TimerTask {
        id: id.clone(),
        creator_id: creator_id.to_string(),
        creator_name: creator_name.to_string(),
        schedule,
        prompt: prompt.to_string(),
        model: model.to_string(),
        thinking: thinking.to_string(),
        created_at: now,
        status: "pending".to_string(),
        logs: Vec::new(),
    };

    store.tasks.push(task);
    save_tasks(&store)?;

    Ok(id)
}

/// 解析时间表达式
fn parse_when(when: &str) -> Result<TaskSchedule, String> {
    // daily:HH:MM
    if let Some(time) = when.strip_prefix("daily:") {
        let parts: Vec<&str> = time.split(':').collect();
        if parts.len() == 2 {
            let h: u32 = parts[0].parse().map_err(|_| "无效的小时")?;
            let m: u32 = parts[1].parse().map_err(|_| "无效的分钟")?;
            if h < 24 && m < 60 {
                return Ok(TaskSchedule::Daily {
                    time: format!("{:02}:{:02}", h, m),
                });
            }
        }
        return Err("格式: daily:HH:MM".to_string());
    }

    // weekly:day:HH:MM  (day: mon/tue/wed/thu/fri/sat/sun)
    if let Some(rest) = when.strip_prefix("weekly:") {
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() == 3 {
            let day = parts[0].to_lowercase();
            let valid_days = [
                "mon", "tue", "wed", "thu", "fri", "sat", "sun",
            ];
            if valid_days.contains(&day.as_str()) {
                let h: u32 = parts[1].parse().map_err(|_| "无效的小时")?;
                let m: u32 = parts[2].parse().map_err(|_| "无效的分钟")?;
                if h < 24 && m < 60 {
                    return Ok(TaskSchedule::Weekly {
                        day,
                        time: format!("{:02}:{:02}", h, m),
                    });
                }
            }
            return Err("格式: weekly:day:HH:MM (day=mon/tue/wed/thu/fri/sat/sun)".to_string());
        }
    }

    // 相对时间: 30min, 2h, 1.5h
    if let Some(n) = when.strip_suffix("min") {
        let minutes: f64 = n.parse().map_err(|_| "无效的时间")?;
        let secs = (minutes * 60.0) as u64;
        let execute_at = Local::now().timestamp() as u64 + secs;
        return Ok(TaskSchedule::Once { execute_at });
    }
    if let Some(n) = when.strip_suffix('h') {
        let hours: f64 = n.parse().map_err(|_| "无效的时间")?;
        let secs = (hours * 3600.0) as u64;
        let execute_at = Local::now().timestamp() as u64 + secs;
        return Ok(TaskSchedule::Once { execute_at });
    }

    // 绝对时间: 2026-06-15T09:00
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(when, "%Y-%m-%dT%H:%M") {
        let execute_at = dt.and_utc().timestamp() as u64;
        let now = Local::now().timestamp() as u64;
        if execute_at <= now {
            return Err("指定的时间已过去".to_string());
        }
        return Ok(TaskSchedule::Once { execute_at });
    }

    Err("无法解析时间，支持的格式: 30min, 2h, daily:08:00, weekly:mon:09:00, 2026-06-15T09:00".to_string())
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
        };

        let status_icon = match task.status.as_str() {
            "pending" => "⏳",
            "completed" => "✅",
            "cancelled" => "❌",
            _ => "❓",
        };

        let log_count = task.logs.len();
        let model_info = if task.model.is_empty() { "pi default".to_string() } else { task.model.clone() };
        let think_info = if task.thinking == "off" { String::new() } else { format!(" | thinking: {}", task.thinking) };

        output.push_str(&format!(
            "  {} [{}] {}\n    创建者: {} | 模型: {}{}\n    状态: {} | 执行 {} 次\n    内容: {}\n\n",
            status_icon,
            task.id.chars().take(8).collect::<String>(),
            schedule_str,
            task.creator_name,
            model_info,
            think_info,
            task.status,
            log_count,
            task.prompt.chars().take(100).collect::<String>(),
        ));
    }

    Ok(output)
}

/// 取消任务
pub fn cancel_task(task_id: &str) -> Result<String, String> {
    let mut store = load_tasks()?;

    let idx = store
        .tasks
        .iter()
        .position(|t| (t.id == task_id || t.id.starts_with(task_id)) && t.status == "pending");

    match idx {
        Some(i) => {
            let prompt = store.tasks[i].prompt.clone();
            store.tasks[i].status = "cancelled".to_string();
            save_tasks(&store)?;
            Ok(format!(
                "✅ 任务已取消: {}",
                prompt.chars().take(60).collect::<String>()
            ))
        }
        None => {
            // 检查是否有匹配但状态不是 pending 的任务
            let exists = store.tasks.iter().any(|t| t.id == task_id || t.id.starts_with(task_id));
            if exists {
                Err(format!("任务状态不是 pending，无法取消"))
            } else {
                Err("未找到该任务".to_string())
            }
        }
    }
}

/// 查看任务日志
pub fn task_logs(task_id: &str) -> Result<String, String> {
    let store = load_tasks()?;

    let task = store
        .tasks
        .iter()
        .find(|t| t.id == task_id || t.id.starts_with(task_id));

    match task {
        Some(t) => {
            if t.logs.is_empty() {
                Ok(format!("📋 任务「{}」暂无执行记录", t.prompt.chars().take(40).collect::<String>()))
            } else {
                let mut output = format!(
                    "📋 任务「{}」执行记录 (共 {} 次):\n\n",
                    t.prompt.chars().take(60).collect::<String>(),
                    t.logs.len()
                );
                for (i, log) in t.logs.iter().enumerate() {
                    let dt = chrono::DateTime::from_timestamp(log.executed_at as i64, 0)
                        .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| log.executed_at.to_string());
                    output.push_str(&format!(
                        "  #{}\n  时间: {}\n  耗时: {}s\n  结果: {}\n\n",
                        i + 1,
                        dt,
                        log.duration_secs,
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
///
/// `send_fn`: 发送消息给用户的回调（user_id, message）
pub async fn start_scheduler(
    send_fn: Arc<dyn Fn(String, String) + Send + Sync + 'static>,
    rpc: Arc<crate::rpc_client::RpcClient>,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

    loop {
        interval.tick().await;
        let _ = tick(&send_fn, &rpc).await;
    }
}

async fn tick(send_fn: &Arc<dyn Fn(String, String) + Send + Sync + 'static>, rpc: &Arc<crate::rpc_client::RpcClient>) {
    let now = Local::now().timestamp() as u64;

    // 先收集所有需要执行的任务 ID
    let due_tasks = match load_tasks() {
        Ok(store) => store
            .tasks
            .iter()
            .filter(|t| t.status == "pending")
            .filter_map(|t| t.next_run().map(|nr| (t.id.clone(), nr)))
            .filter(|(_, nr)| *nr <= now)
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        Err(e) => {
            eprintln!("[Scheduler] 加载任务失败: {}", e);
            return;
        }
    };

    if due_tasks.is_empty() {
        return;
    }

    // 逐个执行到期任务，每次原子操作 tasks.json
    for task_id in &due_tasks {
        // 加载任务信息（执行前快照）
        let task_info = match load_tasks() {
            Ok(ref store) => store.tasks.iter().find(|t| t.id == *task_id).cloned(),
            Err(_) => None,
        };

        let task_info = match task_info {
            Some(t) => t,
            None => continue,
        };

        tracing::info!(
            "[Scheduler] 执行任务: {} (创建者: {})",
            task_info.prompt.chars().take(60).collect::<String>(),
            task_info.creator_name
        );

        let start = std::time::Instant::now();

        let prompt_msg = format!("【定时任务】{}", task_info.prompt);
        let result = rpc.prompt(
            &task_info.creator_id,
            &prompt_msg,
            &[],
        )
        .await;

        let duration = start.elapsed().as_secs();
        let result_text = match &result {
            Ok(r) => r.text.clone(),
            Err(e) => format!("执行失败: {}", e),
        };

        let log = TaskLog {
            executed_at: now,
            result: result_text.clone(),
            duration_secs: duration,
        };

        // 原子更新：加载当前 store，修改该任务，保存
        match load_tasks() {
            Ok(mut store) => {
                if let Some(t) = store.tasks.iter_mut().find(|t| t.id == *task_id) {
                    t.logs.push(log);

                    match &t.schedule {
                        TaskSchedule::Once { .. } => {
                            t.status = "completed".to_string();
                            // 单次任务：发送日志给创建者
                            let msg = format!(
                                "⏰ 定时任务完成: {}\n\n{}",
                                t.prompt, &result_text
                            );
                            send_fn(t.creator_id.clone(), msg);
                        }
                        _ => {
                            // 重复任务：只记录日志
                        }
                    }

                    let _ = save_tasks(&store);
                }
            }
            Err(e) => eprintln!("[Scheduler] 加载任务失败: {}", e),
        }
    }
}
