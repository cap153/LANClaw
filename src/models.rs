use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// ─── LANChat Protocol Messages ───────────────────────────────────────────────

/// 文本消息（LANChat 兼容）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextMessage {
    pub msg_type: String,
    pub from_id: String,
    pub from_name: String,
    pub content: String,
    pub timestamp: u64,
}

/// 握手消息（LANChat 兼容，离线补发用）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeMessage {
    pub protocol: String,
    pub action: String,
    pub from_id: String,
}

// ─── Peer Management ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    pub id: String,
    pub name: String,
    pub addr: String,
    #[serde(skip)]
    pub last_seen: u64,
    #[serde(default)]
    pub is_offline: bool,
    #[serde(default)]
    pub available_memory_mb: u64,
}

pub type PeerMap = Arc<RwLock<HashMap<String, Peer>>>;

// ─── Timer Task ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskLog {
    pub executed_at: u64,
    pub result: String,
    pub duration_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TaskSchedule {
    #[serde(rename = "once")]
    Once { execute_at: u64 },
    #[serde(rename = "daily")]
    Daily { time: String },          // "08:00"
    #[serde(rename = "weekly")]
    Weekly { day: String, time: String }, // day: "mon","tue",... time: "09:00"
    #[serde(rename = "monthly")]
    Monthly { day: u32, time: String },   // day=0 → 月末; time="09:00"
    #[serde(rename = "yearly")]
    Yearly { month: u32, day: u32, time: String }, // month=1-12, day=1-31, time="09:00"
    #[serde(rename = "every")]
    Every { interval_secs: u64 },
}

/// 任务到期时执行的动作
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TaskAction {
    /// 发送预定义的文本给用户
    #[serde(rename = "reply")]
    Reply { message: String },
    /// 执行 bash 命令，将输出发给用户
    #[serde(rename = "exec")]
    Exec { command: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimerTask {
    pub id: String,
    pub creator_id: String,
    pub creator_name: String,
    pub schedule: TaskSchedule,
    pub actions: Vec<TaskAction>,
    pub created_at: u64,
    pub status: String,
    pub logs: Vec<TaskLog>,
}

impl TimerTask {
    pub fn next_run(&self) -> Option<u64> {
        use chrono::{Datelike, Local, NaiveTime, Weekday};
        match &self.schedule {
            TaskSchedule::Once { execute_at } => {
                if self.status == "pending" {
                    Some(*execute_at)
                } else {
                    None
                }
            }
            TaskSchedule::Daily { time } => {
                if self.status != "pending" {
                    return None;
                }
                let parts: Vec<&str> = time.split(':').collect();
                let hour: u32 = parts.get(0).and_then(|s| s.parse().ok()).unwrap_or(8);
                let minute: u32 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
                let now = Local::now();
                let today = now.date_naive();
                let target = NaiveTime::from_hms_opt(hour, minute, 0).unwrap();
                let today_dt = today.and_time(target);
                let today_ts = today_dt.and_utc().timestamp() as u64;
                let now_ts = now.timestamp() as u64;
                if now_ts < today_ts {
                    Some(today_ts)
                } else {
                    // 明天
                    Some(today_ts + 86400)
                }
            }
            TaskSchedule::Weekly { day, time } => {
                if self.status != "pending" {
                    return None;
                }
                let weekday_map: HashMap<&str, Weekday> = [
                    ("mon", Weekday::Mon), ("tue", Weekday::Tue), ("wed", Weekday::Wed),
                    ("thu", Weekday::Thu), ("fri", Weekday::Fri), ("sat", Weekday::Sat),
                    ("sun", Weekday::Sun),
                ].iter().cloned().collect();
                let target_wd = weekday_map.get(day.as_str()).copied().unwrap_or(Weekday::Mon);
                let parts: Vec<&str> = time.split(':').collect();
                let hour: u32 = parts.get(0).and_then(|s| s.parse().ok()).unwrap_or(9);
                let minute: u32 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
                let now = Local::now();
                let target_time = NaiveTime::from_hms_opt(hour, minute, 0).unwrap();
                let mut current = now.date_naive();
                loop {
                    if current.weekday() == target_wd {
                        let dt = current.and_time(target_time);
                        let ts = dt.and_utc().timestamp() as u64;
                        if ts > now.timestamp() as u64 {
                            return Some(ts);
                        }
                    }
                    current = current.succ_opt().unwrap_or(current);
                }
            }
            TaskSchedule::Monthly { day, time } => {
                if self.status != "pending" {
                    return None;
                }
                let parts: Vec<&str> = time.split(':').collect();
                let hour: u32 = parts.get(0).and_then(|s| s.parse().ok()).unwrap_or(9);
                let minute: u32 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
                let target_time = NaiveTime::from_hms_opt(hour, minute, 0).unwrap();
                let now = Local::now();
                let today = now.date_naive();
                let this_month = today.with_day(1).unwrap_or(today);
                // day=0 表示月末
                let target_day = if *day == 0 {
                    // 当月最后一天
                    let next_first = if this_month.month() == 12 {
                        this_month.with_year(this_month.year() + 1).unwrap_or(this_month)
                            .with_month(1).unwrap_or(this_month)
                    } else {
                        this_month.with_month(this_month.month() + 1).unwrap_or(this_month)
                    };
                    next_first.pred_opt().unwrap_or(next_first).day()
                } else {
                    *day
                };

                // 先试本月
                if let Some(candidate) = today.with_day(target_day).map(|d| d.and_time(target_time)) {
                    let ts = candidate.and_utc().timestamp() as u64;
                    if ts > now.timestamp() as u64 {
                        return Some(ts);
                    }
                }

                // 下个月
                let next_month = if this_month.month() == 12 {
                    this_month.with_year(this_month.year() + 1).unwrap_or(this_month)
                        .with_month(1).unwrap_or(this_month)
                } else {
                    this_month.with_month(this_month.month() + 1).unwrap_or(this_month)
                };
                let target_day2 = if *day == 0 {
                    let next_first = if next_month.month() == 12 {
                        next_month.with_year(next_month.year() + 1).unwrap_or(next_month)
                            .with_month(1).unwrap_or(next_month)
                    } else {
                        next_month.with_month(next_month.month() + 1).unwrap_or(next_month)
                    };
                    next_first.pred_opt().unwrap_or(next_first).day()
                } else {
                    *day
                };
                if let Some(candidate) = next_month.with_day(target_day2).map(|d| d.and_time(target_time)) {
                    return Some(candidate.and_utc().timestamp() as u64);
                }

                // fallback: +30 天
                Some(now.timestamp() as u64 + 30 * 86400)
            }
            TaskSchedule::Yearly { month, day, time } => {
                if self.status != "pending" {
                    return None;
                }
                let parts: Vec<&str> = time.split(':').collect();
                let hour: u32 = parts.get(0).and_then(|s| s.parse().ok()).unwrap_or(9);
                let minute: u32 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
                let target_time = NaiveTime::from_hms_opt(hour, minute, 0).unwrap();
                let now = Local::now();
                let today = now.date_naive();

                // 今年
                if let Some(candidate) = today.with_month(*month).and_then(|d| d.with_day(*day)).map(|d| d.and_time(target_time)) {
                    let ts = candidate.and_utc().timestamp() as u64;
                    if ts > now.timestamp() as u64 {
                        return Some(ts);
                    }
                }

                // 明年
                if let Some(candidate) = today.with_year(today.year() + 1)
                    .and_then(|d| d.with_month(*month))
                    .and_then(|d| d.with_day(*day))
                    .map(|d| d.and_time(target_time))
                {
                    return Some(candidate.and_utc().timestamp() as u64);
                }

                Some(now.timestamp() as u64 + 365 * 86400)
            }
            TaskSchedule::Every { interval_secs } => {
                if self.status != "pending" {
                    return None;
                }
                let last = self.logs.last().map(|l| l.executed_at).unwrap_or(self.created_at);
                Some(last + interval_secs)
            }
        }
    }
}

// ─── Task Storage ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStore {
    pub tasks: Vec<TimerTask>,
}

// ─── Pi interaction result ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PiResult {
    pub text: String,
}

// ─── 流式 chunk（从 rpc_client 到 messaging 的内部通道类型） ───

#[derive(Debug, Clone)]
pub enum StreamChunk {
    /// 文本回复或思考内容
    Text { content: String, is_thinking: bool },
    /// 工具调用
    ToolCall { name: String, args: String },
    /// 工具执行结果
    ToolResult { output: String, is_error: bool },
}

// ─── File Complete Event (LANClaw 接收完文件后自动触发) ──────────────

#[derive(Debug, Clone)]
pub struct FileCompleteEvent {
    pub sender_id: String,
    pub file_path: std::path::PathBuf,
    pub file_name: String,
}

// ─── Model Info (from pi get_available_models / set_model) ────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInfo {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub api: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub reasoning: bool,
    #[serde(default)]
    pub context_window: Option<u64>,
    #[serde(default)]
    pub max_tokens: Option<u64>,
}
