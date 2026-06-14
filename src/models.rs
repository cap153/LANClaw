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
    pub is_offline: bool,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimerTask {
    pub id: String,
    pub creator_id: String,
    pub creator_name: String,
    pub schedule: TaskSchedule,
    pub prompt: String,
    pub model: String,
    pub thinking: String,
    pub created_at: u64,
    pub status: String,   // "pending" | "completed" | "cancelled"
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
    pub files: Vec<std::path::PathBuf>,
}
