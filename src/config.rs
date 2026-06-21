use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub name: String,
    pub model: String,
    pub thinking: String,
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            name: "LANClaw".to_string(),
            model: String::new(),
            thinking: "off".to_string(),
            port: 8888,
        }
    }
}

/// Get project directories (Linux: ~/.local/share/lanclaw/, ~/.config/lanclaw/)
pub fn project_dirs() -> ProjectDirs {
    ProjectDirs::from("", "", "lanclaw").expect("无法获取项目目录")
}

pub fn config_path() -> PathBuf {
    let dirs = project_dirs();
    let dir = dirs.config_dir().to_path_buf();
    dir.join("config.json")
}

pub fn data_dir() -> PathBuf {
    project_dirs().data_dir().to_path_buf()
}

impl Config {
    pub fn load() -> Self {
        let path = config_path();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        match std::fs::read_to_string(&path) {
            Ok(content) => {
                serde_json::from_str(&content).unwrap_or_else(|e| {
                    eprintln!("[config] 解析失败 ({}), 使用默认配置", e);
                    let cfg = Config::default();
                    cfg.save();
                    cfg
                })
            }
            Err(_) => {
                let cfg = Config::default();
                cfg.save();
                cfg
            }
        }
    }

    fn save(&self) {
        let path = config_path();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(content) = serde_json::to_string_pretty(self) {
            let _ = std::fs::write(&path, content);
        }
    }

    /// 更新 model 字段并持久化
    pub fn update_model(&mut self, model: &str) {
        self.model = model.to_string();
        self.save();
    }
}

/// Get bot's persistent UUID (stored in data dir)
pub fn bot_id() -> String {
    let dir = data_dir();
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("bot_id.txt");

    if let Ok(id) = std::fs::read_to_string(&path) {
        let trimmed = id.trim().to_string();
        if !trimmed.is_empty() && trimmed.len() == 36 {
            return trimmed;
        }
    }

    let new_id = uuid::Uuid::new_v4().to_string();
    let _ = std::fs::write(&path, &new_id);
    new_id
}

/// Directories for LANClaw data
pub fn sessions_dir() -> PathBuf {
    let dir = data_dir().join("sessions");
    let _ = std::fs::create_dir_all(&dir);
    dir
}

pub fn files_dir() -> PathBuf {
    let dir = data_dir().join("files");
    let _ = std::fs::create_dir_all(&dir);
    dir
}

pub fn peers_path() -> PathBuf {
    data_dir().join("peers.json")
}

pub fn tasks_path() -> PathBuf {
    data_dir().join("tasks.json")
}


