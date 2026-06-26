use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::OnceLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub name: String,
    pub model: String,
    pub thinking: String,
    pub port: u16,
    /// 文件保存路径（可选），默认 ~/Downloads
    pub files: Option<String>,
    /// 数据目录（可选），默认 ~/.local/share/lanclaw
    pub data: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            name: "LANClaw".to_string(),
            model: String::new(),
            thinking: "off".to_string(),
            port: 8888,
            files: None,
            data: None,
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

/// 有效数据目录（OnceLock，启动时通过 init_data_dir 设置）
static EFFECTIVE_DATA_DIR: OnceLock<PathBuf> = OnceLock::new();

/// 启动时初始化数据目录（仅调用一次）
/// 优先级：CLI --data > 配置文件 data > 默认 ~/.local/share/lanclaw
pub fn init_data_dir(cli_override: Option<&str>) -> PathBuf {
    let cfg = Config::load();
    let path = resolve_data_path(cli_override, cfg.data.as_deref());
    let _ = std::fs::create_dir_all(&path);
    let _ = EFFECTIVE_DATA_DIR.set(path.clone());
    tracing::info!("[Config] 数据目录: {}", path.display());
    path
}

fn resolve_data_path(cli: Option<&str>, config: Option<&str>) -> PathBuf {
    // CLI 最高优先级
    if let Some(p) = cli {
        return expand_tilde(p);
    }
    // 配置文件次之
    if let Some(p) = config {
        return expand_tilde(p);
    }
    // 默认
    project_dirs().data_dir().to_path_buf()
}

pub fn data_dir() -> PathBuf {
    EFFECTIVE_DATA_DIR.get().cloned().unwrap_or_else(|| {
        let path = project_dirs().data_dir().to_path_buf();
        let _ = std::fs::create_dir_all(&path);
        path
    })
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

/// 有效文件保存目录（OnceLock，启动时通过 init_files_dir 设置）
static EFFECTIVE_FILES_DIR: OnceLock<PathBuf> = OnceLock::new();

/// 启动时初始化文件保存目录（仅调用一次）
/// 优先级：CLI 参数 > 配置文件 > 默认 ~/Downloads
pub fn init_files_dir(cli_override: Option<&str>) -> PathBuf {
    let cfg = Config::load();
    let path = resolve_files_path(cli_override, cfg.files.as_deref());
    let _ = std::fs::create_dir_all(&path);
    let _ = EFFECTIVE_FILES_DIR.set(path.clone());
    tracing::info!("[Config] 文件保存目录: {}", path.display());
    path
}

fn resolve_files_path(cli: Option<&str>, config: Option<&str>) -> PathBuf {
    // CLI 最高优先级
    if let Some(p) = cli {
        return expand_tilde(p);
    }
    // 配置文件次之
    if let Some(p) = config {
        return expand_tilde(p);
    }
    // 默认 ~/Downloads
    default_files_dir()
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(home) = dirs::home_dir() {
        if path == "~" {
            return home;
        }
        if path.starts_with("~/") {
            return PathBuf::from(path.replacen("~/", &format!("{}/", home.to_string_lossy()), 1));
        }
    }
    PathBuf::from(path)
}

fn default_files_dir() -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join("Downloads"))
        .unwrap_or_else(|| PathBuf::from("/tmp/lanclaw-files"))
}

/// 获取有效的文件保存目录
pub fn files_dir() -> PathBuf {
    EFFECTIVE_FILES_DIR.get().cloned().unwrap_or_else(|| {
        let path = default_files_dir();
        let _ = std::fs::create_dir_all(&path);
        path
    })
}

pub fn peers_path() -> PathBuf {
    data_dir().join("peers.json")
}

pub fn tasks_path() -> PathBuf {
    data_dir().join("tasks.json")
}


