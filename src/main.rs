mod config;
mod models;
mod network;
mod pi_bridge;
mod router;
mod scheduler;
mod skill_gen;

use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::ConnectInfo;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use clap::Parser;
use network::file::AppState;
use network::messaging;
use std::net::SocketAddr;
use std::sync::Arc;

/// LANClaw — LANChat 智能机器人（Pi 驱动）
#[derive(Parser, Debug)]
#[command(name = "lanclaw", version, about)]
struct Cli {
    /// 机器人显示名
    #[arg(long, default_value = "LANClaw")]
    name: String,

    /// Pi 模型（不指定则使用 pi 默认模型）
    #[arg(long)]
    model: Option<String>,

    /// 监听端口（与 LANChat 协议兼容）
    #[arg(long, default_value_t = 8888)]
    port: u16,

    /// 子命令（供 pi 的 bash 工具调用）
    #[command(subcommand)]
    command: Option<TaskCommand>,
}

#[derive(Parser, Debug)]
enum TaskCommand {
    /// 管理定时任务
    #[command(name = "task")]
    Task(TaskArgs),
}

#[derive(Parser, Debug)]
struct TaskArgs {
    #[command(subcommand)]
    action: TaskAction,
}

#[derive(Parser, Debug)]
enum TaskAction {
    /// 添加定时任务
    Add {
        /// 时间：30min, 2h, daily:08:00, weekly:mon:09:00, 2026-06-15T09:00
        when: String,
        /// 任务提示词
        prompt: String,
        /// 创建者用户 ID
        #[arg(long)]
        user_id: String,
        /// 创建者显示名
        #[arg(long, default_value = "用户")]
        user_name: String,
        /// 使用的模型（不指定则使用 pi 默认模型）
        #[arg(long)]
        model: Option<String>,
    },
    /// 列出所有任务
    List,
    /// 取消任务
    Cancel {
        /// 任务 ID（支持前缀匹配）
        id: String,
    },
    /// 查看任务执行日志
    Logs {
        /// 任务 ID（支持前缀匹配）
        id: String,
    },
}

#[tokio::main]
async fn main() {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "lanclaw=info".into()),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();

    // ─── 子命令模式（`lanclaw task ...`） ──────────────────────────
    if let Some(cmd) = cli.command {
        match cmd {
            TaskCommand::Task(task) => match task.action {
                TaskAction::Add {
                    when,
                    prompt,
                    user_id,
                    user_name,
                    model,
                } => match scheduler::add_task(&when, &prompt, &user_id, &user_name, &model.as_deref().unwrap_or_default()) {
                    Ok(id) => println!("✅ 任务已创建 (ID: {})", id),
                    Err(e) => eprintln!("❌ {}", e),
                },
                TaskAction::List => match scheduler::list_tasks() {
                    Ok(output) => println!("{}", output),
                    Err(e) => eprintln!("❌ {}", e),
                },
                TaskAction::Cancel { id } => match scheduler::cancel_task(&id) {
                    Ok(msg) => println!("{}", msg),
                    Err(e) => eprintln!("❌ {}", e),
                },
                TaskAction::Logs { id } => match scheduler::task_logs(&id) {
                    Ok(output) => println!("{}", output),
                    Err(e) => eprintln!("❌ {}", e),
                },
            },
        }
        return;
    }

    // ─── 服务模式 ────────────────────────────────────────────────
    println!("╔══════════════════════════════════╗");
    println!("║        LANClaw v{}           ║", env!("CARGO_PKG_VERSION"));
    println!("╚══════════════════════════════════╝");

    let cfg = config::Config::load();
    let bot_id = config::bot_id();

    // 使用 CLI 参数覆盖配置（不保存到文件）
    let bot_name = if !cli.name.is_empty() { cli.name } else { cfg.name };
    let bot_model = cli.model.clone().unwrap_or(cfg.model);
    let port = cli.port;

    println!("  Bot Name:    {}", bot_name);
    println!("  Bot ID:      {}", &bot_id[..8]);
    println!("  Port:        {}", port);
    println!("  Model:       {}", if bot_model.is_empty() { "pi default" } else { &bot_model });
    println!();
    println!("  Data:        {}", config::data_dir().display());
    println!("  Sessions:    {}", config::sessions_dir().display());
    println!("  Files:       {}", config::files_dir().display());
    println!();

    // 生成技能文件
    let _ = skill_gen::write_skill_file(&bot_name);
    println!("[Skill] 技能文件已生成");

    // 清理输出文件目录
    let _ = pi_bridge::clean_files_out();

    // 初始化 peer 列表
    let peers: models::PeerMap = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));

    // 创建消息通道：network → router
    let (msg_tx, mut msg_rx) = messaging::message_channel();

    let bot_config = Arc::new(router::BotConfig {
        name: bot_name.clone(),
        model: bot_model.clone(),
        bot_id: bot_id.clone(),
    });

    // ─── 启动 UDP 发现 ──────────────────────────────────────────
    let announce_id = bot_id.clone();
    let announce_name = bot_name.clone();
    let announce_port = port;
    tokio::spawn(async move {
        network::discovery::start_announcing(announce_port, announce_id, announce_name).await;
    });

    let listen_id = bot_id.clone();
    let listen_peers = peers.clone();
    let listen_port = port;
    tokio::spawn(async move {
        network::discovery::start_listening(listen_port, listen_id, listen_peers).await;
    });

    // ─── 启动 HTTP + WebSocket 服务器 ────────────────────────────
    let shared_state = Arc::new(AppState {
        msg_tx: msg_tx.clone(),
    });

    // HTTP + WS 路由
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .nest("/", network::file::file_routes())
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(shared_state)
        .into_make_service_with_connect_info::<SocketAddr>();

    let server_port = port;
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", server_port))
            .await
            .expect("无法绑定端口");
        println!("[Server] HTTP+WS 服务启动在端口 {}", server_port);
        axum::serve(listener, app).await.unwrap();
    });

    // ─── 启动调度器 ──────────────────────────────────────────────
    let scheduler_peers = peers.clone();
    let scheduler_config = bot_config.clone();
    let send_fn = Arc::new(move |user_id: String, message: String| {
        let peers = scheduler_peers.clone();
        let config = scheduler_config.clone();
        tokio::spawn(async move {
            let addr = {
                let map = peers.read().await;
                map.get(&user_id).map(|p| p.addr.clone())
            };
            if let Some(addr) = addr {
                let _ = messaging::send_text_message(
                    &addr,
                    config.bot_id.clone(),
                    config.name.clone(),
                    message,
                )
                .await;
            }
        });
    });

    tokio::spawn(async move {
        scheduler::start_scheduler(send_fn).await;
    });

    // ─── 主消息循环 ──────────────────────────────────────────────
    println!("[Ready] 等待消息...");
    println!("────────────────────────────────────────");

    while let Some((msg, peer_addr)) = msg_rx.recv().await {
        let peers = peers.clone();
        let config = bot_config.clone();
        tokio::spawn(async move {
            router::handle_message(msg, peer_addr, peers, &config).await;
        });
    }
}

/// WebSocket 连接处理
async fn ws_handler(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    state: axum::extract::State<Arc<AppState>>,
) -> impl IntoResponse {
    let tx = state.msg_tx.clone();
    ws.on_upgrade(move |socket| handle_socket(socket, tx, addr))
}

async fn handle_socket(socket: WebSocket, tx: messaging::MessageSender, addr: SocketAddr) {
    messaging::handle_ws_connection(socket, tx, addr).await;
}
