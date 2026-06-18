mod config;
mod models;
mod network;
mod rpc_client;
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
use std::sync::atomic::AtomicBool;
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

    /// 思考级别: off, minimal, low, medium, high, xhigh
    #[arg(long, default_value = "off")]
    thinking: String,

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
        /// 时间：30s, 30min, 2h, every:10s, daily:08:00, weekly:mon:09:00, 2026-06-15T09:00
        when: String,
        /// 创建者用户 ID
        #[arg(long)]
        user_id: String,
        /// 创建者显示名
        #[arg(long, default_value = "用户")]
        user_name: String,
        /// 到期回复文本（可多次指定）
        #[arg(long)]
        reply: Vec<String>,
        /// 到期执行命令（可多次指定）
        #[arg(long)]
        exec: Vec<String>,
        /// 到期发送文件（可多次指定）
        #[arg(long)]
        file: Vec<String>,
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
                    reply,
                    exec,
                    file,
                    user_id,
                    user_name,
                } => {
                    let mut actions = Vec::new();
                    for msg in &reply {
                        actions.push(models::TaskAction::Reply { message: msg.clone() });
                    }
                    for cmd in &exec {
                        actions.push(models::TaskAction::Exec { command: cmd.clone() });
                    }
                    for path in &file {
                        actions.push(models::TaskAction::SendFile { path: path.clone() });
                    }
                    if actions.is_empty() {
                        eprintln!("❌ 请指定至少一个 --reply / --exec / --file");
                    } else {
                        match scheduler::add_task(
                            &when, &actions, &user_id, &user_name,
                        ) {
                            Ok(id) => println!("✅ 任务已创建 (ID: {})", id),
                            Err(e) => eprintln!("❌ {}", e),
                        }
                    }
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
    let bot_thinking = cli.thinking.clone();
    let port = cli.port;

    println!("  Bot Name:    {}", bot_name);
    println!("  Bot ID:      {}", &bot_id[..8]);
    println!("  Port:        {}", port);
    println!("  Model:       {}", if bot_model.is_empty() { "pi default" } else { &bot_model });
    println!("  Thinking:    {}", bot_thinking);
    println!();
    println!("  Data:        {}", config::data_dir().display());
    println!("  Sessions:    {}", config::sessions_dir().display());
    println!("  Files:       {}", config::files_dir().display());
    println!();

    // 生成技能文件
    let _ = skill_gen::write_skill_file(&bot_name);
    println!("[Skill] 技能文件已生成");

    // 清理输出文件目录
    let _ = rpc_client::clean_files_out();

    // 初始化 peer 列表
    let peers: models::PeerMap = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));

    // 创建消息通道：network → router
    let (msg_tx, mut msg_rx) = messaging::message_channel();

    // 创建文件完成事件通道：HTTP upload → auto pi processing
    let (file_complete_tx, mut file_complete_rx) =
        tokio::sync::mpsc::unbounded_channel::<crate::models::FileCompleteEvent>();

    // ─── 启动 RPC 客户端（pi --mode rpc 常驻子进程） ──────────
    let rpc_client = match rpc_client::RpcClient::spawn(
        &bot_model,
        &bot_thinking,
    ).await {
        Ok(c) => {
            println!("[RPC] pi RPC 子进程已启动");
            c
        }
        Err(e) => {
            eprintln!("❌ [RPC] 启动失败: {}", e);
            return;
        }
    };

    let bot_config = Arc::new(router::BotConfig {
        name: bot_name.clone(),
        bot_id: bot_id.clone(),
        rpc: rpc_client.clone(),
        switching_model: AtomicBool::new(false),
    });

    // ─── 启动文件自动处理（上传完成 → pi） ──────────────────────
    let fp_peers = peers.clone();
    let fp_rpc = rpc_client.clone();
    let fp_bot_id = bot_id.clone();
    let fp_bot_name = bot_name.clone();
    tokio::spawn(async move {
        use crate::network::file;
        use crate::network::messaging;
        use tokio::sync::mpsc;

        while let Some(event) = file_complete_rx.recv().await {
            if event.sender_id.is_empty() {
                tracing::warn!("[FileAuto] sender_id 为空，跳过");
                continue;
            }

            let peer_addr = {
                let map = fp_peers.read().await;
                map.get(&event.sender_id).map(|p| p.addr.clone())
            };

            let Some(addr) = peer_addr else {
                tracing::warn!("[FileAuto] 发送者 {} 不在线，跳过", &event.sender_id[..8]);
                continue;
            };

            tracing::info!(
                "[FileAuto] 开始处理文件 {} (来自 {})",
                event.file_name,
                &event.sender_id[..8]
            );

            let prompt = format!("用户上传了一个文件: {}", event.file_name);

            // 流式回复
            let (chunk_tx, chunk_rx) = mpsc::channel::<String>(8);
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let bot_id = fp_bot_id.clone();
            let bot_name = fp_bot_name.clone();
            let addr_clone = addr.clone();

            let send_handle = tokio::spawn(async move {
                if let Err(e) = messaging::send_stream_chunks(
                    &addr_clone, bot_id, bot_name, chunk_rx, ts,
                )
                .await
                {
                    tracing::error!("[FileAuto] 流式发送失败: {}", e);
                }
            });

            let result = fp_rpc
                .prompt_stream(&event.sender_id, &prompt, &[event.file_path.clone()], chunk_tx)
                .await;

            match result {
                Ok(pi_result) => {
                    let _ = send_handle.await;
                    // 发送 pi 生成的文件
                    for f in &pi_result.files {
                        if let Err(e) = file::send_file_to_peer(&addr, &fp_bot_id, f).await {
                            tracing::error!("[FileAuto] 发送生成文件失败: {}", e);
                        }
                    }
                }
                Err(e) => {
                    let reply = format!("❌ 文件处理失败: {}", e);
                    let _ = messaging::send_text_message(
                        &addr,
                        fp_bot_id.clone(),
                        fp_bot_name.clone(),
                        reply,
                        None,
                    )
                    .await;
                }
            }
        }

        tracing::error!("[FileAuto] 文件处理通道意外关闭");
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
    let listen_name = bot_name.clone();
    tokio::spawn(async move {
        network::discovery::start_listening(listen_port, listen_id, listen_name, listen_peers).await;
    });

    // ─── 启动 HTTP + WebSocket 服务器 ────────────────────────────
    let shared_state = Arc::new(AppState {
        msg_tx: msg_tx.clone(),
        file_complete_tx: file_complete_tx.clone(),
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
            match addr {
                Some(addr) => {
                    tracing::info!("[Scheduler] 发送消息到 {}: {}", &user_id[..8], message.chars().take(40).collect::<String>());
                    if let Err(e) = messaging::send_text_message(
                        &addr,
                        config.bot_id.clone(),
                        config.name.clone(),
                        message,
                        None,
                    ).await {
                        tracing::error!("[Scheduler] 发送失败: {}", e);
                    }
                }
                None => {
                    tracing::warn!("[Scheduler] 用户 {} 不在 peers 中，无法发送", &user_id[..8]);
                }
            }
        });
    });

    let scheduler_peers2 = peers.clone();
    let scheduler_bot_id = bot_id.clone();
    let scheduler_bot_name = bot_name.clone();
    tokio::spawn(async move {
        scheduler::start_scheduler(send_fn, scheduler_peers2, scheduler_bot_id, scheduler_bot_name).await;
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
