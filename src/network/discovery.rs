use crate::models::Peer;
use crate::models::PeerMap;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::Ipv4Addr;
use std::time::Duration;

const MULTICAST_IP: &str = "224.0.0.167";

/// 创建 UDP socket（广播 + 组播）
fn create_socket(bind_addr: &str, is_listener: bool) -> Result<std::net::UdpSocket, std::io::Error> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(not(target_os = "windows"))]
    socket.set_reuse_port(true)?;

    let addr: std::net::SocketAddr = bind_addr.parse().map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, e)
    })?;
    socket.bind(&addr.into())?;

    let std_socket: std::net::UdpSocket = socket.into();
    std_socket.set_nonblocking(true)?;

    if is_listener {
        let multi_addr: Ipv4Addr = MULTICAST_IP.parse().unwrap();
        let interface: Ipv4Addr = "0.0.0.0".parse().unwrap();
        let _ = std_socket.join_multicast_v4(&multi_addr, &interface);
    } else {
        std_socket.set_broadcast(true)?;
        let _ = std_socket.set_multicast_ttl_v4(1);
    }

    Ok(std_socket)
}

fn get_broadcast_addresses(port: u16) -> Vec<String> {
    let mut addrs = Vec::new();
    addrs.push(format!("255.255.255.255:{}", port));
    addrs.push(format!("{}:{}", MULTICAST_IP, port));
    addrs.push(format!("10.0.0.255:{}", port));
    for i in 0..=255 {
        addrs.push(format!("192.168.{}.255:{}", i, port));
    }
    addrs
}

/// 启动 UDP 广播（每 2 秒宣告自己的存在）
pub async fn start_announcing(port: u16, bot_id: String, bot_name: String) {
    let socket = match create_socket("0.0.0.0:0", false) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[UDP] 创建发送 socket 失败: {}", e);
            return;
        }
    };

    let target_addrs = get_broadcast_addresses(port);
    println!("[UDP] 开始广播心跳...");

    loop {
        // 简单估算可用内存（仅 Linux）
        let mem_hint = 1024u64;

        let msg = format!(
            "LANChat|ONLINE|{}|{}|{}|{}",
            bot_id, bot_name, port, mem_hint
        );

        for addr in &target_addrs {
            let _ = socket.send_to(msg.as_bytes(), addr);
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// 启动 UDP 监听（发现局域网其他用户）
pub async fn start_listening(
    port: u16,
    my_id: String,
    bot_name: String,
    peers: PeerMap,
) {
    let bind_addr = format!("0.0.0.0:{}", port);
    let socket = match create_socket(&bind_addr, true) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[UDP] 创建监听 socket 失败: {}", e);
            return;
        }
    };

    // 转为 tokio UdpSocket 做异步读取
    let socket = tokio::net::UdpSocket::from_std(socket).expect("转换 UDP socket 失败");
    let mut buf = [0u8; 1024];

    println!("[UDP] 开始监听局域网用户...");

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((size, addr)) => {
                let msg = String::from_utf8_lossy(&buf[..size]);
                let parts: Vec<&str> = msg.split('|').collect();

                if parts.len() >= 6 && parts[0] == "LANChat" {
                    let peer_id = parts[2].to_string();
                    let peer_port = parts[4];

                    if peer_id == my_id {
                        continue;
                    }

                    let name = parts[3].to_string();
                    let peer_addr = format!("{}:{}", addr.ip(), peer_port);
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();

                    let mut map = peers.write().await;

                    let entry = map.entry(peer_id.clone()).or_insert_with(|| Peer {
                        id: peer_id.clone(),
                        name: name.clone(),
                        addr: peer_addr.clone(),
                        last_seen: now,
                        is_offline: false,
                    });

                    let was_offline = entry.is_offline;

                    entry.name = name.clone();
                    entry.addr = peer_addr;
                    entry.last_seen = now;
                    entry.is_offline = false;

                    // 持久化 peer 信息（供 send-file CLI 使用）
                    let peers_snapshot = map.clone();
                    std::thread::spawn(move || {
                        if let Ok(content) = serde_json::to_string(&peers_snapshot) {
                            let path = crate::config::peers_path();
                            let _ = std::fs::write(&path, content);
                        }
                    });

                    drop(map);

                    if was_offline {
                        println!("[UDP] 用户上线: {} ({})", name, peer_id);
                    }

                    // 回复心跳给对方（无 |1 标记则为原始心跳，需回复）
                    if parts.len() <= 6 {
                        let reply = format!(
                            "LANChat|ONLINE|{}|{}|{}|0|1",
                            my_id, bot_name, port
                        );
                        let target = format!("{}:{}", addr.ip(), peer_port);
                        let _ = socket.send_to(reply.as_bytes(), target).await;
                    }
                }
            }
            Err(e) => {
                // 非阻塞 socket 无数据时返回 WouldBlock，忽略
                if e.kind() != std::io::ErrorKind::WouldBlock {
                    eprintln!("[UDP] 接收错误: {}", e);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
