# LANClaw

> 一个由 [Pi](https://pi.dev) 编程智能体驱动的、兼容 LANChat 协议的局域网智能机器人。
>
> 🌐 [English Documentation](README.md)

LANClaw 以 LANChat 网络中的对等节点身份运行，接收在线用户的消息和文件，转发给 Pi 进行 AI 处理，然后将回复发回给用户。

## 特性

- 🤖 **LANChat 协议兼容** — 使用与 LANChat 完全相同的 UDP/TCP/HTTP/WebSocket 协议
- 🧠 **Pi AI 驱动** — 通过 `pi -p --session` 实现每用户独立持久化会话
- 🔄 **跨端口自动发现** — 收到心跳自动回复，不同端口/网段的设备无需配置即可互通
- 📁 **文件分析** — 接收用户发来的图片/文档，传给 Pi 进行智能分析
- 📤 **发送文件** — Pi 生成的文件可由 LANClaw 自动发送给用户（如图表、代码）
- ⏰ **定时任务** — 单次提醒和重复任务，由 Pi 技能自动管理
- 🔐 **用户隔离** — 每个用户拥有独立的 Pi session，互不干扰
- 🧹 **无需数据库** — 使用 Pi 的 JSONL session 文件和简单的 JSON 任务文件

## 快速开始

### 前置条件

- 已安装并配置好 [Pi](https://pi.dev)（API key / 登录）
- 局域网中有使用 LANChat 的用户

### 运行

```bash
# 默认模式（端口 8888）
lanclaw

# 自定义名称和模型
lanclaw --name "小助手" --model claude-sonnet-4-20250514

# 关闭思考以加速响应（默认）
lanclaw --thinking off

# 自定义端口（当 LANChat 已占用 8888 时）
lanclaw --port 8889
```

### 配置

复制 `config.example.json` 到 `~/.config/lanclaw/config.json`：

```json
{
  "name": "PiBot",
  "model": "",
  "thinking": "off",
  "port": 8888
}
```

| 字段 | 说明 |
|------|------|
| `name` | 机器人在 LANChat 上的显示名称 |
| `model` | Pi 模型（空字符串 = 使用 Pi 默认模型） |
| `thinking` | Pi 思考级别: off, minimal, low, medium, high, xhigh |
| `port` | 监听端口（与 LANChat 协议使用相同端口） |

命令行参数会覆盖配置文件中的对应设置。

## 工作原理

```
┌─────────┐  LANChat 协议       ┌──────────┐  pi -p --session  ┌──────┐
│ LANChat  │ ◄─── UDP/TCP/WS ──► │ LANClaw  │ ──────────────────► │  Pi  │
│  用户    │ ◄─── HTTP 文件 ──── │  (Rust)  │ ◄────────────────── │      │
└─────────┘                      └──────────┘                    └──────┘
```

1. **UDP 发现** — LANClaw 像 LANChat 一样广播心跳，用户能看到机器人上线
2. **心跳回复** — 收到其他设备的心跳时立即回复一条，不同端口/网段的设备无需手动配置即可自动发现
3. **消息接收** — LANChat 用户通过 WebSocket/TCP 向 LANClaw 端口发送消息
4. **AI 处理** — LANClaw 调用 `pi -p --session <用户ID> "消息"` 处理
5. **回复** — Pi 的文本回复原样返回；Pi 生成的文件自动发送给用户
6. **文件处理** — 用户发送的文件保存后传给 Pi 分析（图片、文档等）
7. **定时任务** — Pi 通过 `lanclaw task` 命令行管理任务，LANClaw 后台调度执行

## 数据存储

```
~/.local/share/lanclaw/
├── sessions/          # Pi session 文件（每个用户一个 .jsonl）
├── files/             # 用户上传的文件
├── files_out/         # Pi 生成的文件（自动发送给用户）
├── tasks.json         # 定时任务存储
├── skill.md           # 动态生成的 Pi 技能文件
└── bot_id.txt         # 机器人持久化 UUID
```

## 用户使用指南

局域网中的 LANChat 用户会在用户列表中看到机器人。你可以：

- **发文字** — 直接对话，Pi 会回答
- **发文件/图片** — 机器人会自动分析内容
- **创建提醒** — 说"30分钟后提醒我"，Pi 会自动创建定时任务
- **查询任务** — 问"有哪些定时任务"，Pi 会查询并展示
- **切换模型** — 发送 `/model` 查看可用模型，`/model select <提供商> <模型ID>` 切换
- **重置会话** — 发送 `/new` 开始全新的对话

## CLI 参考

```bash
# 服务模式
lanclaw [参数]

# 定时任务管理（由 Pi 通过 bash 调用）
lanclaw task add <时间> <提示词> --user-id <用户UUID> [参数]
lanclaw task list
lanclaw task logs <任务ID>
lanclaw task cancel <任务ID>
```

### 任务时间格式

| 格式 | 示例 | 说明 |
|------|------|------|
| `30min` | `lanclaw task add 30min "..." --user-id <id>` | 30 分钟后执行一次 |
| `2h` | `lanclaw task add 2h "..." --user-id <id>` | 2 小时后执行一次 |
| `2026-06-15T09:00` | `lanclaw task add 2026-06-15T09:00 "..." --user-id <id>` | 指定时间执行一次 |
| `daily:08:00` | `lanclaw task add daily:08:00 "..." --user-id <id>` | 每天 08:00 执行 |
| `weekly:mon:09:00` | `lanclaw task add weekly:mon:09:00 "..." --user-id <id>` | 每周一 09:00 执行 |

## 端口冲突

如果本机已运行 LANChat 占用了 8888 端口：

```bash
lanclaw --port 8889
```

不同机器上的 LANClaw 和 LANChat 可以同时使用 8888 端口，互不冲突。

> [!TIP]
> 当 LANClaw 使用不同端口时，在 LANChat 的**手动发现**功能中添加机器人的地址（`<IP>:<端口>`），即可跨端口自动发现。任一端收到心跳后，回复机制会让双方自动互相发现。

## 项目结构

```
lanclaw/                 # LANClaw AI 机器人
├── src/
│   ├── main.rs          # 入口
│   ├── config.rs        # 配置管理
│   ├── models.rs        # 数据模型
│   ├── router.rs        # 消息路由（文本/文件/命令）
│   ├── rpc_client.rs    # Pi RPC 客户端
│   ├── pi_bridge.rs     # Pi 进程管理
│   ├── scheduler.rs     # 定时任务引擎
│   ├── skill_gen.rs     # Pi Skill 文件生成
│   └── network/         # 网络模块
│       ├── discovery.rs # UDP 发现
│       ├── messaging.rs # WebSocket 消息
│       ├── mod.rs       # HTTP 路由
│       └── file.rs      # 文件上传/下载
└── Cargo.toml
```

## 许可证

MIT
