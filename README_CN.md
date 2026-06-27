# LANClaw

> 一个由 [Pi](https://pi.dev) 编程智能体驱动的、兼容 LANChat 协议的局域网智能机器人。
>
> 🌐 [English Documentation](README.md)

LANClaw 以 LANChat 网络中的对等节点身份运行，接收在线用户的消息和文件，转发给 Pi 进行 AI 处理，然后将回复发回给用户。

## 特性

- 🤖 **LANChat 协议兼容** — 使用与 LANChat 完全相同的 UDP/TCP/HTTP/WebSocket 协议
- 🧠 **Pi AI 驱动** — 通过 `pi --mode rpc` JSONL RPC 协议，支持流式文本、思考、工具调用
- 🔄 **跨端口自动发现** — 收到心跳自动回复，不同端口/网段的设备无需配置即可互通
- 📁 **文件分析** — 接收用户发来的图片/文档，传给 Pi 进行智能分析
- 📤 **发送文件** — Pi 生成的文件可由 LANClaw 自动发送给用户（如图表、代码）
- ⏰ **定时任务** — 单次提醒和重复任务，由 Pi 技能自动管理
- 🔐 **用户隔离** — 每个用户拥有独立的 Pi session，互不干扰
- 🧹 **无需数据库** — 使用 Pi 的 JSONL session 文件和简单的 JSON 任务文件
- 🔁 **自动重试** — RPC 回复为空时自动重试最多 3 次，有工具调用时允许空回复
- ⚡ **卡住可打断** — `/new` 或 `/model` 命令可直接杀子进程强制恢复，无需重启 LANClaw

## 快速开始

### 前置条件

- 已安装并配置好 [Pi](https://pi.dev)（API key / 登录）
- 局域网中有使用 [LANChat](https://github.com/cap153/LANChat) 的用户

### AUR

```bash
paru -S lanclaw-bin
```

### Releases

[https://github.com/cap153/LANClaw/releases](https://github.com/cap153/LANClaw/releases)

### 编译

```bash
git clone https://github.com/cap153/LANClaw.git
cd LANClaw
cargo build --release
```

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

# 自定义文件保存路径
lanclaw --files ~/Downloads/lanclaw
```

## 用户使用指南

局域网中的 LANChat 用户会在用户列表中看到机器人。你可以：

- **发文字** — 直接对话，Pi 会回答
- **发文件/图片** — 机器人会自动分析内容
- **创建提醒** — 说"30分钟后提醒我"，Pi 会自动创建定时任务
- **查询任务** — 问"有哪些定时任务"，Pi 会查询并展示
- **切换模型** — 发送 `/model` 查看可用模型，`/model select <提供商> <模型ID>` 切换
- **重置会话** — 发送 `/new` 开始全新的对话（同时打断卡住的 RPC）
- **执行命令** — 发送 `! 命令` 执行 bash 命令，`!! 命令` 静默执行
  - `!` 命令在你**下一条文字消息**时一并发给 Pi 作为上下文
  - 多个 `!` 命令会堆叠，下一条消息时全部发送
  - `!!` 命令静默执行——结果返回给你，但**不**发给 Pi
  - **打断**：命令执行中发送任意消息（文字、`/new` 等）会取消当前命令
  - ⚠️ 避免执行 `nmtui` 这类交互式 TUI 命令——虽然可被新消息打断，但可能让终端进入意外状态

## CLI 参考

```bash
# 服务模式
lanclaw [参数]

# 参数
      --name <NAME>           机器人显示名 [default: LANClaw]
      --model <MODEL>         Pi 模型（不指定则使用 pi 默认模型）
      --thinking <THINKING>   思考级别: off, minimal, low, medium, high, xhigh [default: off]
      --port <PORT>           监听端口 [default: 8888]
      --files <FILES>         文件保存路径 [default: ~/Downloads]
      --data <DATA>           数据目录 [default: ~/.local/share/lanclaw]

# 定时任务管理（由 Pi 通过 bash 调用）
lanclaw task add <时间> --user-id <用户UUID> [参数]
lanclaw task list
lanclaw task logs <任务ID>
lanclaw task cancel <任务ID>

# 发送文件给用户（由 Pi 通过 bash 调用）
lanclaw send-file <文件路径> --user-id <用户UUID>
```

### 任务时间格式

| 格式 | 示例 | 说明 |
|------|------|------|
| `30s` | `lanclaw task add 30s "..." --user-id <id>` | 30 秒后执行一次 |
| `30min` | `lanclaw task add 30min "..." --user-id <id>` | 30 分钟后执行一次 |
| `2h` | `lanclaw task add 2h "..." --user-id <id>` | 2 小时后执行一次 |
| `2026-06-15T09:00` | `lanclaw task add 2026-06-15T09:00 "..." --user-id <id>` | 指定时间执行一次 |
| `every:10s` | `lanclaw task add every:10s "..." --user-id <id>` | 每 10 秒重复执行 |
| `daily:08:00` | `lanclaw task add daily:08:00 "..." --user-id <id>` | 每天 08:00 执行 |
| `weekly:mon:09:00` | `lanclaw task add weekly:mon:09:00 "..." --user-id <id>` | 每周一 09:00 执行 |
| `monthly:15:09:00` | `lanclaw task add monthly:15:09:00 "..." --user-id <id>` | 每月 15 号 09:00 执行 |
| `monthly:last:09:00` | `lanclaw task add monthly:last:09:00 "..." --user-id <id>` | 每月最后一天 09:00 执行 |
| `yearly:03-15:09:00` | `lanclaw task add yearly:03-15:09:00 "..." --user-id <id>` | 每年 3 月 15 日 09:00 执行 |

## 端口冲突

如果本机已运行 LANChat 占用了 8888 端口：

```bash
lanclaw --port 8889
```

不同机器上的 LANClaw 和 LANChat 可以同时使用 8888 端口，互不冲突。

> [!TIP]
> 当 LANClaw 使用不同端口时，在 LANChat 的**添加**功能中添加机器人的地址（`<IP>:<端口>`），即可跨端口发现。任一端收到心跳后，回复机制会让双方自动互相发现。

### 配置

复制 `config.example.json` 到 `~/.config/lanclaw/config.json`：

```json
{
  "name": "PiBot",
  "model": "",
  "thinking": "off",
  "port": 8888,
  "files": null
}
```

| 字段       | 说明                                                |
|------------|-----------------------------------------------------|
| `name`     | 机器人在 LANChat 上的显示名称                       |
| `model`    | Pi 模型（空字符串 = 使用 Pi 默认模型）              |
| `thinking` | Pi 思考级别: off, minimal, low, medium, high, xhigh |
| `port`     | 监听端口（默认与 LANChat 使用相同端口 8888）    |
| `files`    | 文件保存路径（`null` = 默认 `~/Downloads`）         |
| `data`     | 数据目录（`null` = 默认 `~/.local/share/lanclaw`）  |

命令行参数会覆盖配置文件中的对应设置。优先级：`--data` CLI > `data` 配置 > `~/.local/share/lanclaw`。

## 工作原理

```
┌─────────┐  LANChat 协议       ┌──────────┐  pi --mode rpc   ┌──────┐
│ LANChat  │ ◄─── UDP/TCP/WS ──► │ LANClaw  │ ─────────────────► │  Pi  │
│  用户    │ ◄─── HTTP 文件 ──── │  (Rust)  │ ◄──────────────── │      │
└─────────┘                      └──────────┘                  └──────┘
```

1. **UDP 发现** — LANClaw 像 LANChat 一样广播心跳，用户能看到机器人上线
2. **心跳回复** — 收到其他设备的心跳时立即回复一条，不同端口/网段的设备无需手动配置即可自动发现
3. **消息接收** — LANChat 用户通过 WebSocket/TCP 向 LANClaw 端口发送消息
4. **AI 处理** — LANClaw 通过 JSONL RPC 协议（`pi --mode rpc`）向 Pi 发送 prompt，支持流式文本、思考过程、工具调用和工具结果
5. **回复** — Pi 的文本回复流式返回给用户；Pi 生成的文件通过 HTTP 自动发送
6. **文件处理** — 用户发送的文件保存到 `~/Downloads`（可配置）后传给 Pi 分析
7. **卡住恢复** — 如果 RPC 因网络问题卡住，发送 `/new` 或 `/model` 命令可强制杀 pi 子进程并立即重启

## 数据存储

```
~/.local/share/lanclaw/
├── sessions/          # Pi session 文件（每个用户一个 .jsonl）
├── tasks.json         # 定时任务存储
├── skill.md           # 动态生成的 Pi 技能文件
└── bot_id.txt         # 机器人持久化 UUID

~/Downloads/           # 用户上传的文件（可配置）
```

## 项目结构

```
lanclaw/
├── src/
│   ├── main.rs          # 入口 & CLI 参数解析
│   ├── config.rs        # 配置管理
│   ├── models.rs        # 数据模型
│   ├── router.rs        # 消息路由（文本/文件/命令）
│   ├── rpc_client.rs    # Pi RPC 客户端（pi --mode rpc）
│   ├── scheduler.rs     # 定时任务引擎
│   └── network/         # 网络模块
│       ├── discovery.rs # UDP 发现
│       ├── messaging.rs # WebSocket 消息 & 流式传输
│       ├── mod.rs       # HTTP 路由
│       └── file.rs      # 文件上传/下载
├── config.example.json  # 示例配置
└── Cargo.toml
```

## 许可证

MIT
