# LANClaw

> A LANChat-compatible intelligent bot powered by [Pi](https://pi.dev) coding agent.
>
> 📖 [中文文档](README_CN.md)

LANClaw registers itself as a peer on the LANChat network, receives messages and files from online users, forwards them to Pi for AI processing, and sends responses back.

## Features

- 🤖 **LANChat-compatible** — speaks the same UDP/TCP/HTTP/WebSocket protocol as LANChat, no modifications needed
- 🧠 **AI-powered by Pi** — uses Pi's RPC mode (`pi --mode rpc`) for per-user persistent conversations
- 🔄 **Cross-port discovery** — automatically replies to received heartbeats, enabling discovery across different ports and subnets
- 📁 **File analysis** — receives images/documents from users, passes them to Pi for analysis
- 📤 **File sending** — Pi can write files that LANClaw automatically delivers to users
- ⏰ **Scheduled tasks** — one-shot reminders and recurring tasks, managed via Pi skill
- 🔐 **Per-user sessions** — each user has an independent Pi session, persisted to disk
- 🧹 **No database** — uses Pi's session files (JSONL) and a simple JSON file for tasks
- 🔁 **Auto retry** — retries up to 3 times on empty RPC responses; allows empty when tool calls are present
- ⚡ **Interruptible** — send `/new` or `/model` to force-interrupt a stuck RPC and recover immediately

## Quick Start

### Prerequisites

- [Pi](https://pi.dev) installed and configured (API key / login)
- A local network with [LANChat](https://github.com/cap153/LANChat) users

### Run

```bash
# Default mode (port 8888)
lanclaw

# Custom name and model
lanclaw --name "Assistant" --model claude-sonnet-4-20250514

# Disable thinking for faster responses (default)
lanclaw --thinking off

# Custom port (when LANChat already uses 8888)
lanclaw --port 8889

# Custom file save path
lanclaw --files ~/Downloads/lanclaw
```

## Usage for LANChat Users

Users on your LANChat network will see the bot in their peer list. They can:

- **Send text** — talk to the bot, it responds via Pi
- **Send files/images** — the bot analyzes them with Pi
- **Create reminders** — say "remind me in 30 minutes" and Pi creates a scheduled task
- **Query tasks** — ask "what tasks are scheduled?" and Pi checks
- **Switch model** — send `/model` to list available models, `/model select <provider> <modelId>` to switch
- **Reset session** — send `/new` to start a fresh conversation (also interrupts stuck RPC)
- **Execute commands** — send `! command` to run bash commands, `!! command` to run silently
  - `!` command outputs are sent to Pi as context on your **next text message**
  - Multiple `!` commands stack up; all their outputs are sent together on the next message
  - `!!` commands run silently — output is returned to you but **not** sent to Pi
  - **Interruption**: sending any message (text, `/new`, etc.) while a command is running will cancel it
  - ⚠️ Avoid interactive/TUI commands like `nmtui` — while they can be interrupted by a new message, they may leave the terminal in an unexpected state

## CLI Reference

```bash
# Service mode
lanclaw [OPTIONS]

# Options
      --name <NAME>           Bot display name [default: LANClaw]
      --model <MODEL>         Pi model (empty = use Pi default)
      --thinking <THINKING>   Thinking level: off, minimal, low, medium, high, xhigh [default: off]
      --port <PORT>           Listening port [default: 8888]
      --files <FILES>         File save path [default: ~/Downloads]
      --data <DATA>           Data directory [default: ~/.local/share/lanclaw]

# Task management (called by Pi via bash)
lanclaw task add <when> <prompt> --user-id <UUID> [OPTIONS]
lanclaw task list
lanclaw task logs <id>
lanclaw task cancel <id>

# Send file to user (called by Pi via bash)
lanclaw send-file <path> --user-id <UUID>
```

### Task time formats

| Format | Example | Description |
|--------|---------|-------------|
| `30s` | `lanclaw task add 30s "..." --user-id <id>` | One-shot after 30 seconds |
| `30min` | `lanclaw task add 30min "..." --user-id <id>` | One-shot after 30 minutes |
| `2h` | `lanclaw task add 2h "..." --user-id <id>` | One-shot after 2 hours |
| `2026-06-15T09:00` | `lanclaw task add 2026-06-15T09:00 "..." --user-id <id>` | One-shot at absolute time |
| `every:10s` | `lanclaw task add every:10s "..." --user-id <id>` | Repeat every 10 seconds |
| `daily:08:00` | `lanclaw task add daily:08:00 "..." --user-id <id>` | Repeat daily at 08:00 |
| `weekly:mon:09:00` | `lanclaw task add weekly:mon:09:00 "..." --user-id <id>` | Repeat weekly on Monday |
| `monthly:15:09:00` | `lanclaw task add monthly:15:09:00 "..." --user-id <id>` | Repeat monthly on day 15 at 09:00 |
| `monthly:last:09:00` | `lanclaw task add monthly:last:09:00 "..." --user-id <id>` | Repeat on last day of month at 09:00 |
| `yearly:03-15:09:00` | `lanclaw task add yearly:03-15:09:00 "..." --user-id <id>` | Repeat yearly on Mar 15 at 09:00 |

## Port Conflicts

If LANChat is already running on port 8888 on the same machine:

```bash
lanclaw --port 8889
```

LANClaw and LANChat on different machines can both use port 8888 without conflict.

> [!TIP]
> When LANClaw runs on a different port, use LANChat's **Add** feature to add the bot's address (`<IP>:<port>`) for cross-port discovery. Once either side receives a heartbeat, the reply mechanism ensures both sides find each other.

### Configuration

Copy `config.example.json` to `~/.config/lanclaw/config.json`:

```json
{
  "name": "PiBot",
  "model": "",
  "thinking": "off",
  "port": 8888,
  "files": null
}
```

| Field      | Description                                                                      |
|------------|----------------------------------------------------------------------------------|
| `name`     | Bot display name on LANChat                                                      |
| `model`    | Pi model (empty = use Pi default)                                                |
| `thinking` | Pi thinking level: off, minimal, low, medium, high, xhigh                        |
| `port`     | Listening port (use the same port 8888 as the LANChat By default) |
| `files`    | File save path (`null` = default `~/Downloads`)                                  |
| `data`     | Data directory (`null` = default `~/.local/share/lanclaw`)                       |

CLI flags override config file settings. Priority: `--data` CLI > `data` config > `~/.local/share/lanclaw`.

## How It Works

```
┌─────────┐  LANChat Protocol   ┌──────────┐  pi --mode rpc   ┌──────┐
│ LANChat  │ ◄─── UDP/TCP/WS ──► │ LANClaw  │ ─────────────────► │  Pi  │
│  Users   │ ◄─── HTTP file ──── │  (Rust)  │ ◄──────────────── │      │
└─────────┘                      └──────────┘                  └──────┘
```

1. **UDP Discovery** — LANClaw broadcasts heartbeats like any LANChat peer, users see it online
2. **Heartbeat reply** — when receiving a heartbeat from another peer, LANClaw sends one back immediately. This ensures automatic discovery across different ports or subnets with zero configuration
3. **Message Receiving** — LANChat users send messages via WebSocket/TCP to LANClaw's port
4. **AI Processing** — LANClaw sends prompts to Pi via JSONL RPC protocol (`pi --mode rpc`), supporting streaming text, thinking, tool calls and results
5. **Response** — Pi's text reply is streamed back to the user; files Pi generates are uploaded via HTTP
6. **File Handling** — Files from users are saved to `~/Downloads` (configurable) and passed to Pi for analysis
7. **Interrupt Recovery** — If RPC gets stuck (network issues), `/new` or `/model` commands force-kill the pi subprocess and restart it immediately

## Data Storage

```
~/.local/share/lanclaw/
├── sessions/          # Pi session files (one .jsonl per user)
├── tasks.json         # Scheduled tasks
├── skill.md           # Generated Pi skill file
└── bot_id.txt         # Persistent bot UUID

~/Downloads/           # User-uploaded files (configurable)
```

## Project Structure

```
lanclaw/
├── src/
│   ├── main.rs          # Entrance & CLI parsing
│   ├── config.rs        # Configuration management
│   ├── models.rs        # Data types
│   ├── router.rs        # Message routing (text/file/command)
│   ├── rpc_client.rs    # Pi RPC client (pi --mode rpc)
│   ├── scheduler.rs     # Scheduled task engine
│   └── network/         # Network module
│       ├── discovery.rs # UDP discovery
│       ├── messaging.rs # WebSocket messages & streaming
│       ├── mod.rs       # HTTP routing
│       └── file.rs      # File upload/download
├── config.example.json  # Example configuration
└── Cargo.toml
```

## License

MIT
