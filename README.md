# LANClaw

> A LANChat-compatible intelligent bot powered by [Pi](https://pi.dev) coding agent.

LANClaw registers itself as a peer on the LANChat network, receives messages and files from online users, forwards them to Pi for AI processing, and sends responses back — all without modifying a single line of LANChat.

## Features

- 🤖 **LANChat-compatible** — speaks the same UDP/TCP/HTTP/WebSocket protocol as LANChat, no modifications needed
- 🧠 **AI-powered by Pi** — uses `pi -p --session` for per-user persistent conversations
- 📁 **File analysis** — receives images/documents from users, passes them to Pi for analysis
- 📤 **File sending** — Pi can write files that LANClaw automatically delivers to users
- ⏰ **Scheduled tasks** — one-shot reminders and recurring tasks, managed via Pi skill
- 🔐 **Per-user sessions** — each user has an independent Pi session, persisted to disk
- 🧹 **No database** — uses Pi's session files (JSONL) and a simple JSON file for tasks

## Quick Start

### Prerequisites

- [Pi](https://pi.dev) installed and configured (API key / login)
- A local network with LANChat users

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
```

### Configuration

Copy `config.example.json` to `~/.config/lanclaw/config.json`:

```json
{
  "name": "PiBot",
  "model": "",
  "thinking": "off",
  "port": 8888
}
```

| Field | Description |
|-------|-------------|
| `name` | Bot display name on LANChat |
| `model` | Pi model (empty = use Pi default) |
| `thinking` | Pi thinking level: off, minimal, low, medium, high, xhigh |
| `port` | Listening port (same ports as LANChat) |

CLI flags override config file settings.

## How It Works

```
┌─────────┐  LANChat Protocol   ┌──────────┐  pi -p --session  ┌──────┐
│ LANChat  │ ◄─── UDP/TCP/WS ──► │ LANClaw  │ ──────────────────► │  Pi  │
│  Users   │ ◄─── HTTP file ──── │  (Rust)  │ ◄────────────────── │      │
└─────────┘                      └──────────┘                    └──────┘
```

1. **UDP Discovery** — LANClaw broadcasts heartbeats like any LANChat peer, users see it online
2. **Message Receiving** — LANChat users send messages via WebSocket/TCP to LANClaw's port
3. **AI Processing** — LANClaw calls `pi -p --session <user_id> "message"` for each user
4. **Response** — Pi's text reply is sent back; files Pi generates are uploaded to the user
5. **File Handling** — Files from users are saved and passed to Pi for analysis (images, documents)
6. **Scheduled Tasks** — Pi manages tasks via `lanclaw task add/list/cancel/logs` CLI commands

## Data Storage

```
~/.local/share/lanclaw/
├── sessions/          # Pi session files (one .jsonl per user)
├── files/             # User-uploaded files
├── files_out/         # Pi-generated files (auto-sent to users)
├── tasks.json         # Scheduled tasks
├── skill.md           # Generated Pi skill file
└── bot_id.txt         # Persistent bot UUID
```

## Usage for LANChat Users

Users on your LANChat network will see the bot in their peer list. They can:

- **Send text** — talk to the bot, it responds via Pi
- **Send files/images** — the bot analyzes them with Pi
- **Create reminders** — say "remind me in 30 minutes" and Pi creates a scheduled task
- **Query tasks** — ask "what tasks are scheduled?" and Pi checks

The only command LANClaw interprets directly is `/new`, which resets your conversation session.

## CLI Reference

```bash
# Service mode
lanclaw [OPTIONS]

# Task management (called by Pi via bash)
lanclaw task add <when> <prompt> --user-id <UUID> [OPTIONS]
lanclaw task list
lanclaw task logs <id>
lanclaw task cancel <id>
```

### Task time formats

| Format | Example | Description |
|--------|---------|-------------|
| `30min` | `lanclaw task add 30min "..." --user-id <id>` | One-shot after 30 minutes |
| `2h` | `lanclaw task add 2h "..." --user-id <id>` | One-shot after 2 hours |
| `2026-06-15T09:00` | `lanclaw task add 2026-06-15T09:00 "..." --user-id <id>` | One-shot at absolute time |
| `daily:08:00` | `lanclaw task add daily:08:00 "..." --user-id <id>` | Repeat daily at 08:00 |
| `weekly:mon:09:00` | `lanclaw task add weekly:mon:09:00 "..." --user-id <id>` | Repeat weekly on Monday |

## Port Conflicts

If LANChat is already running on port 8888 on the same machine:

```bash
lanclaw --port 8889
```

LANClaw and LANChat on different machines can both use port 8888 without conflict.

## License

MIT
