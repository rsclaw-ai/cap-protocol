# cap-rs

Rust reference implementation of the **CAP (CLI Agent Protocol)** —
an open protocol for discovering, driving, and orchestrating any
command-line AI agent.

[![crates.io](https://img.shields.io/crates/v/cap-rs.svg)](https://crates.io/crates/cap-rs)
[![docs.rs](https://docs.rs/cap-rs/badge.svg)](https://docs.rs/cap-rs)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/rsclaw-ai/cap-protocol/blob/main/LICENSE-MIT)

- Homepage: <https://cap-protocol.org>
- Specification: [cap-v1.md](https://github.com/rsclaw-ai/cap-protocol/blob/main/docs/cap-v1.md)
- Repository: <https://github.com/rsclaw-ai/cap-protocol>

> **Status**: `0.0.1` — first release with real code. Public review of the
> spec is open. API is `#[non_exhaustive]` everywhere during the v0.x cycle.

## What this gives you

| You have | You want | Pick this feature |
|---|---|---|
| Claude Code (`claude` CLI) | structured events, multi-turn, low overhead | `stream-json` |
| Any CLI agent — aider, codex, cursor-agent, … | drive it like a human via terminal | `pty` (default) |
| openclaude (its own gRPC server) | … | `grpc` *(planned)* |
| Zed-compatible ACP agent (claude-agent-acp, opencode acp, codex native) | … | `acp` *(planned)* |
| Remote agent across machines | A2A binding | `a2a` *(planned)* |
| Coordinate several of the above on one project | multi-agent orchestrator | `orchestrator` *(planned)* |

Everything lives behind feature flags. `pty` is on by default — that's the
universal fallback that works with any CLI agent without protocol
negotiation.

## Install

```toml
[dependencies]
# Drive Claude Code with its native stream-json protocol:
cap-rs = { version = "0.0.1", features = ["stream-json"] }

# Drive any CLI agent (default — `pty` is in default features):
cap-rs = "0.0.1"

# Everything (heavy deps included):
cap-rs = { version = "0.0.1", features = ["full"] }
```

## Quick examples

### One-shot: ask Claude Code a question

```rust
use cap_rs::driver::stream_json::ClaudeCodeDriver;
use cap_rs::driver::Driver;
use cap_rs::core::{AgentEvent, ClientFrame, Content};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut driver = ClaudeCodeDriver::spawn(".").await?;

    driver.send(ClientFrame::Prompt {
        content: vec![Content::Text("What is 2 + 2?".into())],
    }).await?;

    // One-shot: signal "no more input" so claude responds and exits.
    driver.finish_input();

    while let Some(event) = driver.next_event().await {
        if let AgentEvent::TextChunk { text, .. } = event {
            print!("{text}");
        }
        if matches!(event, AgentEvent::Done { .. }) {
            break;
        }
    }
    driver.shutdown().await?;
    Ok(())
}
```

### Multi-turn: real-time conversation in one process

```rust
use cap_rs::driver::stream_json::ClaudeCodeDriver;
use cap_rs::driver::Driver;
use cap_rs::core::{AgentEvent, ClientFrame, Content};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Default builder enables session mode — one claude process
    // serves an unbounded number of turns.
    let mut chat = ClaudeCodeDriver::builder(".").spawn().await?;

    for prompt in ["What is 2+2?", "What is 3+3?", "What about 5+5?"] {
        chat.send(ClientFrame::Prompt {
            content: vec![Content::Text(prompt.into())],
        }).await?;

        // Each turn ends with a Done event.
        while let Some(event) = chat.next_event().await {
            match event {
                AgentEvent::TextChunk { text, .. } => print!("{text}"),
                AgentEvent::Done { .. } => { println!(); break; }
                _ => {}
            }
        }
    }

    chat.finish_input();
    chat.shutdown().await?;
    Ok(())
}
```

### Resume an earlier conversation

```rust
let driver = ClaudeCodeDriver::builder(".")
    .resume("00000000-0000-0000-0000-cafebabefffe")
    .spawn()
    .await?;
// claude is now in the same conversation context as before;
// session_id persists across processes.
```

### Drive any CLI agent through PTY

```rust
use cap_rs::driver::pty::{PtyDriver, RawParser};
use cap_rs::driver::Driver;
use cap_rs::core::{AgentEvent, ClientFrame, Content};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut driver = PtyDriver::builder("aider")
        .cwd(".")
        .size(50, 200)
        .env_remove("CLAUDECODE")   // avoid nesting if cap-rs runs inside Claude Code
        .spawn(RawParser)?;

    driver.send(ClientFrame::Prompt {
        content: vec![Content::Text("show me a hello world in Rust".into())],
    }).await?;

    while let Some(event) = driver.next_event().await {
        if let AgentEvent::TextChunk { text, .. } = event {
            print!("{text}");
        }
        if matches!(event, AgentEvent::Done { .. }) { break; }
    }

    driver.shutdown().await?;
    Ok(())
}
```

`RawParser` emits raw bytes (ANSI escapes pass through). For
agent-specific structured events, implement the [`AgentParser`] trait;
example parsers ship in `cap_rs::driver::pty`.

## Examples in the repo

```bash
git clone https://github.com/rsclaw-ai/cap-protocol
cd cap-protocol

cargo run --example claude_hello --features stream-json -- "your prompt"
cargo run --example claude_chat  --features stream-json
cargo run --example pty_hello    --features pty -- bash -c 'echo hi'
cargo run --example pty_hello    --features pty -- aider
```

## Feature flags

| Flag | Default | Pulls in | What it gives you |
|---|---|---|---|
| `pty` | ✅ | `portable-pty`, `vt100` | Universal PTY driver — any CLI agent |
| `stream-json` | | `tokio`, `tokio-util` | Claude SDK / openclaude fast-path |
| `acp` | | *(planned)* | ACP-stdio fast-path (Zed-style agents) |
| `a2a` | | *(planned)* | A2A HTTPS+SSE binding (remote / peer) |
| `grpc` | | *(planned)* | gRPC fast-path (openclaude-style) |
| `orchestrator` | | *(planned)* | Multi-agent coordination layer |
| `full` | | all of the above | Everything |

Heavy native deps (tonic, git2, portable-pty's Windows backend) are
scoped to their feature — turning off a feature removes both code and
deps from your dependency graph.

## Reserved sub-crate names

For future independent semver, these names are reserved at v0.0.0 on
crates.io but **not actively maintained** — depend on `cap-rs` only:

`cap-rs-core` · `cap-rs-pty` · `cap-rs-stream-json` · `cap-rs-acp` ·
`cap-rs-a2a` · `cap-rs-grpc` · `cap-rs-orchestrator`

## Roadmap

- ✅ stream-json driver (Claude Code, multi-turn session mode)
- ✅ PTY driver (universal, with `RawParser` + `VtPlainParser`)
- 🟡 Per-agent PTY parsers (aider, codex, cursor)
- ⚪ ACP-stdio driver (bridges claude-agent-acp / opencode acp / codex)
- ⚪ A2A binding (remote agents, both directions)
- ⚪ gRPC binding (openclaude)
- ⚪ Multi-agent orchestrator (plan propagation, budget, worktrees)

## Authors

Created and maintained by [rsclaw](https://rsclaw.ai), the open-source
agent fleet that uses CAP as its native orchestration layer. See the
spec repository for governance and contribution guidelines.

## License

MIT. See [LICENSE-MIT](https://github.com/rsclaw-ai/cap-protocol/blob/main/LICENSE-MIT).
