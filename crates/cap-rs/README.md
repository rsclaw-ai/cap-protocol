# cap-rs

Rust reference implementation of the **CAP (CLI Agent Protocol)** — an
open protocol for discovering, driving, and orchestrating any
command-line AI agent.

> 🚧 **Status**: Placeholder. Implementation in progress for v0.1.

- Homepage: <https://cap-protocol.org>
- Specification: <https://github.com/rsclaw-ai/cap-protocol/blob/main/docs/cap-v1.md>
- Repository: <https://github.com/rsclaw-ai/cap-protocol>

## Usage (when v0.1 lands)

```toml
[dependencies]
cap-rs = { version = "0.1", features = ["stream-json", "acp", "orchestrator"] }
# `pty` is enabled by default; no need to list it explicitly.
```

## Features

All bindings live behind feature flags. Heavy native dependencies are
scoped to their feature — disabling one removes both code and deps.

| Feature | Eventual deps | Use case |
|---|---|---|
| `pty` (default) | `portable-pty`, `vt100` | Universal PTY driver — works with any CLI agent, even those exposing no structured protocol |
| `stream-json` | `serde`, `tokio` | Fast-path for Claude Code SDK / openclaude |
| `acp` | JSON-RPC framing | Bridges Zed-style ACP agents: claude-agent-acp, opencode acp, codex |
| `a2a` | `reqwest`, `eventsource-stream` | A2A HTTPS+SSE binding for remote peers and fleet |
| `grpc` | `tonic`, `prost` | gRPC fast-path for openclaude-style agents |
| `orchestrator` | `git2` | Multi-agent coordination with workspace isolation |
| `full` | all of the above | Everything |

## Reserved sub-crate names

To keep the door open for future independent semver, we have reserved
these names on crates.io at v0.0.0:

- `cap-rs-core` · `cap-rs-pty` · `cap-rs-stream-json`
- `cap-rs-acp` · `cap-rs-a2a` · `cap-rs-grpc` · `cap-rs-orchestrator`

These are **not actively maintained**. They exist so that if a single
binding ever needs to evolve faster than `cap-rs` as a whole (e.g.
`cap-rs-grpc` if a third party offers to co-maintain), the namespace
is already ours. **Depend on `cap-rs` only.**

## Status (today)

This crate currently exposes only build-time constants
(`CRATE_NAME`, `CRATE_VERSION`, `PROTOCOL_VERSION`). Real protocol
types and driver implementations land progressively in v0.1.

## License

MIT. See [LICENSE-MIT](https://github.com/rsclaw-ai/cap-protocol/blob/main/LICENSE-MIT).
