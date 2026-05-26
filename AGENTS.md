# AGENTS.md - AI Agent Development Guide

This document provides context and conventions for AI agents working on the cap-protocol codebase.

## Project Overview

**CAP (CLI Agent Protocol)** is an open protocol for discovering, driving, and orchestrating CLI-based AI agents. The reference implementation is a Rust workspace providing:

- **cap-rs**: Core library with 7 driver backends (PTY, stream-json, codex-exec, codex-mcp, codex-app-server, ACP, gRPC)
- **cap-rs-orchestrator**: Fleet orchestration engine with declarative YAML configuration
- **cap-cli**: Command-line interface for running fleets

## Architecture

### Driver Layer (`crates/cap-rs/src/driver/`)

All drivers implement the `Driver` trait:

```rust
#[async_trait]
pub trait Driver: Send {
    async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError>;
    async fn next_event(&mut self) -> Option<AgentEvent>;
    async fn shutdown(&mut self) -> Result<(), DriverError>;
    fn is_alive(&self) -> bool;
    fn exit_status(&self) -> Option<DriverExitStatus>;
    fn prompt_after_ready(&self) -> bool;
}
```

**Key drivers:**
- `pty.rs`: PTY-based TUI driver with quiescence detection (reader/parser/writer threads)
- `stream_json.rs`: Claude Code's native streaming JSON protocol
- `codex_mcp.rs`: Codex via stdio MCP server (`codex mcp-server`)
- `grpc.rs`: OpenClaude gRPC server client
- `acp.rs`: Agent Client Protocol over stdio

### Orchestration Layer (`crates/cap-rs-orchestrator/`)

**Session Actor Model:**
- Each session is a tokio task with mpsc channels
- `session.rs`: Per-session event loop (pump_turn, bus_send)
- `registry.rs`: Session lifecycle management
- `executor.rs`: Deterministic state machine for route evaluation

**Fleet DSL (`config.rs`):**
```yaml
fleet:
  base_branch: main
  sessions:
    coder: { driver: claude }
    reviewer: { driver: codex, permissions: allow }
  start: coder
  routes:
    - { when: coder.done, route_to: reviewer }
```

**Route actions:** `route_to`, `fan_out` (broadcast/by_subtask), `collect: human`

## Key Conventions

### Error Handling

- Use `thiserror` for library errors (`OrchestratorError`, `DriverError`)
- Propagate via `?` operator; avoid `.unwrap()` in production code
- `.expect()` only for truly impossible invariants (document why)

### Async Patterns

- All drivers use tokio tasks with `CancellationToken` for graceful shutdown
- Channels: `tokio::sync::mpsc` (bounded) for event buses
- Timeouts: Use `tokio::time::timeout` for all blocking operations

### Feature Flags

```toml
[features]
default = ["pty"]
pty, stream-json, codex, acp, grpc, a2a, orchestrator, full
```

- Heavy deps (tonic, portable-pty) are `optional = true`
- Gate driver modules with `#[cfg(feature = "...")]`
- Test with `--all-features` in CI

### Testing

- **Unit tests**: Inline `#[cfg(test)] mod tests` in each module
- **Integration tests**: `tests/patterns.rs` uses `StubDriver` with `testing` feature
- **Run tests**: `cargo test --workspace --all-features`
- **CI mode**: `cargo test --workspace --all-features -- --nocapture`

### Input Validation

All user-facing inputs validated at parse time:
- `valid_session_id`: ASCII alphanumeric + `_` + `-`, no leading `-`
- `valid_binary_name`: No paths, no shell metacharacters
- `valid_git_ref`: No `..`, no leading `-`

### Permission Model

```rust
pub enum PermissionPolicy {
    Ask,     // Prompt user
    Allow,   // Auto-approve
    Deny,    // Auto-reject
    Bypass,  // Skip permission checks entirely
}
```

Mapped to agent-specific flags:
- Codex: `--approval-policy` + `--sandbox`
- Claude: `--dangerously-skip-permissions`

## File Structure

```
crates/
├── cap-rs/
│   ├── src/
│   │   ├── lib.rs              # Public API, feature gates
│   │   ├── core.rs             # ClientFrame, AgentEvent, Content types
│   │   ├── driver.rs           # Driver trait, DriverError
│   │   └── driver/
│   │       ├── pty.rs          # PTY driver (1562 lines, most complex)
│   │       ├── stream_json.rs  # Claude Code driver
│   │       ├── codex_mcp.rs    # Codex MCP driver
│   │       ├── grpc.rs         # gRPC client driver
│   │       └── acp.rs          # ACP driver
│   └── examples/               # Runnable examples per driver
│
├── cap-rs-orchestrator/
│   ├── src/
│   │   ├── lib.rs              # run() facade, OrchestratorError
│   │   ├── config.rs           # FleetSpec YAML parsing + validation
│   │   ├── executor.rs         # State machine (Run struct)
│   │   ├── session.rs          # Per-session actor
│   │   ├── registry.rs         # Session lifecycle
│   │   ├── worktree.rs         # Git worktree isolation
│   │   ├── factory.rs          # DriverFactory trait
│   │   ├── real_factory.rs     # Concrete driver construction
│   │   └── event.rs            # OrchestratorEvent/Control
│   └── tests/
│       └── patterns.rs         # Integration tests (StubDriver)
│
└── cap-cli/
    └── src/
        └── main.rs             # cap run command
```

## Common Tasks

### Adding a New Driver

1. Create `crates/cap-rs/src/driver/my_driver.rs`
2. Implement `Driver` trait
3. Add feature flag in `crates/cap-rs/Cargo.toml`:
   ```toml
   my_driver = ["dep:tokio", "dep:async-trait", ...]
   ```
4. Gate module: `#[cfg(feature = "my_driver")] pub mod my_driver;`
5. Add to `driver.rs` feature gate list
6. Wire into `real_factory.rs` match arms
7. Add `DriverKind::MyDriver(String)` variant in `config.rs`
8. Update deserializer and validation
9. Add example in `examples/my_driver_hello.rs`

### Modifying Fleet DSL

1. Update `FleetSpec` / `SessionSpec` / `Route` in `config.rs`
2. Update `validate()` for new invariants
3. Update `executor.rs` state machine if route semantics change
4. Add test case in `tests/patterns.rs`
5. Update `examples/` if adding new patterns

### Debugging Session Hangs

Common causes:
- **Missing timeout**: Check all `tokio::select!` arms have timeouts
- **Channel not closed**: Ensure `bus_tx` is dropped when session ends
- **Route cycle**: `detect_route_cycles()` should catch at validate time
- **Failed session not recorded**: Check `failed.insert()` ordering in `executor.rs`

### Working with PTY Driver

The PTY driver has 4 background threads:
- **Reader**: Reads raw bytes from PTY fd
- **Parser**: Converts bytes to `AgentEvent` via `TuiParser`
- **Writer**: Sends `ClientFrame` to PTY fd
- **Child waiter**: Waits for process exit

**Quiescence detection** (turn boundary):
- Idle timeout (default 500ms of no output)
- Ready marker (`›`, `❯`, `>`)
- Prompt gate (waits for TUI to show input prompt)

## Security Considerations

- **SSRF**: Validate all network addresses (gRPC, A2A)
- **Command injection**: Use `valid_binary_name()` for all shell commands
- **Path traversal**: Session IDs cannot contain `/`, `..`, or escape cwd
- **Permission bypass**: `Bypass` policy should require explicit confirmation

## Performance Notes

- **PTY quiescence**: 500ms idle timeout is a tradeoff (responsiveness vs. false positives)
- **Channel bounds**: `bus_tx` is bounded (100) to apply backpressure
- **Worktree cleanup**: Async via `tokio::task::spawn_blocking` (git operations are blocking)

## CI/CD

**Required checks** (`.github/workflows/ci.yml`):
- `cargo fmt --check`
- `cargo clippy --all-features -- -D warnings`
- `cargo test --workspace --all-features`
- `cargo doc --no-deps --all-features`
- MSRV check (1.85)

**Local pre-commit:**
```bash
cargo +nightly fmt --all
cargo clippy --all-features -- -D warnings
cargo test --workspace --all-features
```

## Debugging Tools

- **Tracing**: Set `RUST_LOG=debug` for detailed logs
- **Event inspection**: Use `OrchestratorEvent::Agent` to see raw agent output
- **State dumps**: `Run` struct implements `Debug` — log it to see fleet state

## Known Limitations

- **PTY fragility**: TUI parsing is heuristic-based; new agents may need custom `TuiParser`
- **No retry logic**: Failed sessions are terminal; no automatic retry
- **Single-host only**: No distributed orchestration (yet)
- **No hot reload**: Fleet config is parsed once at startup

## Contributing

1. Run `cargo +nightly fmt --all` before committing
2. Ensure all tests pass with `--all-features`
3. Add integration tests for new orchestration patterns
4. Update this document if changing architecture or conventions
5. Keep `Driver` trait methods object-safe (no `Self` in signatures)

## Resources

- Protocol spec: `docs/spec.md` (if exists)
- Examples: `crates/cap-rs/examples/`
- Test patterns: `crates/cap-rs-orchestrator/tests/patterns.rs`
