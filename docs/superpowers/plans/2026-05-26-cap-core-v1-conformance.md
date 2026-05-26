# CAP Core v1 Conformance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the Rust reference implementation into practical CAP core v1 conformance.

**Architecture:** Add manifest and wire modules to `cap-rs`, update drivers to obey a config-first lifecycle, and update the orchestrator to drive sessions through `SessionConfig -> Ready -> Inject`. Keep `AgentEvent::Done` internal, but provide strict wire helpers that omit it from external CAP streams.

**Tech Stack:** Rust 2024, Tokio, serde, serde_json, serde_yaml, TOML parsing via `toml`, existing driver abstractions, local stub tests.

---

### Task 1: Manifest Module

**Files:**
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs/Cargo.toml`
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs/src/lib.rs`
- Create: `/Users/oopos/dev/cap-protocol/crates/cap-rs/src/manifest.rs`

- [ ] **Step 1: Write manifest parsing tests**

Add tests in `manifest.rs` for:

```rust
#[test]
fn parses_example_manifest_and_applies_defaults() {
    let toml = r#"
[agent]
name = "demo"
binary = "demo"
profiles = []

[startup]
command = ["demo"]
ready_when = { pattern = "^> $" }

[pty]
cols = 120
rows = 40

[capabilities]
streaming_output = true
"#;
    let manifest = AgentManifest::from_toml_str(toml).unwrap();
    assert_eq!(manifest.agent.name, "demo");
    assert_eq!(manifest.pty.bracketed_paste, false);
    assert_eq!(manifest.pty.sigint_cancels_turn, CancelMode::Hard);
    assert_eq!(manifest.parse.idle, vec!["^>\\s*$", "^❯\\s*$"]);
}

#[test]
fn rejects_missing_required_fields() {
    let err = AgentManifest::from_toml_str("[agent]\nname = \"bad\"\n").unwrap_err();
    assert!(err.to_string().contains("missing field"));
}

#[test]
fn rejects_regex_lookaround() {
    let toml = r#"
[agent]
name = "demo"
binary = "demo"
profiles = []

[startup]
command = ["demo"]
ready_when = { pattern = "(?=bad)" }

[pty]
cols = 120
rows = 40

[capabilities]
streaming_output = true
"#;
    let err = AgentManifest::from_toml_str(toml).unwrap_err();
    assert!(err.to_string().contains("unsupported regex"));
}
```

- [ ] **Step 2: Run tests and verify RED**

Run: `cargo test -p cap-rs manifest::tests --all-features`

Expected: fail because `manifest` module does not exist.

- [ ] **Step 3: Implement manifest structs**

Implement `AgentManifest`, `AgentSection`, `StartupSection`, `FastPathSection`,
`PtySection`, `ParseSection`, `CapabilitiesSection`, and `CostSection`.
Expose `from_toml_str`, `from_path`, `validate`, `binding_preferences`, and
`discover_by_name`.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test -p cap-rs manifest::tests --all-features`

Expected: manifest tests pass.

### Task 2: Strict Wire Helpers

**Files:**
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs/src/core.rs`
- Create: `/Users/oopos/dev/cap-protocol/crates/cap-rs/src/wire.rs`
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs/src/lib.rs`

- [ ] **Step 1: Write tests for strict wire filtering**

Add tests proving `AgentEvent::Done` becomes a final `cap.usage` event when it
has usage and is otherwise omitted from strict external streams.

- [ ] **Step 2: Run tests and verify RED**

Run: `cargo test -p cap-rs wire::tests --all-features`

Expected: fail because `wire` module does not exist.

- [ ] **Step 3: Implement `StrictEvent` helpers**

Add `wire::to_strict_events(event: AgentEvent) -> Vec<AgentEvent>` and
`wire::is_core_wire_event(&AgentEvent) -> bool`.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test -p cap-rs wire::tests --all-features`

Expected: strict wire tests pass.

### Task 3: Driver Lifecycle API

**Files:**
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs/src/driver.rs`
- Modify driver modules under `/Users/oopos/dev/cap-protocol/crates/cap-rs/src/driver/`
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs-orchestrator/src/session.rs`

- [ ] **Step 1: Write lifecycle tests**

Add orchestrator tests with a stub driver that records sent frames and emits
`Ready`. Assert first received driver frame is `SessionConfig`, second is
`Prompt`.

- [ ] **Step 2: Run tests and verify RED**

Run: `cargo test -p cap-rs-orchestrator session::tests::sends_session_config_before_prompt --features testing`

Expected: fail because current session actor sends prompt first.

- [ ] **Step 3: Update session actor**

Build `SessionConfig` from session cwd, model, budget, and policy. Send
`ClientFrame::SessionConfig` first. Wait for `Ready` for every driver. Then send
the task as `Prompt`.

- [ ] **Step 4: Update drivers**

Drivers that consume config at construction should accept the first
`SessionConfig` as acknowledged no-op or use it to start lazily. Late
`SessionConfig` should return a typed error.

- [ ] **Step 5: Verify GREEN**

Run: `cargo test -p cap-rs-orchestrator session::tests --features testing`

Expected: session tests pass.

### Task 4: Manifest-backed Factory

**Files:**
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs-orchestrator/src/config.rs`
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs-orchestrator/src/real_factory.rs`
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs-orchestrator/src/factory.rs`

- [ ] **Step 1: Write config tests**

Add tests for `SessionSpec { manifest = "examples/claude-code.toml" }` and
`SessionSpec { agent = "claude-code" }`.

- [ ] **Step 2: Run tests and verify RED**

Run: `cargo test -p cap-rs-orchestrator config::tests::parses_manifest_backed_session --all-features`

Expected: fail because fields do not exist.

- [ ] **Step 3: Add manifest-backed config**

Extend `SessionSpec` with optional `agent` and `manifest`. Keep `driver` for
backward compatibility.

- [ ] **Step 4: Implement manifest-backed factory path**

When manifest or agent is present, resolve manifest and choose binding by
priority. Keep existing hard-coded driver path when only `driver` is present.

- [ ] **Step 5: Verify GREEN**

Run: `cargo test -p cap-rs-orchestrator config::tests real_factory::tests --all-features`

Expected: config and factory tests pass.

### Task 5: Stream-json Shutdown and Parse Errors

**Files:**
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs/src/driver/stream_json.rs`

- [ ] **Step 1: Write tests**

Add a stub child process test or unit-level handle test proving `shutdown`
actually records a killed/disconnected state and malformed JSON can emit
`cap.error { code: "parse_failed" }` in strict mode.

- [ ] **Step 2: Run tests and verify RED**

Run: `cargo test -p cap-rs driver::stream_json::tests --features stream-json`

Expected: new tests fail.

- [ ] **Step 3: Implement fix**

Keep a kill channel or child handle that `shutdown` can use after the waiter
task is spawned. Add strict parsing option and error event emission.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test -p cap-rs driver::stream_json::tests --features stream-json`

Expected: stream-json tests pass.

### Task 6: Mid-turn Injection and Budget Aggregation

**Files:**
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs-orchestrator/src/event.rs`
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs-orchestrator/src/executor.rs`
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs-orchestrator/src/session.rs`

- [ ] **Step 1: Write tests**

Add tests proving `OrchestratorControl::UserMessage` reaches a running driver
while `pump_turn` is waiting for events, and usage events aggregate until a
budget limit sends `Cancel`.

- [ ] **Step 2: Run tests and verify RED**

Run: `cargo test -p cap-rs-orchestrator session::tests::mid_turn_user_message_is_forwarded executor::tests::budget_exceeded_cancels_fleet --features testing`

Expected: fail because inbox is not polled during normal event pumping and no
budget aggregation exists.

- [ ] **Step 3: Implement mid-turn select**

In `pump_turn`, select over `driver.next_event()`, cancellation, and
`inbox_rx.recv()`. Forward prompt, permission, ask-user, and reverse-rpc frames
as appropriate.

- [ ] **Step 4: Implement budget state**

Track aggregate usage in `Run`. When cost exceeds budget, send cancel to live
sessions and emit a failure or fleet cancellation event.

- [ ] **Step 5: Verify GREEN**

Run: `cargo test -p cap-rs-orchestrator --features testing`

Expected: orchestrator tests pass.

### Task 7: High-risk Permission Policy

**Files:**
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs-orchestrator/src/config.rs`
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs-orchestrator/src/session.rs`

- [ ] **Step 1: Write tests**

Add tests proving high-risk permission requests under `Allow` and `Bypass` are
not auto-approved unless `allow_high_risk = true`.

- [ ] **Step 2: Run tests and verify RED**

Run: `cargo test -p cap-rs-orchestrator session::tests::high_risk_is_not_auto_approved --features testing`

Expected: fail because current policy auto-approves allow and bypass.

- [ ] **Step 3: Implement policy**

Add explicit high-risk policy config and deny or ask by default for high-risk
requests.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test -p cap-rs-orchestrator session::tests --features testing`

Expected: permission policy tests pass.

### Task 8: A2A Core Driver and Publisher

**Files:**
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs/Cargo.toml`
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-rs/src/driver.rs`
- Create: `/Users/oopos/dev/cap-protocol/crates/cap-rs/src/driver/a2a.rs`

- [ ] **Step 1: Write mapping tests**

Add tests for AgentCard validation, CAP extension detection, message/send
payload encoding, SSE event parsing, and CAP-to-A2A mapping.

- [ ] **Step 2: Run tests and verify RED**

Run: `cargo test -p cap-rs driver::a2a::tests --features a2a`

Expected: fail because module does not exist.

- [ ] **Step 3: Implement minimal A2A core**

Implement A2A structs and mapping helpers. Add an optional `A2aDriver` using
`reqwest` when feature `a2a` is enabled.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test -p cap-rs driver::a2a::tests --features a2a`

Expected: A2A tests pass.

### Task 9: CLI Manifest Commands

**Files:**
- Modify: `/Users/oopos/dev/cap-protocol/crates/cap-cli/src/main.rs`

- [ ] **Step 1: Write CLI tests if harness exists, otherwise add unit helpers**

Test manifest validation and resolution helpers without spawning live agents.

- [ ] **Step 2: Run tests and verify RED**

Run: `cargo test -p cap-cli --all-features`

Expected: fail until helper functions are implemented.

- [ ] **Step 3: Implement commands**

Add `cap manifest validate <path>`, `cap manifest resolve <name-or-path>`, and
single-agent `cap run --agent <name-or-path> --task <text>` while keeping
existing fleet run behavior.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test -p cap-cli --all-features`

Expected: CLI tests pass.

### Task 10: Final Verification

**Files:**
- All modified files.

- [ ] **Step 1: Format check**

Run: `cargo fmt --all -- --check`

Expected: exit 0.

- [ ] **Step 2: Clippy**

Run: `cargo clippy --all-features --all-targets -- -D warnings`

Expected: exit 0.

- [ ] **Step 3: Tests**

Run: `cargo test --all-features`

Expected: exit 0.

- [ ] **Step 4: Docs**

Run: `RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps`

Expected: exit 0.
