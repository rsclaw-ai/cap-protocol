# cap-rs-orchestrator Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a headless orchestration engine that runs N collaborating CLI agents in one process, driven by a declarative `fleet.yaml`, locally via `cap run`.

**Architecture:** A new `cap-rs-orchestrator` crate depending on `cap-rs`. Actor model — one tokio task per session owns a `Box<dyn Driver>`; everything communicates over `mpsc` channels. A deterministic `executor` state machine interprets the DSL and drives a `SessionRegistry`; an `audit` log records every cross-session route. Real-LLM-free testing via a `StubDriver` + `StubDriverFactory`.

**Tech Stack:** Rust 2024, tokio (multi-thread rt, macros, sync, process, time), serde + serde_yaml, async-trait, thiserror, cap-rs (features `stream-json`, `pty`, `codex`).

**Spec:** `docs/cap-orchestrator-engine-design.md`.

**Key cap-rs types this plan builds on (already exist, do not redefine):**
- `cap_rs::core::ClientFrame` — variants `Prompt { content: Vec<Content> }`, `PermissionResponse { req_id: String, decision: PermissionDecision }`, `Cancel { scope, reason }`, `SessionConfig(..)`, `AskUserAnswer { .. }`.
- `cap_rs::core::AgentEvent` — terminal variant is `Done { stop_reason: StopReason, usage: Usage }`; also `PermissionRequest { req_id, tool, intent, scope, risk_level }`, `TextChunk`, `ToolCallStart/End`, `Error { code, message }`, etc.
- `cap_rs::core::{Content, PermissionDecision, StopReason, RiskLevel}`.
- `cap_rs::driver::{Driver, DriverError}` — trait methods `async send(&mut self, ClientFrame)`, `async next_event(&mut self) -> Option<AgentEvent>`, `async shutdown(&mut self)`. `Driver: Send` (NOT `Sync`).
- Real drivers: `ClaudeCodeDriver::builder(cwd).dangerously_skip_permissions(bool).spawn().await`; `CodexExecDriver::builder(cwd).skip_git_repo_check(bool).arg(..).spawn().await`; `PtyDriver::builder(cmd).cwd(..).spawn(parser)`.

---

## Task 1: Scaffold the `cap-rs-orchestrator` crate

**Files:**
- Create: `crates/cap-rs-orchestrator/Cargo.toml`
- Create: `crates/cap-rs-orchestrator/src/lib.rs`
- Modify: `Cargo.toml` (workspace `members`)

- [ ] **Step 1: Add the crate to the workspace**

In the root `Cargo.toml`, add the member:

```toml
[workspace]
resolver = "3"
members = [
    "crates/cap-rs",
    "crates/cap-cli",
    "crates/cap-rs-orchestrator",
]
```

- [ ] **Step 2: Write the crate manifest**

Create `crates/cap-rs-orchestrator/Cargo.toml`:

```toml
[package]
name         = "cap-rs-orchestrator"
version.workspace      = true
edition.workspace      = true
rust-version.workspace = true
authors.workspace      = true
homepage.workspace     = true
repository.workspace   = true
license.workspace      = true

[dependencies]
cap-rs = { path = "../cap-rs", features = ["stream-json", "pty", "codex"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync", "process", "time"] }
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
serde_json = "1"
async-trait = "0.1"
thiserror = "2"

[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 3: Write a minimal lib.rs**

Create `crates/cap-rs-orchestrator/src/lib.rs`:

```rust
//! cap-rs-orchestrator — headless engine that runs N collaborating CLI agents
//! in one process, driven by a declarative `fleet.yaml`.
//!
//! See `docs/cap-orchestrator-engine-design.md`.
#![warn(missing_debug_implementations)]

/// Errors surfaced by the orchestrator engine.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OrchestratorError {
    #[error("config error: {0}")]
    Config(String),
    #[error("worktree error: {0}")]
    Worktree(String),
    #[error("driver error: {0}")]
    Driver(#[from] cap_rs::driver::DriverError),
    #[error("unknown driver kind: {0}")]
    UnknownDriver(String),
}
```

- [ ] **Step 4: Verify it builds**

Run: `cargo build -p cap-rs-orchestrator`
Expected: compiles clean, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/cap-rs-orchestrator/
git commit -m "feat(orchestrator): scaffold cap-rs-orchestrator crate"
```

---

## Task 2: The DSL config types + validation

**Files:**
- Create: `crates/cap-rs-orchestrator/src/config.rs`
- Modify: `crates/cap-rs-orchestrator/src/lib.rs` (add `pub mod config;`)
- Test: inline `#[cfg(test)]` in `config.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/cap-rs-orchestrator/src/config.rs` with the test module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const PIPELINE: &str = r#"
fleet:
  base_branch: main
  task: "do the thing"
  sessions:
    coder: { driver: claude }
    reviewer: { driver: codex, permissions: allow }
  start: coder
  routes:
    - { when: coder.done,    route_to: reviewer }
"#;

    #[test]
    fn parses_pipeline() {
        let spec = FleetSpec::from_yaml(PIPELINE).unwrap();
        assert_eq!(spec.fleet.base_branch, "main");
        assert_eq!(spec.fleet.sessions.len(), 2);
        assert_eq!(spec.fleet.permissions, PermissionPolicy::Ask); // default
        assert_eq!(
            spec.fleet.sessions["reviewer"].permissions,
            Some(PermissionPolicy::Allow)
        );
        match &spec.fleet.start {
            Start::One(s) => assert_eq!(s, "coder"),
            other => panic!("wrong start: {other:?}"),
        }
        spec.validate().unwrap();
    }

    #[test]
    fn parses_fan_out_and_join() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    lead: { driver: claude }
    a: { driver: codex }
    b: { driver: codex }
    rev: { driver: claude }
  start: lead
  routes:
    - when: lead.done
      fan_out: { to: [a, b], split: by_subtask }
    - when: [a.done, b.done]
      route_to: rev
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        let r0 = &spec.fleet.routes[0];
        assert_eq!(r0.trigger_sessions(), vec!["lead"]);
        match r0.action().unwrap() {
            Action::FanOut(f) => {
                assert_eq!(f.to, vec!["a", "b"]);
                assert_eq!(f.split, Split::BySubtask);
            }
            other => panic!("wrong action: {other:?}"),
        }
        let r1 = &spec.fleet.routes[1];
        assert_eq!(r1.trigger_sessions(), vec!["a", "b"]); // join
        spec.validate().unwrap();
    }

    #[test]
    fn rejects_route_to_unknown_session() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    coder: { driver: claude }
  start: coder
  routes:
    - { when: coder.done, route_to: ghost }
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        let err = spec.validate().unwrap_err();
        assert!(format!("{err}").contains("ghost"), "got: {err}");
    }

    #[test]
    fn rejects_route_with_two_actions() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    a: { driver: claude }
    b: { driver: claude }
  start: a
  routes:
    - when: a.done
      route_to: b
      collect: human
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn parses_pty_driver_kind() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    oc: { driver: "pty:opencode" }
  start: oc
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert_eq!(
            spec.fleet.sessions["oc"].driver,
            DriverKind::Pty("opencode".into())
        );
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p cap-rs-orchestrator config::`
Expected: FAIL — `FleetSpec` etc. not defined / module empty.

- [ ] **Step 3: Implement the config types**

Prepend to `crates/cap-rs-orchestrator/src/config.rs` (above the test module):

```rust
//! Declarative `fleet.yaml` schema + validation.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::OrchestratorError;

pub type SessionId = String;

/// Top-level document: `{ fleet: { ... } }`.
#[derive(Debug, Clone, Deserialize)]
pub struct FleetSpec {
    pub fleet: Fleet,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Fleet {
    pub base_branch: String,
    #[serde(default)]
    pub task: Option<String>,
    /// Fleet-level permission default; per-session may override.
    #[serde(default)]
    pub permissions: PermissionPolicy,
    pub sessions: BTreeMap<SessionId, SessionSpec>,
    pub start: Start,
    #[serde(default)]
    pub routes: Vec<Route>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionSpec {
    pub driver: DriverKind,
    /// `None` means "inherit the fleet-level policy".
    #[serde(default)]
    pub permissions: Option<PermissionPolicy>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionPolicy {
    #[default]
    Ask,
    Allow,
    Deny,
    Bypass,
}

/// `claude` | `codex` | `pty:<command>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverKind {
    Claude,
    Codex,
    Pty(String),
}

impl<'de> Deserialize<'de> for DriverKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(match s.as_str() {
            "claude" => DriverKind::Claude,
            "codex" => DriverKind::Codex,
            other => match other.strip_prefix("pty:") {
                Some(cmd) if !cmd.is_empty() => DriverKind::Pty(cmd.to_string()),
                _ => return Err(serde::de::Error::custom(format!(
                    "unknown driver kind '{other}' (expected claude | codex | pty:<cmd>)"
                ))),
            },
        })
    }
}

/// Entry point: one session or several launched at once.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Start {
    One(SessionId),
    Many(Vec<SessionId>),
}

impl Start {
    pub fn sessions(&self) -> Vec<SessionId> {
        match self {
            Start::One(s) => vec![s.clone()],
            Start::Many(v) => v.clone(),
        }
    }
}

/// A `when:` trigger — a single `X.done` or a list (a join).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Trigger {
    Single(String),
    Join(Vec<String>),
}

/// One routing edge. Exactly one of `route_to` / `fan_out` / `collect` must be set.
#[derive(Debug, Clone, Deserialize)]
pub struct Route {
    pub when: Trigger,
    #[serde(default)]
    pub route_to: Option<SessionId>,
    #[serde(default)]
    pub fan_out: Option<FanOut>,
    #[serde(default)]
    pub collect: Option<Collect>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FanOut {
    pub to: Vec<SessionId>,
    #[serde(default)]
    pub split: Split,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Split {
    #[default]
    Broadcast,
    BySubtask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Collect {
    Human,
}

/// The resolved action of a [`Route`].
#[derive(Debug, Clone)]
pub enum Action {
    RouteTo(SessionId),
    FanOut(FanOut),
    Collect(Collect),
}

impl Trigger {
    /// The session ids referenced by this trigger, stripped of the `.done` suffix.
    /// Returns the raw token unchanged if it has no `.done` suffix (validation
    /// then rejects it).
    fn raw_tokens(&self) -> Vec<&str> {
        match self {
            Trigger::Single(s) => vec![s.as_str()],
            Trigger::Join(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }
}

impl Route {
    /// Session ids this route fires on (the `.done` suffix removed).
    pub fn trigger_sessions(&self) -> Vec<String> {
        self.when
            .raw_tokens()
            .iter()
            .map(|t| t.strip_suffix(".done").unwrap_or(t).to_string())
            .collect()
    }

    /// Resolve the single action, erroring if zero or more than one is set.
    pub fn action(&self) -> Result<Action, OrchestratorError> {
        let count = self.route_to.is_some() as u8
            + self.fan_out.is_some() as u8
            + self.collect.is_some() as u8;
        if count != 1 {
            return Err(OrchestratorError::Config(format!(
                "route on {:?} must have exactly one of route_to/fan_out/collect (found {count})",
                self.trigger_sessions()
            )));
        }
        if let Some(to) = &self.route_to {
            Ok(Action::RouteTo(to.clone()))
        } else if let Some(f) = &self.fan_out {
            Ok(Action::FanOut(f.clone()))
        } else {
            Ok(Action::Collect(self.collect.unwrap()))
        }
    }
}

impl FleetSpec {
    pub fn from_yaml(s: &str) -> Result<Self, OrchestratorError> {
        serde_yaml::from_str(s).map_err(|e| OrchestratorError::Config(e.to_string()))
    }

    /// Static validation: every referenced session exists, every trigger uses
    /// the `.done` form, and every route has exactly one action.
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        let known = |id: &str| self.fleet.sessions.contains_key(id);
        let bad = |what: &str, id: &str| {
            Err(OrchestratorError::Config(format!(
                "{what} references unknown session '{id}'"
            )))
        };

        for s in self.fleet.start.sessions() {
            if !known(&s) {
                return bad("start", &s);
            }
        }
        for route in &self.fleet.routes {
            for token in route.when.raw_tokens() {
                let id = token.strip_suffix(".done").ok_or_else(|| {
                    OrchestratorError::Config(format!(
                        "trigger '{token}' must end in '.done'"
                    ))
                })?;
                if !known(id) {
                    return bad("trigger", id);
                }
            }
            match route.action()? {
                Action::RouteTo(to) => {
                    if !known(&to) {
                        return bad("route_to", &to);
                    }
                }
                Action::FanOut(f) => {
                    for to in &f.to {
                        if !known(to) {
                            return bad("fan_out target", to);
                        }
                    }
                }
                Action::Collect(_) => {}
            }
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Wire the module**

In `crates/cap-rs-orchestrator/src/lib.rs`, add after the error enum:

```rust
pub mod config;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p cap-rs-orchestrator config::`
Expected: PASS — all 5 tests.

- [ ] **Step 6: Commit**

```bash
git add crates/cap-rs-orchestrator/src/config.rs crates/cap-rs-orchestrator/src/lib.rs
git commit -m "feat(orchestrator): fleet.yaml DSL types + validation"
```

---

## Task 3: Orchestrator event/control types + `StubDriver`

**Files:**
- Create: `crates/cap-rs-orchestrator/src/event.rs`
- Create: `crates/cap-rs-orchestrator/src/testing.rs`
- Modify: `crates/cap-rs-orchestrator/src/lib.rs`
- Test: inline `#[cfg(test)]` in `testing.rs`

- [ ] **Step 1: Define the event + control types**

Create `crates/cap-rs-orchestrator/src/event.rs`:

```rust
//! Types crossing the engine↔consumer boundary. This boundary is an in-process
//! `mpsc` channel today and the seam for the future remote (WebSocket) layer.

use cap_rs::core::{AgentEvent, RiskLevel, StopReason};

use crate::config::SessionId;

/// Everything the engine emits outward, tagged by session where applicable.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum OrchestratorEvent {
    SessionStarted { session: SessionId },
    /// A raw agent event, tagged with its originating session.
    Agent { session: SessionId, event: AgentEvent },
    /// A permission request awaiting a human decision (only under `ask` policy).
    Ask {
        session: SessionId,
        req_id: String,
        tool: String,
        risk_level: RiskLevel,
    },
    /// The engine routed one session's output into another's inbox.
    Routed { from: SessionId, to: SessionId },
    SessionDone { session: SessionId, stop_reason: StopReason },
    SessionFailed { session: SessionId, error: String },
    /// A `collect: human` join completed; these candidate sessions await a pick.
    AwaitSelection { candidates: Vec<SessionId> },
    FleetComplete,
}

/// Everything the consumer sends back in (decisions, selections, cancel).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum OrchestratorControl {
    /// Answer to an [`OrchestratorEvent::Ask`].
    Decision {
        session: SessionId,
        req_id: String,
        allow: bool,
    },
    /// Answer to an [`OrchestratorEvent::AwaitSelection`].
    Select { session: SessionId },
    /// Hard-cancel the whole fleet.
    Cancel,
}
```

- [ ] **Step 2: Write the failing StubDriver test**

Create `crates/cap-rs-orchestrator/src/testing.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use cap_rs::core::{ClientFrame, Content, StopReason};
    use cap_rs::driver::Driver;

    #[tokio::test]
    async fn stub_emits_scripted_events_then_done() {
        let mut d = StubDriver::new("s1")
            .text("hello ")
            .text("world")
            .done(StopReason::EndTurn);

        // Driving a prompt in is a no-op for the stub but must not error.
        d.send(ClientFrame::Prompt {
            content: vec![Content::text("hi")],
        })
        .await
        .unwrap();

        let mut texts = String::new();
        let mut saw_done = false;
        while let Some(ev) = d.next_event().await {
            match ev {
                cap_rs::core::AgentEvent::TextChunk { text, .. } => texts.push_str(&text),
                cap_rs::core::AgentEvent::Done { .. } => saw_done = true,
                _ => {}
            }
        }
        assert_eq!(texts, "hello world");
        assert!(saw_done);
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p cap-rs-orchestrator testing::`
Expected: FAIL — `StubDriver` not defined.

- [ ] **Step 4: Implement StubDriver**

Prepend to `crates/cap-rs-orchestrator/src/testing.rs`:

```rust
//! Test doubles: a `Driver` and a driver factory that emit scripted events,
//! so the engine can be tested with zero real LLM / network.

use std::collections::VecDeque;

use cap_rs::core::{AgentEvent, ClientFrame, PermissionScope, RiskLevel, StopReason, TextChannel, Usage};
use cap_rs::driver::{Driver, DriverError};

/// A scripted driver. Build it with chained helpers, then it replays the queued
/// events on successive `next_event()` calls and returns `None` afterwards.
#[derive(Debug, Default)]
pub struct StubDriver {
    name: String,
    queue: VecDeque<AgentEvent>,
    alive: bool,
    /// Set when a permission request is scripted; the next `send` of a
    /// `PermissionResponse` records the decision here for assertions.
    pub last_decision: Option<cap_rs::core::PermissionDecision>,
}

impl StubDriver {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            queue: VecDeque::new(),
            alive: true,
            last_decision: None,
        }
    }

    pub fn text(mut self, t: &str) -> Self {
        self.queue.push_back(AgentEvent::TextChunk {
            msg_id: format!("{}-m", self.name),
            text: t.to_string(),
            channel: TextChannel::Assistant,
        });
        self
    }

    /// Script a permission request the engine must resolve before `done`.
    pub fn permission(mut self, tool: &str, risk: RiskLevel) -> Self {
        self.queue.push_back(AgentEvent::PermissionRequest {
            req_id: format!("{}-req", self.name),
            tool: tool.to_string(),
            intent: serde_json::json!({}),
            scope: PermissionScope::Execute,
            risk_level: risk,
        });
        self
    }

    pub fn done(mut self, stop: StopReason) -> Self {
        self.queue.push_back(AgentEvent::Done {
            stop_reason: stop,
            usage: Usage::default(),
        });
        self
    }
}

#[async_trait::async_trait]
impl Driver for StubDriver {
    async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError> {
        if let ClientFrame::PermissionResponse { decision, .. } = frame {
            self.last_decision = Some(decision);
        }
        Ok(())
    }

    async fn next_event(&mut self) -> Option<AgentEvent> {
        let ev = self.queue.pop_front();
        if ev.is_none() {
            self.alive = false;
        }
        ev
    }

    async fn shutdown(&mut self) -> Result<(), DriverError> {
        self.alive = false;
        Ok(())
    }

    fn is_alive(&self) -> bool {
        self.alive
    }
}
```

- [ ] **Step 5: Wire the modules**

In `crates/cap-rs-orchestrator/src/lib.rs`, add:

```rust
pub mod event;
pub mod testing;
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test -p cap-rs-orchestrator testing::`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/cap-rs-orchestrator/src/event.rs crates/cap-rs-orchestrator/src/testing.rs crates/cap-rs-orchestrator/src/lib.rs
git commit -m "feat(orchestrator): OrchestratorEvent/Control types + StubDriver test double"
```

---

## Task 4: The session actor

**Files:**
- Create: `crates/cap-rs-orchestrator/src/session.rs`
- Modify: `crates/cap-rs-orchestrator/src/lib.rs`
- Test: inline `#[cfg(test)]` in `session.rs`

**Concurrency note (critical):** `Driver` is `Send` not `Sync`, and both `send` and `next_event` take `&mut self`. We must NEVER `tokio::select!` two arms that each borrow the driver. The actor therefore alternates: it awaits an inbox frame (no driver borrow), sends it, then pumps `next_event()` until `Done`. While pumping, it selects `next_event()` against a cancel token only (the token future does not borrow the driver). A scripted permission request under `ask` policy is resolved by awaiting the inbox for a `PermissionResponse` — again, no concurrent driver borrow.

- [ ] **Step 1: Write the failing tests**

Create `crates/cap-rs-orchestrator/src/session.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PermissionPolicy;
    use crate::event::OrchestratorEvent;
    use crate::testing::StubDriver;
    use cap_rs::core::{ClientFrame, Content, PermissionDecision, RiskLevel, StopReason};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    fn prompt(s: &str) -> ClientFrame {
        ClientFrame::Prompt { content: vec![Content::text(s)] }
    }

    #[tokio::test]
    async fn pumps_events_and_signals_done() {
        let driver = Box::new(StubDriver::new("a").text("hi").done(StopReason::EndTurn));
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let token = CancellationToken::new();
        let handle = spawn_session("a".into(), driver, PermissionPolicy::Allow, bus_tx, token);

        handle.inbox.send(prompt("go")).await.unwrap();

        let mut kinds = Vec::new();
        while let Some(ev) = bus_rx.recv().await {
            match ev {
                OrchestratorEvent::SessionStarted { .. } => kinds.push("started"),
                OrchestratorEvent::Agent { .. } => kinds.push("agent"),
                OrchestratorEvent::SessionDone { stop_reason, .. } => {
                    assert_eq!(stop_reason, StopReason::EndTurn);
                    kinds.push("done");
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(kinds, vec!["started", "agent", "done"]);
        handle.join.await.unwrap();
    }

    #[tokio::test]
    async fn allow_policy_auto_approves_permission() {
        let driver = Box::new(
            StubDriver::new("a")
                .permission("Bash", RiskLevel::Medium)
                .done(StopReason::EndTurn),
        );
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let token = CancellationToken::new();
        let handle = spawn_session("a".into(), driver, PermissionPolicy::Allow, bus_tx, token);
        handle.inbox.send(prompt("go")).await.unwrap();

        // No Ask event should ever appear under Allow policy.
        let mut saw_ask = false;
        while let Some(ev) = bus_rx.recv().await {
            match ev {
                OrchestratorEvent::Ask { .. } => saw_ask = true,
                OrchestratorEvent::SessionDone { .. } => break,
                _ => {}
            }
        }
        assert!(!saw_ask, "Allow policy must not surface an Ask");
    }

    #[tokio::test]
    async fn ask_policy_surfaces_ask_and_awaits_decision() {
        let driver = Box::new(
            StubDriver::new("a")
                .permission("Bash", RiskLevel::High)
                .done(StopReason::EndTurn),
        );
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let token = CancellationToken::new();
        let handle = spawn_session("a".into(), driver, PermissionPolicy::Ask, bus_tx, token);
        handle.inbox.send(prompt("go")).await.unwrap();

        // Wait for the Ask, then answer it via the inbox.
        loop {
            match bus_rx.recv().await.unwrap() {
                OrchestratorEvent::Ask { req_id, .. } => {
                    handle
                        .inbox
                        .send(ClientFrame::PermissionResponse {
                            req_id,
                            decision: PermissionDecision::AllowOnce,
                        })
                        .await
                        .unwrap();
                    break;
                }
                _ => {}
            }
        }
        // It should still reach Done after the decision.
        let mut saw_done = false;
        while let Some(ev) = bus_rx.recv().await {
            if let OrchestratorEvent::SessionDone { .. } = ev {
                saw_done = true;
                break;
            }
        }
        assert!(saw_done);
    }
}
```

- [ ] **Step 2: Add the `tokio-util` dependency**

In `crates/cap-rs-orchestrator/Cargo.toml`, add to `[dependencies]`:

```toml
tokio-util = "0.7"
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p cap-rs-orchestrator session::`
Expected: FAIL — `spawn_session` / `SessionHandle` not defined.

- [ ] **Step 4: Implement the session actor**

Prepend to `crates/cap-rs-orchestrator/src/session.rs`:

```rust
//! One tokio task per session, owning a `Box<dyn Driver>`. Communicates only
//! over channels — no shared mutable state, no `Mutex<Driver>`.

use cap_rs::core::{AgentEvent, ClientFrame, PermissionDecision};
use cap_rs::driver::Driver;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::{PermissionPolicy, SessionId};
use crate::event::OrchestratorEvent;

/// A live session: its inbox sender + the task handle.
#[derive(Debug)]
pub struct SessionHandle {
    pub inbox: mpsc::Sender<ClientFrame>,
    pub join: JoinHandle<()>,
}

/// Spawn the actor task. Returns immediately; the task runs until its driver
/// exits, the inbox closes, or the cancel token fires.
pub fn spawn_session(
    id: SessionId,
    mut driver: Box<dyn Driver>,
    policy: PermissionPolicy,
    bus: mpsc::Sender<OrchestratorEvent>,
    cancel: CancellationToken,
) -> SessionHandle {
    let (inbox_tx, mut inbox_rx) = mpsc::channel::<ClientFrame>(32);

    let join = tokio::spawn(async move {
        let _ = bus
            .send(OrchestratorEvent::SessionStarted { session: id.clone() })
            .await;

        // Outer loop: wait for a frame to drive a turn.
        loop {
            let frame = tokio::select! {
                biased;
                _ = cancel.cancelled() => { let _ = driver.shutdown().await; return; }
                maybe = inbox_rx.recv() => match maybe {
                    Some(f) => f,
                    None => { let _ = driver.shutdown().await; return; }
                }
            };

            if let Err(e) = driver.send(frame).await {
                let _ = bus
                    .send(OrchestratorEvent::SessionFailed {
                        session: id.clone(),
                        error: e.to_string(),
                    })
                    .await;
                return;
            }

            // Inner loop: pump events until this turn ends.
            if !pump_turn(&id, &mut driver, policy, &bus, &mut inbox_rx, &cancel).await {
                return; // driver exited or cancelled
            }
        }
    });

    SessionHandle { inbox: inbox_tx, join }
}

/// Pump events until `Done`. Returns `true` if the turn ended normally (the
/// outer loop should wait for the next frame), `false` if the actor should stop.
async fn pump_turn(
    id: &SessionId,
    driver: &mut Box<dyn Driver>,
    policy: PermissionPolicy,
    bus: &mpsc::Sender<OrchestratorEvent>,
    inbox_rx: &mut mpsc::Receiver<ClientFrame>,
    cancel: &CancellationToken,
) -> bool {
    loop {
        let ev = tokio::select! {
            biased;
            _ = cancel.cancelled() => { let _ = driver.shutdown().await; return false; }
            ev = driver.next_event() => ev,
        };

        let Some(ev) = ev else {
            // Driver exited mid-turn without a Done.
            let _ = bus
                .send(OrchestratorEvent::SessionFailed {
                    session: id.clone(),
                    error: "driver exited before completing the turn".into(),
                })
                .await;
            return false;
        };

        match ev {
            AgentEvent::Done { stop_reason, .. } => {
                let _ = bus
                    .send(OrchestratorEvent::Agent {
                        session: id.clone(),
                        event: AgentEvent::Done {
                            stop_reason,
                            usage: Default::default(),
                        },
                    })
                    .await;
                let _ = bus
                    .send(OrchestratorEvent::SessionDone {
                        session: id.clone(),
                        stop_reason,
                    })
                    .await;
                return true;
            }
            AgentEvent::PermissionRequest {
                ref req_id,
                ref tool,
                risk_level,
                ..
            } => {
                let req_id = req_id.clone();
                let tool = tool.clone();
                // Surface the raw event for observers regardless of policy.
                let _ = bus
                    .send(OrchestratorEvent::Agent {
                        session: id.clone(),
                        event: ev.clone(),
                    })
                    .await;

                let decision = match policy {
                    PermissionPolicy::Allow | PermissionPolicy::Bypass => {
                        PermissionDecision::AllowOnce
                    }
                    PermissionPolicy::Deny => PermissionDecision::Deny,
                    PermissionPolicy::Ask => {
                        let _ = bus
                            .send(OrchestratorEvent::Ask {
                                session: id.clone(),
                                req_id: req_id.clone(),
                                tool,
                                risk_level,
                            })
                            .await;
                        // Block on the inbox for the decision (driver not borrowed here).
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => { let _ = driver.shutdown().await; return false; }
                            maybe = inbox_rx.recv() => match maybe {
                                Some(ClientFrame::PermissionResponse { decision, .. }) => decision,
                                _ => PermissionDecision::Deny,
                            }
                        }
                    }
                };

                if let Err(e) = driver
                    .send(ClientFrame::PermissionResponse { req_id, decision })
                    .await
                {
                    let _ = bus
                        .send(OrchestratorEvent::SessionFailed {
                            session: id.clone(),
                            error: e.to_string(),
                        })
                        .await;
                    return false;
                }
            }
            other => {
                let _ = bus
                    .send(OrchestratorEvent::Agent {
                        session: id.clone(),
                        event: other,
                    })
                    .await;
            }
        }
    }
}
```

- [ ] **Step 5: Wire the module**

In `crates/cap-rs-orchestrator/src/lib.rs`, add:

```rust
pub mod session;
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p cap-rs-orchestrator session::`
Expected: PASS — all 3 tests.

- [ ] **Step 7: Commit**

```bash
git add crates/cap-rs-orchestrator/
git commit -m "feat(orchestrator): session actor with per-policy permission handling"
```

---

## Task 5: Worktree management

**Files:**
- Create: `crates/cap-rs-orchestrator/src/worktree.rs`
- Modify: `crates/cap-rs-orchestrator/src/lib.rs`
- Test: inline `#[cfg(test)]` in `worktree.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/cap-rs-orchestrator/src/worktree.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_returns_distinct_dirs_per_session() {
        let wt = NoopWorktreeManager::new();
        let a = wt.create("a", "main").unwrap();
        let b = wt.create("b", "main").unwrap();
        assert!(a.exists());
        assert!(b.exists());
        assert_ne!(a, b);
        wt.cleanup("a").unwrap();
    }

    #[test]
    fn git_creates_a_worktree_off_base_branch() {
        // Build a throwaway git repo with one commit on `main`.
        let repo = tempfile::tempdir().unwrap();
        let run = |args: &[&str]| {
            let ok = std::process::Command::new("git")
                .args(args)
                .current_dir(repo.path())
                .status()
                .unwrap()
                .success();
            assert!(ok, "git {args:?} failed");
        };
        run(&["init", "-q", "-b", "main"]);
        run(&["config", "user.email", "t@t"]);
        run(&["config", "user.name", "t"]);
        std::fs::write(repo.path().join("f.txt"), "x").unwrap();
        run(&["add", "."]);
        run(&["commit", "-qm", "init"]);

        let wt = GitWorktreeManager::new(repo.path());
        let dir = wt.create("worker", "main").unwrap();
        assert!(dir.join("f.txt").exists(), "worktree should contain repo files");
        wt.cleanup("worker").unwrap();
        assert!(!dir.exists(), "cleanup should remove the worktree dir");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p cap-rs-orchestrator worktree::`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement the worktree managers**

Prepend to `crates/cap-rs-orchestrator/src/worktree.rs`:

```rust
//! Per-session workspace allocation. Default is one git worktree per session,
//! branched off the fleet's `base_branch`.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::config::SessionId;
use crate::OrchestratorError;

/// Allocates and cleans up a workspace per session.
pub trait WorktreeManager: Send + Sync {
    /// Create (or reuse) the workspace for `session`, returning its path.
    fn create(&self, session: &SessionId, base_branch: &str) -> Result<PathBuf, OrchestratorError>;
    /// Remove the workspace for `session`. Idempotent.
    fn cleanup(&self, session: &SessionId) -> Result<(), OrchestratorError>;
}

/// Real implementation: `git worktree add <root>/.cap/<session> -b cap/<session> <base>`.
#[derive(Debug, Clone)]
pub struct GitWorktreeManager {
    repo: PathBuf,
}

impl GitWorktreeManager {
    pub fn new(repo: impl AsRef<Path>) -> Self {
        Self { repo: repo.as_ref().to_path_buf() }
    }

    fn dir_for(&self, session: &SessionId) -> PathBuf {
        self.repo.join(".cap").join(session)
    }

    fn git(&self, args: &[&str]) -> Result<(), OrchestratorError> {
        let out = Command::new("git")
            .args(args)
            .current_dir(&self.repo)
            .output()
            .map_err(|e| OrchestratorError::Worktree(format!("spawning git failed: {e}")))?;
        if !out.status.success() {
            return Err(OrchestratorError::Worktree(format!(
                "git {args:?} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(())
    }
}

impl WorktreeManager for GitWorktreeManager {
    fn create(&self, session: &SessionId, base_branch: &str) -> Result<PathBuf, OrchestratorError> {
        let dir = self.dir_for(session);
        let dir_str = dir.to_string_lossy().to_string();
        let branch = format!("cap/{session}");
        self.git(&["worktree", "add", "-b", &branch, &dir_str, base_branch])?;
        Ok(dir)
    }

    fn cleanup(&self, session: &SessionId) -> Result<(), OrchestratorError> {
        let dir = self.dir_for(session);
        let dir_str = dir.to_string_lossy().to_string();
        // Best-effort: ignore failure if it was never created.
        let _ = self.git(&["worktree", "remove", "--force", &dir_str]);
        Ok(())
    }
}

/// Test/dev implementation: a throwaway temp dir per session, no git.
#[derive(Debug)]
pub struct NoopWorktreeManager {
    root: tempfile::TempDir,
}

impl NoopWorktreeManager {
    pub fn new() -> Self {
        Self { root: tempfile::tempdir().expect("create temp dir") }
    }
}

impl Default for NoopWorktreeManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WorktreeManager for NoopWorktreeManager {
    fn create(&self, session: &SessionId, _base_branch: &str) -> Result<PathBuf, OrchestratorError> {
        let dir = self.root.path().join(session);
        std::fs::create_dir_all(&dir)
            .map_err(|e| OrchestratorError::Worktree(e.to_string()))?;
        Ok(dir)
    }

    fn cleanup(&self, _session: &SessionId) -> Result<(), OrchestratorError> {
        Ok(()) // TempDir cleans itself on drop.
    }
}
```

- [ ] **Step 4: Move `tempfile` to a normal dependency**

`NoopWorktreeManager` ships in the library (used by the executor's test factory wiring), so `tempfile` must be a regular dependency. In `crates/cap-rs-orchestrator/Cargo.toml`, move `tempfile = "3"` from `[dev-dependencies]` to `[dependencies]`.

- [ ] **Step 5: Wire the module**

In `crates/cap-rs-orchestrator/src/lib.rs`, add:

```rust
pub mod worktree;
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p cap-rs-orchestrator worktree::`
Expected: PASS. (The git test needs `git` on PATH — it is, in this repo's dev/CI env.)

- [ ] **Step 7: Commit**

```bash
git add crates/cap-rs-orchestrator/
git commit -m "feat(orchestrator): git + noop worktree managers"
```

---

## Task 6: The driver factory abstraction + `SessionRegistry`

**Files:**
- Create: `crates/cap-rs-orchestrator/src/registry.rs`
- Modify: `crates/cap-rs-orchestrator/src/testing.rs` (add `StubDriverFactory`)
- Modify: `crates/cap-rs-orchestrator/src/lib.rs`
- Test: inline `#[cfg(test)]` in `registry.rs`

The registry creates worktrees and spawns session actors. To stay testable without real agent binaries, driver construction goes through a `DriverFactory` trait. The real factory lands in Task 9.

- [ ] **Step 1: Define `DriverFactory` and add `StubDriverFactory`**

Create `crates/cap-rs-orchestrator/src/factory.rs`:

```rust
//! Constructs a concrete `Driver` for a session. Behind a trait so tests use
//! scripted stubs and the engine uses real CLI agents.

use std::path::Path;

use async_trait::async_trait;
use cap_rs::driver::Driver;

use crate::config::{DriverKind, PermissionPolicy, SessionId};
use crate::OrchestratorError;

#[async_trait]
pub trait DriverFactory: Send + Sync {
    /// Build a driver for `session` running in `cwd`. `policy` lets the factory
    /// pass each agent's native bypass flag when `policy == Bypass`.
    async fn build(
        &self,
        session: &SessionId,
        kind: &DriverKind,
        cwd: &Path,
        policy: PermissionPolicy,
    ) -> Result<Box<dyn Driver>, OrchestratorError>;
}
```

Then add to `crates/cap-rs-orchestrator/src/lib.rs`:

```rust
pub mod factory;
```

Append a stub factory to `crates/cap-rs-orchestrator/src/testing.rs` (outside the existing `#[cfg(test)] mod tests`):

```rust
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::config::{DriverKind, PermissionPolicy, SessionId};
use crate::factory::DriverFactory;
use crate::OrchestratorError;

/// A factory that hands out pre-scripted `StubDriver`s by session id.
#[derive(Debug, Default)]
pub struct StubDriverFactory {
    scripts: Mutex<HashMap<SessionId, StubDriver>>,
}

impl StubDriverFactory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register the driver a given session id should receive.
    pub fn with(self, session: &str, driver: StubDriver) -> Self {
        self.scripts
            .lock()
            .unwrap()
            .insert(session.to_string(), driver);
        self
    }
}

#[async_trait]
impl DriverFactory for StubDriverFactory {
    async fn build(
        &self,
        session: &SessionId,
        _kind: &DriverKind,
        _cwd: &Path,
        _policy: PermissionPolicy,
    ) -> Result<Box<dyn cap_rs::driver::Driver>, OrchestratorError> {
        self.scripts
            .lock()
            .unwrap()
            .remove(session)
            .map(|d| Box::new(d) as Box<dyn cap_rs::driver::Driver>)
            .ok_or_else(|| OrchestratorError::Config(format!("no stub for session '{session}'")))
    }
}
```

- [ ] **Step 2: Write the failing registry tests**

Create `crates/cap-rs-orchestrator/src/registry.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DriverKind, PermissionPolicy};
    use crate::event::OrchestratorEvent;
    use crate::testing::{StubDriver, StubDriverFactory};
    use crate::worktree::NoopWorktreeManager;
    use cap_rs::core::{ClientFrame, Content, StopReason};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn spawn_then_route_a_frame_to_a_session() {
        let factory = StubDriverFactory::new()
            .with("w", StubDriver::new("w").text("done").done(StopReason::EndTurn));
        let wt = NoopWorktreeManager::new();
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let mut reg = SessionRegistry::new();

        reg.spawn(
            "w".into(),
            &DriverKind::Claude,
            PermissionPolicy::Allow,
            "main",
            &factory,
            &wt,
            &bus_tx,
            &cancel,
        )
        .await
        .unwrap();

        reg.route("w", ClientFrame::Prompt { content: vec![Content::text("hi")] })
            .await
            .unwrap();

        let mut saw_done = false;
        while let Some(ev) = bus_rx.recv().await {
            if let OrchestratorEvent::SessionDone { session, .. } = ev {
                assert_eq!(session, "w");
                saw_done = true;
                break;
            }
        }
        assert!(saw_done);
        reg.shutdown().await;
    }

    #[tokio::test]
    async fn route_to_unknown_session_errors() {
        let mut reg = SessionRegistry::new();
        let err = reg
            .route("nope", ClientFrame::Prompt { content: vec![] })
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("nope"));
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p cap-rs-orchestrator registry::`
Expected: FAIL — `SessionRegistry` not defined.

- [ ] **Step 4: Implement the registry**

Prepend to `crates/cap-rs-orchestrator/src/registry.rs`:

```rust
//! Owns all live sessions: maps id → inbox sender + task handle.

use std::collections::HashMap;

use cap_rs::core::ClientFrame;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::{DriverKind, PermissionPolicy, SessionId};
use crate::event::OrchestratorEvent;
use crate::factory::DriverFactory;
use crate::session::{spawn_session, SessionHandle};
use crate::worktree::WorktreeManager;
use crate::OrchestratorError;

#[derive(Debug, Default)]
pub struct SessionRegistry {
    sessions: HashMap<SessionId, SessionHandle>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_live(&self, id: &str) -> bool {
        self.sessions.contains_key(id)
    }

    /// Allocate a worktree, build the driver, and spawn the actor.
    #[allow(clippy::too_many_arguments)]
    pub async fn spawn(
        &mut self,
        id: SessionId,
        kind: &DriverKind,
        policy: PermissionPolicy,
        base_branch: &str,
        factory: &dyn DriverFactory,
        worktree: &dyn WorktreeManager,
        bus: &mpsc::Sender<OrchestratorEvent>,
        cancel: &CancellationToken,
    ) -> Result<(), OrchestratorError> {
        let cwd = worktree.create(&id, base_branch)?;
        let driver = factory.build(&id, kind, &cwd, policy).await?;
        let handle = spawn_session(id.clone(), driver, policy, bus.clone(), cancel.clone());
        self.sessions.insert(id, handle);
        Ok(())
    }

    /// Deliver a frame to a session's inbox.
    pub async fn route(&self, to: &str, frame: ClientFrame) -> Result<(), OrchestratorError> {
        let handle = self.sessions.get(to).ok_or_else(|| {
            OrchestratorError::Config(format!("route to unknown/dead session '{to}'"))
        })?;
        handle
            .inbox
            .send(frame)
            .await
            .map_err(|_| OrchestratorError::Config(format!("session '{to}' inbox is closed")))
    }

    /// Drop all inboxes and await every task to finish.
    pub async fn shutdown(&mut self) {
        // Dropping the inbox senders lets each actor's outer loop exit.
        let handles: Vec<_> = self.sessions.drain().map(|(_, h)| h).collect();
        for h in handles {
            drop(h.inbox);
            let _ = h.join.await;
        }
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p cap-rs-orchestrator registry::`
Expected: PASS — both tests.

- [ ] **Step 6: Commit**

```bash
git add crates/cap-rs-orchestrator/
git commit -m "feat(orchestrator): DriverFactory trait + SessionRegistry"
```

---

## Task 7: The audit log

**Files:**
- Create: `crates/cap-rs-orchestrator/src/audit.rs`
- Modify: `crates/cap-rs-orchestrator/src/lib.rs`
- Test: inline `#[cfg(test)]` in `audit.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/cap-rs-orchestrator/src/audit.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_routes_in_order_with_increasing_seq() {
        let mut log = AuditLog::new();
        log.record_route("a", "b");
        log.record_route("b", "c");
        let records = log.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].seq, 0);
        assert_eq!(records[1].seq, 1);
        assert_eq!(records[0].from, "a");
        assert_eq!(records[0].to, "b");
        assert!(records[1].at >= records[0].at);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cap-rs-orchestrator audit::`
Expected: FAIL — `AuditLog` not defined.

- [ ] **Step 3: Implement the audit log**

Prepend to `crates/cap-rs-orchestrator/src/audit.rs`:

```rust
//! Immutable, ordered record of every cross-session route the engine performs.
//! Human-auditable per CAP's "orchestrator-mediated, human-auditable" rule.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::SessionId;

/// One routing event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    /// Monotonic sequence number, starting at 0.
    pub seq: u64,
    /// Milliseconds since the Unix epoch when the route happened.
    pub at: u128,
    pub from: SessionId,
    pub to: SessionId,
}

#[derive(Debug, Default)]
pub struct AuditLog {
    records: Vec<AuditRecord>,
}

impl AuditLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_route(&mut self, from: &str, to: &str) -> &AuditRecord {
        let at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        self.records.push(AuditRecord {
            seq: self.records.len() as u64,
            at,
            from: from.to_string(),
            to: to.to_string(),
        });
        self.records.last().unwrap()
    }

    pub fn records(&self) -> &[AuditRecord] {
        &self.records
    }
}
```

- [ ] **Step 4: Wire the module**

In `crates/cap-rs-orchestrator/src/lib.rs`, add:

```rust
pub mod audit;
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p cap-rs-orchestrator audit::`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/cap-rs-orchestrator/
git commit -m "feat(orchestrator): cross-session route audit log"
```

---

## Task 8: The executor state machine

**Files:**
- Create: `crates/cap-rs-orchestrator/src/executor.rs`
- Modify: `crates/cap-rs-orchestrator/src/lib.rs`
- Test: `crates/cap-rs-orchestrator/tests/patterns.rs` (integration)

The executor ties everything together. It spawns the `start` sessions, listens to the bus, and on each `SessionDone` consults the routes: deliver to the next session (`route_to`), fan out (`fan_out`), or surface a selection (`collect`). Joins fire only when all listed sessions have completed.

- [ ] **Step 1: Write the failing integration test (pipeline)**

Create `crates/cap-rs-orchestrator/tests/patterns.rs`:

```rust
use cap_rs::core::StopReason;
use cap_rs_orchestrator::config::FleetSpec;
use cap_rs_orchestrator::event::OrchestratorEvent;
use cap_rs_orchestrator::executor::Executor;
use cap_rs_orchestrator::testing::{StubDriver, StubDriverFactory};
use cap_rs_orchestrator::worktree::NoopWorktreeManager;

/// Drain the engine to completion, returning the ordered list of
/// (event-tag) strings plus the audit route pairs.
async fn run_to_completion(
    spec: FleetSpec,
    factory: StubDriverFactory,
) -> (Vec<String>, Vec<(String, String)>) {
    let wt = NoopWorktreeManager::new();
    let (mut handle, mut events) = Executor::start(spec, factory, wt, "the task")
        .await
        .expect("executor start");

    let mut tags = Vec::new();
    while let Some(ev) = events.recv().await {
        match &ev {
            OrchestratorEvent::SessionStarted { session } => tags.push(format!("start:{session}")),
            OrchestratorEvent::SessionDone { session, .. } => tags.push(format!("done:{session}")),
            OrchestratorEvent::Routed { from, to } => tags.push(format!("route:{from}->{to}")),
            OrchestratorEvent::AwaitSelection { candidates } => {
                tags.push(format!("select:{}", candidates.join(",")));
            }
            OrchestratorEvent::FleetComplete => {
                tags.push("complete".into());
                break;
            }
            _ => {}
        }
    }
    let audit = handle.audit_pairs().await;
    (tags, audit)
}

#[tokio::test]
async fn pipeline_a_then_b() {
    let spec = FleetSpec::from_yaml(
        r#"
fleet:
  base_branch: main
  sessions:
    coder: { driver: claude, permissions: allow }
    reviewer: { driver: codex, permissions: allow }
  start: coder
  routes:
    - { when: coder.done, route_to: reviewer }
"#,
    )
    .unwrap();
    let factory = StubDriverFactory::new()
        .with("coder", StubDriver::new("coder").text("wrote code").done(StopReason::EndTurn))
        .with("reviewer", StubDriver::new("reviewer").text("looks ok").done(StopReason::EndTurn));

    let (tags, audit) = run_to_completion(spec, factory).await;

    assert_eq!(tags.iter().filter(|t| t.starts_with("done:")).count(), 2);
    let route_pos = tags.iter().position(|t| t == "route:coder->reviewer").unwrap();
    let coder_done = tags.iter().position(|t| t == "done:coder").unwrap();
    let reviewer_done = tags.iter().position(|t| t == "done:reviewer").unwrap();
    assert!(coder_done < route_pos, "route must follow coder done");
    assert!(route_pos < reviewer_done, "reviewer done must follow the route");
    assert!(tags.last().unwrap() == "complete");
    assert_eq!(audit, vec![("coder".to_string(), "reviewer".to_string())]);
}

#[tokio::test]
async fn lead_worker_fan_out_then_join() {
    let spec = FleetSpec::from_yaml(
        r#"
fleet:
  base_branch: main
  sessions:
    lead: { driver: claude, permissions: allow }
    a: { driver: codex, permissions: allow }
    b: { driver: codex, permissions: allow }
    rev: { driver: claude, permissions: allow }
  start: lead
  routes:
    - when: lead.done
      fan_out: { to: [a, b], split: broadcast }
    - when: [a.done, b.done]
      route_to: rev
"#,
    )
    .unwrap();
    let factory = StubDriverFactory::new()
        .with("lead", StubDriver::new("lead").text("plan").done(StopReason::EndTurn))
        .with("a", StubDriver::new("a").text("a-work").done(StopReason::EndTurn))
        .with("b", StubDriver::new("b").text("b-work").done(StopReason::EndTurn))
        .with("rev", StubDriver::new("rev").text("merged").done(StopReason::EndTurn));

    let (tags, audit) = run_to_completion(spec, factory).await;

    // rev must start only after both a and b are done (the join).
    let rev_start = tags.iter().position(|t| t == "start:rev").unwrap();
    let a_done = tags.iter().position(|t| t == "done:a").unwrap();
    let b_done = tags.iter().position(|t| t == "done:b").unwrap();
    assert!(a_done < rev_start && b_done < rev_start, "join must wait for both");
    assert!(audit.contains(&("lead".into(), "a".into())));
    assert!(audit.contains(&("lead".into(), "b".into())));
    assert!(audit.contains(&("a".into(), "rev".into())) || audit.contains(&("b".into(), "rev".into())));
    assert_eq!(tags.last().unwrap(), "complete");
}

#[tokio::test]
async fn parallel_race_collects_for_human() {
    let spec = FleetSpec::from_yaml(
        r#"
fleet:
  base_branch: main
  sessions:
    x: { driver: claude, permissions: allow }
    y: { driver: codex, permissions: allow }
  start: [x, y]
  routes:
    - when: [x.done, y.done]
      collect: human
"#,
    )
    .unwrap();
    let factory = StubDriverFactory::new()
        .with("x", StubDriver::new("x").text("sol-x").done(StopReason::EndTurn))
        .with("y", StubDriver::new("y").text("sol-y").done(StopReason::EndTurn));

    let (tags, _audit) = run_to_completion(spec, factory).await;
    assert!(tags.iter().any(|t| t == "select:x,y"), "tags: {tags:?}");
    assert_eq!(tags.last().unwrap(), "complete");
}

#[tokio::test]
async fn lead_worker_by_subtask_split() {
    let spec = FleetSpec::from_yaml(
        r#"
fleet:
  base_branch: main
  sessions:
    lead: { driver: claude, permissions: allow }
    a: { driver: codex, permissions: allow }
    b: { driver: codex, permissions: allow }
  start: lead
  routes:
    - when: lead.done
      fan_out: { to: [a, b], split: by_subtask }
"#,
    )
    .unwrap();

    // The lead ends its turn with a cap-subtasks block (fence built at runtime
    // so this test source has no literal triple-backticks).
    let fence = "`".repeat(3);
    let lead_out =
        format!("Here is the plan.\n{fence}cap-subtasks\n[\"task for A\", \"task for B\"]\n{fence}\n");
    let factory = StubDriverFactory::new()
        .with("lead", StubDriver::new("lead").text(&lead_out).done(StopReason::EndTurn))
        .with("a", StubDriver::new("a").text("did A").done(StopReason::EndTurn))
        .with("b", StubDriver::new("b").text("did B").done(StopReason::EndTurn));

    let (tags, audit) = run_to_completion(spec, factory).await;

    // The by_subtask split must parse the block and route to both targets.
    assert!(audit.contains(&("lead".into(), "a".into())), "audit: {audit:?}");
    assert!(audit.contains(&("lead".into(), "b".into())), "audit: {audit:?}");
    assert!(tags.iter().any(|t| t == "done:a"));
    assert!(tags.iter().any(|t| t == "done:b"));
    assert_eq!(tags.last().unwrap(), "complete");
    // NOTE: asserting that worker `a` received exactly "task for A" would need a
    // prompt-capturing stub; this test exercises the parse + per-target routing.
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p cap-rs-orchestrator --test patterns`
Expected: FAIL — `Executor` not defined.

- [ ] **Step 3: Implement the executor**

Create `crates/cap-rs-orchestrator/src/executor.rs`:

```rust
//! Deterministic state machine. Owns the registry + audit log; interprets the
//! DSL to drive fan-out, joins, and routing. Runs in its own task; the consumer
//! reads `OrchestratorEvent`s from the returned channel.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use cap_rs::core::{AgentEvent, ClientFrame, Content, PermissionDecision, StopReason, TextChannel};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::audit::AuditLog;
use crate::config::{Action, FleetSpec, PermissionPolicy, SessionId, Split};
use crate::event::{OrchestratorControl, OrchestratorEvent};
use crate::factory::DriverFactory;
use crate::registry::SessionRegistry;
use crate::worktree::WorktreeManager;
use crate::OrchestratorError;

/// A handle to a running fleet: query the audit log, answer asks, cancel.
#[derive(Debug)]
pub struct ExecutorHandle {
    cancel: CancellationToken,
    control: mpsc::Sender<OrchestratorControl>,
    audit: Arc<Mutex<AuditLog>>,
}

impl ExecutorHandle {
    /// Snapshot the audit log as `(from, to)` pairs in order. Readable even
    /// after the fleet completes — the log is shared, not message-passed.
    pub async fn audit_pairs(&mut self) -> Vec<(SessionId, SessionId)> {
        self.audit
            .lock()
            .unwrap()
            .records()
            .iter()
            .map(|r| (r.from.clone(), r.to.clone()))
            .collect()
    }

    /// Answer an [`OrchestratorEvent::Ask`] (only needed under `ask` policy).
    pub async fn decide(&self, session: SessionId, req_id: String, allow: bool) {
        let _ = self
            .control
            .send(OrchestratorControl::Decision { session, req_id, allow })
            .await;
    }

    /// A cloneable control sender — e.g. for a Ctrl-C task to send `Cancel`.
    pub fn control_sender(&self) -> mpsc::Sender<OrchestratorControl> {
        self.control.clone()
    }

    /// Hard-cancel the whole fleet.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

/// Builds the lead prompt content for a session.
fn task_prompt(task: &str) -> cap_rs::core::ClientFrame {
    cap_rs::core::ClientFrame::Prompt {
        content: vec![Content::text(task)],
    }
}

pub struct Executor;

impl Executor {
    /// Start the fleet. Returns a handle plus the outbound event stream.
    pub async fn start<F, W>(
        spec: FleetSpec,
        factory: F,
        worktree: W,
        task: &str,
    ) -> Result<(ExecutorHandle, mpsc::Receiver<OrchestratorEvent>), OrchestratorError>
    where
        F: DriverFactory + 'static,
        W: WorktreeManager + 'static,
    {
        spec.validate()?;

        let (out_tx, out_rx) = mpsc::channel::<OrchestratorEvent>(256);
        let (bus_tx, bus_rx) = mpsc::channel::<OrchestratorEvent>(256);
        let (control_tx, control_rx) = mpsc::channel::<OrchestratorControl>(32);
        let cancel = CancellationToken::new();
        let audit = Arc::new(Mutex::new(AuditLog::new()));

        let handle = ExecutorHandle {
            cancel: cancel.clone(),
            control: control_tx,
            audit: Arc::clone(&audit),
        };

        let task = task.to_string();
        tokio::spawn(async move {
            let mut run = Run {
                spec,
                factory,
                worktree,
                task,
                registry: SessionRegistry::new(),
                audit,
                done: HashSet::new(),
                buffers: HashMap::new(),
                out: out_tx,
                bus_tx,
                cancel,
            };
            run.drive(bus_rx, control_rx).await;
        });

        Ok((handle, out_rx))
    }
}

struct Run<F: DriverFactory, W: WorktreeManager> {
    spec: FleetSpec,
    factory: F,
    worktree: W,
    task: String,
    registry: SessionRegistry,
    audit: Arc<Mutex<AuditLog>>,
    done: HashSet<SessionId>,
    /// Accumulated assistant text per session, used to parse `by_subtask` blocks.
    buffers: HashMap<SessionId, String>,
    out: mpsc::Sender<OrchestratorEvent>,
    bus_tx: mpsc::Sender<OrchestratorEvent>,
    cancel: CancellationToken,
}

impl<F: DriverFactory, W: WorktreeManager> Run<F, W> {
    /// Effective permission policy for a session (per-session override or fleet default).
    fn policy_for(&self, id: &str) -> PermissionPolicy {
        self.spec.fleet.sessions[id]
            .permissions
            .unwrap_or(self.spec.fleet.permissions)
    }

    async fn spawn(&mut self, id: &SessionId) -> bool {
        let kind = self.spec.fleet.sessions[id].driver.clone();
        let policy = self.policy_for(id);
        let base = self.spec.fleet.base_branch.clone();
        match self
            .registry
            .spawn(
                id.clone(),
                &kind,
                policy,
                &base,
                &self.factory,
                &self.worktree,
                &self.bus_tx,
                &self.cancel,
            )
            .await
        {
            Ok(()) => true,
            Err(e) => {
                let _ = self
                    .out
                    .send(OrchestratorEvent::SessionFailed {
                        session: id.clone(),
                        error: e.to_string(),
                    })
                    .await;
                false
            }
        }
    }

    /// Deliver the initial task prompt to a freshly spawned session.
    async fn kick(&self, id: &SessionId) {
        let _ = self.registry.route(id, task_prompt(&self.task)).await;
    }

    async fn drive(
        &mut self,
        mut bus_rx: mpsc::Receiver<OrchestratorEvent>,
        mut control_rx: mpsc::Receiver<OrchestratorControl>,
    ) {
        // Spawn + kick the start sessions.
        for id in self.spec.fleet.start.sessions() {
            if self.spawn(&id).await {
                self.kick(&id).await;
            }
        }

        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => break,
                Some(ctrl) = control_rx.recv() => self.on_control(ctrl).await,
                maybe = bus_rx.recv() => {
                    let Some(ev) = maybe else { break };

                    // Accumulate assistant text so `by_subtask` can parse the lead's output.
                    if let OrchestratorEvent::Agent {
                        session,
                        event: AgentEvent::TextChunk { text, channel: TextChannel::Assistant, .. },
                    } = &ev
                    {
                        self.buffers.entry(session.clone()).or_default().push_str(text);
                    }

                    // Forward every engine event to the consumer.
                    let _ = self.out.send(ev.clone()).await;

                    if let OrchestratorEvent::SessionDone { session, stop_reason } = ev {
                        if self.on_session_done(&session, stop_reason).await {
                            let _ = self.out.send(OrchestratorEvent::FleetComplete).await;
                            break;
                        }
                    }
                }
            }
        }

        self.registry.shutdown().await;
    }

    /// Handle a control message from the consumer (decision / cancel / select).
    async fn on_control(&mut self, ctrl: OrchestratorControl) {
        match ctrl {
            OrchestratorControl::Decision { session, req_id, allow } => {
                let decision = if allow {
                    PermissionDecision::AllowOnce
                } else {
                    PermissionDecision::Deny
                };
                let _ = self
                    .registry
                    .route(&session, ClientFrame::PermissionResponse { req_id, decision })
                    .await;
            }
            OrchestratorControl::Cancel => self.cancel.cancel(),
            // v1: selection is informational; the human merges the chosen worktree.
            OrchestratorControl::Select { .. } => {}
        }
    }

    /// React to a session finishing. Returns `true` when the fleet is complete.
    async fn on_session_done(&mut self, session: &SessionId, _stop: StopReason) -> bool {
        self.done.insert(session.clone());

        // Decide what to fire. A join fires only once — when its last member
        // completes — because every member is in `done` only then.
        enum Fire {
            /// Deliver `frame` into session `to`.
            Route(SessionId, ClientFrame),
            /// Surface a human selection over `candidates`.
            Select(Vec<SessionId>),
            /// The lead's `by_subtask` output was missing or unparseable.
            FailLead(String),
        }

        let mut fires: Vec<Fire> = Vec::new();

        for route in &self.spec.fleet.routes {
            let triggers = route.trigger_sessions();
            if !triggers.iter().any(|t| t == session) {
                continue;
            }
            if !triggers.iter().all(|t| self.done.contains(t)) {
                continue; // join not yet complete
            }
            match route.action().expect("validated") {
                Action::RouteTo(to) => fires.push(Fire::Route(to, task_prompt(&self.task))),
                Action::FanOut(f) => match f.split {
                    Split::Broadcast => {
                        for to in f.to {
                            fires.push(Fire::Route(to, task_prompt(&self.task)));
                        }
                    }
                    Split::BySubtask => {
                        let buf = self.buffers.get(session).cloned().unwrap_or_default();
                        match parse_subtasks(&buf) {
                            Some(items) if !items.is_empty() => {
                                for (i, to) in f.to.iter().enumerate() {
                                    let sub = items[i % items.len()].clone();
                                    fires.push(Fire::Route(to.clone(), task_prompt(&sub)));
                                }
                            }
                            _ => fires.push(Fire::FailLead(
                                "fan_out by_subtask: lead emitted no parseable \
                                 cap-subtasks JSON-array block"
                                    .into(),
                            )),
                        }
                    }
                },
                Action::Collect(_) => fires.push(Fire::Select(triggers.clone())),
            }
        }

        let from = session.clone();
        for fire in fires {
            match fire {
                Fire::Route(to, frame) => {
                    if !self.registry.is_live(&to) && !self.spawn(&to).await {
                        continue;
                    }
                    self.audit.lock().unwrap().record_route(&from, &to);
                    let _ = self
                        .out
                        .send(OrchestratorEvent::Routed {
                            from: from.clone(),
                            to: to.clone(),
                        })
                        .await;
                    let _ = self.registry.route(&to, frame).await;
                }
                Fire::Select(candidates) => {
                    let _ = self
                        .out
                        .send(OrchestratorEvent::AwaitSelection { candidates })
                        .await;
                }
                Fire::FailLead(error) => {
                    let _ = self
                        .out
                        .send(OrchestratorEvent::SessionFailed {
                            session: from.clone(),
                            error,
                        })
                        .await;
                }
            }
        }

        self.fleet_complete()
    }

    /// The fleet is complete when every session that can ever run is done and
    /// no route is still pending.
    fn fleet_complete(&self) -> bool {
        // Sessions that appear as a route target may not have spawned yet.
        let mut reachable: HashSet<SessionId> = self.spec.fleet.start.sessions().into_iter().collect();
        for route in &self.spec.fleet.routes {
            if let Ok(action) = route.action() {
                match action {
                    Action::RouteTo(to) => {
                        reachable.insert(to);
                    }
                    Action::FanOut(f) => reachable.extend(f.to),
                    Action::Collect(_) => {}
                }
            }
        }
        reachable.iter().all(|s| self.done.contains(s))
    }
}

/// Parse a fenced `cap-subtasks` block — a JSON array of strings — out of agent
/// text. The fence is three backticks; the delimiter is built at runtime so
/// this source stays free of literal triple-backticks.
fn parse_subtasks(text: &str) -> Option<Vec<String>> {
    let fence = "`".repeat(3);
    let open = format!("{fence}cap-subtasks");
    let start = text.find(&open)? + open.len();
    let rest = &text[start..];
    let end = rest.find(&fence)?;
    serde_json::from_str::<Vec<String>>(rest[..end].trim()).ok()
}
```

- [ ] **Step 4: Wire the module**

In `crates/cap-rs-orchestrator/src/lib.rs`, add:

```rust
pub mod executor;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p cap-rs-orchestrator --test patterns`
Expected: PASS — all 3 patterns (pipeline, lead/worker, race).

- [ ] **Step 6: Run the full crate test gate**

Run: `cargo test -p cap-rs-orchestrator`
Then: `cargo clippy -p cap-rs-orchestrator --all-targets -- -D warnings`
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add crates/cap-rs-orchestrator/
git commit -m "feat(orchestrator): deterministic executor — pipeline/fan-out/join/collect"
```

---

## Task 9: Real driver factory + `Orchestrator::run` façade

**Files:**
- Create: `crates/cap-rs-orchestrator/src/real_factory.rs`
- Modify: `crates/cap-rs-orchestrator/src/lib.rs`
- Create: `crates/cap-rs-orchestrator/examples/pipeline_smoke.rs`

This task wires real CLI agents. It cannot be unit-tested without the agent binaries + a network, so it is verified via an example smoke run (consistent with `docs/STATUS.md`'s "live smoke via examples" approach).

- [ ] **Step 1: Implement the real factory**

Create `crates/cap-rs-orchestrator/src/real_factory.rs`:

```rust
//! Builds real `cap-rs` drivers. `Bypass` policy passes each agent's native
//! "skip approvals" flag through to the underlying CLI.

use std::path::Path;

use async_trait::async_trait;
use cap_rs::driver::stream_json::ClaudeCodeDriver;
use cap_rs::driver::codex::CodexExecDriver;
use cap_rs::driver::pty::PtyDriver;
use cap_rs::driver::Driver;

use crate::config::{DriverKind, PermissionPolicy, SessionId};
use crate::factory::DriverFactory;
use crate::OrchestratorError;

#[derive(Debug, Default)]
pub struct RealDriverFactory;

#[async_trait]
impl DriverFactory for RealDriverFactory {
    async fn build(
        &self,
        _session: &SessionId,
        kind: &DriverKind,
        cwd: &Path,
        policy: PermissionPolicy,
    ) -> Result<Box<dyn Driver>, OrchestratorError> {
        let bypass = policy == PermissionPolicy::Bypass;
        match kind {
            DriverKind::Claude => {
                let driver = ClaudeCodeDriver::builder(cwd)
                    .dangerously_skip_permissions(bypass)
                    .spawn()
                    .await?;
                Ok(Box::new(driver))
            }
            DriverKind::Codex => {
                let mut b = CodexExecDriver::builder(cwd).skip_git_repo_check(true);
                if bypass {
                    b = b.arg("--dangerously-bypass-approvals-and-sandbox");
                }
                Ok(Box::new(b.spawn().await?))
            }
            DriverKind::Pty(command) => {
                // PTY agents have no permission protocol; they run unsandboxed.
                let driver = PtyDriver::builder(command)
                    .cwd(cwd)
                    .spawn(cap_rs::driver::pty::VtPlain::new())
                    .map_err(OrchestratorError::Driver)?;
                Ok(Box::new(driver))
            }
        }
    }
}
```

> **Note for the implementer:** confirm the PTY parser constructor name against
> `crates/cap-rs/src/driver/pty.rs` (the parser presets there include
> `aider()`, `python_repl()`, `generic_repl()`). Use `generic_repl()` if
> `VtPlain::new()` is not the public name — the `spawn` signature takes any
> `impl AgentParser`. Pick the preset that yields plain-text output for the
> chosen CLI.

- [ ] **Step 2: Add the `Orchestrator::run` façade to lib.rs**

In `crates/cap-rs-orchestrator/src/lib.rs`, add after the module declarations:

```rust
pub mod real_factory;

use crate::config::FleetSpec;
use crate::event::OrchestratorEvent;
use crate::executor::{Executor, ExecutorHandle};
use crate::real_factory::RealDriverFactory;
use crate::worktree::GitWorktreeManager;

/// Convenience façade: run a fleet against real CLI agents in `repo`.
pub async fn run(
    spec: FleetSpec,
    repo: impl AsRef<std::path::Path>,
    task: &str,
) -> Result<
    (ExecutorHandle, tokio::sync::mpsc::Receiver<OrchestratorEvent>),
    OrchestratorError,
> {
    let worktree = GitWorktreeManager::new(repo);
    Executor::start(spec, RealDriverFactory, worktree, task).await
}
```

- [ ] **Step 3: Write a smoke example**

Create `crates/cap-rs-orchestrator/examples/pipeline_smoke.rs`:

```rust
//! Live smoke: runs a two-session pipeline against real agents in the current
//! git repo. Requires the relevant CLI binaries on PATH + valid auth.
//!
//! Run: cargo run -p cap-rs-orchestrator --example pipeline_smoke

use cap_rs_orchestrator::config::FleetSpec;
use cap_rs_orchestrator::event::OrchestratorEvent;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let spec = FleetSpec::from_yaml(
        r#"
fleet:
  base_branch: HEAD
  sessions:
    coder: { driver: claude, permissions: bypass }
  start: coder
"#,
    )?;

    let repo = std::env::current_dir()?;
    let (_handle, mut events) =
        cap_rs_orchestrator::run(spec, repo, "Say hello in one short sentence.").await?;

    while let Some(ev) = events.recv().await {
        match ev {
            OrchestratorEvent::Agent { session, event } => {
                println!("[{session}] {event:?}");
            }
            OrchestratorEvent::FleetComplete => {
                println!("== fleet complete ==");
                break;
            }
            other => println!(":: {other:?}"),
        }
    }
    Ok(())
}
```

Add to `crates/cap-rs-orchestrator/Cargo.toml`:

```toml
[dev-dependencies]
anyhow = "1"
```

- [ ] **Step 4: Verify it compiles (build, don't run)**

Run: `cargo build -p cap-rs-orchestrator --examples`
Expected: compiles. (Adjust the PTY parser name per the Step 1 note if the build complains.)

- [ ] **Step 5: Commit**

```bash
git add crates/cap-rs-orchestrator/
git commit -m "feat(orchestrator): real driver factory (bypass passthrough) + run() façade"
```

---

## Task 10: `cap run <fleet.yaml>` CLI command

**Files:**
- Modify: `crates/cap-cli/src/main.rs`
- Modify: `crates/cap-cli/Cargo.toml`

This adds the local driver for the engine: `cap run fleet.yaml [--task "..."] [--bypass]`. It prints the event stream and answers `ask` prompts on stdin.

- [ ] **Step 1: Inspect the existing CLI structure**

Read `crates/cap-cli/src/main.rs` and `crates/cap-cli/Cargo.toml` to learn the existing argument-parsing approach (clap vs hand-rolled) and follow it. The steps below assume `clap` with a subcommand enum; adapt to the existing pattern if it differs.

- [ ] **Step 2: Add dependencies**

In `crates/cap-cli/Cargo.toml`, add to `[dependencies]`:

```toml
cap-rs-orchestrator = { path = "../cap-rs-orchestrator" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "io-std", "io-util"] }
anyhow = "1"
```

- [ ] **Step 3: Add the `run` subcommand handler**

Add this function to `crates/cap-cli/src/main.rs` and wire it to a `Run` subcommand carrying `path: PathBuf`, `task: Option<String>`, and `bypass: bool` flags:

```rust
use std::path::PathBuf;

use cap_rs_orchestrator::config::FleetSpec;
use cap_rs_orchestrator::event::OrchestratorEvent;
use tokio::io::{AsyncBufReadExt, BufReader};

pub async fn cmd_run(path: PathBuf, task: Option<String>, bypass: bool) -> anyhow::Result<()> {
    let yaml = std::fs::read_to_string(&path)?;
    let mut spec = FleetSpec::from_yaml(&yaml).map_err(|e| anyhow::anyhow!("{e}"))?;
    if bypass {
        spec.fleet.permissions = cap_rs_orchestrator::config::PermissionPolicy::Bypass;
    }
    spec.validate().map_err(|e| anyhow::anyhow!("{e}"))?;

    let effective_task = task
        .or_else(|| spec.fleet.task.clone())
        .ok_or_else(|| anyhow::anyhow!("no task: pass --task or set fleet.task"))?;

    let repo = std::env::current_dir()?;
    let (handle, mut events) =
        cap_rs_orchestrator::run(spec, repo, &effective_task).await.map_err(|e| anyhow::anyhow!("{e}"))?;

    // Cancel the fleet on Ctrl-C via a cloned control sender (keep `handle`
    // here so the main loop can answer `ask` prompts with `handle.decide`).
    let control = handle.control_sender();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("\n^C — cancelling fleet…");
            let _ = control.send(cap_rs_orchestrator::event::OrchestratorControl::Cancel).await;
        }
    });

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();

    while let Some(ev) = events.recv().await {
        match ev {
            OrchestratorEvent::SessionStarted { session } => println!("▶ {session} started"),
            OrchestratorEvent::Agent { session, event } => println!("[{session}] {event:?}"),
            OrchestratorEvent::Routed { from, to } => println!("→ routed {from} → {to}"),
            OrchestratorEvent::SessionDone { session, stop_reason } => {
                println!("✓ {session} done ({stop_reason:?})")
            }
            OrchestratorEvent::SessionFailed { session, error } => {
                println!("✗ {session} failed: {error}")
            }
            OrchestratorEvent::Ask { session, req_id, tool, risk_level } => {
                println!("⚠ {session} wants to use {tool} (risk: {risk_level:?}) — allow? [y/N]");
                let line = stdin.next_line().await?.unwrap_or_default();
                let allow = matches!(line.trim(), "y" | "Y" | "yes");
                handle.decide(session, req_id, allow).await;
            }
            OrchestratorEvent::AwaitSelection { candidates } => {
                println!("⊙ pick a winner among: {}", candidates.join(", "));
            }
            OrchestratorEvent::FleetComplete => {
                println!("== fleet complete ==");
                break;
            }
        }
    }
    Ok(())
}
```

> **Note for the implementer:** the `Ask` round-trip is fully wired —
> `handle.decide(session, req_id, allow)` sends an `OrchestratorControl::Decision`
> that the executor's `on_control` turns into a `ClientFrame::PermissionResponse`
> routed to the session inbox (which the session actor is blocked awaiting under
> `ask` policy). `allow`/`deny`/`bypass` policies need no human round-trip.

- [ ] **Step 4: Verify it builds**

Run: `cargo build -p cap-cli`
Expected: compiles.

- [ ] **Step 5: Smoke-test the happy path with a non-interactive fleet**

Create a scratch `fleet.yaml` with `permissions: allow` and a single `pty:` agent that exits quickly (e.g. `pty:echo` is not a REPL — instead point at a real installed CLI), or run against `claude` with `--bypass`. Then:

Run: `cargo run -p cap-cli -- run fleet.yaml --task "hello"`
Expected: prints session lifecycle ending in `== fleet complete ==`.

- [ ] **Step 6: Commit**

```bash
git add crates/cap-cli/
git commit -m "feat(cli): cap run <fleet.yaml> — local orchestrator driver"
```

---

## Final verification

- [ ] **Run the whole workspace test gate** (mirrors `.github/workflows/ci.yml`):

```bash
cargo test --all-features
cargo clippy --all-features --all-targets -- -D warnings
cargo fmt --all -- --check
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
```

Expected: all green. The orchestrator's deterministic test gate (config, session,
worktree, registry, audit, executor patterns) passes with zero real LLM/network.

- [ ] **Update `docs/STATUS.md`** with a short note that the orchestrator engine
  (sub-project 1) has landed, what works (the four patterns against stubs, plus
  the interactive `ask` round-trip), and what's deferred (the remote/mobile layers).

---

## Notes on what is deliberately deferred (from the spec §8)

- **Dynamic peer-messaging topology** — not in the DSL; v1 routes are declarative.
- **Auto-merge of worktrees** — the engine surfaces branch names + diffs only.
- **`on_error: abort | continue`** — current behavior: a failed session blocks its
  downstream routes but siblings continue; no config knob yet.
- **Budget aggregation** — `Usage` flows through `AgentEvent`; summing is a later add.
- **`collect: human` selection effect** — v1 surfaces `AwaitSelection` and records
  the pick (`OrchestratorControl::Select`) but does not act on it; the human merges
  the chosen worktree manually.
- **Sub-projects 2–5** — remote transport, push, mobile app, tunnel.
