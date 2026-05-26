//! Codex CLI driver — `codex exec --json` mode.
//!
//! Wire format: line-delimited JSON over codex's stdout. Schema is
//! tagged-union by `type` field, matching codex-rs's [`exec_events.rs`].
//! Events fall into two families:
//!
//! - **Thread/turn lifecycle**: `thread.started`, `turn.started`,
//!   `turn.completed` (with usage), `turn.failed`, `error`.
//! - **Items**: `item.started`, `item.updated`, `item.completed`, each
//!   carrying a `ThreadItem` (codex-rs side) whose `type` discriminates
//!   among `agent_message`, `reasoning`, `command_execution`,
//!   `file_change`, `mcp_tool_call`, `collab_tool_call`, `web_search`,
//!   `todo_list`, `error`.
//!
//! Codex `exec` is one-shot per process — for multi-turn conversations
//! the same `thread_id` is reused across spawns (`codex exec resume
//! <thread_id> "next prompt"`). The driver exposes `.thread_id()` once
//! the [`AgentEvent::Ready`] frame arrives, and a builder
//! `.resume(<id>)` accepts a thread_id for the next process.
//!
//! [`exec_events.rs`]: https://github.com/openai/codex/blob/main/codex-rs/exec/src/exec_events.rs

use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, mpsc};
use tracing::{debug, trace, warn};

use crate::core::{AgentEvent, ClientFrame, StopReason, TextChannel, Usage};
use crate::driver::{Driver, DriverError, DriverExitStatus};

/// Driver for OpenAI's `codex` CLI, using its `exec --json` mode.
pub struct CodexExecDriver {
    /// Receiver of events parsed from codex stdout.
    reader_rx: mpsc::Receiver<AgentEvent>,

    /// Last observed thread_id (capturable via [`Self::thread_id`]).
    thread_id: std::sync::Arc<Mutex<Option<String>>>,

    /// Child handle for shutdown.
    child: Option<Child>,

    exited: std::sync::Arc<std::sync::atomic::AtomicBool>,
    exit_status: std::sync::Arc<std::sync::Mutex<Option<DriverExitStatus>>>,
}

impl std::fmt::Debug for CodexExecDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodexExecDriver").finish_non_exhaustive()
    }
}

impl CodexExecDriver {
    /// Convenience: spawn one-shot with a prompt, default options.
    pub async fn spawn(
        cwd: impl AsRef<Path>,
        prompt: impl Into<String>,
    ) -> Result<Self, DriverError> {
        Self::builder(cwd).prompt(prompt).spawn().await
    }

    pub fn builder(cwd: impl AsRef<Path>) -> CodexExecBuilder {
        CodexExecBuilder {
            bin: None,
            cwd: cwd.as_ref().to_path_buf(),
            model: None,
            prompt: None,
            resume_thread: None,
            skip_git_repo_check: true,
            extra_args: Vec::new(),
            configs: Vec::new(),
        }
    }

    /// Returns the thread_id reported by codex once `Ready` has fired.
    /// `None` if codex hasn't yet emitted `thread.started`.
    pub async fn thread_id(&self) -> Option<String> {
        self.thread_id.lock().await.clone()
    }
}

#[async_trait]
impl Driver for CodexExecDriver {
    async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError> {
        // codex exec is one-shot — the only input is the prompt passed at
        // spawn-time. Multi-turn means a fresh spawn with .resume(thread_id).
        if matches!(frame, ClientFrame::SessionConfig(_)) {
            return Ok(());
        }
        let code = match &frame {
            ClientFrame::Cancel { .. } => "cap_cancel_unsupported",
            _ => "cap_queued_input_unsupported",
        };
        Err(DriverError::AgentError {
            code: code.into(),
            message: "codex exec is one-shot per process; use builder().resume(thread_id) for the next turn"
                .into(),
        })
    }

    async fn next_event(&mut self) -> Option<AgentEvent> {
        self.reader_rx.recv().await
    }

    async fn shutdown(&mut self) -> Result<(), DriverError> {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let waited = child.wait().await;
            let mut slot = self.exit_status.lock().expect("exit_status mutex poisoned");
            if slot.is_none() {
                *slot = Some(match waited {
                    Ok(s) => match s.code() {
                        Some(code) => DriverExitStatus::Exited { code: Some(code) },
                        None => DriverExitStatus::Killed,
                    },
                    Err(_) => DriverExitStatus::Disconnected,
                });
            }
        }
        self.exited
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn is_alive(&self) -> bool {
        !self.exited.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn exit_status(&self) -> Option<DriverExitStatus> {
        self.exit_status.lock().ok().and_then(|g| g.clone())
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CodexExecBuilder {
    bin: Option<String>,
    cwd: PathBuf,
    model: Option<String>,
    prompt: Option<String>,
    resume_thread: Option<String>,
    skip_git_repo_check: bool,
    extra_args: Vec<String>,
    configs: Vec<(String, String)>,
}

impl CodexExecBuilder {
    /// Override the binary used (default: `codex` on PATH, or `$CODEX_BIN`).
    pub fn bin(mut self, b: impl Into<String>) -> Self {
        self.bin = Some(b.into());
        self
    }

    /// Set the model (passed as `-m <model>`).
    pub fn model(mut self, m: impl Into<String>) -> Self {
        self.model = Some(m.into());
        self
    }

    /// The user prompt for this turn (passed as the positional argument).
    pub fn prompt(mut self, p: impl Into<String>) -> Self {
        self.prompt = Some(p.into());
        self
    }

    /// Resume a previous thread by `thread_id`. Causes `codex exec resume <id>`
    /// to be invoked instead of a fresh exec.
    pub fn resume(mut self, thread_id: impl Into<String>) -> Self {
        self.resume_thread = Some(thread_id.into());
        self
    }

    /// Whether to pass `--skip-git-repo-check` (default `true`). codex
    /// refuses to run in a non-git workspace by default; CAP usage often
    /// happens in arbitrary dirs.
    pub fn skip_git_repo_check(mut self, on: bool) -> Self {
        self.skip_git_repo_check = on;
        self
    }

    /// Append an arbitrary CLI argument (escape hatch).
    pub fn arg(mut self, a: impl Into<String>) -> Self {
        self.extra_args.push(a.into());
        self
    }

    /// Set a TOML config override (passed as `-c key=value`).
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.push((key.into(), value.into()));
        self
    }

    /// Spawn codex.
    pub async fn spawn(self) -> Result<CodexExecDriver, DriverError> {
        let bin = self
            .bin
            .clone()
            .or_else(|| std::env::var("CODEX_BIN").ok())
            .unwrap_or_else(|| "codex".to_string());

        let mut cmd = Command::new(&bin);
        cmd.arg("exec");
        if let Some(t) = &self.resume_thread {
            cmd.arg("resume").arg(t);
        }
        cmd.arg("--json");
        if self.skip_git_repo_check {
            cmd.arg("--skip-git-repo-check");
        }
        for (k, v) in &self.configs {
            cmd.arg("-c").arg(format!("{k}={v}"));
        }
        if let Some(m) = &self.model {
            cmd.arg("-m").arg(m);
        }
        for a in &self.extra_args {
            cmd.arg(a);
        }
        if let Some(p) = &self.prompt {
            cmd.arg(p);
        }

        cmd.current_dir(&self.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        // codex inherits everything by default — minimal env hygiene.
        for var in ["CODEX_HEADLESS", "CODEX_INTERACTIVE"] {
            cmd.env_remove(var);
        }

        debug!(bin = %bin, cwd = %self.cwd.display(), resume = ?self.resume_thread, "spawning codex exec");

        let mut child = cmd.spawn().map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                DriverError::BinaryNotFound(bin.clone())
            } else {
                DriverError::SpawnFailed(e)
            }
        })?;

        // codex doesn't read stdin in exec mode (prompt is on argv),
        // but we close it cleanly to avoid the child blocking on it.
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.shutdown().await;
            drop(stdin);
        }

        let stdout = child.stdout.take().ok_or(DriverError::AgentExited)?;
        let stderr = child.stderr.take().ok_or(DriverError::AgentExited)?;

        let (reader_tx, reader_rx) = mpsc::channel::<AgentEvent>(64);
        let thread_id = std::sync::Arc::new(Mutex::new(None));
        let exited = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let exit_status = std::sync::Arc::new(std::sync::Mutex::new(None));

        tokio::spawn(reader_task(
            stdout,
            reader_tx.clone(),
            std::sync::Arc::clone(&thread_id),
            std::sync::Arc::clone(&exited),
        ));
        tokio::spawn(stderr_drain(stderr));

        Ok(CodexExecDriver {
            reader_rx,
            thread_id,
            child: Some(child),
            exited,
            exit_status,
        })
    }
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

async fn reader_task(
    stdout: tokio::process::ChildStdout,
    tx: mpsc::Sender<AgentEvent>,
    thread_id: std::sync::Arc<Mutex<Option<String>>>,
    exited: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    let mut lines = BufReader::new(stdout).lines();
    // Per-item delta tracking for streaming agent_message text.
    let mut last_text_for: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                trace!(line = %line, "← codex");
                let frame: Value = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(_) => {
                        // codex interleaves tracing logs in plain text on stdout
                        // under some conditions — skip non-JSON lines.
                        continue;
                    }
                };
                for ev in parse_codex_frame(&frame, &mut last_text_for, &thread_id).await {
                    if tx.send(ev).await.is_err() {
                        exited.store(true, std::sync::atomic::Ordering::Relaxed);
                        return;
                    }
                }
            }
            Ok(None) => {
                debug!("codex reader: stdout EOF");
                exited.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }
            Err(e) => {
                warn!(error = %e, "codex reader: read error");
                exited.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }
        }
    }
}

async fn stderr_drain(stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => debug!(target: "cap_rs::codex::stderr", "{}", line),
            Ok(None) => return,
            Err(e) => {
                warn!(error = %e, "stderr read error");
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Codex schema → CAP AgentEvent mapping
// ---------------------------------------------------------------------------

async fn parse_codex_frame(
    frame: &Value,
    last_text_for: &mut std::collections::HashMap<String, String>,
    thread_id: &std::sync::Arc<Mutex<Option<String>>>,
) -> Vec<AgentEvent> {
    let kind = frame.get("type").and_then(Value::as_str).unwrap_or("");
    match kind {
        "thread.started" => {
            let tid = frame
                .get("thread_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            *thread_id.lock().await = Some(tid.clone());
            vec![AgentEvent::Ready {
                session_id: tid,
                version: crate::core::CAP_PROTOCOL_VERSION.into(),
                model: None,
            }]
        }

        "turn.started" => Vec::new(), // no equivalent in CAP — implicit

        "turn.completed" => {
            let usage = parse_codex_usage(frame.get("usage"));
            vec![AgentEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage,
            }]
        }

        "turn.failed" => {
            let msg = frame
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            vec![
                AgentEvent::Error {
                    code: "codex_turn_failed".into(),
                    message: msg,
                    retryable: false,
                    details: None,
                },
                AgentEvent::Done {
                    stop_reason: StopReason::Error,
                    usage: Usage::default(),
                },
            ]
        }

        "error" => {
            let msg = frame
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            vec![AgentEvent::Error {
                code: "codex_error".into(),
                message: msg,
                retryable: false,
                details: None,
            }]
        }

        "item.started" | "item.updated" | "item.completed" => {
            let item = match frame.get("item") {
                Some(i) => i,
                None => return Vec::new(),
            };
            let item_id = item
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
            let cleanup_id = if kind == "item.completed"
                && (item_type == "agent_message" || item_type == "reasoning")
            {
                Some(item_id.clone())
            } else {
                None
            };

            let events = match item_type {
                "agent_message" => {
                    let text = item
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let prev = last_text_for.get(&item_id).cloned().unwrap_or_default();
                    let delta = if text.starts_with(&prev) {
                        text[prev.len()..].to_string()
                    } else {
                        // Item text reset (rare); emit the full text.
                        text.clone()
                    };
                    last_text_for.insert(item_id.clone(), text);
                    if delta.is_empty() {
                        return Vec::new();
                    }
                    vec![AgentEvent::TextChunk {
                        msg_id: item_id,
                        text: delta,
                        channel: TextChannel::Assistant,
                    }]
                }

                "reasoning" => {
                    let text = item
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let prev = last_text_for.get(&item_id).cloned().unwrap_or_default();
                    let delta = if text.starts_with(&prev) {
                        text[prev.len()..].to_string()
                    } else {
                        text.clone()
                    };
                    last_text_for.insert(item_id.clone(), text);
                    if delta.is_empty() {
                        return Vec::new();
                    }
                    vec![AgentEvent::Thought {
                        msg_id: item_id,
                        text: delta,
                    }]
                }

                "command_execution" => {
                    if kind == "item.started" {
                        let cmd = item.get("command").cloned().unwrap_or(Value::Null);
                        vec![AgentEvent::ToolCallStart {
                            call_id: item_id,
                            name: "Bash".into(),
                            input: cmd,
                        }]
                    } else if kind == "item.completed" {
                        let output = item
                            .get("output")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let status = item.get("status").and_then(Value::as_str).unwrap_or("");
                        let is_error = matches!(status, "failed" | "error" | "cancelled");
                        vec![AgentEvent::ToolCallEnd {
                            call_id: item_id,
                            output,
                            is_error,
                            duration: item
                                .get("duration_ms")
                                .or_else(|| item.get("durationMs"))
                                .and_then(Value::as_u64)
                                .map(std::time::Duration::from_millis),
                        }]
                    } else {
                        Vec::new()
                    }
                }

                "mcp_tool_call" | "collab_tool_call" => {
                    if kind == "item.started" {
                        let name = item
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or(item_type)
                            .to_string();
                        let input = item.get("arguments").cloned().unwrap_or(Value::Null);
                        vec![AgentEvent::ToolCallStart {
                            call_id: item_id,
                            name,
                            input,
                        }]
                    } else if kind == "item.completed" {
                        let output = item
                            .get("result")
                            .map(|v| {
                                v.as_str()
                                    .map(String::from)
                                    .unwrap_or_else(|| v.to_string())
                            })
                            .unwrap_or_default();
                        vec![AgentEvent::ToolCallEnd {
                            call_id: item_id,
                            output,
                            is_error: false,
                            duration: item
                                .get("duration_ms")
                                .or_else(|| item.get("durationMs"))
                                .and_then(Value::as_u64)
                                .map(std::time::Duration::from_millis),
                        }]
                    } else {
                        Vec::new()
                    }
                }

                "todo_list" => {
                    // Convert codex's todo list into a CAP plan.
                    let entries: Vec<crate::core::PlanEntry> = item
                        .get("items")
                        .and_then(Value::as_array)
                        .map(|arr| {
                            arr.iter()
                                .enumerate()
                                .map(|(i, e)| codex_todo_to_plan_entry(i, e))
                                .collect()
                        })
                        .unwrap_or_default();
                    if entries.is_empty() {
                        Vec::new()
                    } else {
                        vec![AgentEvent::Plan { entries }]
                    }
                }

                "error" => {
                    let msg = item
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    vec![AgentEvent::Error {
                        code: "codex_item_error".into(),
                        message: msg,
                        retryable: false,
                        details: None,
                    }]
                }

                _ => {
                    trace!(item_type, "unknown codex item type, skipping");
                    Vec::new()
                }
            };

            // Release per-item delta state once codex confirms the item is
            // done — long sessions otherwise accumulate every agent_message
            // and reasoning blob in memory until process exit.
            if let Some(id) = cleanup_id {
                last_text_for.remove(&id);
            }

            events
        }

        other => {
            trace!(frame_type = other, "unknown codex frame type, skipping");
            Vec::new()
        }
    }
}

fn codex_todo_to_plan_entry(idx: usize, entry: &Value) -> crate::core::PlanEntry {
    use crate::core::{PlanEntry, PlanStatus};
    let text = entry
        .get("text")
        .or_else(|| entry.get("content"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let status = match entry.get("status").and_then(Value::as_str) {
        Some("pending") | Some("todo") | None => PlanStatus::Pending,
        Some("in_progress") | Some("active") => PlanStatus::InProgress,
        Some("completed") | Some("done") => PlanStatus::Completed,
        Some("cancelled") => PlanStatus::Cancelled,
        Some(_) => PlanStatus::Blocked,
    };
    PlanEntry {
        id: entry
            .get("id")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| format!("t{idx}")),
        content: text,
        status,
        priority: None,
        _meta: None,
    }
}

fn parse_codex_usage(usage: Option<&Value>) -> Usage {
    let u = usage.cloned().unwrap_or(Value::Null);
    Usage {
        input_tokens: u.get("input_tokens").and_then(Value::as_u64).unwrap_or(0),
        output_tokens: u.get("output_tokens").and_then(Value::as_u64).unwrap_or(0),
        cache_read_tokens: u
            .get("cached_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_creation_tokens: 0,
        thinking_tokens: u
            .get("reasoning_output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cost_usd_estimate: None,
        duration: None,
        model_id: None,
        stop_reason: Some(StopReason::EndTurn),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parse_thread_started() {
        let v: Value = serde_json::from_str(
            r#"{"type":"thread.started","thread_id":"019e3ac0-dc0e-7f12-81b6-9127bbdca87f"}"#,
        )
        .unwrap();
        let tid = std::sync::Arc::new(Mutex::new(None));
        let mut map = std::collections::HashMap::new();
        let events = parse_codex_frame(&v, &mut map, &tid).await;
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::Ready { session_id, .. } => {
                assert_eq!(session_id, "019e3ac0-dc0e-7f12-81b6-9127bbdca87f");
            }
            other => panic!("wrong: {other:?}"),
        }
        assert_eq!(
            tid.lock().await.as_deref(),
            Some("019e3ac0-dc0e-7f12-81b6-9127bbdca87f")
        );
    }

    #[tokio::test]
    async fn parse_agent_message_delta() {
        let mut map = std::collections::HashMap::new();
        let tid = std::sync::Arc::new(Mutex::new(None));

        let v1: Value = serde_json::from_str(
            r#"{"type":"item.started","item":{"id":"i1","type":"agent_message","text":""}}"#,
        )
        .unwrap();
        let _ = parse_codex_frame(&v1, &mut map, &tid).await;

        let v2: Value = serde_json::from_str(
            r#"{"type":"item.updated","item":{"id":"i1","type":"agent_message","text":"hello"}}"#,
        )
        .unwrap();
        let evs = parse_codex_frame(&v2, &mut map, &tid).await;
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            AgentEvent::TextChunk { text, .. } => assert_eq!(text, "hello"),
            o => panic!("wrong: {o:?}"),
        }

        let v3: Value = serde_json::from_str(
            r#"{"type":"item.updated","item":{"id":"i1","type":"agent_message","text":"hello world"}}"#,
        )
        .unwrap();
        let evs = parse_codex_frame(&v3, &mut map, &tid).await;
        match &evs[0] {
            AgentEvent::TextChunk { text, .. } => assert_eq!(text, " world"),
            o => panic!("wrong: {o:?}"),
        }
    }

    #[tokio::test]
    async fn parse_turn_completed_with_usage() {
        let v: Value = serde_json::from_str(
            r#"{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":20,"cached_input_tokens":5,"reasoning_output_tokens":3}}"#,
        )
        .unwrap();
        let mut map = std::collections::HashMap::new();
        let tid = std::sync::Arc::new(Mutex::new(None));
        let evs = parse_codex_frame(&v, &mut map, &tid).await;
        match &evs[0] {
            AgentEvent::Done { usage, .. } => {
                assert_eq!(usage.input_tokens, 10);
                assert_eq!(usage.output_tokens, 20);
                assert_eq!(usage.cache_read_tokens, 5);
            }
            o => panic!("wrong: {o:?}"),
        }
    }

    #[tokio::test]
    async fn parse_command_execution_tool_call() {
        let mut map = std::collections::HashMap::new();
        let tid = std::sync::Arc::new(Mutex::new(None));
        let v: Value = serde_json::from_str(
            r#"{"type":"item.started","item":{"id":"c1","type":"command_execution","command":["ls","-la"]}}"#,
        )
        .unwrap();
        let evs = parse_codex_frame(&v, &mut map, &tid).await;
        match &evs[0] {
            AgentEvent::ToolCallStart { name, .. } => assert_eq!(name, "Bash"),
            o => panic!("wrong: {o:?}"),
        }
    }

    #[tokio::test]
    async fn parse_error_event() {
        let v: Value =
            serde_json::from_str(r#"{"type":"error","message":"connection lost"}"#).unwrap();
        let mut map = std::collections::HashMap::new();
        let tid = std::sync::Arc::new(Mutex::new(None));
        let evs = parse_codex_frame(&v, &mut map, &tid).await;
        match &evs[0] {
            AgentEvent::Error { message, .. } => assert_eq!(message, "connection lost"),
            o => panic!("wrong: {o:?}"),
        }
    }
}
