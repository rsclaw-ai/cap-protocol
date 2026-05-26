//! Codex MCP driver — `codex mcp-server` over stdio JSON-RPC (MCP).
//!
//! codex now ships an MCP server (stdio JSON-RPC 2.0). It exposes a `codex`
//! tool whose `tools/call` response carries `structuredContent: {threadId,
//! content}` — the clean final assistant message. Crucially the SAME server
//! also emits codex's rich streaming event vocabulary as **`codex/event`**
//! notifications during the turn: `agent_message_content_delta`,
//! `item_started`/`item_completed` (Reasoning, AgentMessage, command/MCP tool
//! calls), `task_started`/`task_complete`, `token_count`. So this is a fully
//! structured replacement for the PTY screen-scraping path — no chrome, no
//! scrollback noise, no idle-marker heuristics.
//!
//! Wire format verified against real `codex mcp-server` v0.133:
//! - MCP handshake: `initialize` → `notifications/initialized` (no response).
//! - Turn:  `tools/call { name: "codex", arguments: { prompt, cwd, ... } }`.
//!   The response arrives when the turn ends, with `structuredContent`.
//! - Streaming: `codex/event` notifications with an inner `params.msg.type`
//!   discriminator carrying codex's event payload.
//!
//! ## Event mapping
//!
//! | codex/event msg.type | CAP event |
//! |---|---|
//! | `session_configured` | (capture thread/session id) |
//! | `agent_message_content_delta` | [`AgentEvent::TextChunk`] |
//! | `item_started` (Reasoning) | [`AgentEvent::Thought`] (placeholder) |
//! | `item_started` (CommandExecution/FunctionCall/McpToolCall) | [`AgentEvent::ToolCallStart`] |
//! | `item_completed` (Command/Function/Mcp tool) | [`AgentEvent::ToolCallEnd`] |
//! | `token_count` | [`AgentEvent::Usage`] |
//! | `stream_error` / `warning` | logged (transient transport flap, not surfaced) |
//! | `tools/call` **response** | [`AgentEvent::Done`] with the final `content` |
//!
//! The old `app-server` upstream-WebSocket blocker is still in the wild
//! (`stream_error: Reconnecting...`) but codex now auto-falls-back to HTTPS;
//! the driver just consumes the warnings and lets codex retry transparently.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace, warn};

use crate::core::{AgentEvent, ClientFrame, Content, StopReason, TextChannel, Usage};
use crate::driver::{Driver, DriverError, DriverExitStatus};

const JSONRPC_VERSION: &str = "2.0";
const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Driver for `codex mcp-server` (stdio MCP).
pub struct CodexMcpDriver {
    writer_tx: Option<mpsc::Sender<String>>,
    reader_rx: mpsc::Receiver<AgentEvent>,
    child: Option<Child>,

    thread_id: Arc<Mutex<Option<String>>>,
    next_id: Arc<AtomicU64>,
    last_tool_call_id: Arc<AtomicU64>,

    /// Codex sandbox + approval policy (set at construction; applied on each
    /// tools/call). Kept here so the driver can issue the call later.
    sandbox: String,
    approval_policy: String,
    cwd: PathBuf,
    model: Option<String>,

    exited: Arc<AtomicBool>,
    exit_status: Arc<Mutex<Option<DriverExitStatus>>>,
}

impl std::fmt::Debug for CodexMcpDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodexMcpDriver")
            .field("thread_id", &self.thread_id())
            .field("alive", &self.is_alive())
            .finish()
    }
}

impl CodexMcpDriver {
    /// Spawn `codex mcp-server` in `cwd` with sensible defaults
    /// (approval-policy = `never`, sandbox = `workspace-write`).
    pub async fn spawn(cwd: impl AsRef<Path>) -> Result<Self, DriverError> {
        Self::builder(cwd).spawn().await
    }

    pub fn builder(cwd: impl AsRef<Path>) -> CodexMcpBuilder {
        CodexMcpBuilder {
            bin: None,
            cwd: cwd.as_ref().to_path_buf(),
            model: None,
            approval_policy: "never".into(),
            sandbox: "workspace-write".into(),
        }
    }

    /// Codex's thread id (captured from the first `session_configured` event).
    pub fn thread_id(&self) -> Option<String> {
        self.thread_id.lock().ok().and_then(|g| g.clone())
    }
}

#[async_trait]
impl Driver for CodexMcpDriver {
    async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError> {
        let tx = self.writer_tx.as_ref().ok_or(DriverError::AgentExited)?;

        match frame {
            ClientFrame::SessionConfig(_) => Err(DriverError::AgentError {
                code: "cap_session_config_inline_unsupported".into(),
                message: "Codex MCP consumes session config at spawn — re-spawn to change it"
                    .into(),
            }),

            ClientFrame::Prompt { content } => {
                let prompt_text = content
                    .iter()
                    .filter_map(|c| match c {
                        Content::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("");
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                self.last_tool_call_id.store(id, Ordering::Relaxed);
                let mut args = serde_json::Map::new();
                args.insert("prompt".into(), json!(prompt_text));
                args.insert("cwd".into(), json!(self.cwd.display().to_string()));
                args.insert("approval-policy".into(), json!(self.approval_policy));
                args.insert("sandbox".into(), json!(self.sandbox));
                if let Some(m) = &self.model {
                    args.insert("model".into(), json!(m));
                }
                let req = json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": id,
                    "method": "tools/call",
                    "params": { "name": "codex", "arguments": Value::Object(args) }
                });
                tx.send(req.to_string())
                    .await
                    .map_err(|_| DriverError::AgentExited)
            }

            ClientFrame::Cancel { .. } => {
                // MCP cancellation notification. Codex MCP may or may not honor
                // it mid-turn; spec-compliant servers can stop the in-flight
                // request. Fire-and-forget.
                let note = json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "method": "notifications/cancelled",
                    "params": { "requestId": self.last_tool_call_id.load(Ordering::Relaxed) }
                });
                tx.send(note.to_string())
                    .await
                    .map_err(|_| DriverError::AgentExited)
            }

            ClientFrame::PermissionResponse { .. }
            | ClientFrame::AskUserAnswer { .. }
            | ClientFrame::ReverseRpcResult { .. } => {
                // codex MCP runs autonomously per approval-policy/sandbox; it
                // does not surface mid-turn approval requests back to the
                // client today, so these frames have no transport.
                Err(DriverError::AgentError {
                    code: "cap_codex_mcp_no_approval_callback".into(),
                    message:
                        "codex mcp-server does not request approvals back to the client; configure \
                         approval-policy/sandbox at spawn instead"
                            .into(),
                })
            }
        }
    }

    async fn next_event(&mut self) -> Option<AgentEvent> {
        self.reader_rx.recv().await
    }

    async fn shutdown(&mut self) -> Result<(), DriverError> {
        self.writer_tx = None;
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
        self.exited.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn is_alive(&self) -> bool {
        !self.exited.load(Ordering::Relaxed)
    }

    fn exit_status(&self) -> Option<DriverExitStatus> {
        self.exit_status.lock().ok().and_then(|g| g.clone())
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CodexMcpBuilder {
    bin: Option<String>,
    cwd: PathBuf,
    model: Option<String>,
    approval_policy: String,
    sandbox: String,
}

impl CodexMcpBuilder {
    pub fn bin(mut self, b: impl Into<String>) -> Self {
        self.bin = Some(b.into());
        self
    }
    pub fn model(mut self, m: impl Into<String>) -> Self {
        self.model = Some(m.into());
        self
    }
    /// Codex `approval-policy`: `untrusted` | `on-failure` | `on-request` | `never`.
    pub fn approval_policy(mut self, p: impl Into<String>) -> Self {
        self.approval_policy = p.into();
        self
    }
    /// Codex `sandbox`: `read-only` | `workspace-write` | `danger-full-access`.
    pub fn sandbox(mut self, s: impl Into<String>) -> Self {
        self.sandbox = s.into();
        self
    }

    pub async fn spawn(self) -> Result<CodexMcpDriver, DriverError> {
        let bin = self
            .bin
            .clone()
            .or_else(|| std::env::var("CODEX_BIN").ok())
            .unwrap_or_else(|| "codex".to_string());

        let mut cmd = Command::new(&bin);
        cmd.arg("mcp-server")
            .current_dir(&self.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        debug!(bin = %bin, cwd = %self.cwd.display(), "spawning codex mcp-server");

        let mut child = cmd.spawn().map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                DriverError::BinaryNotFound(bin.clone())
            } else {
                DriverError::SpawnFailed(e)
            }
        })?;

        let stdin = child.stdin.take().ok_or(DriverError::AgentExited)?;
        let stdout = child.stdout.take().ok_or(DriverError::AgentExited)?;
        let stderr = child.stderr.take().ok_or(DriverError::AgentExited)?;

        let (writer_tx, writer_rx) = mpsc::channel::<String>(32);
        let (reader_tx, reader_rx) = mpsc::channel::<AgentEvent>(256);

        let thread_id = Arc::new(Mutex::new(None));
        let next_id = Arc::new(AtomicU64::new(1));
        let last_tool_call_id = Arc::new(AtomicU64::new(0));
        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let streamed_this_turn = Arc::new(AtomicBool::new(false));
        let ready_emitted = Arc::new(AtomicBool::new(false));
        let exited = Arc::new(AtomicBool::new(false));
        let exit_status = Arc::new(Mutex::new(None));

        tokio::spawn(writer_task(stdin, writer_rx));
        tokio::spawn(reader_task(
            stdout,
            reader_tx.clone(),
            Arc::clone(&thread_id),
            Arc::clone(&pending),
            Arc::clone(&streamed_this_turn),
            Arc::clone(&ready_emitted),
            self.model.clone(),
            Arc::clone(&exited),
        ));
        tokio::spawn(stderr_drain(stderr));

        let driver = CodexMcpDriver {
            writer_tx: Some(writer_tx),
            reader_rx,
            child: Some(child),
            thread_id: Arc::clone(&thread_id),
            next_id: Arc::clone(&next_id),
            last_tool_call_id: Arc::clone(&last_tool_call_id),
            sandbox: self.sandbox.clone(),
            approval_policy: self.approval_policy.clone(),
            cwd: self.cwd.clone(),
            model: self.model.clone(),
            exited,
            exit_status,
        };
        // `streamed_this_turn` and `ready_emitted` live inside the reader task
        // only — the driver doesn't need a clone, the parser thread already has one.
        drop(streamed_this_turn);
        drop(ready_emitted);

        // MCP handshake.
        let init_id = next_id.fetch_add(1, Ordering::Relaxed);
        let init_req = json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": init_id,
            "method": "initialize",
            "params": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "clientInfo": { "name": "cap-rs", "version": env!("CARGO_PKG_VERSION") },
                "capabilities": {}
            }
        });
        send_and_await(&driver, init_id, init_req, &pending).await?;

        // notifications/initialized — fire-and-forget, no id.
        driver
            .writer_tx
            .as_ref()
            .ok_or(DriverError::AgentExited)?
            .send(
                json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "method": "notifications/initialized"
                })
                .to_string(),
            )
            .await
            .map_err(|_| DriverError::AgentExited)?;

        Ok(driver)
    }
}

// ---------------------------------------------------------------------------
// JSON-RPC plumbing
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonRpcResult {
    inner: Result<Value, (i64, String)>,
}

async fn send_and_await(
    driver: &CodexMcpDriver,
    id: u64,
    req: Value,
    pending: &Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>>,
) -> Result<Value, DriverError> {
    let (otx, orx) = oneshot::channel();
    pending
        .lock()
        .expect("pending mutex poisoned")
        .insert(id, otx);
    if driver
        .writer_tx
        .as_ref()
        .ok_or(DriverError::AgentExited)?
        .send(req.to_string())
        .await
        .is_err()
    {
        pending.lock().expect("pending mutex poisoned").remove(&id);
        return Err(DriverError::AgentExited);
    }
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, orx).await {
        Ok(Ok(JsonRpcResult { inner: Ok(v) })) => Ok(v),
        Ok(Ok(JsonRpcResult {
            inner: Err((code, message)),
        })) => Err(DriverError::AgentError {
            code: format!("codex_mcp_jsonrpc_{code}"),
            message,
        }),
        Ok(Err(_)) => Err(DriverError::AgentExited),
        Err(_) => {
            pending.lock().expect("pending mutex poisoned").remove(&id);
            Err(DriverError::AgentError {
                code: "cap_handshake_timeout".into(),
                message: format!(
                    "codex mcp-server did not respond to request {id} in {HANDSHAKE_TIMEOUT:?}"
                ),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

async fn writer_task(mut stdin: tokio::process::ChildStdin, mut rx: mpsc::Receiver<String>) {
    while let Some(line) = rx.recv().await {
        trace!(line = %line, "→ codex-mcp");
        if stdin.write_all(line.as_bytes()).await.is_err() {
            return;
        }
        if !line.ends_with('\n') {
            let _ = stdin.write_all(b"\n").await;
        }
        let _ = stdin.flush().await;
    }
}

async fn reader_task(
    stdout: tokio::process::ChildStdout,
    tx: mpsc::Sender<AgentEvent>,
    thread_id: Arc<Mutex<Option<String>>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>>,
    streamed_this_turn: Arc<AtomicBool>,
    ready_emitted: Arc<AtomicBool>,
    model: Option<String>,
    exited: Arc<AtomicBool>,
) {
    let mut lines = BufReader::new(stdout).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }
                trace!(line = %line, "← codex-mcp");
                let frame: Value = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, "codex-mcp reader: malformed JSON, skipping");
                        continue;
                    }
                };
                for ev in process_frame(&frame, &thread_id, &pending, &streamed_this_turn, &ready_emitted, &model) {
                    if tx.send(ev).await.is_err() {
                        exited.store(true, Ordering::Relaxed);
                        return;
                    }
                }
            }
            Ok(None) | Err(_) => {
                exited.store(true, Ordering::Relaxed);
                return;
            }
        }
    }
}

async fn stderr_drain(stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => debug!(target: "cap_rs::codex_mcp::stderr", "{}", line),
            Ok(None) => return,
            Err(e) => {
                warn!(error = %e, "stderr read error");
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Frame dispatch
// ---------------------------------------------------------------------------

fn process_frame(
    frame: &Value,
    thread_id: &Arc<Mutex<Option<String>>>,
    pending: &Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>>,
    streamed_this_turn: &Arc<AtomicBool>,
    ready_emitted: &Arc<AtomicBool>,
    model: &Option<String>,
) -> Vec<AgentEvent> {
    let has_method = frame.get("method").is_some();

    // Response to one of our requests (has id, no method).
    if frame.get("id").is_some() && !has_method {
        return handle_response(frame, pending, streamed_this_turn);
    }

    let method = frame.get("method").and_then(Value::as_str).unwrap_or("");
    let params = frame.get("params").cloned().unwrap_or(Value::Null);

    // Server-initiated request (has method + id). Codex MCP shouldn't initiate
    // anything we don't auto-handle; ignore to avoid surprises.
    if frame.get("id").is_some() {
        trace!(method, "codex-mcp: unhandled server request, ignoring");
        return Vec::new();
    }

    // Notification. We care about codex/event (the rich stream).
    if method == "codex/event" {
        return handle_codex_event(&params, thread_id, streamed_this_turn, ready_emitted, model);
    }
    Vec::new()
}

fn handle_response(
    frame: &Value,
    pending: &Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>>,
    streamed_this_turn: &Arc<AtomicBool>,
) -> Vec<AgentEvent> {
    let id = frame.get("id").and_then(Value::as_u64);
    let err_payload = frame.get("error").cloned();
    let result = match &err_payload {
        Some(err) => JsonRpcResult {
            inner: Err((
                err.get("code").and_then(Value::as_i64).unwrap_or(0),
                err.get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            )),
        },
        None => JsonRpcResult {
            inner: Ok(frame.get("result").cloned().unwrap_or(Value::Null)),
        },
    };

    // Claimed by a handshake awaiter?
    if let Some(id) = id {
        if let Some(tx) = pending.lock().expect("pending mutex poisoned").remove(&id) {
            let _ = tx.send(result);
            return Vec::new();
        }
    }

    // Unclaimed response: this is the tools/call result for the prompt turn.
    if let Some(err) = err_payload {
        return vec![AgentEvent::Error {
            code: err
                .get("code")
                .and_then(Value::as_i64)
                .map(|c| format!("codex_mcp_jsonrpc_{c}"))
                .unwrap_or_else(|| "codex_mcp_error".into()),
            message: err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            retryable: false,
            details: None,
        }];
    }

    let res = frame.get("result").cloned().unwrap_or(Value::Null);
    let already_streamed = streamed_this_turn.swap(false, Ordering::Relaxed);
    let mut events = Vec::new();
    // Only emit the structuredContent as a TextChunk when nothing was streamed
    // — otherwise the deltas already carried the same text, and emitting it
    // again would route it twice to downstream sessions.
    if !already_streamed {
        if let Some(content) = res
            .pointer("/structuredContent/content")
            .and_then(Value::as_str)
        {
            events.push(AgentEvent::TextChunk {
                msg_id: res
                    .pointer("/structuredContent/threadId")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                text: content.to_string(),
                channel: TextChannel::Assistant,
            });
        }
    }
    events.push(AgentEvent::Done {
        stop_reason: StopReason::EndTurn,
        usage: Usage {
            stop_reason: Some(StopReason::EndTurn),
            ..Usage::default()
        },
    });
    events
}

fn handle_codex_event(
    params: &Value,
    thread_id: &Arc<Mutex<Option<String>>>,
    streamed_this_turn: &Arc<AtomicBool>,
    ready_emitted: &Arc<AtomicBool>,
    model: &Option<String>,
) -> Vec<AgentEvent> {
    let msg = params.get("msg").cloned().unwrap_or(Value::Null);
    let kind = msg.get("type").and_then(Value::as_str).unwrap_or("");
    match kind {
        "session_configured" => {
            if let Some(tid) = msg.get("thread_id").and_then(Value::as_str) {
                let mut slot = thread_id.lock().expect("thread_id mutex poisoned");
                if slot.is_none() {
                    *slot = Some(tid.to_string());
                }
                if !ready_emitted.swap(true, Ordering::Relaxed) {
                    return vec![AgentEvent::Ready {
                        session_id: tid.to_string(),
                        version: crate::core::CAP_PROTOCOL_VERSION.into(),
                        model: model.clone(),
                    }];
                }
            }
            Vec::new()
        }
        "agent_message_content_delta" => {
            let delta = msg.get("delta").and_then(Value::as_str).unwrap_or("");
            if delta.is_empty() {
                return Vec::new();
            }
            // Record that this turn streamed, so the tools/call response
            // suppresses its redundant structuredContent TextChunk.
            streamed_this_turn.store(true, Ordering::Relaxed);
            vec![AgentEvent::TextChunk {
                msg_id: msg
                    .get("item_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                text: delta.to_string(),
                channel: TextChannel::Assistant,
            }]
        }
        "item_started" => {
            let item = msg.get("item").cloned().unwrap_or(Value::Null);
            let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
            let item_id = item
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            match item_type {
                "CommandExecution" | "command_execution" => {
                    vec![AgentEvent::ToolCallStart {
                        call_id: item_id,
                        name: "Bash".into(),
                        input: item.get("command").cloned().unwrap_or(Value::Null),
                    }]
                }
                "FunctionCall" | "function_call" | "McpToolCall" | "mcp_tool_call" => {
                    let name = item
                        .get("name")
                        .or_else(|| item.get("tool"))
                        .and_then(Value::as_str)
                        .unwrap_or("tool")
                        .to_string();
                    vec![AgentEvent::ToolCallStart {
                        call_id: item_id,
                        name,
                        input: item.get("arguments").cloned().unwrap_or(Value::Null),
                    }]
                }
                _ => Vec::new(),
            }
        }
        "item_completed" => {
            let item = msg.get("item").cloned().unwrap_or(Value::Null);
            let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
            let item_id = item
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            match item_type {
                "CommandExecution" | "command_execution" => {
                    let output = item
                        .get("output")
                        .or_else(|| item.get("aggregated_output"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    let status = item.get("status").and_then(Value::as_str).unwrap_or("");
                    vec![AgentEvent::ToolCallEnd {
                        call_id: item_id,
                        output,
                        is_error: status == "failed",
                    }]
                }
                "FunctionCall" | "function_call" | "McpToolCall" | "mcp_tool_call" => {
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
                    }]
                }
                _ => Vec::new(),
            }
        }
        "token_count" => {
            // codex emits token_count progress events; surface as a Usage
            // snapshot (no breakdown beyond totals here).
            let total = msg.get("total_tokens").and_then(Value::as_u64).unwrap_or(0);
            if total == 0 {
                return Vec::new();
            }
            vec![AgentEvent::Usage {
                usage: Usage {
                    input_tokens: msg.get("input_tokens").and_then(Value::as_u64).unwrap_or(0),
                    output_tokens: msg
                        .get("output_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    ..Usage::default()
                },
            }]
        }
        // task_started / task_complete / agent_message: redundant with the
        // tools/call response (which carries the authoritative Done + content).
        // hook_*, mcp_startup_*, raw_response_item, user_message, warning,
        // stream_error: codex internals or transient errors codex handles
        // itself — log via trace, do not surface.
        _ => {
            trace!(kind, "codex-mcp: ignoring codex/event");
            Vec::new()
        }
    }
}

// ---------------------------------------------------------------------------
// Tests — frames captured from real `codex mcp-server` v0.133
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_pending() -> Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>> {
        Arc::new(Mutex::new(HashMap::new()))
    }
    fn empty_thread() -> Arc<Mutex<Option<String>>> {
        Arc::new(Mutex::new(None))
    }
    fn streamed_flag() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }
    fn ready_flag() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }
    fn no_model() -> Option<String> {
        None
    }

    #[test]
    fn session_configured_captures_thread_id() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"codex/event","params":{
               "_meta":{"requestId":3,"threadId":"abc"},
               "msg":{"type":"session_configured","thread_id":"abc","session_id":"abc"}}}"#,
        )
        .unwrap();
        let tid = empty_thread();
        let events = process_frame(&frame, &tid, &empty_pending(), &streamed_flag(), &ready_flag(), &no_model());
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], AgentEvent::Ready { session_id, model, .. } if session_id == "abc" && model.is_none()));
        assert_eq!(tid.lock().unwrap().clone(), Some("abc".into()));
    }

    #[test]
    fn agent_message_content_delta_becomes_textchunk() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"codex/event","params":{
               "msg":{"type":"agent_message_content_delta","item_id":"msg_x","delta":"hello"}}}"#,
        )
        .unwrap();
        let events = process_frame(&frame, &empty_thread(), &empty_pending(), &streamed_flag(), &ready_flag(), &no_model());
        assert!(matches!(&events[0],
            AgentEvent::TextChunk { text, msg_id, .. } if text == "hello" && msg_id == "msg_x"));
    }

    #[test]
    fn item_started_command_execution_becomes_toolcallstart() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"codex/event","params":{
               "msg":{"type":"item_started","item":{"type":"CommandExecution","id":"i1",
               "command":["ls","-la"]}}}}"#,
        )
        .unwrap();
        let events = process_frame(&frame, &empty_thread(), &empty_pending(), &streamed_flag(), &ready_flag(), &no_model());
        assert!(matches!(&events[0],
            AgentEvent::ToolCallStart { call_id, name, .. } if call_id == "i1" && name == "Bash"));
    }

    #[test]
    fn item_completed_command_failed_becomes_toolcallend_with_error() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"codex/event","params":{
               "msg":{"type":"item_completed","item":{"type":"CommandExecution","id":"i1",
               "output":"command not found","status":"failed"}}}}"#,
        )
        .unwrap();
        let events = process_frame(&frame, &empty_thread(), &empty_pending(), &streamed_flag(), &ready_flag(), &no_model());
        assert!(matches!(&events[0],
            AgentEvent::ToolCallEnd { call_id, is_error, output }
            if call_id == "i1" && *is_error && output == "command not found"));
    }

    #[test]
    fn tools_call_response_emits_textchunk_and_done() {
        // The real tools/call response shape: structuredContent: {threadId, content}.
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":3,"result":{
               "structuredContent":{"threadId":"abc","content":"hello"},
               "content":[{"type":"text","text":"hello"}]}}"#,
        )
        .unwrap();
        let events = process_frame(&frame, &empty_thread(), &empty_pending(), &streamed_flag(), &ready_flag(), &no_model());
        assert_eq!(events.len(), 2, "got {events:?}");
        assert!(matches!(&events[0],
            AgentEvent::TextChunk { text, msg_id, .. } if text == "hello" && msg_id == "abc"));
        assert!(matches!(
            &events[1],
            AgentEvent::Done {
                stop_reason: StopReason::EndTurn,
                ..
            }
        ));
    }

    /// Once a turn has streamed deltas, the structuredContent on the response
    /// is the SAME text — emitting it as a TextChunk would route the message
    /// twice. The dedup flag suppresses the redundant emission.
    #[test]
    fn streamed_turn_suppresses_structured_content_textchunk() {
        let streamed = streamed_flag();
        // First a delta arrives (turn was streamed).
        let delta: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"codex/event","params":{
               "msg":{"type":"agent_message_content_delta","item_id":"m","delta":"hello"}}}"#,
        )
        .unwrap();
        let evs = process_frame(&delta, &empty_thread(), &empty_pending(), &streamed, &ready_flag(), &no_model());
        assert!(matches!(&evs[0], AgentEvent::TextChunk { text, .. } if text == "hello"));
        assert!(streamed.load(Ordering::Relaxed), "flag set by delta");

        // Now the tools/call response arrives with the same content — must
        // ONLY emit Done (no duplicate TextChunk).
        let resp: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":3,"result":{
               "structuredContent":{"threadId":"t","content":"hello"}}}"#,
        )
        .unwrap();
        let evs = process_frame(&resp, &empty_thread(), &empty_pending(), &streamed, &ready_flag(), &no_model());
        assert_eq!(evs.len(), 1, "got {evs:?}");
        assert!(matches!(&evs[0], AgentEvent::Done { .. }));
        // Flag resets per turn so a future turn can fall through to the fallback.
        assert!(!streamed.load(Ordering::Relaxed));
    }

    #[test]
    fn claimed_handshake_response_does_not_fire_done() {
        let pending = empty_pending();
        let (tx, mut rx) = oneshot::channel();
        pending.lock().unwrap().insert(2, tx);
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":2,"result":{"protocolVersion":"2024-11-05"}}"#,
        )
        .unwrap();
        let events = process_frame(&frame, &empty_thread(), &pending, &streamed_flag(), &ready_flag(), &no_model());
        assert!(events.is_empty(), "claimed response yields no event");
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn unknown_codex_event_is_ignored() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"codex/event","params":{
               "msg":{"type":"mcp_startup_update","server":"x","status":{"state":"ready"}}}}"#,
        )
        .unwrap();
        assert!(
            process_frame(&frame, &empty_thread(), &empty_pending(), &streamed_flag(), &ready_flag(), &no_model()).is_empty()
        );
    }
}
