//! Codex app-server driver — `codex app-server --listen stdio://`.
//!
//! Wire format: JSON-RPC 2.0 over line-delimited JSON on the child's stdio.
//! This is the protocol the official VSCode codex extension speaks; it is
//! marked **experimental** by the codex CLI but is the cleanest fast-path
//! for programmatic clients today.
//!
//! Compared to [`super::codex::CodexExecDriver`] (one-shot `codex exec --json`),
//! the app-server protocol is multi-turn first-class: a single child process
//! serves an unbounded number of turns over the same thread, supports
//! mid-turn cancel (`turn/interrupt`), and supports mid-turn user injection
//! (`thread/inject_items`). It also surfaces structured approval requests
//! that the orchestrator can route through CAP's [`AgentEvent::PermissionRequest`].
//!
//! Lifecycle on `spawn`:
//!
//! 1. Spawn `codex app-server --listen stdio://`.
//! 2. Send `initialize` with `clientInfo = { name: "cap-rs", version }`,
//!    await response.
//! 3. Send `thread/start` (or `thread/resume` if a thread id was supplied),
//!    await response, capture `thread_id`.
//! 4. Driver becomes usable: each `Driver::send(Prompt)` issues a fresh
//!    `turn/start`.
//!
//! Spec mapping: see [docs/cap-v1.md §6.3 + spec mapping below].
//!
//! ## Server notifications → CAP events
//!
//! | codex method | CAP event |
//! |---|---|
//! | `thread/started` | [`AgentEvent::Ready`] |
//! | `item/agentMessage/delta` | [`AgentEvent::TextChunk`] |
//! | `item/reasoning/textDelta` | [`AgentEvent::Thought`] |
//! | `item/started` (command/mcpTool) | [`AgentEvent::ToolCallStart`] |
//! | `item/completed` (command/mcpTool) | [`AgentEvent::ToolCallEnd`] |
//! | `item/commandExecution/outputDelta` | [`AgentEvent::ToolCallDelta`] |
//! | `item/plan/delta` / `turn/plan/updated` | [`AgentEvent::Plan`] |
//! | `thread/tokenUsage/updated` | [`AgentEvent::Usage`] (progress) |
//! | `turn/completed` | [`AgentEvent::Done`] |
//! | `error` / server-side error responses | [`AgentEvent::Error`] |
//!
//! ## Server-initiated requests → CAP events
//!
//! Codex sends JSON-RPC *requests* (with id) for human-in-the-loop
//! decisions. These map to CAP's approval flow:
//!
//! | codex method | CAP event |
//! |---|---|
//! | `execCommandApproval` | [`AgentEvent::PermissionRequest`] (scope=Execute) |
//! | `applyPatchApproval` / `fileChangeRequestApproval` | [`AgentEvent::PermissionRequest`] (scope=Write) |
//! | `permissionsRequestApproval` | [`AgentEvent::PermissionRequest`] |
//! | `mcpServerElicitationRequest` / `toolRequestUserInput` | [`AgentEvent::AskUser`] |
//!
//! Reply with [`ClientFrame::PermissionResponse`] / [`ClientFrame::AskUserAnswer`];
//! the driver looks up the originating JSON-RPC id by the CAP `req_id` /
//! `ask_id` and sends the appropriate response.

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

use crate::core::{
    AgentEvent, AskKind, ClientFrame, Content, PermissionDecision, PermissionScope, RiskLevel,
    StopReason, TextChannel, Usage,
};
use crate::driver::{Driver, DriverError, DriverExitStatus};

const JSONRPC_VERSION: &str = "2.0";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Driver for OpenAI's `codex app-server` JSON-RPC protocol.
pub struct CodexAppServerDriver {
    writer_tx: Option<mpsc::Sender<String>>,
    reader_rx: mpsc::Receiver<AgentEvent>,
    child: Option<Child>,

    thread_id: Arc<Mutex<Option<String>>>,
    next_id: Arc<AtomicU64>,

    /// CAP req_id (stringified JSON-RPC id) → JSON-RPC id Value, for
    /// routing PermissionResponse / AskUserAnswer back to the right server
    /// request.
    pending_approvals: Arc<Mutex<HashMap<String, Value>>>,

    exited: Arc<AtomicBool>,
    exit_status: Arc<Mutex<Option<DriverExitStatus>>>,
}

impl std::fmt::Debug for CodexAppServerDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodexAppServerDriver")
            .field("thread_id", &self.thread_id())
            .field("alive", &self.is_alive())
            .finish()
    }
}

impl CodexAppServerDriver {
    /// Spawn codex app-server in the given working directory and run the
    /// `initialize` + `thread/start` handshake. Returns a ready driver.
    pub async fn spawn(cwd: impl AsRef<Path>) -> Result<Self, DriverError> {
        Self::builder(cwd).spawn().await
    }

    /// Begin building a codex app-server session with custom options.
    pub fn builder(cwd: impl AsRef<Path>) -> CodexAppServerBuilder {
        CodexAppServerBuilder {
            bin: None,
            cwd: cwd.as_ref().to_path_buf(),
            model: None,
            resume_thread: None,
            approval_policy: None,
            sandbox: None,
            base_instructions: None,
        }
    }

    /// Thread ID assigned by codex during `thread/start`, or the resumed
    /// thread id if [`CodexAppServerBuilder::resume`] was supplied.
    pub fn thread_id(&self) -> Option<String> {
        self.thread_id.lock().ok().and_then(|g| g.clone())
    }
}

#[async_trait]
impl Driver for CodexAppServerDriver {
    async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError> {
        let tx = self.writer_tx.as_ref().ok_or(DriverError::AgentExited)?;

        match frame {
            ClientFrame::SessionConfig(_) => Ok(()),

            ClientFrame::Prompt { content } => {
                let tid = self.thread_id().ok_or_else(|| DriverError::AgentError {
                    code: "cap_session_config_missing".into(),
                    message: "thread not started; send SessionConfig first or use builder".into(),
                })?;
                let input = content_to_user_input(&content);
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                let req = json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": id,
                    "method": "turn/start",
                    "params": { "threadId": tid, "input": input }
                });
                tx.send(req.to_string())
                    .await
                    .map_err(|_| DriverError::AgentExited)?;
                Ok(())
            }

            ClientFrame::Cancel { scope, .. } => {
                use crate::core::CancelScope::*;
                let tid = self.thread_id().ok_or_else(|| DriverError::AgentError {
                    code: "cap_session_config_missing".into(),
                    message: "thread not started".into(),
                })?;
                let method = match scope {
                    CurrentTurn => "turn/interrupt",
                    // codex app-server doesn't have a single-method session
                    // cancel; closing the child is the analogue. The
                    // orchestrator should call shutdown() in that case.
                    Session => {
                        return Err(DriverError::AgentError {
                            code: "cap_cancel_unsupported".into(),
                            message:
                                "Cancel { Session } not supported — call Driver::shutdown instead"
                                    .into(),
                        });
                    }
                };
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                let req = json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": id,
                    "method": method,
                    "params": { "threadId": tid }
                });
                tx.send(req.to_string())
                    .await
                    .map_err(|_| DriverError::AgentExited)?;
                Ok(())
            }

            ClientFrame::AskUserAnswer { ask_id, value } => {
                let jsonrpc_id = self
                    .pending_approvals
                    .lock()
                    .expect("pending_approvals mutex poisoned")
                    .remove(&ask_id)
                    .ok_or_else(|| DriverError::AgentError {
                        code: "cap_invalid_answer".into(),
                        message: format!("no pending elicitation with ask_id {ask_id}"),
                    })?;
                let resp = json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": jsonrpc_id,
                    "result": value,
                });
                tx.send(resp.to_string())
                    .await
                    .map_err(|_| DriverError::AgentExited)?;
                Ok(())
            }

            ClientFrame::PermissionResponse { req_id, decision } => {
                let jsonrpc_id = self
                    .pending_approvals
                    .lock()
                    .expect("pending_approvals mutex poisoned")
                    .remove(&req_id)
                    .ok_or_else(|| DriverError::AgentError {
                        code: "cap_unknown_permission".into(),
                        message: format!("no pending approval with req_id {req_id}"),
                    })?;
                let decision_str = match decision {
                    PermissionDecision::AllowOnce => "approved",
                    PermissionDecision::AllowAlways => "approvedForSession",
                    PermissionDecision::Deny => "denied",
                };
                let resp = json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": jsonrpc_id,
                    "result": { "decision": decision_str }
                });
                tx.send(resp.to_string())
                    .await
                    .map_err(|_| DriverError::AgentExited)?;
                Ok(())
            }
            ClientFrame::ReverseRpcResult { .. } => Err(DriverError::AgentError {
                code: "cap_reverse_rpc_unsupported".into(),
                message: "codex app-server driver does not emit reverse RPC".into(),
            }),
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

/// Configuration for [`CodexAppServerDriver`].
#[derive(Debug, Clone)]
pub struct CodexAppServerBuilder {
    bin: Option<String>,
    cwd: PathBuf,
    model: Option<String>,
    resume_thread: Option<String>,
    approval_policy: Option<String>,
    sandbox: Option<String>,
    base_instructions: Option<String>,
}

impl CodexAppServerBuilder {
    /// Override the binary used (default: `codex` on PATH, or `$CODEX_BIN`).
    pub fn bin(mut self, b: impl Into<String>) -> Self {
        self.bin = Some(b.into());
        self
    }

    /// Override the model used for the thread.
    pub fn model(mut self, m: impl Into<String>) -> Self {
        self.model = Some(m.into());
        self
    }

    /// Resume an existing thread by ID. Issues `thread/resume` during
    /// handshake instead of `thread/start`.
    pub fn resume(mut self, thread_id: impl Into<String>) -> Self {
        self.resume_thread = Some(thread_id.into());
        self
    }

    /// codex `approvalPolicy` value (e.g. `"on-request"`, `"untrusted"`,
    /// `"never"`). Default is codex's own setting.
    pub fn approval_policy(mut self, p: impl Into<String>) -> Self {
        self.approval_policy = Some(p.into());
        self
    }

    /// codex `sandbox` mode (e.g. `"read-only"`, `"workspace-write"`,
    /// `"dangerFullAccess"`).
    pub fn sandbox(mut self, s: impl Into<String>) -> Self {
        self.sandbox = Some(s.into());
        self
    }

    /// Inject codex `baseInstructions` (system-prompt equivalent).
    pub fn base_instructions(mut self, b: impl Into<String>) -> Self {
        self.base_instructions = Some(b.into());
        self
    }

    /// Spawn the app-server and run the initialize + thread handshake.
    pub async fn spawn(self) -> Result<CodexAppServerDriver, DriverError> {
        let bin = self
            .bin
            .clone()
            .or_else(|| std::env::var("CODEX_BIN").ok())
            .unwrap_or_else(|| "codex".to_string());

        let mut cmd = Command::new(&bin);
        cmd.arg("app-server")
            .arg("--listen")
            .arg("stdio://")
            .current_dir(&self.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        debug!(bin = %bin, cwd = %self.cwd.display(), "spawning codex app-server");

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

        // Channels: writer_tx accepts already-serialised JSON-RPC frames;
        // reader_rx receives CAP events ready for the consumer.
        let (writer_tx, writer_rx) = mpsc::channel::<String>(32);
        let (reader_tx, reader_rx) = mpsc::channel::<AgentEvent>(128);

        let thread_id = Arc::new(Mutex::new(None));
        let next_id = Arc::new(AtomicU64::new(1));
        let pending_responses: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pending_approvals: Arc<Mutex<HashMap<String, Value>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let exited = Arc::new(AtomicBool::new(false));
        let exit_status = Arc::new(Mutex::new(None));

        tokio::spawn(writer_task(stdin, writer_rx));
        tokio::spawn(reader_task(
            stdout,
            reader_tx.clone(),
            Arc::clone(&thread_id),
            Arc::clone(&pending_responses),
            Arc::clone(&pending_approvals),
            Arc::clone(&exited),
        ));
        tokio::spawn(stderr_drain(stderr));

        let driver = CodexAppServerDriver {
            writer_tx: Some(writer_tx),
            reader_rx,
            child: Some(child),
            thread_id: Arc::clone(&thread_id),
            next_id: Arc::clone(&next_id),
            pending_approvals,
            exited,
            exit_status,
        };

        // Handshake.
        let init_id = next_id.fetch_add(1, Ordering::Relaxed);
        let init_req = json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": init_id,
            "method": "initialize",
            "params": {
                "clientInfo": {
                    "name": "cap-rs",
                    "version": env!("CARGO_PKG_VERSION"),
                }
            }
        });
        send_and_await(&driver, init_id, init_req, &pending_responses).await?;

        // Start (or resume) a thread.
        let thread_id_value = if let Some(tid) = &self.resume_thread {
            let req_id = next_id.fetch_add(1, Ordering::Relaxed);
            let req = json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": req_id,
                "method": "thread/resume",
                "params": { "threadId": tid }
            });
            let resp = send_and_await(&driver, req_id, req, &pending_responses).await?;
            extract_thread_id_from_response(&resp).unwrap_or_else(|| tid.clone())
        } else {
            let req_id = next_id.fetch_add(1, Ordering::Relaxed);
            let mut params = serde_json::Map::new();
            params.insert("cwd".into(), json!(self.cwd.display().to_string()));
            if let Some(p) = &self.approval_policy {
                params.insert("approvalPolicy".into(), json!(p));
            }
            if let Some(s) = &self.sandbox {
                params.insert("sandbox".into(), json!(s));
            }
            if let Some(b) = &self.base_instructions {
                params.insert("baseInstructions".into(), json!(b));
            }
            if let Some(m) = &self.model {
                params.insert("model".into(), json!(m));
            }
            let req = json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": req_id,
                "method": "thread/start",
                "params": Value::Object(params),
            });
            let resp = send_and_await(&driver, req_id, req, &pending_responses).await?;
            extract_thread_id_from_response(&resp).ok_or_else(|| {
                DriverError::Parse("thread/start response missing thread.id".into())
            })?
        };

        // Apply model override after thread is created (codex routes it via
        // turn-level overrides, but we also surface it on the Ready event).
        *thread_id.lock().expect("thread_id mutex poisoned") = Some(thread_id_value.clone());

        // Emit a synthetic Ready event so callers see the same lifecycle
        // shape as other drivers, even though `thread/started` is also
        // forwarded by the reader task.
        let model = self.model.clone();
        let _ = reader_tx
            .send(AgentEvent::Ready {
                session_id: thread_id_value,
                version: crate::core::CAP_PROTOCOL_VERSION.into(),
                model,
            })
            .await;

        Ok(driver)
    }
}

// ---------------------------------------------------------------------------
// JSON-RPC plumbing
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct JsonRpcResult {
    /// `Ok(value)` on success, `Err((code, message))` on error.
    inner: Result<Value, (i64, String)>,
}

async fn send_and_await(
    driver: &CodexAppServerDriver,
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
            code: format!("codex_jsonrpc_{code}"),
            message,
        }),
        Ok(Err(_)) => Err(DriverError::AgentExited),
        Err(_) => {
            pending.lock().expect("pending mutex poisoned").remove(&id);
            Err(DriverError::AgentError {
                code: "cap_handshake_timeout".into(),
                message: format!(
                    "codex app-server did not respond to request {id} within {:?}",
                    HANDSHAKE_TIMEOUT
                ),
            })
        }
    }
}

fn extract_thread_id_from_response(resp: &Value) -> Option<String> {
    resp.pointer("/thread/id")
        .and_then(Value::as_str)
        .map(String::from)
        .or_else(|| {
            resp.get("threadId")
                .and_then(Value::as_str)
                .map(String::from)
        })
}

fn content_to_user_input(content: &[Content]) -> Vec<Value> {
    content
        .iter()
        .filter_map(|c| match c {
            Content::Text { text } => Some(json!({"type": "text", "text": text})),
            // codex `UserInput::Image` expects a URL (file:// or data:);
            // raw Arc<[u8]> can't be uploaded inline today. Skip with a
            // tracing warn so callers know.
            Content::Image { .. } => {
                warn!("codex_app_server: Content::Image not yet supported; skipping");
                None
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

async fn writer_task(mut stdin: tokio::process::ChildStdin, mut rx: mpsc::Receiver<String>) {
    while let Some(line) = rx.recv().await {
        trace!(line = %line, "→ codex app-server");
        if let Err(e) = stdin.write_all(line.as_bytes()).await {
            warn!(error = %e, "writer: write failed");
            return;
        }
        if !line.ends_with('\n') {
            let _ = stdin.write_all(b"\n").await;
        }
        let _ = stdin.flush().await;
    }
    debug!("writer task: input channel closed, exiting");
}

async fn reader_task(
    stdout: tokio::process::ChildStdout,
    tx: mpsc::Sender<AgentEvent>,
    thread_id: Arc<Mutex<Option<String>>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>>,
    pending_approvals: Arc<Mutex<HashMap<String, Value>>>,
    exited: Arc<AtomicBool>,
) {
    let mut lines = BufReader::new(stdout).lines();
    let mut delta_state = DeltaState::default();

    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                trace!(line = %line, "← codex app-server");
                let frame: Value = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, raw = %line, "reader: malformed JSON, skipping");
                        continue;
                    }
                };
                let events = process_frame(
                    &frame,
                    &thread_id,
                    &pending,
                    &pending_approvals,
                    &mut delta_state,
                );
                for ev in events {
                    if tx.send(ev).await.is_err() {
                        exited.store(true, Ordering::Relaxed);
                        return;
                    }
                }
            }
            Ok(None) => {
                debug!("reader: stdout EOF");
                exited.store(true, Ordering::Relaxed);
                return;
            }
            Err(e) => {
                warn!(error = %e, "reader: read error");
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
            Ok(Some(line)) => debug!(target: "cap_rs::codex_app_server::stderr", "{}", line),
            Ok(None) => return,
            Err(e) => {
                warn!(error = %e, "stderr read error");
                return;
            }
        }
    }
}

/// Per-item tracking for text/reasoning delta accumulation. The codex
/// protocol already emits incremental `delta` fields, so we don't need
/// to compute deltas ourselves — but we DO want to drop entries on
/// `item/completed` to keep memory bounded across long threads.
#[derive(Debug, Default)]
struct DeltaState {
    active_items: std::collections::HashSet<String>,
}

// ---------------------------------------------------------------------------
// Frame dispatch
// ---------------------------------------------------------------------------

fn process_frame(
    frame: &Value,
    thread_id: &Arc<Mutex<Option<String>>>,
    pending: &Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>>,
    pending_approvals: &Arc<Mutex<HashMap<String, Value>>>,
    delta_state: &mut DeltaState,
) -> Vec<AgentEvent> {
    // Response to our request: has `id` AND (`result` OR `error`), no `method`.
    if frame.get("id").is_some() && frame.get("method").is_none() {
        let id = frame.get("id").and_then(Value::as_u64);
        let is_error = frame.get("error").is_some();
        let err_payload = frame.get("error").cloned();
        let result = if let Some(err) = err_payload.as_ref() {
            JsonRpcResult {
                inner: Err((
                    err.get("code").and_then(Value::as_i64).unwrap_or(0),
                    err.get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                )),
            }
        } else {
            JsonRpcResult {
                inner: Ok(frame.get("result").cloned().unwrap_or(Value::Null)),
            }
        };

        if let Some(id) = id {
            let claimed = pending.lock().expect("pending mutex poisoned").remove(&id);
            if let Some(tx) = claimed {
                let _ = tx.send(result);
                return Vec::new();
            }
        }

        // No awaiter — usually a response to a fire-and-forget request
        // (turn/start, turn/interrupt). Drop successes silently; surface
        // errors so the orchestrator sees them.
        if is_error && let Some(err) = err_payload {
            let code = err
                .get("code")
                .and_then(Value::as_i64)
                .map(|c| format!("codex_jsonrpc_{c}"))
                .unwrap_or_else(|| "codex_jsonrpc_error".into());
            let message = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            return vec![AgentEvent::Error {
                code,
                message,
                retryable: false,
                details: None,
            }];
        }
        return Vec::new();
    }

    let method = frame.get("method").and_then(Value::as_str).unwrap_or("");
    let params = frame.get("params").cloned().unwrap_or(Value::Null);

    // Server-initiated request: has `method` AND `id` AND `params`. We
    // must respond to it. Approval methods become PermissionRequest /
    // AskUser events; everything else gets a stub auto-deny so the agent
    // doesn't deadlock.
    if frame.get("id").is_some() {
        return handle_server_request(
            method,
            &frame.get("id").cloned().unwrap_or(Value::Null),
            &params,
            pending_approvals,
        );
    }

    // Pure notification (no `id`).
    parse_notification(method, &params, thread_id, delta_state)
}

fn handle_server_request(
    method: &str,
    id: &Value,
    params: &Value,
    pending_approvals: &Arc<Mutex<HashMap<String, Value>>>,
) -> Vec<AgentEvent> {
    // CAP req_id is the stringified JSON-RPC id so we can route the
    // orchestrator's response back without leaking codex internals.
    let req_id = match id {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => return Vec::new(),
    };

    match method {
        "execCommandApproval" => {
            pending_approvals
                .lock()
                .expect("pending_approvals mutex poisoned")
                .insert(req_id.clone(), id.clone());
            let cmd = params.get("command").cloned().unwrap_or(Value::Null);
            vec![AgentEvent::PermissionRequest {
                req_id,
                tool: "Bash".into(),
                intent: cmd,
                scope: PermissionScope::Execute,
                risk_level: RiskLevel::Medium,
            }]
        }
        "applyPatchApproval" | "fileChangeRequestApproval" => {
            pending_approvals
                .lock()
                .expect("pending_approvals mutex poisoned")
                .insert(req_id.clone(), id.clone());
            vec![AgentEvent::PermissionRequest {
                req_id,
                tool: "Edit".into(),
                intent: params.clone(),
                scope: PermissionScope::Write,
                risk_level: RiskLevel::Medium,
            }]
        }
        "permissionsRequestApproval" => {
            pending_approvals
                .lock()
                .expect("pending_approvals mutex poisoned")
                .insert(req_id.clone(), id.clone());
            vec![AgentEvent::PermissionRequest {
                req_id,
                tool: "Permissions".into(),
                intent: params.clone(),
                scope: PermissionScope::Write,
                risk_level: RiskLevel::High,
            }]
        }
        "mcpServerElicitationRequest" | "toolRequestUserInput" => {
            pending_approvals
                .lock()
                .expect("pending_approvals mutex poisoned")
                .insert(req_id.clone(), id.clone());
            let prompt = params
                .pointer("/message")
                .or_else(|| params.pointer("/prompt"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let schema = params
                .get("requestedSchema")
                .or_else(|| params.get("schema"))
                .cloned();
            let ask_kind = match schema {
                Some(s) => AskKind::Schema { schema: s },
                None => AskKind::FreeText,
            };
            vec![AgentEvent::AskUser {
                ask_id: req_id,
                prompt,
                ask_kind,
                options: Vec::new(),
                timeout_seconds: None,
            }]
        }
        other => {
            trace!(method = other, "unknown server request, ignoring");
            Vec::new()
        }
    }
}

fn parse_notification(
    method: &str,
    params: &Value,
    thread_id: &Arc<Mutex<Option<String>>>,
    delta_state: &mut DeltaState,
) -> Vec<AgentEvent> {
    match method {
        "thread/started" => {
            if let Some(tid) = params.pointer("/thread/id").and_then(Value::as_str) {
                let mut slot = thread_id.lock().expect("thread_id mutex poisoned");
                if slot.is_none() {
                    *slot = Some(tid.to_string());
                }
            }
            // Ready already emitted synthetically during spawn.
            Vec::new()
        }

        "turn/started" => Vec::new(),

        "item/started" => {
            let item = params.get("item").cloned().unwrap_or(Value::Null);
            let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
            let item_id = item
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            delta_state.active_items.insert(item_id.clone());

            match item_type {
                "command_execution" | "commandExecution" => {
                    let cmd = item.get("command").cloned().unwrap_or(Value::Null);
                    vec![AgentEvent::ToolCallStart {
                        call_id: item_id,
                        name: "Bash".into(),
                        input: cmd,
                    }]
                }
                "mcp_tool_call" | "mcpToolCall" => {
                    let name = item
                        .get("name")
                        .or_else(|| item.get("toolName"))
                        .and_then(Value::as_str)
                        .unwrap_or("mcp_tool")
                        .to_string();
                    let input = item.get("arguments").cloned().unwrap_or(Value::Null);
                    vec![AgentEvent::ToolCallStart {
                        call_id: item_id,
                        name,
                        input,
                    }]
                }
                _ => Vec::new(),
            }
        }

        "item/completed" => {
            let item = params.get("item").cloned().unwrap_or(Value::Null);
            let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
            let item_id = item
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            delta_state.active_items.remove(&item_id);

            match item_type {
                "command_execution" | "commandExecution" => {
                    let output = item
                        .get("output")
                        .or_else(|| item.get("aggregatedOutput"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    let status = item.get("status").and_then(Value::as_str).unwrap_or("");
                    let is_error =
                        !matches!(status, "completed" | "in_progress" | "inProgress" | "");
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
                }
                "mcp_tool_call" | "mcpToolCall" => {
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
                }
                _ => Vec::new(),
            }
        }

        "item/agentMessage/delta" => {
            let delta = params.get("delta").and_then(Value::as_str).unwrap_or("");
            if delta.is_empty() {
                return Vec::new();
            }
            let msg_id = params
                .get("itemId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            vec![AgentEvent::TextChunk {
                msg_id,
                text: delta.to_string(),
                channel: TextChannel::Assistant,
            }]
        }

        "item/reasoning/textDelta" | "item/reasoning/summaryTextDelta" => {
            let delta = params.get("delta").and_then(Value::as_str).unwrap_or("");
            if delta.is_empty() {
                return Vec::new();
            }
            let msg_id = params
                .get("itemId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            vec![AgentEvent::Thought {
                msg_id,
                text: delta.to_string(),
            }]
        }

        "item/commandExecution/outputDelta" => {
            let chunk = params
                .get("delta")
                .or_else(|| params.get("chunk"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if chunk.is_empty() {
                return Vec::new();
            }
            let call_id = params
                .get("itemId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            vec![AgentEvent::ToolCallDelta {
                call_id,
                output_chunk: chunk,
            }]
        }

        "item/plan/delta" | "turn/plan/updated" => {
            let plan = params
                .pointer("/plan")
                .or_else(|| params.pointer("/items"))
                .or(Some(params))
                .cloned()
                .unwrap_or(Value::Null);
            let entries = parse_plan_entries(&plan);
            if entries.is_empty() {
                Vec::new()
            } else {
                vec![AgentEvent::Plan { entries }]
            }
        }

        "thread/tokenUsage/updated" => {
            let usage = parse_usage(params.get("tokenUsage").unwrap_or(&Value::Null), None);
            vec![AgentEvent::Usage { usage }]
        }

        "turn/completed" => {
            let turn = params.get("turn").cloned().unwrap_or(Value::Null);
            let status = turn.get("status").and_then(Value::as_str).unwrap_or("");
            let stop_reason = match status {
                "completed" => StopReason::EndTurn,
                "interrupted" => StopReason::Cancelled,
                "failed" => StopReason::Error,
                _ => StopReason::EndTurn,
            };
            let usage = parse_usage(
                turn.pointer("/tokenUsage").unwrap_or(&Value::Null),
                turn.get("durationMs").and_then(Value::as_u64),
            );
            let mut events = Vec::new();
            if let Some(err) = turn.get("error").and_then(|e| e.as_object()) {
                let message = err
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("turn failed")
                    .to_string();
                events.push(AgentEvent::Error {
                    code: "codex_turn_failed".into(),
                    message,
                    retryable: false,
                    details: None,
                });
            }
            events.push(AgentEvent::Done {
                stop_reason,
                usage: Usage {
                    stop_reason: Some(stop_reason),
                    ..usage
                },
            });
            events
        }

        "error" | "thread/realtime/error" => {
            // ErrorNotification.params = { error: TurnError, threadId, turnId, willRetry }.
            // TurnError = { message: string, additionalDetails?: string, codexErrorInfo?: {...} }.
            let err = params.get("error").unwrap_or(params);
            let message = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let details = err
                .get("additionalDetails")
                .and_then(Value::as_str)
                .unwrap_or("");
            let code_info = err
                .pointer("/codexErrorInfo/code")
                .and_then(Value::as_str)
                .unwrap_or("");
            let will_retry = params
                .get("willRetry")
                .and_then(Value::as_bool)
                .unwrap_or(false);

            let full_message = match (details.is_empty(), will_retry) {
                (true, false) => message,
                (true, true) => format!("{message} (will retry)"),
                (false, false) => format!("{message}\n{details}"),
                (false, true) => format!("{message}\n{details}\n(will retry)"),
            };
            let code = if code_info.is_empty() {
                "codex_error".to_string()
            } else {
                format!("codex_{code_info}")
            };
            vec![AgentEvent::Error {
                code,
                message: full_message,
                retryable: false,
                details: None,
            }]
        }

        _ => {
            trace!(method, "ignoring codex notification");
            Vec::new()
        }
    }
}

fn parse_plan_entries(plan: &Value) -> Vec<crate::core::PlanEntry> {
    use crate::core::{PlanEntry, PlanStatus};
    let arr = plan
        .as_array()
        .or_else(|| plan.get("items").and_then(Value::as_array))
        .or_else(|| plan.get("entries").and_then(Value::as_array));
    let Some(arr) = arr else { return Vec::new() };
    arr.iter()
        .enumerate()
        .map(|(idx, e)| {
            let text = e
                .get("text")
                .or_else(|| e.get("content"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let status = match e.get("status").and_then(Value::as_str) {
                Some("in_progress" | "inProgress" | "active") => PlanStatus::InProgress,
                Some("completed" | "done") => PlanStatus::Completed,
                Some("cancelled") => PlanStatus::Cancelled,
                Some("blocked") => PlanStatus::Blocked,
                _ => PlanStatus::Pending,
            };
            PlanEntry {
                id: e
                    .get("id")
                    .and_then(Value::as_str)
                    .map(String::from)
                    .unwrap_or_else(|| format!("t{idx}")),
                content: text,
                status,
                priority: None,
                _meta: None,
            }
        })
        .collect()
}

fn parse_usage(token_usage: &Value, duration_ms: Option<u64>) -> Usage {
    // The protocol exposes a {last, total} breakdown; surface the `last`
    // turn's numbers — orchestrators that want running totals can sum
    // events themselves.
    let bucket = token_usage
        .get("last")
        .or(Some(token_usage))
        .unwrap_or(&Value::Null);
    Usage {
        input_tokens: bucket
            .get("inputTokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: bucket
            .get("outputTokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_read_tokens: bucket
            .get("cachedInputTokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_creation_tokens: 0,
        thinking_tokens: bucket
            .get("reasoningOutputTokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cost_usd_estimate: None,
        duration: duration_ms.map(Duration::from_millis),
        model_id: None,
        stop_reason: None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_pending() -> Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>> {
        Arc::new(Mutex::new(HashMap::new()))
    }

    fn empty_approvals() -> Arc<Mutex<HashMap<String, Value>>> {
        Arc::new(Mutex::new(HashMap::new()))
    }

    fn empty_thread() -> Arc<Mutex<Option<String>>> {
        Arc::new(Mutex::new(None))
    }

    #[test]
    fn parse_agent_message_delta() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"item/agentMessage/delta",
                "params":{"itemId":"i1","threadId":"t","turnId":"u","delta":"hello"}}"#,
        )
        .unwrap();
        let mut ds = DeltaState::default();
        let events = process_frame(
            &frame,
            &empty_thread(),
            &empty_pending(),
            &empty_approvals(),
            &mut ds,
        );
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::TextChunk { text, channel, .. } => {
                assert_eq!(text, "hello");
                assert_eq!(*channel, TextChannel::Assistant);
            }
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn parse_reasoning_delta_as_thought() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"item/reasoning/textDelta",
                "params":{"itemId":"r1","threadId":"t","turnId":"u","delta":"hmm"}}"#,
        )
        .unwrap();
        let mut ds = DeltaState::default();
        let events = process_frame(
            &frame,
            &empty_thread(),
            &empty_pending(),
            &empty_approvals(),
            &mut ds,
        );
        assert!(matches!(&events[0], AgentEvent::Thought { text, .. } if text == "hmm"));
    }

    #[test]
    fn parse_turn_completed_emits_done_with_usage() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"turn/completed",
                "params":{"threadId":"t","turn":{
                    "id":"u","items":[],"status":"completed","durationMs":1234,
                    "tokenUsage":{"last":{
                        "inputTokens":10,"outputTokens":20,"cachedInputTokens":5,
                        "reasoningOutputTokens":3,"totalTokens":38
                    }}
                }}}"#,
        )
        .unwrap();
        let mut ds = DeltaState::default();
        let events = process_frame(
            &frame,
            &empty_thread(),
            &empty_pending(),
            &empty_approvals(),
            &mut ds,
        );
        match &events[0] {
            AgentEvent::Done { stop_reason, usage } => {
                assert_eq!(*stop_reason, StopReason::EndTurn);
                assert_eq!(usage.input_tokens, 10);
                assert_eq!(usage.output_tokens, 20);
                assert_eq!(usage.cache_read_tokens, 5);
                assert_eq!(usage.thinking_tokens, 3);
                assert_eq!(usage.duration.map(|d| d.as_millis() as u64), Some(1234));
            }
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn parse_exec_command_approval_emits_permission_request() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":42,"method":"execCommandApproval",
                "params":{"command":["rm","-rf","/tmp/x"],"cwd":"/"}}"#,
        )
        .unwrap();
        let approvals = empty_approvals();
        let mut ds = DeltaState::default();
        let events = process_frame(
            &frame,
            &empty_thread(),
            &empty_pending(),
            &approvals,
            &mut ds,
        );
        match &events[0] {
            AgentEvent::PermissionRequest {
                req_id,
                tool,
                scope,
                risk_level,
                ..
            } => {
                assert_eq!(req_id, "42");
                assert_eq!(tool, "Bash");
                assert_eq!(*scope, PermissionScope::Execute);
                assert_eq!(*risk_level, RiskLevel::Medium);
            }
            other => panic!("wrong: {other:?}"),
        }
        // The JSON-RPC id 42 must now be in pending_approvals for routing
        // PermissionResponse back to codex.
        assert!(approvals.lock().unwrap().contains_key("42"));
    }

    #[test]
    fn parse_response_completes_pending_oneshot() {
        let pending = empty_pending();
        let (tx, mut rx) = oneshot::channel();
        pending.lock().unwrap().insert(7, tx);
        let frame: Value =
            serde_json::from_str(r#"{"jsonrpc":"2.0","id":7,"result":{"thread":{"id":"abc"}}}"#)
                .unwrap();
        let mut ds = DeltaState::default();
        let events = process_frame(
            &frame,
            &empty_thread(),
            &pending,
            &empty_approvals(),
            &mut ds,
        );
        assert!(events.is_empty());
        let res = rx.try_recv().expect("oneshot should be ready");
        assert_eq!(
            res.inner
                .unwrap()
                .pointer("/thread/id")
                .and_then(Value::as_str),
            Some("abc")
        );
    }

    #[test]
    fn parse_error_notification_extracts_nested_message() {
        // Real shape: ErrorNotification.params = {error: TurnError, threadId, turnId, willRetry}.
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"error",
                "params":{
                    "error":{
                        "message":"rate limit hit",
                        "additionalDetails":"retry in 5s",
                        "codexErrorInfo":{"code":"rate_limit"}
                    },
                    "threadId":"t","turnId":"u","willRetry":true
                }}"#,
        )
        .unwrap();
        let mut ds = DeltaState::default();
        let events = process_frame(
            &frame,
            &empty_thread(),
            &empty_pending(),
            &empty_approvals(),
            &mut ds,
        );
        match &events[0] {
            AgentEvent::Error { code, message, .. } => {
                assert_eq!(code, "codex_rate_limit");
                assert!(message.contains("rate limit hit"));
                assert!(message.contains("retry in 5s"));
                assert!(message.contains("will retry"));
            }
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn jsonrpc_error_response_without_awaiter_surfaces_as_event() {
        // turn/start fired-and-forgotten then errors out — we must NOT
        // drop the error silently.
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":99,"error":{"code":-32602,"message":"invalid input"}}"#,
        )
        .unwrap();
        let mut ds = DeltaState::default();
        let events = process_frame(
            &frame,
            &empty_thread(),
            &empty_pending(),
            &empty_approvals(),
            &mut ds,
        );
        match &events[0] {
            AgentEvent::Error { code, message, .. } => {
                assert_eq!(code, "codex_jsonrpc_-32602");
                assert_eq!(message, "invalid input");
            }
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn user_input_skips_image_for_now() {
        use std::sync::Arc;
        let content = vec![
            Content::text("hi"),
            Content::Image {
                mime: "image/png".into(),
                data: Arc::from(&[0u8, 1, 2][..]),
            },
        ];
        let input = content_to_user_input(&content);
        assert_eq!(input.len(), 1);
        assert_eq!(input[0]["type"], "text");
        assert_eq!(input[0]["text"], "hi");
    }
}
