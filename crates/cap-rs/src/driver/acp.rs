//! ACP driver — Agent Client Protocol over line-delimited JSON-RPC 2.0 on
//! stdio. ACP is agent-agnostic (Zed's standard, spoken by opencode, Gemini
//! CLI's ACP mode, claude-code-acp adapters, …); this driver speaks it
//! generically and is pointed at a concrete agent via the spawned command
//! (e.g. `opencode acp`).
//!
//! Wire format verified against real `opencode acp` (v1.14):
//!
//! Lifecycle on `spawn`:
//! 1. Spawn the ACP agent command.
//! 2. `initialize` { protocolVersion, clientCapabilities } → result with the
//!    agent's capabilities. We advertise **no** fs/terminal capability, so the
//!    agent does its own file I/O and never calls back for `fs/*`.
//! 3. `session/new` { cwd, mcpServers } → result with `sessionId`.
//! 4. Driver becomes usable: each `Driver::send(Prompt)` issues `session/prompt`.
//!
//! ## Agent → client messages
//!
//! | ACP message | CAP event |
//! |---|---|
//! | `session/update` · `agent_message_chunk` | [`AgentEvent::TextChunk`] |
//! | `session/update` · `agent_thought_chunk` | [`AgentEvent::Thought`] |
//! | `session/update` · `tool_call` | [`AgentEvent::ToolCallStart`] |
//! | `session/update` · `tool_call_update` (terminal) | [`AgentEvent::ToolCallEnd`] |
//! | `session/update` · `plan` | [`AgentEvent::Plan`] |
//! | `session/request_permission` (request) | [`AgentEvent::PermissionRequest`] |
//! | `session/prompt` **response** (`stopReason`) | [`AgentEvent::Done`] |
//!
//! Note the turn boundary: unlike a notification, ACP signals end-of-turn as
//! the JSON-RPC *response* to `session/prompt`, carrying `stopReason` + `usage`.

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
const ACP_PROTOCOL_VERSION: u64 = 1;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Driver for any agent speaking the Agent Client Protocol over stdio.
pub struct AcpDriver {
    writer_tx: Option<mpsc::Sender<String>>,
    reader_rx: mpsc::Receiver<AgentEvent>,
    child: Option<Child>,

    session_id: Arc<Mutex<Option<String>>>,
    next_id: Arc<AtomicU64>,

    /// CAP req_id (stringified JSON-RPC id) → (JSON-RPC id, options array) for
    /// the pending `session/request_permission`, so PermissionResponse can
    /// reply with the right `optionId`.
    pending_perms: Arc<Mutex<HashMap<String, (Value, Value)>>>,

    exited: Arc<AtomicBool>,
    exit_status: Arc<Mutex<Option<DriverExitStatus>>>,
}

impl std::fmt::Debug for AcpDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcpDriver")
            .field("session_id", &self.session_id())
            .field("alive", &self.is_alive())
            .finish()
    }
}

impl AcpDriver {
    /// Spawn `opencode acp` in `cwd` and run the initialize + session/new
    /// handshake.
    pub async fn opencode(cwd: impl AsRef<Path>) -> Result<Self, DriverError> {
        Self::builder("opencode", cwd).arg("acp").spawn().await
    }

    /// Begin building an ACP session for an arbitrary agent command.
    pub fn builder(command: impl Into<String>, cwd: impl AsRef<Path>) -> AcpBuilder {
        AcpBuilder {
            command: command.into(),
            args: Vec::new(),
            cwd: cwd.as_ref().to_path_buf(),
        }
    }

    /// The ACP session id assigned during `session/new`.
    pub fn session_id(&self) -> Option<String> {
        self.session_id.lock().ok().and_then(|g| g.clone())
    }
}

#[async_trait]
impl Driver for AcpDriver {
    async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError> {
        let tx = self.writer_tx.as_ref().ok_or(DriverError::AgentExited)?;
        let sid = self.session_id().ok_or_else(|| DriverError::AgentError {
            code: "cap_acp_no_session".into(),
            message: "ACP session not established".into(),
        })?;

        match frame {
            ClientFrame::SessionConfig(_) => Err(DriverError::AgentError {
                code: "cap_session_config_inline_unsupported".into(),
                message: "ACP consumes session config at session/new — re-spawn to change it"
                    .into(),
            }),

            ClientFrame::Prompt { content } => {
                // Fire-and-forget at the JSON-RPC level: the response (carrying
                // stopReason) arrives later and the reader maps it to Done.
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                let req = json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": id,
                    "method": "session/prompt",
                    "params": { "sessionId": sid, "prompt": content_to_prompt(&content) }
                });
                tx.send(req.to_string())
                    .await
                    .map_err(|_| DriverError::AgentExited)
            }

            ClientFrame::Cancel { .. } => {
                // ACP cancel is a notification (no id, no response).
                let note = json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "method": "session/cancel",
                    "params": { "sessionId": sid }
                });
                tx.send(note.to_string())
                    .await
                    .map_err(|_| DriverError::AgentExited)
            }

            ClientFrame::PermissionResponse { req_id, decision } => {
                let (jsonrpc_id, options) = self
                    .pending_perms
                    .lock()
                    .expect("pending_perms mutex poisoned")
                    .remove(&req_id)
                    .ok_or_else(|| DriverError::AgentError {
                        code: "cap_unknown_permission".into(),
                        message: format!("no pending permission with req_id {req_id}"),
                    })?;
                let outcome = match select_option(&options, decision) {
                    Some(option_id) => json!({ "outcome": "selected", "optionId": option_id }),
                    None => {
                        let fallback_id = options.as_array()
                            .and_then(|arr| arr.first())
                            .and_then(|o| o.get("optionId").and_then(Value::as_str))
                            .unwrap_or("");
                        json!({ "outcome": "cancelled", "optionId": fallback_id })
                    },
                };
                let resp = json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": jsonrpc_id,
                    "result": { "outcome": outcome }
                });
                tx.send(resp.to_string())
                    .await
                    .map_err(|_| DriverError::AgentExited)
            }

            ClientFrame::AskUserAnswer { .. } => Err(DriverError::AgentError {
                code: "cap_acp_askuser_unsupported".into(),
                message: "ACP surfaces decisions as permission requests, not free-form asks".into(),
            }),

            ClientFrame::ReverseRpcResult { .. } => Err(DriverError::AgentError {
                code: "cap_reverse_rpc_unsupported".into(),
                message: "ACP driver does not emit reverse RPC".into(),
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

/// Configuration for [`AcpDriver`].
#[derive(Debug, Clone)]
pub struct AcpBuilder {
    command: String,
    args: Vec<String>,
    cwd: PathBuf,
}

impl AcpBuilder {
    pub fn arg(mut self, a: impl Into<String>) -> Self {
        self.args.push(a.into());
        self
    }

    /// Spawn the agent and run the `initialize` + `session/new` handshake.
    pub async fn spawn(self) -> Result<AcpDriver, DriverError> {
        let mut cmd = Command::new(&self.command);
        cmd.args(&self.args)
            .current_dir(&self.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        debug!(command = %self.command, cwd = %self.cwd.display(), "spawning ACP agent");

        let mut child = cmd.spawn().map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                DriverError::BinaryNotFound(self.command.clone())
            } else {
                DriverError::SpawnFailed(e)
            }
        })?;

        let stdin = child.stdin.take().ok_or(DriverError::AgentExited)?;
        let stdout = child.stdout.take().ok_or(DriverError::AgentExited)?;
        let stderr = child.stderr.take().ok_or(DriverError::AgentExited)?;

        let (writer_tx, writer_rx) = mpsc::channel::<String>(32);
        let (reader_tx, reader_rx) = mpsc::channel::<AgentEvent>(128);

        let session_id = Arc::new(Mutex::new(None));
        let next_id = Arc::new(AtomicU64::new(1));
        let pending_responses: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pending_perms: Arc<Mutex<HashMap<String, (Value, Value)>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let exited = Arc::new(AtomicBool::new(false));
        let exit_status = Arc::new(Mutex::new(None));

        tokio::spawn(writer_task(stdin, writer_rx));
        tokio::spawn(reader_task(
            stdout,
            reader_tx.clone(),
            Arc::clone(&pending_responses),
            Arc::clone(&pending_perms),
            Arc::clone(&exited),
        ));
        tokio::spawn(stderr_drain(stderr));

        let driver = AcpDriver {
            writer_tx: Some(writer_tx),
            reader_rx,
            child: Some(child),
            session_id: Arc::clone(&session_id),
            next_id: Arc::clone(&next_id),
            pending_perms,
            exited,
            exit_status,
        };

        // 1. initialize — advertise no fs/terminal capability so the agent does
        //    its own I/O and never calls back to us for files.
        let init_id = next_id.fetch_add(1, Ordering::Relaxed);
        let init_req = json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": init_id,
            "method": "initialize",
            "params": {
                "protocolVersion": ACP_PROTOCOL_VERSION,
                "clientCapabilities": { "fs": { "readTextFile": false, "writeTextFile": false } }
            }
        });
        send_and_await(&driver, init_id, init_req, &pending_responses).await?;

        // 2. session/new
        let new_id = next_id.fetch_add(1, Ordering::Relaxed);
        let new_req = json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": new_id,
            "method": "session/new",
            "params": { "cwd": self.cwd.display().to_string(), "mcpServers": [] }
        });
        let new_resp = send_and_await(&driver, new_id, new_req, &pending_responses).await?;
        let sid = new_resp
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| DriverError::Parse("session/new response missing sessionId".into()))?
            .to_string();
        *session_id.lock().expect("session_id mutex poisoned") = Some(sid.clone());

        // Surface the configured model (if any) on Ready, for parity with other
        // drivers. opencode reports it via configOptions[id=model].currentValue.
        let model = extract_model(&new_resp);
        let _ = reader_tx
            .send(AgentEvent::Ready {
                session_id: sid,
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
    inner: Result<Value, (i64, String)>,
}

async fn send_and_await(
    driver: &AcpDriver,
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
            code: format!("acp_jsonrpc_{code}"),
            message,
        }),
        Ok(Err(_)) => Err(DriverError::AgentExited),
        Err(_) => {
            pending.lock().expect("pending mutex poisoned").remove(&id);
            Err(DriverError::AgentError {
                code: "cap_handshake_timeout".into(),
                message: format!(
                    "ACP agent did not respond to request {id} in {HANDSHAKE_TIMEOUT:?}"
                ),
            })
        }
    }
}

/// Pick the ACP `optionId` matching a CAP decision, from the request's options
/// (each `{ optionId, name, kind }`, kind ∈ allow_once|allow_always|reject_*).
fn select_option(options: &Value, decision: PermissionDecision) -> Option<String> {
    let arr = options.as_array()?;
    let want_allow = matches!(
        decision,
        PermissionDecision::AllowOnce | PermissionDecision::AllowAlways
    );
    let prefer_always = matches!(decision, PermissionDecision::AllowAlways);
    let kind_of = |o: &Value| {
        o.get("kind")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_lowercase()
    };
    let id_of = |o: &Value| o.get("optionId").and_then(Value::as_str).map(String::from);
    // Try an exact-ish kind match first, then fall back to allow/reject family.
    if prefer_always {
        if let Some(o) = arr.iter().find(|o| kind_of(o).contains("allow_always")) {
            return id_of(o);
        }
    }
    arr.iter()
        .find(|o| {
            let k = kind_of(o);
            if want_allow {
                k.contains("allow")
            } else {
                k.contains("reject") || k.contains("deny")
            }
        })
        .and_then(id_of)
}

fn content_to_prompt(content: &[Content]) -> Vec<Value> {
    content
        .iter()
        .filter_map(|c| match c {
            Content::Text { text } => Some(json!({ "type": "text", "text": text })),
            Content::Image { .. } => {
                warn!("acp: Content::Image not yet supported; skipping");
                None
            }
        })
        .collect()
}

fn extract_model(session_new_result: &Value) -> Option<String> {
    session_new_result
        .get("configOptions")
        .and_then(Value::as_array)?
        .iter()
        .find(|o| o.get("id").and_then(Value::as_str) == Some("model"))
        .and_then(|o| o.get("currentValue").and_then(Value::as_str))
        .map(String::from)
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

async fn writer_task(mut stdin: tokio::process::ChildStdin, mut rx: mpsc::Receiver<String>) {
    while let Some(line) = rx.recv().await {
        trace!(line = %line, "→ acp");
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
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>>,
    pending_perms: Arc<Mutex<HashMap<String, (Value, Value)>>>,
    exited: Arc<AtomicBool>,
) {
    let mut lines = BufReader::new(stdout).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }
                trace!(line = %line, "← acp");
                let frame: Value = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, "acp reader: malformed JSON, skipping");
                        continue;
                    }
                };
                for ev in process_frame(&frame, &pending, &pending_perms) {
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
            Ok(Some(line)) => debug!(target: "cap_rs::acp::stderr", "{}", line),
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
    pending: &Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>>,
    pending_perms: &Arc<Mutex<HashMap<String, (Value, Value)>>>,
) -> Vec<AgentEvent> {
    let has_method = frame.get("method").is_some();

    // Response to one of our requests: has `id`, no `method`.
    if frame.get("id").is_some() && !has_method {
        return handle_response(frame, pending);
    }

    let method = frame.get("method").and_then(Value::as_str).unwrap_or("");
    let params = frame.get("params").cloned().unwrap_or(Value::Null);

    // Server-initiated request: has `method` AND `id` (must be answered).
    if frame.get("id").is_some() {
        return handle_server_request(
            method,
            &frame.get("id").cloned().unwrap_or(Value::Null),
            &params,
            pending_perms,
        );
    }

    // Notification (no `id`).
    parse_notification(method, &params)
}

fn handle_response(
    frame: &Value,
    pending: &Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>>,
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

    // Claimed by a handshake awaiter? Hand it the oneshot and stop.
    if let Some(id) = id {
        if let Some(tx) = pending.lock().expect("pending mutex poisoned").remove(&id) {
            let _ = tx.send(result);
            return Vec::new();
        }
    }

    // Unclaimed response: this is the session/prompt turn result. Map a
    // stopReason to Done, an error to Error.
    if let Some(err) = err_payload {
        return vec![AgentEvent::Error {
            code: err
                .get("code")
                .and_then(Value::as_i64)
                .map(|c| format!("acp_jsonrpc_{c}"))
                .unwrap_or_else(|| "acp_error".into()),
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
    if let Some(stop) = res.get("stopReason").and_then(Value::as_str) {
        let stop_reason = map_stop_reason(stop);
        return vec![AgentEvent::Done {
            stop_reason,
            usage: prompt_usage(res.get("usage"), stop_reason),
        }];
    }
    Vec::new()
}

fn handle_server_request(
    method: &str,
    id: &Value,
    params: &Value,
    pending_perms: &Arc<Mutex<HashMap<String, (Value, Value)>>>,
) -> Vec<AgentEvent> {
    let req_id = match id {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => return Vec::new(),
    };
    match method {
        "session/request_permission" => {
            let options = params
                .get("options")
                .cloned()
                .unwrap_or(Value::Array(vec![]));
            pending_perms
                .lock()
                .expect("pending_perms mutex poisoned")
                .insert(req_id.clone(), (id.clone(), options));
            // The tool being gated lives under params.toolCall.
            let tool_call = params.get("toolCall").cloned().unwrap_or(Value::Null);
            let tool = tool_call
                .get("title")
                .or_else(|| tool_call.get("kind"))
                .and_then(Value::as_str)
                .unwrap_or("tool")
                .to_string();
            vec![AgentEvent::PermissionRequest {
                req_id,
                tool,
                intent: tool_call,
                scope: PermissionScope::Execute,
                risk_level: RiskLevel::Medium,
            }]
        }
        "session/elicit" => {
            // ACP elicitation carries the prompt under `question` or `prompt`.
            let prompt = params
                .get("question")
                .or_else(|| params.get("prompt"))
                .and_then(Value::as_str)
                .unwrap_or("agent requests input")
                .to_string();
            let form = params.get("form").cloned();
            let ask_kind = match &form {
                Some(v) if v.get("type").and_then(Value::as_str) == Some("boolean") => {
                    AskKind::YesNo
                }
                Some(v) if v.get("oneOf").is_some() || v.get("enum").is_some() => {
                    AskKind::Schema {
                        schema: form.unwrap_or(Value::Null),
                    }
                }
                _ => AskKind::FreeText,
            };
            vec![AgentEvent::AskUser {
                ask_id: req_id,
                prompt,
                ask_kind,
                options: vec![],
                timeout_seconds: None,
            }]
        }
        // We advertised no fs/terminal capability, so the agent should never
        // call back for those. Anything else is ignored (the agent does not
        // block on it).
        other => {
            trace!(method = other, "acp: unhandled server request, ignoring");
            Vec::new()
        }
    }
}

fn parse_notification(method: &str, params: &Value) -> Vec<AgentEvent> {
    if method != "session/update" {
        return Vec::new();
    }
    let update = params.get("update").cloned().unwrap_or(Value::Null);
    let kind = update
        .get("sessionUpdate")
        .and_then(Value::as_str)
        .unwrap_or("");
    let msg_id = update
        .get("messageId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    match kind {
        "agent_message_chunk" => {
            let text = content_text(update.get("content"));
            if text.is_empty() {
                return Vec::new();
            }
            vec![AgentEvent::TextChunk {
                msg_id,
                text,
                channel: TextChannel::Assistant,
            }]
        }
        "agent_thought_chunk" => {
            let text = content_text(update.get("content"));
            if text.is_empty() {
                return Vec::new();
            }
            vec![AgentEvent::Thought { msg_id, text }]
        }
        "tool_call" => {
            let call_id = update
                .get("toolCallId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let name = update
                .get("kind")
                .or_else(|| update.get("title"))
                .and_then(Value::as_str)
                .unwrap_or("tool")
                .to_string();
            vec![AgentEvent::ToolCallStart {
                call_id,
                name,
                input: update.get("rawInput").cloned().unwrap_or(Value::Null),
            }]
        }
        "tool_call_update" => {
            let status = update.get("status").and_then(Value::as_str).unwrap_or("");
            // Only the terminal states close the tool call.
            if !matches!(status, "completed" | "failed") {
                return Vec::new();
            }
            let call_id = update
                .get("toolCallId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            vec![AgentEvent::ToolCallEnd {
                call_id,
                output: content_text(update.get("content")),
                is_error: status == "failed",
            }]
        }
        "plan" => {
            let entries = parse_plan_entries(update.get("entries").unwrap_or(&Value::Null));
            if entries.is_empty() {
                Vec::new()
            } else {
                vec![AgentEvent::Plan { entries }]
            }
        }
        // usage_update / available_commands_update / user_message_chunk: progress
        // and echo noise — the authoritative usage rides on the Done result.
        _ => Vec::new(),
    }
}

/// Pull text out of an ACP content block or array of blocks.
fn content_text(content: Option<&Value>) -> String {
    let Some(c) = content else {
        return String::new();
    };
    if let Some(arr) = c.as_array() {
        return arr.iter().map(block_text).collect();
    }
    block_text(c)
}

fn block_text(block: &Value) -> String {
    match block.get("type").and_then(Value::as_str) {
        Some("text") => block
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        _ => String::new(),
    }
}

fn map_stop_reason(s: &str) -> StopReason {
    match s {
        "end_turn" => StopReason::EndTurn,
        "max_tokens" | "max_turn_requests" => StopReason::MaxTokens,
        "refusal" => StopReason::Error,
        "cancelled" | "canceled" => StopReason::Cancelled,
        _ => StopReason::EndTurn,
    }
}

fn prompt_usage(usage: Option<&Value>, stop_reason: StopReason) -> Usage {
    let u = usage.cloned().unwrap_or(Value::Null);
    Usage {
        input_tokens: u.get("inputTokens").and_then(Value::as_u64).unwrap_or(0),
        output_tokens: u.get("outputTokens").and_then(Value::as_u64).unwrap_or(0),
        cache_read_tokens: 0,
        cache_creation_tokens: 0,
        thinking_tokens: u.get("thoughtTokens").and_then(Value::as_u64).unwrap_or(0),
        cost_usd_estimate: None,
        duration: None,
        model_id: None,
        stop_reason: Some(stop_reason),
    }
}

fn parse_plan_entries(entries: &Value) -> Vec<crate::core::PlanEntry> {
    use crate::core::{PlanEntry, PlanStatus};
    let Some(arr) = entries.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .enumerate()
        .map(|(idx, e)| PlanEntry {
            id: e
                .get("id")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| format!("t{idx}")),
            content: e
                .get("content")
                .or_else(|| e.get("text"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            status: match e.get("status").and_then(Value::as_str) {
                Some("in_progress") => PlanStatus::InProgress,
                Some("completed") => PlanStatus::Completed,
                _ => PlanStatus::Pending,
            },
            priority: None,
            _meta: None,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests — frames captured from real `opencode acp` (v1.14)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_pending() -> Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult>>>> {
        Arc::new(Mutex::new(HashMap::new()))
    }
    fn empty_perms() -> Arc<Mutex<HashMap<String, (Value, Value)>>> {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[test]
    fn agent_message_chunk_becomes_textchunk() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s",
               "update":{"sessionUpdate":"agent_message_chunk","messageId":"m1",
               "content":{"type":"text","text":"hello"}}}}"#,
        )
        .unwrap();
        let events = process_frame(&frame, &empty_pending(), &empty_perms());
        assert!(matches!(&events[0],
            AgentEvent::TextChunk { text, channel, msg_id }
            if text == "hello" && *channel == TextChannel::Assistant && msg_id == "m1"));
    }

    #[test]
    fn agent_thought_chunk_becomes_thought() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s",
               "update":{"sessionUpdate":"agent_thought_chunk","messageId":"m1",
               "content":{"type":"text","text":"hmm"}}}}"#,
        )
        .unwrap();
        let events = process_frame(&frame, &empty_pending(), &empty_perms());
        assert!(matches!(&events[0], AgentEvent::Thought { text, .. } if text == "hmm"));
    }

    #[test]
    fn prompt_response_with_stop_reason_becomes_done() {
        // The real session/prompt result shape (unclaimed = not a handshake).
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn",
               "usage":{"totalTokens":29796,"inputTokens":29782,"outputTokens":2,"thoughtTokens":12}}}"#,
        )
        .unwrap();
        let events = process_frame(&frame, &empty_pending(), &empty_perms());
        match &events[0] {
            AgentEvent::Done { stop_reason, usage } => {
                assert_eq!(*stop_reason, StopReason::EndTurn);
                assert_eq!(usage.input_tokens, 29782);
                assert_eq!(usage.output_tokens, 2);
                assert_eq!(usage.thinking_tokens, 12);
            }
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn handshake_response_completes_oneshot_not_done() {
        // A claimed response (handshake) must NOT produce a Done event.
        let pending = empty_pending();
        let (tx, mut rx) = oneshot::channel();
        pending.lock().unwrap().insert(2, tx);
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_abc","configOptions":[]}}"#,
        )
        .unwrap();
        let events = process_frame(&frame, &pending, &empty_perms());
        assert!(
            events.is_empty(),
            "claimed handshake response yields no event"
        );
        let got = rx.try_recv().expect("oneshot ready");
        assert_eq!(
            got.inner.unwrap().get("sessionId").and_then(Value::as_str),
            Some("ses_abc")
        );
    }

    #[test]
    fn request_permission_emits_request_and_stores_id() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":7,"method":"session/request_permission","params":{
               "sessionId":"s","toolCall":{"title":"bash","kind":"execute"},
               "options":[{"optionId":"allow","name":"Allow","kind":"allow_once"},
                          {"optionId":"deny","name":"Reject","kind":"reject_once"}]}}"#,
        )
        .unwrap();
        let perms = empty_perms();
        let events = process_frame(&frame, &empty_pending(), &perms);
        assert!(matches!(&events[0],
            AgentEvent::PermissionRequest { req_id, tool, .. } if req_id == "7" && tool == "bash"));
        let (_, options) = perms.lock().unwrap().get("7").cloned().expect("stored");
        // The stored options drive optionId selection on the response.
        assert_eq!(
            select_option(&options, PermissionDecision::AllowOnce).as_deref(),
            Some("allow")
        );
        assert_eq!(
            select_option(&options, PermissionDecision::Deny).as_deref(),
            Some("deny")
        );
    }

    #[test]
    fn unknown_session_update_is_ignored() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s",
               "update":{"sessionUpdate":"available_commands_update","availableCommands":[]}}}"#,
        )
        .unwrap();
        assert!(process_frame(&frame, &empty_pending(), &empty_perms()).is_empty());
    }

    #[test]
    fn session_elicit_becomes_ask_user() {
        let frame: Value = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":8,"method":"session/elicit","params":{
               "sessionId":"s","question":"Which DB?",
               "form":{"type":"string","oneOf":[{"const":"pg","title":"PostgreSQL"},
                                                 {"const":"sqlite","title":"SQLite"}]}}}"#,
        )
        .unwrap();
        let events = process_frame(&frame, &empty_pending(), &empty_perms());
        match &events[0] {
            AgentEvent::AskUser { ask_id, prompt, ask_kind, .. } => {
                assert_eq!(ask_id, "8");
                assert_eq!(prompt, "Which DB?");
                assert!(matches!(ask_kind, AskKind::Schema { .. }));
            }
            other => panic!("expected AskUser, got: {other:?}"),
        }
    }
}
