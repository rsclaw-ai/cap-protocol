//! Stream-JSON driver — fast-path for Claude Code SDK and compatible CLIs.
//!
//! Wire format: line-delimited JSON over the agent process's stdio.
//! Each line is one JSON object; messages flow bidirectionally.
//!
//! Spec mapping: see [docs/cap-v1.md §6.2 + Appendix C.1](https://github.com/rsclaw-ai/cap-protocol/blob/main/docs/cap-v1.md).
//!
//! Supported agents today:
//! - **Claude Code** via `claude -p --input-format=stream-json --output-format=stream-json`
//! - **Codex** via the same Claude-compatible stream-json shape
//!
//! openclaude and other Anthropic-SDK-compatible CLIs should also work
//! with `ClaudeCodeDriver::builder(cwd).bin("openclaude").spawn()`.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use crate::core::{AgentEvent, ClientFrame, Content, StopReason, TextChannel, Usage};
use crate::driver::{Driver, DriverError, DriverExitStatus};

/// Driver that talks to the Claude Code CLI (or any stream-json compatible
/// agent) via the SDK's `--input-format=stream-json --output-format=stream-json`
/// protocol.
#[derive(Debug)]
pub struct ClaudeCodeDriver {
    /// Channel to send ClientFrames to the writer task.
    /// `None` once [`Self::finish_input`] has been called — agent will
    /// see stdin EOF and begin its terminal sequence.
    writer_tx: Option<mpsc::Sender<String>>,

    /// Channel to receive AgentEvents from the reader task.
    reader_rx: mpsc::Receiver<AgentEvent>,

    /// Child handle for lifecycle management.
    child: Option<Child>,

    /// Set by the reader task on stdout EOF or by `shutdown`.
    exited: std::sync::Arc<std::sync::atomic::AtomicBool>,

    /// Populated by `shutdown` after the child reaps, or by the reader
    /// task with `Disconnected` if the channel dies before shutdown.
    exit_status: std::sync::Arc<std::sync::Mutex<Option<DriverExitStatus>>>,
}

impl ClaudeCodeDriver {
    /// Signal that no more user input will be sent in this session. This
    /// closes claude's stdin, after which claude will process any pending
    /// frames, emit its final `result` frame, and exit.
    ///
    /// For one-shot interactions this should be called immediately after
    /// the final [`Driver::send`]. For long-running sessions (multi-turn
    /// interactive use) leave stdin open and rely on [`Driver::shutdown`]
    /// to terminate.
    pub fn finish_input(&mut self) {
        self.writer_tx = None;
    }
}

impl ClaudeCodeDriver {
    /// Spawn a fresh Claude Code session in the given working directory,
    /// using the binary on PATH (or `$CLAUDE_BIN` env override).
    ///
    /// Defaults to **persistent session mode** via `--replay-user-messages`
    /// — one claude process serves an unbounded number of turns. Call
    /// [`Self::finish_input`] when you're done so claude can exit
    /// gracefully. For multi-turn use, just keep calling
    /// [`Driver::send`].
    pub async fn spawn(cwd: impl AsRef<Path>) -> Result<Self, DriverError> {
        Self::builder(cwd).spawn().await
    }

    /// Begin building a Claude Code session with custom options.
    pub fn builder(cwd: impl AsRef<Path>) -> ClaudeCodeDriverBuilder {
        ClaudeCodeDriverBuilder {
            bin: None,
            cwd: cwd.as_ref().to_path_buf(),
            model: None,
            session_id: None,
            resume: None,
            replay_user_messages: true,
            // Permission-bypass is opt-in. CAP spec §13.1 treats injected
            // input as privileged, and the driver has no way to route
            // claude's permission prompts back through CAP yet — so the
            // safe default is to leave claude's prompting on. Callers that
            // accept the trade-off invoke `.dangerously_skip_permissions(true)`.
            dangerously_skip_permissions: false,
            is_opencode: false,
            is_codex: false,
        }
    }

    /// Builder pre-configured for OpenCode via stream-json.
    ///
    /// Spawns `opencode run --output-format stream-json` and reads
    /// Claude Code-compatible NDJSON frames from stdout. The prompt is
    /// delivered via stdin (same as Claude Code), so the existing
    /// `send(ClientFrame::Prompt)` flow works unchanged.
    ///
    /// ```no_run
    /// # async fn run() -> anyhow::Result<()> {
    /// use cap_rs::driver::stream_json::ClaudeCodeDriver;
    ///
    /// let driver = ClaudeCodeDriver::opencode_builder(".")
    ///     .spawn()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn opencode_builder(cwd: impl AsRef<Path>) -> ClaudeCodeDriverBuilder {
        ClaudeCodeDriverBuilder {
            bin: Some("opencode".to_string()),
            cwd: cwd.as_ref().to_path_buf(),
            model: None,
            session_id: None,
            resume: None,
            replay_user_messages: false,
            dangerously_skip_permissions: false,
            is_opencode: true,
            is_codex: false,
        }
    }

    /// Builder pre-configured for Codex via stream-json.
    ///
    /// Spawns `codex exec --input-format stream-json --output-format
    /// stream-json` and reads Claude Code-compatible NDJSON frames from
    /// stdout. Codex's exec subcommand has a native multi-turn loop
    /// behind these two flags — it stays alive until stdin EOF, reading
    /// successive `{"type":"user", ...}` frames and emitting
    /// `system/init`, `assistant`, `result` frames identical in shape
    /// to claudecode's protocol.
    ///
    /// Replaces the older `codex_mcp` driver path for the cap_live use
    /// case: stream-json gives us first-class `Thought`/`TextChunk`
    /// streaming via the existing claudecode parser, and there's no
    /// MCP `tools/call` JSON-RPC envelope to traverse — turns are
    /// noticeably faster.
    ///
    /// ```no_run
    /// # async fn run() -> anyhow::Result<()> {
    /// use cap_rs::driver::stream_json::ClaudeCodeDriver;
    ///
    /// let driver = ClaudeCodeDriver::codex_builder(".")
    ///     .spawn()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn codex_builder(cwd: impl AsRef<Path>) -> ClaudeCodeDriverBuilder {
        ClaudeCodeDriverBuilder {
            bin: Some("codex".to_string()),
            cwd: cwd.as_ref().to_path_buf(),
            model: None,
            session_id: None,
            resume: None,
            replay_user_messages: false,
            // Driver caller decides whether to bypass codex sandbox
            // prompts via `.dangerously_skip_permissions(true)` —
            // maps to `--dangerously-bypass-approvals-and-sandbox`
            // for codex (mirrors the spec §13.1 same-semantics flag
            // used for claudecode).
            dangerously_skip_permissions: false,
            is_opencode: false,
            is_codex: true,
        }
    }

    async fn spawn_inner(b: ClaudeCodeDriverBuilder) -> Result<Self, DriverError> {
        let ClaudeCodeDriverBuilder {
            bin,
            cwd,
            model,
            session_id,
            resume,
            replay_user_messages,
            dangerously_skip_permissions,
            is_opencode,
            is_codex,
        } = b;

        let bin = if is_opencode {
            std::env::var("OPENCODE_BIN")
                .ok()
                .or(bin)
                .unwrap_or_else(|| "opencode".to_string())
        } else if is_codex {
            std::env::var("CODEX_BIN")
                .ok()
                .or(bin)
                .unwrap_or_else(|| "codex".to_string())
        } else {
            std::env::var("CLAUDE_BIN")
                .ok()
                .or(bin)
                .unwrap_or_else(|| "claude".to_string())
        };

        let mut cmd = Command::new(&bin);

        if is_codex {
            // Codex: `codex exec [resume <thread_id>] --input-format stream-json
            //         --output-format stream-json --skip-git-repo-check
            //         --sandbox workspace-write`.
            //
            // Native multi-turn: codex's `exec` subcommand reads
            // successive `{"type":"user", ...}` frames from stdin and
            // stays alive until stdin EOF — no `--persist` flag
            // analogous to opencode is required. Output frames are
            // Claude-compatible (system/init, assistant text/thinking
            // chunks, result), so the existing claudecode parser
            // handles them unchanged.
            //
            // Resume: `codex exec resume <thread_id>` is a subcommand
            // (not a flag) — codex picks the named thread off disk
            // and replays its history into the new process's memory.
            //
            // sandbox=workspace-write matches the prior codex_mcp
            // builder's default and is the right policy for cap_live
            // use (sub-agent runs inside its own cwd, no escape).
            // Permission-bypass (--dangerously-bypass-approvals-and-sandbox)
            // is opt-in via `.dangerously_skip_permissions(true)`.
            cmd.arg("exec");
            if let Some(rid) = &resume {
                cmd.arg("resume").arg(rid);
            }
            cmd.arg("--input-format")
                .arg("stream-json")
                .arg("--output-format")
                .arg("stream-json")
                .arg("--skip-git-repo-check")
                .arg("--sandbox")
                .arg("workspace-write")
                .current_dir(&cwd)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            if dangerously_skip_permissions {
                cmd.arg("--dangerously-bypass-approvals-and-sandbox");
            }
            if let Some(m) = &model {
                cmd.arg("-m").arg(m);
            }
        } else if is_opencode {
            // OpenCode: `opencode run --output-format stream-json --persist`
            // `--persist` keeps opencode alive across turns — without it,
            // opencode reads ONE prompt then exits, which makes
            // multi-turn `cap_live` sessions hit a 300s timeout on the
            // second turn (no Done frame ever comes for the second
            // prompt because the process is gone). The persist mode
            // landed in opencode 1.15.16+ behind this flag; older
            // binaries will reject `--persist` at argv parse time,
            // which surfaces as `BinaryNotFound`-equivalent.
            //
            // Backward compatibility: cap-rs reader_task's EOF→Done
            // synthesis still covers older opencode binaries
            // running without --persist (single-shot mode); the only
            // observable difference is that multi-turn sessions
            // re-spawn the process each turn.
            cmd.arg("run")
                .arg("--output-format")
                .arg("stream-json")
                .arg("--persist")
                .current_dir(&cwd)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            if let Some(m) = &model {
                cmd.arg("--model").arg(m);
            }
            // Resume an existing opencode session by id. opencode's
            // `--session <id>` resumes that specific session's
            // history; without it opencode creates a fresh session
            // every spawn.
            if let Some(rid) = &resume {
                cmd.arg("--session").arg(rid);
            }
        } else {
            // Claude Code: `claude -p --input-format=stream-json --output-format=stream-json`
            cmd.arg("-p")
                .arg("--input-format=stream-json")
                .arg("--output-format=stream-json")
                .arg("--verbose")
                .current_dir(&cwd)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);

            if dangerously_skip_permissions {
                cmd.arg("--dangerously-skip-permissions");
            }
            if replay_user_messages {
                cmd.arg("--replay-user-messages");
            }
            if let Some(m) = &model {
                cmd.arg("--model").arg(m);
            }
            if let Some(sid) = &session_id {
                cmd.arg("--session-id").arg(sid);
            }
            if let Some(rid) = &resume {
                cmd.arg("--resume").arg(rid);
            }
        }

        // Strip parent-session env vars so claude doesn't refuse to launch
        // when cap-rs itself is running inside another Claude Code session.
        // See "Claude Code cannot be launched inside another Claude Code session"
        // — claude bails when CLAUDECODE is set in its environment.
        for var in [
            "CLAUDECODE",
            "CLAUDE_CODE_ENTRYPOINT",
            "CLAUDE_CODE_SSE_PORT",
            "CLAUDE_CODE_OAUTH_TOKEN",
            "CLAUDE_CODE_SESSION_ID",
            "CLAUDE_SESSION_ID",
        ] {
            cmd.env_remove(var);
        }
        if is_codex {
            // Codex looks at these to detect "we're already inside a
            // codex session" and bails the same way claude does.
            for var in ["CODEX_HEADLESS", "CODEX_INTERACTIVE"] {
                cmd.env_remove(var);
            }
        }

        debug!(
            bin = %bin,
            cwd = %cwd.display(),
            is_opencode,
            is_codex,
            session_mode = replay_user_messages,
            resume = ?resume,
            session_id = ?session_id,
            "spawning agent",
        );

        let mut child = cmd.spawn().map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                DriverError::BinaryNotFound(bin.clone())
            } else {
                DriverError::SpawnFailed(e)
            }
        })?;

        let stdout = child.stdout.take().ok_or(DriverError::AgentExited)?;
        let stderr = child.stderr.take().ok_or(DriverError::AgentExited)?;

        let (reader_tx, reader_rx) = mpsc::channel::<AgentEvent>(64);

        let exited = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let exit_status = std::sync::Arc::new(std::sync::Mutex::new(None));

        // Writer task: forward queued lines to the agent's stdin.
        // Both Claude Code and OpenCode receive prompts via stdin.
        let stdin = child.stdin.take().ok_or(DriverError::AgentExited)?;
        let (writer_tx, writer_rx) = mpsc::channel::<String>(32);
        tokio::spawn(writer_task(stdin, writer_rx));

        // Reader task: parse NDJSON from stdout into AgentEvents.
        tokio::spawn(reader_task(
            stdout,
            reader_tx,
            std::sync::Arc::clone(&exited),
        ));

        // Stderr drain — log only, don't surface as events.
        tokio::spawn(stderr_drain(stderr));

        Ok(Self {
            writer_tx: Some(writer_tx),
            reader_rx,
            child: Some(child),
            exited,
            exit_status,
        })
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Fluent configuration for [`ClaudeCodeDriver`].
///
/// ```no_run
/// # async fn run() -> anyhow::Result<()> {
/// use cap_rs::driver::stream_json::ClaudeCodeDriver;
///
/// // Persistent multi-turn session (default).
/// let chat = ClaudeCodeDriver::builder("/path/to/workspace").spawn().await?;
///
/// // One-shot, with a specific model.
/// let oneshot = ClaudeCodeDriver::builder(".")
///     .model("claude-opus-4-7")
///     .replay_user_messages(false)
///     .spawn()
///     .await?;
///
/// // Resume an earlier session.
/// let resumed = ClaudeCodeDriver::builder(".")
///     .resume("00000000-0000-0000-0000-deadbeefcafe")
///     .spawn()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ClaudeCodeDriverBuilder {
    bin: Option<String>,
    cwd: PathBuf,
    model: Option<String>,
    session_id: Option<String>,
    resume: Option<String>,
    replay_user_messages: bool,
    dangerously_skip_permissions: bool,
    /// When true, use OpenCode CLI shape instead of Claude Code.
    is_opencode: bool,
    /// When true, use Codex CLI shape (`codex exec --input-format
    /// stream-json --output-format stream-json`). Mutually exclusive
    /// with `is_opencode`; both false = claudecode/openclaude.
    is_codex: bool,
}

impl ClaudeCodeDriverBuilder {
    /// Override the binary used (default: `claude` on PATH, or `$CLAUDE_BIN`).
    pub fn bin(mut self, bin: impl Into<String>) -> Self {
        self.bin = Some(bin.into());
        self
    }

    /// Override the model (default: claude's own default).
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Use a specific session UUID for this session (must be a valid UUID
    /// per claude's `--session-id` requirements). If unset, claude
    /// generates one and reports it in the `Ready` event.
    pub fn session_id(mut self, uuid: impl Into<String>) -> Self {
        self.session_id = Some(uuid.into());
        self
    }

    /// Resume a previously persisted conversation by session UUID. Pass
    /// the `session_id` you got from a prior session's `Ready` event.
    pub fn resume(mut self, uuid: impl Into<String>) -> Self {
        self.resume = Some(uuid.into());
        self
    }

    /// Whether to start in **persistent session mode** (default: `true`).
    ///
    /// When `true`, claude stays alive after each turn waiting for more
    /// user messages — this is what enables real-time multi-turn
    /// conversation in a single process. When `false`, claude reads one
    /// prompt, responds, and exits (one-shot, lower latency to first
    /// answer but no follow-ups in the same process).
    ///
    /// Implementation note: this maps directly to claude's
    /// `--replay-user-messages` flag.
    pub fn replay_user_messages(mut self, on: bool) -> Self {
        self.replay_user_messages = on;
        self
    }

    /// Whether to pass `--dangerously-skip-permissions` (default: `false`).
    ///
    /// When `false` (the safe default), claude prompts for permission on
    /// tool calls — the driver does not currently forward those prompts
    /// over CAP, so the agent simply blocks until a human intervenes
    /// in the terminal claude is attached to.
    ///
    /// Set to `true` ONLY when you accept that the driver auto-approves
    /// every tool call. Required for non-interactive batch use, but per
    /// CAP spec §13.1 this is a privileged escalation — orchestrators
    /// SHOULD gate the choice behind a user-visible policy.
    pub fn dangerously_skip_permissions(mut self, on: bool) -> Self {
        self.dangerously_skip_permissions = on;
        self
    }

    /// Spawn the configured Claude Code session.
    pub async fn spawn(self) -> Result<ClaudeCodeDriver, DriverError> {
        ClaudeCodeDriver::spawn_inner(self).await
    }
}

#[async_trait]
impl Driver for ClaudeCodeDriver {
    async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError> {
        let tx = self.writer_tx.as_ref().ok_or(DriverError::AgentExited)?;
        let line = encode_client_frame(&frame)?;
        if line.is_empty() {
            return Ok(());
        }
        trace!(line = %line, "→ agent");
        tx.send(line).await.map_err(|_| DriverError::AgentExited)?;
        Ok(())
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
                    Ok(s) => {
                        if let Some(code) = s.code() {
                            DriverExitStatus::Exited { code: Some(code) }
                        } else {
                            // killed by signal
                            DriverExitStatus::Killed
                        }
                    }
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

    fn prompt_after_ready(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Writer / reader / stderr tasks
// ---------------------------------------------------------------------------

async fn writer_task(mut stdin: tokio::process::ChildStdin, mut rx: mpsc::Receiver<String>) {
    while let Some(line) = rx.recv().await {
        if let Err(e) = stdin.write_all(line.as_bytes()).await {
            warn!(error = %e, "writer task: write failed, exiting");
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
    exited: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    // Track whether a real terminal `Done` event has been emitted by
    // a parsed stream frame (claudecode's `{"type":"result"}`).
    //
    // Why this matters: opencode's `opencode run --output-format
    // stream-json` is one-shot per process and DOES NOT emit a
    // claudecode-style `result` terminator. It just streams its
    // assistant messages and exits. Without a synthetic Done on EOF,
    // CapLiveManager waits up to PROMPT_TIMEOUT (300s) for a Done
    // that will never come — every opencode turn appears to "hang"
    // 5 minutes after completion before erroring. With this synth,
    // the EOF on opencode's stdout becomes the Done signal.
    //
    // claudecode normally emits `result` before EOF, so this synth
    // only fires in pathological cases there (driver killed, sudden
    // exit) where it's still the right behaviour — the upstream
    // CapLiveManager waiter would otherwise hang forever.
    let mut done_emitted = false;
    let mut lines = BufReader::new(stdout).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                trace!(line = %line, "← agent");
                for event in parse_stream_line(&line, false) {
                    trace!(event = ?event, "parsed event");
                    if matches!(event, AgentEvent::Done { .. }) {
                        done_emitted = true;
                    }
                    if tx.send(event).await.is_err() {
                        exited.store(true, std::sync::atomic::Ordering::Relaxed);
                        return;
                    }
                }
            }
            Ok(None) => {
                debug!("reader: stdout EOF");
                if !done_emitted {
                    // Synthesise a Done so waiters don't hang. We
                    // can't reconstruct full Usage from here, but
                    // EndTurn + empty Usage is the right shape for
                    // "session ended cleanly without a result frame"
                    // (opencode's normal path) or "process disappeared
                    // mid-turn" (claudecode crash).
                    debug!("reader: synthesising Done on EOF (no result frame)");
                    let _ = tx
                        .send(AgentEvent::Done {
                            stop_reason: StopReason::EndTurn,
                            usage: Usage::default(),
                        })
                        .await;
                }
                exited.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }
            Err(e) => {
                warn!(error = %e, "reader: read error");
                if !done_emitted {
                    let _ = tx
                        .send(AgentEvent::Done {
                            stop_reason: StopReason::Error,
                            usage: Usage::default(),
                        })
                        .await;
                }
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
            Ok(Some(line)) => debug!(target: "cap_rs::stream_json::stderr", "{}", line),
            Ok(None) => return,
            Err(e) => {
                warn!(error = %e, "stderr read error");
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Wire encoding / decoding
// ---------------------------------------------------------------------------

fn encode_client_frame(frame: &ClientFrame) -> Result<String, DriverError> {
    match frame {
        ClientFrame::Prompt { content } => {
            let parts: Vec<Value> = content
                .iter()
                .map(|c| match c {
                    Content::Text { text } => json!({"type": "text", "text": text}),
                    Content::Image { mime, data } => json!({
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": mime,
                            "data": base64_encode(data.as_ref()),
                        }
                    }),
                })
                .collect();
            let frame_json = json!({
                "type": "user",
                "message": {
                    "role": "user",
                    "content": parts
                }
            });
            Ok(frame_json.to_string())
        }
        ClientFrame::Cancel { .. } => {
            // Claude SDK has no in-band cancel — callers should invoke
            // [`Driver::shutdown`] instead. We surface this as a typed
            // error matching spec §14.2 `-32008 cap_cancel_unsupported`
            // rather than smuggling a no-op frame onto the wire.
            Err(DriverError::AgentError {
                code: "cap_cancel_unsupported".into(),
                message: "stream-json binding has no in-band cancel; call Driver::shutdown".into(),
            })
        }
        ClientFrame::SessionConfig(_) => Ok(String::new()),
        ClientFrame::AskUserAnswer { ask_id, value } => {
            // Map to a text continuation. Claude doesn't have a native
            // structured-answer protocol, so we serialize the value.
            let text = format!("[answer to {ask_id}]: {value}");
            Ok(json!({
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [{"type": "text", "text": text}]
                }
            })
            .to_string())
        }
        ClientFrame::PermissionResponse { req_id, decision } => {
            let text = format!("[permission {req_id}]: {decision:?}");
            Ok(json!({
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [{"type": "text", "text": text}]
                }
            })
            .to_string())
        }
        ClientFrame::ReverseRpcResult { .. } => Err(DriverError::AgentError {
            code: "cap_reverse_rpc_unsupported".into(),
            message: "stream-json driver does not emit reverse RPC".into(),
        }),
    }
}

/// Parse one Claude stream-json frame into zero or more CAP events.
fn parse_stream_line(line: &str, strict: bool) -> Vec<AgentEvent> {
    match serde_json::from_str::<Value>(line) {
        Ok(value) => parse_stream_frame(&value),
        Err(e) => {
            warn!(error = %e, raw = %line, "reader: malformed JSON");
            if strict {
                vec![AgentEvent::Error {
                    code: "parse_failed".into(),
                    message: e.to_string(),
                    retryable: false,
                    details: Some(json!({ "raw": line })),
                }]
            } else {
                Vec::new()
            }
        }
    }
}

fn parse_stream_frame(frame: &Value) -> Vec<AgentEvent> {
    let kind = frame.get("type").and_then(Value::as_str).unwrap_or("");
    match kind {
        "system" => match frame.get("subtype").and_then(Value::as_str).unwrap_or("") {
            "init" => vec![AgentEvent::Ready {
                session_id: frame
                    .get("session_id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                version: crate::core::CAP_PROTOCOL_VERSION.into(),
                model: frame.get("model").and_then(Value::as_str).map(String::from),
            }],
            _ => vec![],
        },

        "assistant" => {
            let msg = frame.get("message").cloned().unwrap_or(Value::Null);
            let msg_id = msg
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let content = msg
                .get("content")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();

            let mut events = Vec::new();
            for block in content {
                let btype = block.get("type").and_then(Value::as_str).unwrap_or("");
                match btype {
                    "text" => {
                        let text = block
                            .get("text")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        if !text.is_empty() {
                            events.push(AgentEvent::TextChunk {
                                msg_id: msg_id.clone(),
                                text,
                                channel: TextChannel::Assistant,
                            });
                        }
                    }
                    "thinking" => {
                        let text = block
                            .get("thinking")
                            .and_then(Value::as_str)
                            .or_else(|| block.get("text").and_then(Value::as_str))
                            .unwrap_or_default()
                            .to_string();
                        if !text.is_empty() {
                            events.push(AgentEvent::Thought {
                                msg_id: msg_id.clone(),
                                text,
                            });
                        }
                    }
                    "tool_use" => {
                        events.push(AgentEvent::ToolCallStart {
                            call_id: block
                                .get("id")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            name: block
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            input: block.get("input").cloned().unwrap_or(Value::Null),
                        });
                    }
                    _ => {
                        trace!(block_type = btype, "ignoring unknown assistant block");
                    }
                }
            }
            events
        }

        "user" => {
            // Tool results come back from claude as user messages.
            let content = frame
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let mut events = Vec::new();
            for block in content {
                if block.get("type").and_then(Value::as_str) == Some("tool_result") {
                    let call_id = block
                        .get("tool_use_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let output = extract_tool_result_output(&block);
                    let is_error = block
                        .get("is_error")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    events.push(AgentEvent::ToolCallEnd {
                        call_id,
                        output,
                        is_error,
                        duration: block
                            .get("duration_ms")
                            .and_then(Value::as_u64)
                            .map(std::time::Duration::from_millis),
                    });
                }
            }
            events
        }

        "result" => {
            let subtype = frame
                .get("subtype")
                .and_then(Value::as_str)
                .unwrap_or("success");
            if subtype.starts_with("error") {
                let error = frame.get("error").cloned().unwrap_or(Value::Null);
                let code = error
                    .get("type")
                    .or_else(|| error.get("code"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("agent error")
                    .to_string();
                let usage = parse_usage(frame);
                vec![
                    AgentEvent::Error {
                        code,
                        message,
                        retryable: false,
                        details: Some(error),
                    },
                    AgentEvent::Done {
                        stop_reason: StopReason::Error,
                        usage,
                    },
                ]
            } else {
                let usage = parse_usage(frame);
                let stop_reason = usage.stop_reason.unwrap_or(StopReason::EndTurn);
                vec![AgentEvent::Done { stop_reason, usage }]
            }
        }

        "stream_event" => {
            // Token-level streaming deltas (content_block_delta).
            // Emitted by Claude Code with --include-partial-messages and by
            // OpenCode's --output-format stream-json encoder.
            let ev = frame.get("event").cloned().unwrap_or(Value::Null);
            let etype = ev.get("type").and_then(Value::as_str).unwrap_or("");
            if etype != "content_block_delta" {
                return vec![];
            }
            let delta = ev.get("delta").cloned().unwrap_or(Value::Null);
            let dtype = delta.get("type").and_then(Value::as_str).unwrap_or("");
            match dtype {
                "text_delta" => {
                    let text = delta
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    if text.is_empty() {
                        vec![]
                    } else {
                        vec![AgentEvent::TextChunk {
                            msg_id: String::new(),
                            text,
                            channel: TextChannel::Assistant,
                        }]
                    }
                }
                "thinking_delta" => {
                    let text = delta
                        .get("thinking")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    if text.is_empty() {
                        vec![]
                    } else {
                        vec![AgentEvent::Thought {
                            msg_id: String::new(),
                            text,
                        }]
                    }
                }
                _ => vec![],
            }
        }

        other => {
            trace!(frame_type = other, "ignoring unknown stream-json frame");
            vec![]
        }
    }
}

fn extract_tool_result_output(block: &Value) -> String {
    match block.get("content") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn parse_usage(frame: &Value) -> Usage {
    let u = frame.get("usage").cloned().unwrap_or(Value::Null);
    let stop_reason = frame
        .get("subtype")
        .and_then(Value::as_str)
        .map(|s| match s {
            "success" => StopReason::EndTurn,
            "error_max_turns" => StopReason::MaxTokens,
            s if s.starts_with("error") => StopReason::Error,
            _ => StopReason::EndTurn,
        });
    Usage {
        input_tokens: u.get("input_tokens").and_then(Value::as_u64).unwrap_or(0),
        output_tokens: u.get("output_tokens").and_then(Value::as_u64).unwrap_or(0),
        cache_read_tokens: u
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_creation_tokens: u
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        thinking_tokens: u
            .get("thinking_tokens")
            .or_else(|| u.get("reasoning_output_tokens"))
            .or_else(|| frame.get("thinking_tokens"))
            .or_else(|| frame.get("reasoning_output_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cost_usd_estimate: frame.get("total_cost_usd").and_then(Value::as_f64),
        duration: frame
            .get("duration_ms")
            .and_then(Value::as_u64)
            .map(std::time::Duration::from_millis),
        // `modelUsage` is a map keyed by model_id with per-model usage —
        // pick the entry with the most output tokens rather than the
        // dictionary's first key, which would be insertion-order-dependent
        // and effectively random when multiple models served the turn.
        model_id: frame
            .get("modelUsage")
            .and_then(Value::as_object)
            .and_then(|m| {
                m.iter()
                    .max_by_key(|(_, v)| {
                        v.get("output_tokens").and_then(Value::as_u64).unwrap_or(0)
                    })
                    .map(|(k, _)| k.clone())
            }),
        stop_reason,
    }
}

// base64 implementation lives in `crate::core::base64` so the serde adapter
// for Content::Image shares the same encoder.
use crate::core::base64::encode as base64_encode;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_init_frame() {
        let v: Value = serde_json::from_str(
            r#"{"type":"system","subtype":"init","session_id":"sess_1","model":"claude-opus-4-7"}"#,
        )
        .unwrap();
        let events = parse_stream_frame(&v);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], AgentEvent::Ready { .. }));
    }

    #[test]
    fn parse_assistant_text() {
        let v: Value = serde_json::from_str(
            r#"{"type":"assistant","message":{"id":"msg_1","content":[{"type":"text","text":"hello"}]}}"#,
        )
        .unwrap();
        let events = parse_stream_frame(&v);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::TextChunk { text, .. } => assert_eq!(text, "hello"),
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn parse_tool_use() {
        let v: Value = serde_json::from_str(
            r#"{"type":"assistant","message":{"id":"m","content":[
                {"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}}
            ]}}"#,
        )
        .unwrap();
        let events = parse_stream_frame(&v);
        match &events[0] {
            AgentEvent::ToolCallStart { name, .. } => assert_eq!(name, "Bash"),
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn parse_result_with_usage() {
        let v: Value = serde_json::from_str(
            r#"{"type":"result","subtype":"success","duration_ms":1500,"total_cost_usd":0.0021,
                "usage":{"input_tokens":10,"output_tokens":20,"thinking_tokens":3,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}"#,
        )
        .unwrap();
        let events = parse_stream_frame(&v);
        match &events[0] {
            AgentEvent::Done { usage, stop_reason } => {
                assert_eq!(*stop_reason, StopReason::EndTurn);
                assert_eq!(usage.input_tokens, 10);
                assert_eq!(usage.output_tokens, 20);
                assert_eq!(usage.thinking_tokens, 3);
                assert_eq!(usage.cost_usd_estimate, Some(0.0021));
            }
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn encode_simple_prompt() {
        let frame = ClientFrame::Prompt {
            content: vec![Content::text("hi")],
        };
        let line = encode_client_frame(&frame).unwrap();
        let v: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["type"], "user");
        assert_eq!(v["message"]["content"][0]["text"], "hi");
    }

    #[test]
    fn base64_rfc4648_vectors() {
        // RFC 4648 §10 standard test vectors.
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");

        // Binary edge cases.
        assert_eq!(base64_encode(&[0u8; 3]), "AAAA");
        assert_eq!(base64_encode(&[0xffu8; 3]), "////");

        // Every byte value 0..=255 should round through cleanly.
        let all_bytes: Vec<u8> = (0u8..=255).collect();
        let encoded = base64_encode(&all_bytes);
        // ceil(256/3)*4 = 344.
        assert_eq!(encoded.len(), 344);
        assert!(
            encoded
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=')
        );
    }

    #[test]
    fn parse_stream_event_text_delta() {
        let v: Value = serde_json::from_str(
            r#"{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}}"#,
        )
        .unwrap();
        let events = parse_stream_frame(&v);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::TextChunk { text, channel, .. } => {
                assert_eq!(text, "Hello");
                assert_eq!(*channel, TextChannel::Assistant);
            }
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn parse_stream_event_thinking_delta() {
        let v: Value = serde_json::from_str(
            r#"{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Let me think..."}}}"#,
        )
        .unwrap();
        let events = parse_stream_frame(&v);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::Thought { text, .. } => assert_eq!(text, "Let me think..."),
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn parse_stream_event_ignores_unknown() {
        let v: Value = serde_json::from_str(
            r#"{"type":"stream_event","event":{"type":"message_start","message":{}}}"#,
        )
        .unwrap();
        let events = parse_stream_frame(&v);
        assert!(events.is_empty());
    }

    #[test]
    fn strict_parse_line_emits_parse_failed_error_for_malformed_json() {
        let events = parse_stream_line("{not json", true);
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::Error {
                code, retryable, ..
            } => {
                assert_eq!(code, "parse_failed");
                assert!(!retryable);
            }
            other => panic!("expected parse_failed error, got {other:?}"),
        }
    }
}
