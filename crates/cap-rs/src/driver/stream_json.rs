//! Stream-JSON driver — fast-path for Claude Code SDK and openclaude.
//!
//! Wire format: line-delimited JSON over the agent process's stdio.
//! Each line is one JSON object; messages flow bidirectionally.
//!
//! Spec mapping: see [docs/cap-v1.md §6.2 + Appendix C.1](https://github.com/rsclaw-ai/cap-protocol/blob/main/docs/cap-v1.md).
//!
//! Supported agent today:
//! - **Claude Code** via `claude -p --input-format=stream-json --output-format=stream-json`
//!
//! openclaude and other Anthropic-SDK-compatible CLIs should also work
//! with [`ClaudeCodeDriver::spawn_with`] pointing at their binary.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use crate::core::{AgentEvent, ClientFrame, Content, StopReason, TextChannel, Usage};
use crate::driver::{Driver, DriverError};

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
            dangerously_skip_permissions: true,
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
        } = b;

        let bin = bin
            .or_else(|| std::env::var("CLAUDE_BIN").ok())
            .unwrap_or_else(|| "claude".to_string());

        let mut cmd = Command::new(&bin);
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

        debug!(
            bin = %bin,
            cwd = %cwd.display(),
            session_mode = replay_user_messages,
            resume = ?resume,
            session_id = ?session_id,
            "spawning claude",
        );

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
        let (reader_tx, reader_rx) = mpsc::channel::<AgentEvent>(64);

        // Writer task: forward queued lines to claude's stdin.
        tokio::spawn(writer_task(stdin, writer_rx));

        // Reader task: parse NDJSON from stdout into AgentEvents.
        tokio::spawn(reader_task(stdout, reader_tx));

        // Stderr drain — log only, don't surface as events.
        tokio::spawn(stderr_drain(stderr));

        Ok(Self {
            writer_tx: Some(writer_tx),
            reader_rx,
            child: Some(child),
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

    /// Whether to pass `--dangerously-skip-permissions` (default: `true`).
    /// When `false`, claude will prompt for permission on tool calls;
    /// the driver currently has no way to route those prompts back
    /// through CAP — set this to `false` only if you don't care about
    /// auto-approving (or you trust the agent to deny dangerous ops).
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
        let tx = self
            .writer_tx
            .as_ref()
            .ok_or(DriverError::AgentExited)?;
        let line = encode_client_frame(&frame)?;
        trace!(line = %line, "→ claude");
        tx.send(line).await.map_err(|_| DriverError::AgentExited)?;
        Ok(())
    }

    async fn next_event(&mut self) -> Option<AgentEvent> {
        self.reader_rx.recv().await
    }

    async fn shutdown(&mut self) -> Result<(), DriverError> {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Writer / reader / stderr tasks
// ---------------------------------------------------------------------------

async fn writer_task(
    mut stdin: tokio::process::ChildStdin,
    mut rx: mpsc::Receiver<String>,
) {
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

async fn reader_task(stdout: tokio::process::ChildStdout, tx: mpsc::Sender<AgentEvent>) {
    let mut lines = BufReader::new(stdout).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                trace!(line = %line, "← claude");
                let value: Value = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, raw = %line, "reader: malformed JSON, skipping");
                        continue;
                    }
                };
                for event in parse_stream_frame(&value) {
                    if tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
            Ok(None) => {
                debug!("reader: stdout EOF");
                return;
            }
            Err(e) => {
                warn!(error = %e, "reader: read error");
                return;
            }
        }
    }
}

async fn stderr_drain(stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        debug!(target: "cap_rs::stream_json::stderr", "{}", line);
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
                    Content::Text(t) => json!({"type": "text", "text": t}),
                    Content::Image { mime, data } => json!({
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": mime,
                            "data": base64_encode(data),
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
        ClientFrame::Cancel => {
            // Claude SDK has no in-band cancel — use shutdown() instead.
            // We emit a benign frame to satisfy the channel; the higher
            // layer should call shutdown().
            Ok(json!({"type": "control", "subtype": "cancel"}).to_string())
        }
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
    }
}

/// Parse one Claude stream-json frame into zero or more CAP events.
fn parse_stream_frame(frame: &Value) -> Vec<AgentEvent> {
    let kind = frame.get("type").and_then(Value::as_str).unwrap_or("");
    match kind {
        "system" => match frame.get("subtype").and_then(Value::as_str).unwrap_or("") {
            "init" => vec![AgentEvent::Ready {
                session_id: frame
                    .get("session_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                model: frame
                    .get("model")
                    .and_then(Value::as_str)
                    .map(String::from),
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
                    });
                }
            }
            events
        }

        "result" => {
            let usage = parse_usage(frame);
            let stop_reason = match frame.get("subtype").and_then(Value::as_str) {
                Some("success") => StopReason::EndTurn,
                Some("error_max_turns") => StopReason::MaxTokens,
                Some("error_during_execution") => StopReason::Error,
                Some(other) if other.starts_with("error") => StopReason::Error,
                _ => StopReason::EndTurn,
            };
            vec![AgentEvent::Done { stop_reason, usage }]
        }

        "stream_event" => {
            // Partial message deltas (only with --include-partial-messages).
            // We don't request them in the spawn args, so skip.
            vec![]
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
        cost_usd_estimate: frame.get("total_cost_usd").and_then(Value::as_f64),
        duration: frame
            .get("duration_ms")
            .and_then(Value::as_u64)
            .map(std::time::Duration::from_millis),
        model_id: frame
            .get("modelUsage")
            .and_then(Value::as_object)
            .and_then(|m| m.keys().next().cloned()),
    }
}

// Tiny base64 — pulled in to avoid an extra dep for the rare image case.
fn base64_encode(data: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(((data.len() + 2) / 3) * 4);
    let mut i = 0;
    while i + 3 <= data.len() {
        let b = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8) | (data[i + 2] as u32);
        out.push(T[((b >> 18) & 63) as usize] as char);
        out.push(T[((b >> 12) & 63) as usize] as char);
        out.push(T[((b >> 6) & 63) as usize] as char);
        out.push(T[(b & 63) as usize] as char);
        i += 3;
    }
    let rem = data.len() - i;
    if rem == 1 {
        let b = (data[i] as u32) << 16;
        out.push(T[((b >> 18) & 63) as usize] as char);
        out.push(T[((b >> 12) & 63) as usize] as char);
        out.push_str("==");
    } else if rem == 2 {
        let b = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8);
        out.push(T[((b >> 18) & 63) as usize] as char);
        out.push(T[((b >> 12) & 63) as usize] as char);
        out.push(T[((b >> 6) & 63) as usize] as char);
        out.push('=');
    }
    out
}

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
                "usage":{"input_tokens":10,"output_tokens":20,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}"#,
        )
        .unwrap();
        let events = parse_stream_frame(&v);
        match &events[0] {
            AgentEvent::Done { usage, stop_reason } => {
                assert_eq!(*stop_reason, StopReason::EndTurn);
                assert_eq!(usage.input_tokens, 10);
                assert_eq!(usage.output_tokens, 20);
                assert_eq!(usage.cost_usd_estimate, Some(0.0021));
            }
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn encode_simple_prompt() {
        let frame = ClientFrame::Prompt {
            content: vec![Content::Text("hi".into())],
        };
        let line = encode_client_frame(&frame).unwrap();
        let v: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["type"], "user");
        assert_eq!(v["message"]["content"][0]["text"], "hi");
    }
}
