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

use std::collections::HashMap;
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
    prompt_after_ready: bool,
    turn_pending: std::sync::Arc<std::sync::atomic::AtomicBool>,
    is_rscode: bool,
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
            permission_mode: None,
            // Permission-bypass is opt-in. CAP spec §13.1 treats injected
            // input as privileged, and the driver has no way to route
            // claude's permission prompts back through CAP yet — so the
            // safe default is to leave claude's prompting on. Callers that
            // accept the trade-off invoke `.dangerously_skip_permissions(true)`.
            dangerously_skip_permissions: false,
            is_opencode: false,
            is_codex: false,
            is_rscode: false,
            continue_last: false,
            // Stream-json CLIs read their first stdin frame before emitting
            // `system/init`; waiting for Ready here deadlocks the session.
            prompt_after_ready: false,
            extra_args: Vec::new(),
            env: HashMap::new(),
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
            bin: None,
            cwd: cwd.as_ref().to_path_buf(),
            model: None,
            session_id: None,
            resume: None,
            replay_user_messages: false,
            permission_mode: None,
            dangerously_skip_permissions: false,
            is_opencode: true,
            is_codex: false,
            is_rscode: false,
            continue_last: false,
            prompt_after_ready: false,
            extra_args: Vec::new(),
            env: HashMap::new(),
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
            bin: None,
            cwd: cwd.as_ref().to_path_buf(),
            model: None,
            session_id: None,
            resume: None,
            replay_user_messages: false,
            permission_mode: None,
            // Driver caller decides whether to bypass codex sandbox
            // prompts via `.dangerously_skip_permissions(true)` —
            // maps to `--dangerously-bypass-approvals-and-sandbox`
            // for codex (mirrors the spec §13.1 same-semantics flag
            // used for claudecode).
            dangerously_skip_permissions: false,
            is_opencode: false,
            is_codex: true,
            is_rscode: false,
            continue_last: false,
            prompt_after_ready: false,
            extra_args: Vec::new(),
            env: HashMap::new(),
        }
    }

    /// Builder pre-configured for RSCode's stream-json protocol.
    ///
    /// RSCode uses a compact `{"type":"user","text":"..."}` input frame,
    /// its own command-line flags, and an event envelope on stdout.
    pub fn rscode_builder(cwd: impl AsRef<Path>) -> ClaudeCodeDriverBuilder {
        ClaudeCodeDriverBuilder {
            bin: None,
            cwd: cwd.as_ref().to_path_buf(),
            model: None,
            session_id: None,
            resume: None,
            replay_user_messages: false,
            permission_mode: None,
            dangerously_skip_permissions: false,
            is_opencode: false,
            is_codex: false,
            is_rscode: true,
            continue_last: false,
            // RSCode waits for its first stdin frame before emitting events.
            prompt_after_ready: false,
            extra_args: Vec::new(),
            env: HashMap::new(),
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
            permission_mode,
            dangerously_skip_permissions,
            is_opencode,
            is_codex,
            is_rscode,
            continue_last,
            prompt_after_ready,
            extra_args,
            env,
        } = b;

        let bin = if is_opencode {
            select_bin(bin, std::env::var("OPENCODE_BIN").ok(), "opencode")
        } else if is_codex {
            select_bin(bin, std::env::var("CODEX_BIN").ok(), "codex")
        } else if is_rscode {
            select_bin(bin, std::env::var("RSCODE_BIN").ok(), "rscode")
        } else {
            select_bin(bin, std::env::var("CLAUDE_BIN").ok(), "claude")
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
            // Codex flag layout matters: `resume <thread_id>` is a
            // sub-subcommand of `exec`, and its OWN parser only
            // accepts globally-declared flags (--input-format,
            // --output-format, --skip-git-repo-check). The
            // exec-level shared flags (--sandbox,
            // --dangerously-bypass-approvals-and-sandbox, -m) MUST
            // appear BETWEEN `exec` and `resume`, otherwise resume's
            // parser rejects them with "unexpected argument".
            //
            // Layout:
            //   codex exec [shared-flags] [resume <id>] [global flags]
            //              ^^^^^^^^^^^^^^^^^^^^^^^^^^^
            //              order matters — emit shared flags first
            cmd.arg("exec").arg("--sandbox").arg("workspace-write");
            if dangerously_skip_permissions {
                cmd.arg("--dangerously-bypass-approvals-and-sandbox");
            }
            if let Some(m) = &model {
                cmd.arg("-m").arg(m);
            }
            if let Some(rid) = &resume {
                cmd.arg("resume").arg(rid);
            } else if continue_last {
                // `codex exec resume --last` picks the most recent
                // saved thread for this cwd.
                cmd.arg("resume").arg("--last");
            }
            cmd.arg("--input-format")
                .arg("stream-json")
                .arg("--output-format")
                .arg("stream-json")
                .arg("--skip-git-repo-check")
                .current_dir(&cwd)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
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
            // every spawn. `--continue` (no id) picks the most
            // recent session.
            if let Some(rid) = &resume {
                cmd.arg("--session").arg(rid);
            } else if continue_last {
                cmd.arg("--continue");
            }
        } else if is_rscode {
            // RSCode uses a compact text input frame, its own flag names,
            // and an event envelope on stdout.
            let session = session_id.as_deref().or(resume.as_deref());
            cmd.args(rscode_args(dangerously_skip_permissions, session))
                .current_dir(&cwd)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
        } else {
            // Claude Code: `claude -p --input-format=stream-json --output-format=stream-json`
            cmd.arg("-p")
                .arg("--input-format=stream-json")
                .arg("--output-format=stream-json")
                .current_dir(&cwd)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);

            // --verbose is claude/openclaude-specific; other compatible
            // CLIs (e.g. qodercli) reject it as an unknown flag.
            let bin_stem = std::path::Path::new(&bin)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or(&bin);
            if matches!(bin_stem, "claude" | "openclaude") {
                cmd.arg("--verbose");
            }

            if dangerously_skip_permissions {
                cmd.arg("--dangerously-skip-permissions");
            }
            if let Some(mode) = &permission_mode {
                cmd.arg("--permission-mode").arg(mode);
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
            } else if continue_last {
                // claudecode's `--continue` resumes the most recent
                // session for this cwd, equivalent to `claude /sessions`
                // → pick first.
                cmd.arg("--continue");
            }
        }

        cmd.args(extra_args).envs(env);

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
            is_rscode,
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
        let turn_pending = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

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
            std::sync::Arc::clone(&turn_pending),
            is_opencode,
        ));

        // Stderr drain — log only, don't surface as events.
        tokio::spawn(stderr_drain(stderr));

        Ok(Self {
            writer_tx: Some(writer_tx),
            reader_rx,
            child: Some(child),
            exited,
            exit_status,
            prompt_after_ready,
            turn_pending,
            is_rscode,
        })
    }
}

fn select_bin(explicit: Option<String>, env_override: Option<String>, default: &str) -> String {
    explicit
        .or(env_override)
        .unwrap_or_else(|| default.to_string())
}

fn rscode_args(auto_approve: bool, session: Option<&str>) -> Vec<String> {
    let mut args = vec![
        "run".to_string(),
        "--input-stream-json".to_string(),
        "--output-stream-json".to_string(),
    ];
    if auto_approve {
        args.push("--yes".to_string());
    }
    if let Some(session) = session {
        args.push("--session".to_string());
        args.push(session.to_string());
    }
    args
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
    permission_mode: Option<String>,
    dangerously_skip_permissions: bool,
    /// When true, use OpenCode CLI shape instead of Claude Code.
    is_opencode: bool,
    /// When true, use Codex CLI shape (`codex exec --input-format
    /// stream-json --output-format stream-json`). Mutually exclusive
    /// with `is_opencode`; both false = claudecode/openclaude.
    is_codex: bool,
    /// When true, use RSCode CLI flags and parse its event envelope.
    is_rscode: bool,
    /// When true, pass the agent CLI's "resume last session" flag
    /// (claudecode `--continue`, opencode `--continue`, codex
    /// `exec resume --last`). Mutually exclusive with `resume` —
    /// `.resume(uuid)` clears this; `.continue_last(true)` clears
    /// `resume`.
    continue_last: bool,
    prompt_after_ready: bool,
    /// Arguments appended after the driver's protocol-specific arguments.
    extra_args: Vec<String>,
    /// Environment variables applied only to the child agent process.
    env: HashMap<String, String>,
}

impl ClaudeCodeDriverBuilder {
    /// Override the binary used (default: `claude` on PATH, or `$CLAUDE_BIN`).
    pub fn bin(mut self, bin: impl Into<String>) -> Self {
        self.bin = Some(bin.into());
        self
    }

    /// Append agent-specific arguments after the driver's required protocol
    /// flags. Callers must not repeat flags managed by the selected driver.
    pub fn extra_args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.extra_args = args.into_iter().map(Into::into).collect();
        self
    }

    /// Set environment variables for the spawned agent process only.
    pub fn envs(mut self, env: HashMap<String, String>) -> Self {
        self.env = env;
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
        self.continue_last = false;
        self
    }

    /// Resume the MOST RECENT persisted session for this agent in the
    /// current cwd. Equivalent to `claude --continue` / `opencode run
    /// --continue` / `codex exec resume --last`. Mutually exclusive
    /// with `.resume(uuid)`; setting one clears the other.
    pub fn continue_last(mut self, on: bool) -> Self {
        self.continue_last = on;
        if on {
            self.resume = None;
        }
        self
    }

    /// Whether callers must wait for a Ready event before sending the first prompt.
    /// Some compatible CLIs emit their init frame only after receiving input.
    pub fn prompt_after_ready(mut self, on: bool) -> Self {
        self.prompt_after_ready = on;
        self
    }

    /// Set a Claude-compatible CLI permission mode.
    pub fn permission_mode(mut self, mode: impl Into<String>) -> Self {
        self.permission_mode = Some(mode.into());
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
        let starts_turn = matches!(
            &frame,
            ClientFrame::Prompt { .. }
                | ClientFrame::AskUserAnswer { .. }
                | ClientFrame::PermissionResponse { .. }
        );
        let line = if self.is_rscode {
            encode_rscode_client_frame(&frame)?
        } else {
            encode_client_frame(&frame)?
        };
        if line.is_empty() {
            return Ok(());
        }
        if starts_turn {
            self.turn_pending
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        trace!(line = %line, "→ agent");
        if tx.send(line).await.is_err() {
            self.turn_pending
                .store(false, std::sync::atomic::Ordering::Relaxed);
            return Err(DriverError::AgentExited);
        }
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
        self.prompt_after_ready
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
    turn_pending: std::sync::Arc<std::sync::atomic::AtomicBool>,
    eof_is_success: bool,
) {
    // OpenCode's `opencode run --output-format
    // stream-json` is one-shot per process and DOES NOT emit a
    // claudecode-style `result` terminator. It just streams its
    // assistant messages and exits. Without a synthetic Done on EOF,
    // CapLiveManager waits up to PROMPT_TIMEOUT (300s) for a Done
    // that will never come — every opencode turn appears to "hang"
    // 5 minutes after completion before erroring. With this synth,
    // the EOF on opencode's stdout becomes the Done signal.
    //
    // Persistent agents must emit a result. Their EOF-before-result path
    // surfaces an Error and lets the actor classify the turn as failed.
    // De-dupe the streamed-then-snapshotted assistant text. OpenCode's
    // `--output-format stream-json` (and claudecode with
    // `--include-partial-messages`) emit the assistant text TWICE: first
    // token-by-token via `stream_event`/`text_delta` (TextChunk with an EMPTY
    // msg_id), then again as the complete `assistant` frame block (TextChunk
    // with a SET msg_id). Forwarding both doubles the reply ("OoposOopos").
    // Once we've streamed text for the current message, drop the matching
    // full-text snapshot; reset at each tool-call/message boundary and Done.
    let mut streamed_text = false;
    // Track text already forwarded this turn. CodeBuddy may place the final
    // answer only in `result.result`; compare content instead of suppressing
    // the result merely because any earlier assistant text was seen.
    let mut assistant_text = String::new();
    let mut lines = BufReader::new(stdout).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                trace!(line = %line, "← agent");
                let value = serde_json::from_str::<Value>(&line).ok();
                let mut events = parse_stream_line(&line, false);
                if let Some(event) = value
                    .as_ref()
                    .and_then(|frame| result_text_fallback(frame, &assistant_text))
                {
                    events.insert(0, event);
                }
                for event in events {
                    trace!(event = ?event, "parsed event");
                    let mut skip = false;
                    match &event {
                        AgentEvent::TextChunk {
                            msg_id, channel, ..
                        } if *channel == TextChannel::Assistant => {
                            if msg_id.is_empty() {
                                // A streamed token delta — arm the de-dupe.
                                streamed_text = true;
                            } else if streamed_text {
                                // Full-text snapshot of already-streamed text.
                                skip = true;
                            }
                        }
                        AgentEvent::ToolCallStart { .. } => streamed_text = false,
                        _ => {}
                    }
                    if matches!(event, AgentEvent::Done { .. }) {
                        turn_pending.store(false, std::sync::atomic::Ordering::Relaxed);
                        streamed_text = false;
                        assistant_text.clear();
                    } else if matches!(event, AgentEvent::Error { .. }) {
                        turn_pending.store(false, std::sync::atomic::Ordering::Relaxed);
                    }
                    if skip {
                        trace!("reader: dropping duplicate assistant snapshot");
                        continue;
                    }
                    if let AgentEvent::TextChunk {
                        text,
                        channel: TextChannel::Assistant,
                        ..
                    } = &event
                    {
                        assistant_text.push_str(text);
                    }
                    if tx.send(event).await.is_err() {
                        exited.store(true, std::sync::atomic::Ordering::Relaxed);
                        return;
                    }
                }
            }
            Ok(None) => {
                debug!("reader: stdout EOF");
                let pending = turn_pending.load(std::sync::atomic::Ordering::Relaxed);
                if let Some(event) = eof_event(pending, eof_is_success) {
                    let _ = tx.send(event).await;
                }
                exited.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }
            Err(e) => {
                warn!(error = %e, "reader: read error");
                if turn_pending.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = tx
                        .send(AgentEvent::Error {
                            code: "stream_read_failed".into(),
                            message: e.to_string(),
                            retryable: false,
                            details: None,
                        })
                        .await;
                }
                exited.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }
        }
    }
}

fn eof_event(turn_pending: bool, eof_is_success: bool) -> Option<AgentEvent> {
    if !turn_pending {
        None
    } else if eof_is_success {
        Some(AgentEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: Usage::default(),
        })
    } else {
        Some(AgentEvent::Error {
            code: "agent_eof_before_result".into(),
            message: "agent stdout closed before a terminal result frame".into(),
            retryable: false,
            details: None,
        })
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

fn encode_rscode_client_frame(frame: &ClientFrame) -> Result<String, DriverError> {
    let text = match frame {
        ClientFrame::Prompt { content } => {
            let mut text = String::new();
            for part in content {
                match part {
                    Content::Text { text: part } => text.push_str(part),
                    Content::Image { .. } => {
                        return Err(DriverError::AgentError {
                            code: "cap_rscode_image_unsupported".into(),
                            message: "RSCode stream-json input accepts text prompts only".into(),
                        });
                    }
                }
            }
            text
        }
        ClientFrame::AskUserAnswer { ask_id, value } => {
            format!("[answer to {ask_id}]: {value}")
        }
        ClientFrame::PermissionResponse { req_id, decision } => {
            format!("[permission {req_id}]: {decision:?}")
        }
        ClientFrame::SessionConfig(_) => return Ok(String::new()),
        ClientFrame::Cancel { .. } => {
            return Err(DriverError::AgentError {
                code: "cap_cancel_unsupported".into(),
                message: "stream-json binding has no in-band cancel; call Driver::shutdown".into(),
            });
        }
        ClientFrame::ReverseRpcResult { .. } => {
            return Err(DriverError::AgentError {
                code: "cap_reverse_rpc_unsupported".into(),
                message: "stream-json driver does not emit reverse RPC".into(),
            });
        }
    };

    Ok(json!({ "type": "user", "text": text }).to_string())
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

        "event" => parse_rscode_event(frame),

        "error" => vec![AgentEvent::Error {
            code: "agent_error".into(),
            message: frame
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("agent error")
                .to_string(),
            retryable: false,
            details: Some(frame.clone()),
        }],

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

fn parse_rscode_event(frame: &Value) -> Vec<AgentEvent> {
    let Some(event) = frame.get("event").and_then(Value::as_object) else {
        return Vec::new();
    };
    let Some((kind, payload)) = event.iter().next() else {
        return Vec::new();
    };
    let turn_id = payload
        .get("turn")
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .unwrap_or_default();

    match kind.as_str() {
        "ThinkingDelta" => payload
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(|text| {
                vec![AgentEvent::Thought {
                    msg_id: turn_id,
                    text: text.to_string(),
                }]
            })
            .unwrap_or_default(),
        "AssistantDelta" => payload
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(|text| {
                vec![AgentEvent::TextChunk {
                    msg_id: String::new(),
                    text: text.to_string(),
                    channel: TextChannel::Assistant,
                }]
            })
            .unwrap_or_default(),
        "AssistantFinal" => {
            let mut events = Vec::new();
            for block in payload
                .get("blocks")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(block) = block.as_object() else {
                    continue;
                };
                if let Some(text) = block
                    .get("Text")
                    .and_then(|v| v.get("text"))
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                {
                    events.push(AgentEvent::TextChunk {
                        msg_id: turn_id.clone(),
                        text: text.to_string(),
                        channel: TextChannel::Assistant,
                    });
                }
            }
            events
        }
        "ToolRequested" => vec![AgentEvent::ToolCallStart {
            call_id: payload
                .get("call")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            name: payload
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            input: payload.get("input").cloned().unwrap_or(Value::Null),
        }],
        "ToolCompleted" => vec![AgentEvent::ToolCallEnd {
            call_id: payload
                .get("call")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            output: payload
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            is_error: payload
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            duration: None,
        }],
        "Cancelled" => vec![AgentEvent::Done {
            stop_reason: StopReason::Cancelled,
            usage: Usage::default(),
        }],
        _ => Vec::new(),
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

fn result_text_fallback(frame: &Value, assistant_text: &str) -> Option<AgentEvent> {
    if frame.get("type").and_then(Value::as_str) != Some("result")
        || frame
            .get("subtype")
            .and_then(Value::as_str)
            .is_some_and(|s| s.starts_with("error"))
    {
        return None;
    }
    let text = frame
        .get("result")
        .or_else(|| frame.get("final_text"))
        .and_then(Value::as_str)?;
    if text.is_empty() {
        return None;
    }
    let text = if text == assistant_text {
        return None;
    } else if !assistant_text.is_empty() {
        text.strip_prefix(assistant_text).unwrap_or(text)
    } else {
        text
    };
    if text.is_empty() {
        return None;
    }
    Some(AgentEvent::TextChunk {
        msg_id: frame
            .get("uuid")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        text: text.to_owned(),
        channel: TextChannel::Assistant,
    })
}

fn parse_usage(frame: &Value) -> Usage {
    let u = frame
        .get("usage")
        .or_else(|| frame.get("total_usage"))
        .cloned()
        .unwrap_or(Value::Null);
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
    fn structured_stream_drivers_send_the_first_prompt_before_ready() {
        assert!(!ClaudeCodeDriver::builder(".").prompt_after_ready);
        assert!(!ClaudeCodeDriver::opencode_builder(".").prompt_after_ready);
        assert!(!ClaudeCodeDriver::codex_builder(".").prompt_after_ready);
        assert!(!ClaudeCodeDriver::rscode_builder(".").prompt_after_ready);
    }

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
    fn result_text_fallback_recovers_codebuddy_later_turns() {
        let v: Value = serde_json::from_str(
            r#"{"type":"result","subtype":"success","uuid":"r1","result":"later text"}"#,
        )
        .unwrap();
        match result_text_fallback(&v, "earlier tool preface") {
            Some(AgentEvent::TextChunk { text, .. }) => assert_eq!(text, "later text"),
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn result_text_fallback_deduplicates_exact_text_and_emits_suffix() {
        let v: Value =
            serde_json::from_str(r#"{"type":"result","subtype":"success","result":"hello world"}"#)
                .unwrap();
        assert!(result_text_fallback(&v, "hello world").is_none());
        match result_text_fallback(&v, "hello ") {
            Some(AgentEvent::TextChunk { text, .. }) => assert_eq!(text, "world"),
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn parses_rscode_events_and_terminal_result() {
        let thought: Value = serde_json::from_str(
            r#"{"type":"event","event":{"ThinkingDelta":{"turn":3,"text":"hmm"}}}"#,
        )
        .unwrap();
        assert!(matches!(
            &parse_stream_frame(&thought)[0],
            AgentEvent::Thought { text, .. } if text == "hmm"
        ));

        let delta: Value = serde_json::from_str(
            r#"{"type":"event","event":{"AssistantDelta":{"turn":3,"text":"ready"}}}"#,
        )
        .unwrap();
        assert!(matches!(
            &parse_stream_frame(&delta)[0],
            AgentEvent::TextChunk { text, .. } if text == "ready"
        ));

        let result: Value = serde_json::from_str(
            r#"{"type":"result","final_text":"ready","total_usage":{"input_tokens":4,"output_tokens":2}}"#,
        )
        .unwrap();
        match &parse_stream_frame(&result)[0] {
            AgentEvent::Done { usage, .. } => {
                assert_eq!(usage.input_tokens, 4);
                assert_eq!(usage.output_tokens, 2);
            }
            other => panic!("wrong: {other:?}"),
        }
        assert!(result_text_fallback(&result, "ready").is_none());
    }

    #[test]
    fn parses_rscode_tool_events() {
        let requested: Value = serde_json::from_str(
            r#"{"type":"event","event":{"ToolRequested":{"call":"t1","name":"Read","input":{"path":"x"}}}}"#,
        )
        .unwrap();
        assert!(matches!(
            &parse_stream_frame(&requested)[0],
            AgentEvent::ToolCallStart { call_id, name, .. }
                if call_id == "t1" && name == "Read"
        ));

        let completed: Value = serde_json::from_str(
            r#"{"type":"event","event":{"ToolCompleted":{"call":"t1","content":"ok","is_error":false}}}"#,
        )
        .unwrap();
        assert!(matches!(
            &parse_stream_frame(&completed)[0],
            AgentEvent::ToolCallEnd { output, is_error, .. }
                if output == "ok" && !is_error
        ));
    }

    #[test]
    fn rscode_uses_its_native_stream_flags() {
        assert_eq!(
            rscode_args(true, Some("session-1")),
            vec![
                "run",
                "--input-stream-json",
                "--output-stream-json",
                "--yes",
                "--session",
                "session-1",
            ]
        );
    }

    #[test]
    fn explicit_binary_wins_over_generic_environment_override() {
        assert_eq!(
            select_bin(
                Some("codebuddy".into()),
                Some("claude-custom".into()),
                "claude"
            ),
            "codebuddy"
        );
    }

    #[test]
    fn builder_preserves_agent_specific_args_and_env() {
        let mut env = HashMap::new();
        env.insert("QODER_PROFILE".to_string(), "production".to_string());
        let builder = ClaudeCodeDriver::builder(".")
            .extra_args(["--qoder-feature=enabled"])
            .envs(env);

        assert_eq!(builder.extra_args, vec!["--qoder-feature=enabled"]);
        assert_eq!(
            builder.env.get("QODER_PROFILE"),
            Some(&"production".to_string())
        );
    }

    #[test]
    fn eof_is_only_success_for_one_shot_flavors() {
        assert!(eof_event(false, false).is_none());
        assert!(matches!(
            eof_event(true, true),
            Some(AgentEvent::Done {
                stop_reason: StopReason::EndTurn,
                ..
            })
        ));
        assert!(matches!(
            eof_event(true, false),
            Some(AgentEvent::Error { ref code, .. })
                if code == "agent_eof_before_result"
        ));
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
    fn encode_rscode_prompt_uses_native_text_field() {
        let frame = ClientFrame::Prompt {
            content: vec![Content::text("hello "), Content::text("rscode")],
        };
        let line = encode_rscode_client_frame(&frame).unwrap();
        let v: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v, json!({ "type": "user", "text": "hello rscode" }));
        assert!(v.get("message").is_none());
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
