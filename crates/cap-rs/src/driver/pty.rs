//! PTY driver — the universal substrate for any CLI agent.
//!
//! Spawns the agent under a pseudo-terminal pair so it behaves as if running
//! interactively in a real terminal: TUIs render normally, `isatty()` returns
//! true, signals (SIGINT) work, terminal resize is supported.
//!
//! Per spec §6.1 PTY is the REQUIRED universal binding. Every CLI agent —
//! including ones that expose no structured protocol whatsoever — can be
//! driven through PTY. Output is a raw ANSI byte stream; an
//! [`AgentParser`] converts it to [`AgentEvent`]s.
//!
//! The first available parser is [`RawParser`], which emits every chunk of
//! bytes as a [`AgentEvent::TextChunk`]. Per-agent structured parsers
//! (Claude Code TUI, aider, codex CLI, …) layer on top.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use crate::core::{AgentEvent, ClientFrame, Content, StopReason, TextChannel, Usage};
use crate::driver::{Driver, DriverError, DriverExitStatus};

// ---------------------------------------------------------------------------
// AgentParser
// ---------------------------------------------------------------------------

/// Translates raw PTY bytes into structured [`AgentEvent`]s.
///
/// Implementations are agent-specific. They may use [`vt100`] internally to
/// track screen state, or treat the byte stream as a raw text stream, or
/// any combination thereof.
///
/// Parsers run on the PTY reader thread (sync) — keep work bounded.
pub trait AgentParser: Send + 'static {
    /// Short identifier (used for logs).
    fn name(&self) -> &str;

    /// Process a new chunk of bytes from the agent's PTY. Return any
    /// CAP events extracted (zero or more). May be empty if the parser
    /// needs more data to make a decision.
    fn on_bytes(&mut self, bytes: &[u8]) -> Vec<AgentEvent>;

    /// Called once when PTY EOF is observed (agent exited). Return any
    /// final events (e.g. a synthesised [`AgentEvent::Done`]).
    fn on_eof(&mut self) -> Vec<AgentEvent> {
        Vec::new()
    }
}

/// The dumbest possible parser — every byte chunk becomes a text event.
/// Useful for plumbing verification and for agents we haven't written a
/// real parser for yet. ANSI escapes pass through unmodified.
#[derive(Debug, Default)]
pub struct RawParser;

impl AgentParser for RawParser {
    fn name(&self) -> &str {
        "raw"
    }

    fn on_bytes(&mut self, bytes: &[u8]) -> Vec<AgentEvent> {
        if bytes.is_empty() {
            return Vec::new();
        }
        vec![AgentEvent::TextChunk {
            msg_id: String::new(),
            text: String::from_utf8_lossy(bytes).into_owned(),
            channel: TextChannel::Assistant,
        }]
    }

    fn on_eof(&mut self) -> Vec<AgentEvent> {
        vec![AgentEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: Usage::default(),
        }]
    }
}

/// Parser that runs every byte through a [`vt100::Parser`] and emits the
/// diff of the visible screen as plain-text [`AgentEvent::TextChunk`]
/// events. ANSI escapes are absorbed; you get only the rendered output.
pub struct VtPlainParser {
    vt: vt100::Parser,
    last_screen: String,
}

impl std::fmt::Debug for VtPlainParser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VtPlainParser")
            .field("last_screen_len", &self.last_screen.len())
            .finish()
    }
}

impl VtPlainParser {
    pub fn new(rows: u16, cols: u16) -> Self {
        Self {
            vt: vt100::Parser::new(rows, cols, 10_000),
            last_screen: String::new(),
        }
    }
}

impl AgentParser for VtPlainParser {
    fn name(&self) -> &str {
        "vt100-plain"
    }

    fn on_bytes(&mut self, bytes: &[u8]) -> Vec<AgentEvent> {
        self.vt.process(bytes);
        let screen = self.vt.screen().contents();
        if screen == self.last_screen {
            return Vec::new();
        }
        // Emit just the part that's new at the end (naive append-only diff).
        let delta = if screen.starts_with(&self.last_screen) {
            screen[self.last_screen.len()..].to_string()
        } else {
            // Screen scrolled / repainted — emit the full screen.
            format!("\n--- screen repaint ---\n{}", screen)
        };
        self.last_screen = screen;
        if delta.is_empty() {
            return Vec::new();
        }
        vec![AgentEvent::TextChunk {
            msg_id: String::new(),
            text: delta,
            channel: TextChannel::Assistant,
        }]
    }

    fn on_eof(&mut self) -> Vec<AgentEvent> {
        vec![AgentEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: Usage::default(),
        }]
    }
}

// ---------------------------------------------------------------------------
// ReplParser — REPL / prompt-delimited agents (aider, python, plandex, …)
// ---------------------------------------------------------------------------

/// Parser for prompt-delimited REPL-style agents. Strips ANSI via
/// [`vt100`], buffers text by line, and treats a configurable prompt
/// pattern as a turn boundary:
///
/// - Output **between** prompts is emitted as [`AgentEvent::TextChunk`].
/// - When the prompt re-appears on the current line, a synthetic
///   [`AgentEvent::Done`] is emitted (turn boundary).
/// - A configurable yes-no question regex emits
///   [`AgentEvent::AskUser`].
///
/// Ships with constructors for common REPLs:
///
/// - [`ReplParser::aider`] — `> ` prompt, `\? .*\[Y/n\]` confirmation
/// - [`ReplParser::python_repl`] — `>>> ` prompt, `... ` continuation
/// - [`ReplParser::generic_repl`] — simple `> ` or `❯ ` prompts
pub struct ReplParser {
    name: &'static str,
    vt: vt100::Parser,
    /// Plain (ANSI-stripped) buffer of bytes since the last turn boundary.
    buffer: String,
    /// Where to start scanning on next on_bytes call (to avoid re-scanning
    /// already-emitted text).
    emit_cursor: usize,
    prompts: Vec<regex_lite::Regex>,
    ask_yes_no: Option<regex_lite::Regex>,
    /// Have we seen a prompt yet (first prompt = "ready", subsequent = "done")
    seen_first_prompt: bool,
    /// Tag for synthesized event message_id, monotonic per turn.
    turn: u64,
}

impl std::fmt::Debug for ReplParser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplParser")
            .field("name", &self.name)
            .field("buffered", &self.buffer.len())
            .field("turn", &self.turn)
            .finish()
    }
}

impl ReplParser {
    /// Build a parser for an `aider` session.
    pub fn aider() -> Self {
        Self::new(
            "aider",
            &[r"^>\s*$"],              // top-level ">"
            Some(r"\?\s*\[Y/n\]\s*$"), // "(Y/n)" confirmation
        )
    }

    /// Build a parser for `python3 -i` / `python` interactive REPL.
    pub fn python_repl() -> Self {
        Self::new("python-repl", &[r"^>>>\s*$", r"^\.\.\.\s*$"], None)
    }

    /// Build a parser for any generic REPL using `> ` or `❯ ` prompts.
    pub fn generic_repl() -> Self {
        Self::new("generic-repl", &[r"^>\s*$", r"^❯\s*$"], None)
    }

    /// Custom constructor — give your own prompt regexes (anchored on
    /// the start of a logical line) and an optional yes/no detector.
    pub fn new(name: &'static str, prompts: &[&str], ask_yes_no: Option<&str>) -> Self {
        let prompts = prompts
            .iter()
            .map(|p| regex_lite::Regex::new(p).expect("invalid prompt regex"))
            .collect();
        let ask_yes_no = ask_yes_no.map(|p| regex_lite::Regex::new(p).expect("invalid regex"));
        Self {
            name,
            vt: vt100::Parser::new(50, 200, 10_000),
            buffer: String::new(),
            emit_cursor: 0,
            prompts,
            ask_yes_no,
            seen_first_prompt: false,
            turn: 0,
        }
    }

    fn current_screen(&mut self) -> String {
        self.vt.screen().contents()
    }

    /// Scan the bottom of a freshly-repainted screen for prompt / yes-no
    /// boundary markers. Walks the last few non-empty lines and returns
    /// any synthesized events. Used when an append-only delta isn't
    /// available because the screen scrolled.
    fn scan_for_boundary(&mut self, screen: &str) -> Vec<AgentEvent> {
        let mut events = Vec::new();
        let tail: Vec<&str> = screen
            .lines()
            .rev()
            .filter(|l| !l.trim().is_empty())
            .take(4)
            .collect();
        for line in tail {
            let trimmed = line.trim_end_matches(['\n', '\r']);
            if self.prompts.iter().any(|re| re.is_match(trimmed)) {
                if self.seen_first_prompt {
                    events.push(AgentEvent::Done {
                        stop_reason: StopReason::EndTurn,
                        usage: Usage::default(),
                    });
                    self.turn += 1;
                } else {
                    self.seen_first_prompt = true;
                    events.push(AgentEvent::Ready {
                        session_id: format!("{}-pid{}", self.name, std::process::id()),
                        model: None,
                    });
                }
                return events;
            }
            if let Some(re) = &self.ask_yes_no {
                if re.is_match(trimmed) {
                    events.push(AgentEvent::AskUser {
                        ask_id: format!("ask_{}", self.turn),
                        prompt: trimmed.to_string(),
                        ask_kind: crate::core::AskKind::YesNo,
                        options: Vec::new(),
                    });
                    return events;
                }
            }
        }
        events
    }
}

impl AgentParser for ReplParser {
    fn name(&self) -> &str {
        self.name
    }

    fn on_bytes(&mut self, bytes: &[u8]) -> Vec<AgentEvent> {
        self.vt.process(bytes);
        let screen = self.current_screen();
        if !screen.starts_with(&self.buffer) {
            // Screen scrolled / repainted. We can't reconstruct a clean
            // delta against the old buffer, but we still need to detect
            // boundary signals — otherwise a TUI agent that repaints on
            // every turn would never emit Done. Scan the bottom of the
            // freshly painted screen for a prompt or yes/no line and
            // synthesize the appropriate event before resetting state.
            let events = self.scan_for_boundary(&screen);
            self.buffer = screen.clone();
            self.emit_cursor = screen.len();
            return events;
        }
        let delta = screen[self.buffer.len()..].to_string();
        if delta.is_empty() {
            return Vec::new();
        }
        self.buffer = screen;

        // Walk the delta line by line. Detect prompts on a logical-line
        // basis; emit text in between and synthetic events at boundaries.
        let mut events = Vec::new();
        let mut last_line_was_prompt = false;

        // Compose: emit only complete lines (terminated by \n). Carry
        // over any trailing partial line by NOT moving emit_cursor past it.
        let region = &self.buffer[self.emit_cursor..];
        let mut consumed = 0usize;
        for line in region.split_inclusive('\n') {
            // Trim trailing newline for matching but include in the emit.
            let trimmed = line.trim_end_matches(['\n', '\r']);
            let is_prompt = self.prompts.iter().any(|re| re.is_match(trimmed));
            let is_yesno = self
                .ask_yes_no
                .as_ref()
                .map(|re| re.is_match(trimmed))
                .unwrap_or(false);

            if is_prompt {
                if self.seen_first_prompt {
                    // Turn boundary.
                    events.push(AgentEvent::Done {
                        stop_reason: StopReason::EndTurn,
                        usage: Usage::default(),
                    });
                    self.turn += 1;
                } else {
                    self.seen_first_prompt = true;
                    events.push(AgentEvent::Ready {
                        session_id: format!("{}-pid{}", self.name, std::process::id()),
                        model: None,
                    });
                }
                last_line_was_prompt = true;
                consumed += line.len();
                continue;
            }
            if is_yesno {
                events.push(AgentEvent::AskUser {
                    ask_id: format!("ask_{}", self.turn),
                    prompt: trimmed.to_string(),
                    ask_kind: crate::core::AskKind::YesNo,
                    options: Vec::new(),
                });
                consumed += line.len();
                continue;
            }
            // Ordinary content — emit as TextChunk.
            if !trimmed.is_empty() || !last_line_was_prompt {
                events.push(AgentEvent::TextChunk {
                    msg_id: format!("turn_{}", self.turn),
                    text: line.to_string(),
                    channel: TextChannel::Assistant,
                });
            }
            last_line_was_prompt = false;
            consumed += line.len();
        }
        self.emit_cursor += consumed;
        events
    }

    fn on_eof(&mut self) -> Vec<AgentEvent> {
        // Flush any remaining buffered partial line.
        let mut events = Vec::new();
        let tail = self.buffer[self.emit_cursor..].to_string();
        if !tail.is_empty() {
            events.push(AgentEvent::TextChunk {
                msg_id: format!("turn_{}", self.turn),
                text: tail,
                channel: TextChannel::Assistant,
            });
        }
        events.push(AgentEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: Usage::default(),
        });
        events
    }
}

// ---------------------------------------------------------------------------
// PtyDriver
// ---------------------------------------------------------------------------

/// Driver that spawns an agent under a pseudo-terminal.
///
/// Construction is via the [`PtyDriverBuilder`] returned by [`Self::builder`].
pub struct PtyDriver {
    /// Channel for raw bytes to the agent's stdin.
    /// `None` once [`Self::close_input`] is called.
    input_tx: Option<mpsc::Sender<Vec<u8>>>,

    /// Channel of events from the parser.
    event_rx: mpsc::Receiver<AgentEvent>,

    /// Master PTY handle — kept alive so the slave doesn't get HUP. Also
    /// used for resize.
    master: Box<dyn MasterPty + Send>,

    /// Whether the child has exited (set by child-waiter thread).
    exited: std::sync::Arc<std::sync::atomic::AtomicBool>,

    /// Final exit status, set once the child has reaped.
    exit_status: std::sync::Arc<std::sync::Mutex<Option<DriverExitStatus>>>,
}

impl std::fmt::Debug for PtyDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PtyDriver")
            .field("input_open", &self.input_tx.is_some())
            .field("alive", &self.is_alive())
            .field("exit_status", &self.exit_status())
            .finish()
    }
}

impl PtyDriver {
    /// Begin building a PTY-driven agent session.
    pub fn builder(command: impl Into<String>) -> PtyDriverBuilder {
        PtyDriverBuilder {
            command: command.into(),
            args: Vec::new(),
            cwd: None,
            env: Vec::new(),
            env_remove: Vec::new(),
            size: PtySize {
                rows: 50,
                cols: 200,
                pixel_width: 0,
                pixel_height: 0,
            },
        }
    }

    /// Resize the PTY. Forwards SIGWINCH to the child.
    pub fn resize(&self, rows: u16, cols: u16) -> Result<(), DriverError> {
        self.master
            .resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| DriverError::Io(std::io::Error::other(e.to_string())))
    }

    /// Close the input channel so the agent sees stdin EOF (if it cares).
    /// For most TUI agents this has no visible effect — they're driven via
    /// keystrokes, not stdin EOF.
    pub fn close_input(&mut self) {
        self.input_tx = None;
    }

    /// Send raw bytes directly to the agent's PTY input. Useful for
    /// keystrokes (Ctrl+C = `\x03`, Tab = `\t`, arrow keys = `\x1b[A` …).
    pub async fn send_bytes(&mut self, bytes: &[u8]) -> Result<(), DriverError> {
        let tx = self.input_tx.as_ref().ok_or(DriverError::AgentExited)?;
        tx.send(bytes.to_vec())
            .await
            .map_err(|_| DriverError::AgentExited)?;
        Ok(())
    }
}

#[async_trait]
impl Driver for PtyDriver {
    async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError> {
        match frame {
            ClientFrame::Prompt { content } => {
                for c in content {
                    if let Content::Text { text } = c {
                        self.send_bytes(text.as_bytes()).await?;
                    }
                    // Image / other content not meaningful for raw PTY.
                }
                // Most CLI agents commit on Enter.
                self.send_bytes(b"\r").await?;
                Ok(())
            }
            ClientFrame::Cancel { .. } => {
                // Ctrl+C — gracefully cancel current turn for most TUI agents.
                // Scope `Session` would warrant SIGTERM via the master PTY,
                // but spec leaves the choice to the binding; we send ETX for
                // both today.
                self.send_bytes(b"\x03").await
            }
            ClientFrame::SessionConfig(_) => {
                // PTY agents take config via spawn-time argv/env. An inline
                // `cap.session.config` after the session is up has no
                // wire equivalent on raw stdin.
                Err(DriverError::AgentError {
                    code: "cap_session_config_inline_unsupported".into(),
                    message: "PTY binding consumes SessionConfig at spawn".into(),
                })
            }
            ClientFrame::AskUserAnswer { value, .. } => {
                // Best-effort: type the answer + Enter.
                let text = value
                    .as_str()
                    .map(String::from)
                    .unwrap_or_else(|| value.to_string());
                self.send_bytes(text.as_bytes()).await?;
                self.send_bytes(b"\r").await
            }
            ClientFrame::PermissionResponse { decision, .. } => {
                use crate::core::PermissionDecision::*;
                let key: &[u8] = match decision {
                    AllowOnce | AllowAlways => b"y\r",
                    _ => b"n\r",
                };
                self.send_bytes(key).await
            }
        }
    }

    async fn next_event(&mut self) -> Option<AgentEvent> {
        self.event_rx.recv().await
    }

    async fn shutdown(&mut self) -> Result<(), DriverError> {
        // Drop input first so writer thread exits.
        self.input_tx = None;
        // If the waiter thread hasn't published an exit yet, record that
        // the orchestrator initiated shutdown.
        let mut slot = self.exit_status.lock().expect("exit_status mutex poisoned");
        if slot.is_none() {
            *slot = Some(DriverExitStatus::Killed);
            self.exited
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        // Closing the master forces slave HUP; the reader thread will see
        // EOF and exit. We rely on Drop to deallocate.
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

#[derive(Debug)]
pub struct PtyDriverBuilder {
    command: String,
    args: Vec<String>,
    cwd: Option<PathBuf>,
    env: Vec<(String, String)>,
    env_remove: Vec<String>,
    size: PtySize,
}

impl PtyDriverBuilder {
    pub fn arg(mut self, a: impl Into<String>) -> Self {
        self.args.push(a.into());
        self
    }

    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for a in args {
            self.args.push(a.into());
        }
        self
    }

    pub fn cwd(mut self, p: impl AsRef<Path>) -> Self {
        self.cwd = Some(p.as_ref().to_path_buf());
        self
    }

    pub fn env(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.env.push((k.into(), v.into()));
        self
    }

    pub fn env_remove(mut self, k: impl Into<String>) -> Self {
        self.env_remove.push(k.into());
        self
    }

    pub fn size(mut self, rows: u16, cols: u16) -> Self {
        self.size.rows = rows;
        self.size.cols = cols;
        self
    }

    /// Spawn the agent under a PTY and start the reader / writer tasks.
    pub fn spawn<P: AgentParser>(self, parser: P) -> Result<PtyDriver, DriverError> {
        let PtyDriverBuilder {
            command,
            args,
            cwd,
            env,
            env_remove,
            size,
        } = self;

        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(size)
            .map_err(|e| DriverError::SpawnFailed(std::io::Error::other(e.to_string())))?;

        // Build the command. CommandBuilder inherits the parent process
        // env by default; explicitly start from inherited env and apply
        // our adjustments.
        let mut builder = CommandBuilder::new(&command);
        builder.env_clear();
        for (k, v) in std::env::vars_os() {
            // env_remove takes precedence over inherited env.
            let k_str = k.to_string_lossy();
            if env_remove.iter().any(|r| *r == *k_str) {
                continue;
            }
            builder.env(k, v);
        }
        for a in args {
            builder.arg(a);
        }
        if let Some(p) = cwd {
            builder.cwd(p);
        }
        // User-supplied overrides land last so they win over inherited env.
        for (k, v) in env {
            builder.env(k, v);
        }

        debug!(command = %command, "spawning PTY agent");

        let child = pair
            .slave
            .spawn_command(builder)
            .map_err(|e| DriverError::SpawnFailed(std::io::Error::other(e.to_string())))?;

        let reader = pair
            .master
            .try_clone_reader()
            .map_err(|e| DriverError::Io(std::io::Error::other(e.to_string())))?;
        let writer = pair
            .master
            .take_writer()
            .map_err(|e| DriverError::Io(std::io::Error::other(e.to_string())))?;

        let (input_tx, input_rx) = mpsc::channel::<Vec<u8>>(64);
        let (event_tx, event_rx) = mpsc::channel::<AgentEvent>(256);
        let exited = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let exit_status = std::sync::Arc::new(std::sync::Mutex::new(None));

        spawn_reader_thread(
            reader,
            parser,
            event_tx.clone(),
            std::sync::Arc::clone(&exited),
        );
        spawn_writer_thread(writer, input_rx);
        spawn_child_waiter(
            child,
            event_tx,
            std::sync::Arc::clone(&exited),
            std::sync::Arc::clone(&exit_status),
        );

        // Drop slave — only master is kept.
        drop(pair.slave);

        Ok(PtyDriver {
            input_tx: Some(input_tx),
            event_rx,
            master: pair.master,
            exited,
            exit_status,
        })
    }
}

// ---------------------------------------------------------------------------
// Background threads (PTY API is sync; we bridge to async via channels)
// ---------------------------------------------------------------------------

fn spawn_reader_thread<P: AgentParser>(
    mut reader: Box<dyn std::io::Read + Send>,
    mut parser: P,
    tx: mpsc::Sender<AgentEvent>,
    exited: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    std::thread::Builder::new()
        .name("cap-rs-pty-reader".into())
        .spawn(move || {
            let mut buf = [0u8; 8192];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => {
                        trace!("PTY reader: EOF");
                        break;
                    }
                    Ok(n) => {
                        let events = parser.on_bytes(&buf[..n]);
                        for ev in events {
                            if tx.blocking_send(ev).is_err() {
                                trace!("PTY reader: receiver dropped, exiting");
                                return;
                            }
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => {
                        warn!(error = %e, "PTY reader: read error");
                        break;
                    }
                }
            }
            for ev in parser.on_eof() {
                let _ = tx.blocking_send(ev);
            }
            exited.store(true, std::sync::atomic::Ordering::Relaxed);
        })
        .expect("failed to spawn PTY reader thread");
}

fn spawn_writer_thread(
    mut writer: Box<dyn std::io::Write + Send>,
    mut rx: mpsc::Receiver<Vec<u8>>,
) {
    std::thread::Builder::new()
        .name("cap-rs-pty-writer".into())
        .spawn(move || {
            while let Some(bytes) = rx.blocking_recv() {
                if let Err(e) = writer.write_all(&bytes) {
                    warn!(error = %e, "PTY writer: write failed");
                    return;
                }
                if let Err(e) = writer.flush() {
                    warn!(error = %e, "PTY writer: flush failed");
                    return;
                }
            }
            trace!("PTY writer: input channel closed, exiting");
        })
        .expect("failed to spawn PTY writer thread");
}

fn spawn_child_waiter(
    mut child: Box<dyn portable_pty::Child + Send + Sync>,
    event_tx: mpsc::Sender<AgentEvent>,
    exited: std::sync::Arc<std::sync::atomic::AtomicBool>,
    exit_status: std::sync::Arc<std::sync::Mutex<Option<DriverExitStatus>>>,
) {
    std::thread::Builder::new()
        .name("cap-rs-pty-waiter".into())
        .spawn(move || {
            let status = child.wait();
            let mut slot = exit_status.lock().expect("exit_status mutex poisoned");
            if slot.is_none() {
                *slot = Some(match status {
                    Ok(s) => DriverExitStatus::Exited {
                        code: i32::try_from(s.exit_code()).ok(),
                    },
                    Err(_) => DriverExitStatus::Disconnected,
                });
            }
            drop(slot);
            exited.store(true, std::sync::atomic::Ordering::Relaxed);
            drop(event_tx);
        })
        .expect("failed to spawn PTY child waiter thread");
}
