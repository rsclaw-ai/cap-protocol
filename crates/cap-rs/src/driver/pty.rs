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
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use crate::core::{AgentEvent, CancelScope, ClientFrame, Content, StopReason, TextChannel, Usage};
use crate::driver::{Driver, DriverError, DriverExitStatus};

type ChildKillerHandle =
    std::sync::Arc<std::sync::Mutex<Box<dyn portable_pty::ChildKiller + Send + Sync>>>;

// ---------------------------------------------------------------------------
// PromptGate
// ---------------------------------------------------------------------------

/// Shared state the driver hands a quiescence parser so it can reason about the
/// conversation lifecycle. The driver writes it on `send(Prompt)`; the parser
/// reads (and clears `pending_submit`) on its idle ticks.
#[derive(Debug, Default)]
pub struct PromptGate {
    /// A real prompt has been sent. Before this, settles are startup noise and
    /// must not produce `Done`.
    pub armed: bool,
    /// Text of the most recent prompt whose submission hasn't been confirmed.
    /// `Some` between `send(Prompt)` and the parser observing the input box
    /// clear. While `Some` and the text is still sitting in the box, the parser
    /// re-sends Enter (a PTY agent that wasn't input-ready drops the first one).
    pub pending_submit: Option<String>,
    /// Whether the agent has enabled bracketed-paste mode (DECSET 2004). The
    /// parser tracks it from the byte stream; `send(Prompt)` uses it to wrap a
    /// prompt so multi-line text lands as one paste instead of each newline
    /// being interpreted as a keypress/submit by the TUI.
    pub bracketed_paste: bool,
}

/// Bracketed-paste framing (xterm DECSET 2004). Wrapping pasted text in these
/// tells a TUI "this is a paste, not typing" — newlines inside are inert.
const BRACKETED_PASTE_START: &[u8] = b"\x1b[200~";
const BRACKETED_PASTE_END: &[u8] = b"\x1b[201~";

/// Wrap prompt text in bracketed-paste markers.
fn bracketed_wrap(text: &str) -> Vec<u8> {
    let mut v =
        Vec::with_capacity(text.len() + BRACKETED_PASTE_START.len() + BRACKETED_PASTE_END.len());
    v.extend_from_slice(BRACKETED_PASTE_START);
    v.extend_from_slice(text.as_bytes());
    v.extend_from_slice(BRACKETED_PASTE_END);
    v
}

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

    /// How long the byte stream must stay silent before [`Self::on_idle`] is
    /// invoked. `None` (the default) disables idle detection entirely — the
    /// parser is purely byte-driven, exactly as before.
    ///
    /// TUI agents (codex, opencode) emit no structured turn-completion frame;
    /// the only reliable signal that a turn finished is that the agent stopped
    /// emitting bytes and is sitting at its input prompt. Returning `Some(dur)`
    /// arms a timer so [`Self::on_idle`] fires after `dur` of quiescence.
    fn idle_timeout(&self) -> Option<Duration> {
        None
    }

    /// Called when the byte stream has been silent for [`Self::idle_timeout`].
    /// Default: no-op. Quiescence-based parsers use this to synthesise a
    /// turn boundary ([`AgentEvent::Ready`] / [`AgentEvent::Done`]) once the
    /// agent settles at its prompt.
    ///
    /// A spinner or streamed tokens keep bytes flowing, so this is *not*
    /// called mid-turn — only when the agent has genuinely gone quiet.
    fn on_idle(&mut self) -> Vec<AgentEvent> {
        Vec::new()
    }

    /// Optional shared [`PromptGate`] the driver writes on `send(Prompt)`. A
    /// quiescence parser uses it to gate turn-completion (no `Done` before the
    /// first real prompt) and to confirm a prompt actually submitted. `None`
    /// (the default) means the parser does no prompt gating.
    fn prompt_gate(&self) -> Option<std::sync::Arc<std::sync::Mutex<PromptGate>>> {
        None
    }

    /// Bytes the parser wants written back to the agent's stdin (e.g. a re-sent
    /// Enter when a prompt's submission was dropped). The parser thread drains
    /// this after each `on_bytes`/`on_idle` and forwards to the PTY. Default
    /// empty: parsers that never inject input return nothing.
    fn drain_input(&mut self) -> Vec<Vec<u8>> {
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
    ask_options: Option<regex_lite::Regex>,
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
            ask_options: None,
            seen_first_prompt: false,
            turn: 0,
        }
    }

    pub fn with_ask_options(mut self, pattern: &str) -> Self {
        self.ask_options = Some(regex_lite::Regex::new(pattern).expect("invalid regex"));
        self
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
                        session_id: Some(format!("{}-pid{}", self.name, std::process::id())),
                        version: crate::core::CAP_PROTOCOL_VERSION.into(),
                        model: None,
                    });
                }
                return events;
            }
            if let Some(re) = &self.ask_yes_no
                && re.is_match(trimmed)
            {
                events.push(AgentEvent::AskUser {
                    ask_id: format!("ask_{}", self.turn),
                    prompt: trimmed.to_string(),
                    ask_kind: crate::core::AskKind::YesNo,
                    options: Vec::new(),
                    timeout_seconds: None,
                });
                return events;
            }
            if let Some(re) = &self.ask_options
                && re.is_match(trimmed)
            {
                events.push(AgentEvent::AskUser {
                    ask_id: format!("ask_{}", self.turn),
                    prompt: trimmed.to_string(),
                    ask_kind: crate::core::AskKind::Options,
                    options: parse_inline_options(trimmed),
                    timeout_seconds: None,
                });
                return events;
            }
        }
        events
    }
}

fn parse_inline_options(line: &str) -> Vec<crate::core::AskOption> {
    let candidate = line
        .split_once(':')
        .map(|(_, rest)| rest)
        .unwrap_or(line)
        .trim();
    let split = candidate
        .split(['|', '/', ','])
        .map(str::trim)
        .filter(|s| !s.is_empty());
    split
        .enumerate()
        .map(|(idx, label)| crate::core::AskOption {
            id: label
                .split_whitespace()
                .next()
                .filter(|s| !s.is_empty())
                .unwrap_or(label)
                .trim_matches(['[', ']', '(', ')'])
                .to_string()
                .if_empty_else(|| format!("option_{}", idx + 1)),
            label: label.to_string(),
        })
        .collect()
}

trait StringEmptyExt {
    fn if_empty_else(self, f: impl FnOnce() -> String) -> String;
}

impl StringEmptyExt for String {
    fn if_empty_else(self, f: impl FnOnce() -> String) -> String {
        if self.is_empty() { f() } else { self }
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
        if self.emit_cursor > self.buffer.len() {
            self.emit_cursor = self.buffer.len();
        }
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
                        session_id: Some(format!("{}-pid{}", self.name, std::process::id())),
                        version: crate::core::CAP_PROTOCOL_VERSION.into(),
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
                    timeout_seconds: None,
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
// TuiParser — full-screen TUI agents (codex, opencode, …)
// ---------------------------------------------------------------------------

/// Parser for full-screen TUI agents that never emit a structured
/// turn-completion frame (codex's interactive TUI, opencode, …).
///
/// These agents repaint the whole terminal every frame, run spinners while
/// thinking, and keep an input box on screen at all times — so "the prompt is
/// visible" does NOT mean "the turn is done". The only reliable boundary is a
/// **hybrid** of two signals:
///
/// 1. **Idle settle** — the byte stream goes silent for [`Self::idle_timeout`].
///    While the agent thinks/streams it emits bytes (spinner ticks, tokens),
///    so silence only happens once it has truly stopped. This is the
///    agent-agnostic backbone.
/// 2. **Ready marker** — the bottom of the rendered (ANSI-stripped) screen
///    matches a configurable prompt regex (codex's `›`, opencode's `❯`, …).
///    This guards against a mid-turn lull that isn't actually at the prompt.
///
/// On the **first** settle-at-prompt the parser emits [`AgentEvent::Ready`];
/// every subsequent one emits the final screen as an [`AgentEvent::TextChunk`]
/// followed by [`AgentEvent::Done`]. Intermediate frames are not streamed —
/// for orchestration the authoritative turn output is the screen at
/// quiescence (stripping TUI chrome out of that screen is a separate, harder
/// problem, deliberately left as a follow-up; this parser's job is the
/// boundary).
///
/// The ready markers are a per-agent tuning knob, not gospel: real TUIs drift
/// across versions and startup modals can spoof a prompt. Validate the regex
/// against a captured session ([`Self::custom`]) when wiring a new agent.
pub struct TuiParser {
    name: &'static str,
    vt: vt100::Parser,
    /// Last rendered (ANSI-stripped) screen contents.
    last_screen: String,
    /// Bottom-of-screen prompt patterns that mean "ready for input".
    ready_markers: Vec<regex_lite::Regex>,
    /// Byte-silence that counts as the agent having settled.
    idle: Duration,
    /// Have we emitted the first `Ready` yet (startup vs turn boundary).
    seen_first_ready: bool,
    /// Has the screen changed since the last boundary we emitted? Prevents a
    /// second idle tick (no new output) from re-firing `Done`.
    dirty_since_boundary: bool,
    /// Shared with the driver: `armed` (a real prompt was sent) and
    /// `pending_submit` (a prompt whose Enter we're confirming landed).
    gate: std::sync::Arc<std::sync::Mutex<PromptGate>>,
    /// Bytes to write back to the agent (a re-sent Enter); drained by the
    /// parser thread.
    to_send: Vec<Vec<u8>>,
    /// How many times we've re-sent Enter for the current pending prompt.
    /// Bounded so a detection miss can't loop forever.
    resubmit_attempts: u32,
    /// Monotonic turn counter, tags synthesized event message ids.
    turn: u64,
}

/// Max times to re-send Enter for one prompt before giving up and treating the
/// next settle as a turn boundary anyway.
const MAX_RESUBMITS: u32 = 4;

impl std::fmt::Debug for TuiParser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TuiParser")
            .field("name", &self.name)
            .field("turn", &self.turn)
            .field("dirty", &self.dirty_since_boundary)
            .finish()
    }
}

impl TuiParser {
    /// Parser tuned for OpenAI's `codex` interactive TUI. Ready marker `›`
    /// captured from a live session.
    pub fn codex() -> Self {
        Self::custom("codex", &[r"›"], Duration::from_millis(800))
    }

    /// Parser tuned for `opencode`. Markers are best-effort across the common
    /// prompt glyphs; validate against a real capture when finalizing.
    pub fn opencode() -> Self {
        Self::custom(
            "opencode",
            &[r"❯", r"›", r">\s*$"],
            Duration::from_millis(800),
        )
    }

    /// Parser tuned for `openclaude`. Uses `>` prompt from the reference
    /// manifest. Falls back to the generic marker set as well.
    pub fn openclaude() -> Self {
        Self::custom(
            "openclaude",
            &[r">\s*$", r"›", r"❯"],
            Duration::from_millis(800),
        )
    }

    /// Generic full-screen TUI fallback for an unknown `pty:<cmd>` agent.
    /// Accepts the usual prompt glyphs. Turn detection is best-effort until a
    /// real marker is captured for the specific agent.
    /// Tuned for aider chat (<https://github.com/paul-gauthier/aider>).
    /// Uses `>` prompt marker and 800 ms idle timeout.
    pub fn aider() -> Self {
        Self::custom("aider", &[r">\s*$", r"❯"], Duration::from_millis(800))
    }

    pub fn generic() -> Self {
        Self::custom(
            "tui",
            &[r"›", r"❯", r">\s*$", r"\$\s*$"],
            Duration::from_millis(800),
        )
    }

    /// Custom constructor: name, bottom-of-screen ready-prompt regexes, and the
    /// byte-silence window that counts as "settled".
    pub fn custom(name: &'static str, markers: &[&str], idle: Duration) -> Self {
        let ready_markers = markers
            .iter()
            .map(|p| regex_lite::Regex::new(p).expect("invalid ready-marker regex"))
            .collect();
        Self {
            name,
            vt: vt100::Parser::new(50, 200, 10_000),
            last_screen: String::new(),
            ready_markers,
            idle,
            seen_first_ready: false,
            dirty_since_boundary: false,
            gate: std::sync::Arc::new(std::sync::Mutex::new(PromptGate::default())),
            to_send: Vec::new(),
            resubmit_attempts: 0,
            turn: 0,
        }
    }

    /// Update the shared bracketed-paste flag from a raw byte chunk by looking
    /// for DECSET 2004 enable/disable (`ESC [ ? 2004 h|l`). Only the last
    /// occurrence in the chunk matters.
    fn detect_bracketed_paste(&self, bytes: &[u8]) {
        const ENABLE: &[u8] = b"\x1b[?2004h";
        const DISABLE: &[u8] = b"\x1b[?2004l";
        let last_enable = bytes.windows(ENABLE.len()).rposition(|w| w == ENABLE);
        let last_disable = bytes.windows(DISABLE.len()).rposition(|w| w == DISABLE);
        let new_state = match (last_enable, last_disable) {
            (Some(e), Some(d)) => e > d,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => return,
        };
        self.gate
            .lock()
            .expect("gate mutex poisoned")
            .bracketed_paste = new_state;
    }

    /// The bottom-most line that matches a ready marker — the live input box
    /// (as opposed to an earlier prompt echoed up into the transcript).
    fn input_box_line(&self, screen: &str) -> Option<String> {
        screen
            .lines()
            .rev()
            .find(|line| {
                let trimmed = line.trim_end_matches(['\n', '\r']);
                self.ready_markers.iter().any(|re| re.is_match(trimmed))
            })
            .map(|l| l.to_string())
    }

    /// Strip trailing prompt/status lines from a screen capture so the
    /// TextChunk sent downstream contains only the assistant's output, not
    /// the TUI chrome (input prompt, empty lines at the bottom).
    fn strip_chrome(&self, screen: &str) -> String {
        let lines: Vec<&str> = screen.lines().collect();
        let keep: Vec<&str> = lines
            .iter()
            .rev()
            .skip_while(|line| {
                let trimmed = line.trim_end_matches(['\n', '\r']);
                trimmed.is_empty() || self.ready_markers.iter().any(|re| re.is_match(trimmed))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .copied()
            .collect();
        if keep.is_empty() {
            return screen.to_string();
        }
        if keep.len() == lines.len() {
            return screen.to_string();
        }
        let mut out = keep.join("\n");
        out.push('\n');
        out
    }

    /// True if any of the last few non-empty screen lines matches a ready
    /// marker (the input prompt is showing at the bottom).
    fn at_prompt(&self, screen: &str) -> bool {
        screen
            .lines()
            .rev()
            .filter(|l| !l.trim().is_empty())
            .take(4)
            .any(|line| {
                let trimmed = line.trim_end_matches(['\n', '\r']);
                self.ready_markers.iter().any(|re| re.is_match(trimmed))
            })
    }
}

impl AgentParser for TuiParser {
    fn name(&self) -> &str {
        self.name
    }

    fn on_bytes(&mut self, bytes: &[u8]) -> Vec<AgentEvent> {
        // Track bracketed-paste mode from the raw stream (engine-agnostic), so
        // send(Prompt) can wrap multi-line prompts safely.
        self.detect_bracketed_paste(bytes);
        // Feed the terminal emulator and note whether anything actually
        // changed. We do NOT stream intermediate frames — the authoritative
        // turn output is the screen captured at the next quiescence boundary.
        self.vt.process(bytes);
        let screen = self.vt.screen().contents();
        if screen != self.last_screen {
            self.last_screen = screen;
            self.dirty_since_boundary = true;
        }
        Vec::new()
    }

    fn idle_timeout(&self) -> Option<Duration> {
        Some(self.idle)
    }

    fn prompt_gate(&self) -> Option<std::sync::Arc<std::sync::Mutex<PromptGate>>> {
        Some(std::sync::Arc::clone(&self.gate))
    }

    fn drain_input(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.to_send)
    }

    fn on_idle(&mut self) -> Vec<AgentEvent> {
        // Nothing new since the last boundary → the agent is just parked at
        // its prompt. Stay silent (don't re-fire Done every idle tick).
        if !self.dirty_since_boundary {
            return Vec::new();
        }
        // Quiet, but is it quiet *at the prompt*? If not (mid-turn lull, a
        // tool running with no output, a modal), wait for more signal.
        if !self.at_prompt(&self.last_screen) {
            return Vec::new();
        }

        let (armed, pending) = {
            let g = self.gate.lock().expect("prompt gate poisoned");
            (g.armed, g.pending_submit.clone())
        };

        // A turn boundary only counts once a real prompt has been sent; before
        // that, settles are startup noise (boot frames, modals) and emitting
        // Done would route empty output downstream. Don't consume the dirty
        // flag pre-arm — keep tracking so the first armed settle still fires.
        if !armed {
            self.dirty_since_boundary = false;
            if !self.seen_first_ready {
                self.seen_first_ready = true;
                return vec![AgentEvent::Ready {
                    session_id: Some(format!("{}-pid{}", self.name, std::process::id())),
                    version: crate::core::CAP_PROTOCOL_VERSION.into(),
                    model: None,
                }];
            }
            return Vec::new();
        }

        // Armed. Before declaring the turn done, confirm the prompt actually
        // submitted: a PTY agent that wasn't input-ready drops the first Enter,
        // leaving our prompt text sitting in the input box. If it's still there,
        // re-send Enter (bounded) and wait rather than route the un-run screen.
        if let Some(text) = &pending {
            let needle: String = text.trim().chars().take(40).collect();
            let still_in_box = !needle.is_empty()
                && self
                    .input_box_line(&self.last_screen)
                    .is_some_and(|line| line.contains(&needle));
            if still_in_box && self.resubmit_attempts < MAX_RESUBMITS {
                self.resubmit_attempts += 1;
                self.to_send.push(b"\r".to_vec());
                // Don't consume dirty: we want the next settle re-evaluated.
                return Vec::new();
            }
            // Submitted (box cleared) or we've exhausted retries → the prompt is
            // resolved; stop tracking it.
            self.gate
                .lock()
                .expect("prompt gate poisoned")
                .pending_submit = None;
            self.resubmit_attempts = 0;
            if still_in_box {
                // Gave up re-sending; fall through to emit a boundary anyway so
                // we don't hang the fleet, but the screen will show the unsent
                // prompt (a loud, debuggable symptom rather than a silent hang).
                warn!(
                    parser = self.name,
                    "prompt may not have submitted after retries"
                );
            }
        }

        self.dirty_since_boundary = false;
        self.turn += 1;
        let text = self.strip_chrome(&self.last_screen);
        vec![
            AgentEvent::TextChunk {
                msg_id: format!("turn_{}", self.turn),
                text,
                channel: TextChannel::Assistant,
            },
            AgentEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: Usage::default(),
            },
        ]
    }

    fn on_eof(&mut self) -> Vec<AgentEvent> {
        let mut events = Vec::new();
        if self.dirty_since_boundary && !self.last_screen.is_empty() {
            events.push(AgentEvent::TextChunk {
                msg_id: format!("turn_{}", self.turn),
                text: self.strip_chrome(&self.last_screen),
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

    /// Signal handle cloned from the child before the waiter owns it.
    child_killer: ChildKillerHandle,

    /// Child process id, when the platform exposes one.
    process_id: Option<u32>,

    /// Grace period between SIGTERM and SIGKILL for `Cancel { Session }`.
    hard_cancel_grace: Duration,

    /// Parser-supplied gate, written on each `Prompt` so a quiescence parser
    /// knows the conversation started and can confirm the prompt submitted.
    /// `None` when the parser does no prompt gating.
    prompt_gate: Option<std::sync::Arc<std::sync::Mutex<PromptGate>>>,
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
            hard_cancel_grace: Duration::from_secs(5),
            include_raw_bytes: false,
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

    fn kill_child(&self) -> Result<(), DriverError> {
        kill_child_handle(&self.child_killer)
    }
}

#[async_trait]
impl Driver for PtyDriver {
    async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError> {
        match frame {
            ClientFrame::Prompt { content } => {
                let mut prompt_text = String::new();
                for c in content {
                    if let Content::Text { text } = c {
                        prompt_text.push_str(&text);
                    }
                    // Image / other content not meaningful for raw PTY.
                }
                // If the agent enabled bracketed paste, wrap the text so a
                // multi-line prompt (e.g. routed upstream output) lands as one
                // paste rather than each newline submitting/triggering the TUI.
                let bracketed = self
                    .prompt_gate
                    .as_ref()
                    .map(|g| g.lock().expect("prompt gate poisoned").bracketed_paste)
                    .unwrap_or(false);
                if bracketed {
                    self.send_bytes(&bracketed_wrap(&prompt_text)).await?;
                } else {
                    self.send_bytes(prompt_text.as_bytes()).await?;
                }
                // Let the text settle into the agent's input widget before the
                // Enter. A `\r` sent back-to-back races ahead of a TUI ingesting
                // the text and gets dropped, leaving the prompt stuck in the
                // input box. Harmless latency for line-based agents.
                tokio::time::sleep(Duration::from_millis(150)).await;
                self.send_bytes(b"\r").await?;
                // Tell a quiescence parser the conversation has started (settles
                // now count as turns) and which prompt to confirm submitted —
                // if the Enter was dropped, the parser re-sends it.
                if let Some(gate) = &self.prompt_gate {
                    let mut g = gate.lock().expect("prompt gate poisoned");
                    g.armed = true;
                    g.pending_submit = Some(prompt_text);
                }
                Ok(())
            }
            ClientFrame::Cancel { scope, .. } => {
                match scope {
                    CancelScope::CurrentTurn => self.send_bytes(b"\x03").await, // Ctrl+C
                    CancelScope::Session => {
                        hard_cancel_session(
                            std::sync::Arc::clone(&self.child_killer),
                            self.process_id,
                            std::sync::Arc::clone(&self.exited),
                            self.hard_cancel_grace,
                        )
                        .await
                    }
                }
            }
            ClientFrame::SessionConfig(_) => {
                // PTY agents consume config at spawn-time. The orchestrator
                // still sends the CAP-required first frame; acknowledge it so
                // the lifecycle remains spec-ordered.
                Ok(())
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
            ClientFrame::ReverseRpcResult { .. } => Err(DriverError::AgentError {
                code: "cap_reverse_rpc_unsupported".into(),
                message: "PTY driver does not emit reverse RPC".into(),
            }),
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
        let _ = self.kill_child();
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

    fn prompt_after_ready(&self) -> bool {
        // A PTY/TUI agent must boot to its input prompt before it can accept a
        // prompt; sending earlier loses it into a not-ready terminal.
        true
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
    hard_cancel_grace: Duration,
    include_raw_bytes: bool,
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

    pub fn hard_cancel_grace(mut self, grace: Duration) -> Self {
        self.hard_cancel_grace = grace;
        self
    }

    pub fn include_raw_bytes(mut self, include: bool) -> Self {
        self.include_raw_bytes = include;
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
            hard_cancel_grace,
            include_raw_bytes,
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
            // Strip sensitive env vars by default to prevent credential
            // leakage to spawned agents.
            if is_sensitive_env_var(&k_str) {
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
        let process_id = child.process_id();
        let child_killer = std::sync::Arc::new(std::sync::Mutex::new(child.clone_killer()));

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

        // Grab the parser's prompt gate (if any) before it moves into the
        // parser thread, so the driver can flip it on the first Prompt.
        let prompt_gate = parser.prompt_gate();

        // Raw bytes flow reader-thread → parser-thread. The split exists so the
        // parser thread can apply an idle timer (`recv_timeout`) that the
        // blocking PTY `read()` loop in the reader thread cannot provide.
        let (raw_tx, raw_rx) = std::sync::mpsc::channel::<Vec<u8>>();
        spawn_reader_thread(reader, raw_tx)?;
        spawn_parser_thread(
            parser,
            raw_rx,
            event_tx.clone(),
            input_tx.clone(),
            std::sync::Arc::clone(&exited),
            include_raw_bytes,
        )?;
        spawn_writer_thread(writer, input_rx)?;
        spawn_child_waiter(
            child,
            event_tx,
            std::sync::Arc::clone(&exited),
            std::sync::Arc::clone(&exit_status),
        )?;

        // Drop slave — only master is kept.
        drop(pair.slave);

        Ok(PtyDriver {
            input_tx: Some(input_tx),
            event_rx,
            master: pair.master,
            exited,
            exit_status,
            child_killer,
            process_id,
            hard_cancel_grace,
            prompt_gate,
        })
    }
}

// ---------------------------------------------------------------------------
// Background threads (PTY API is sync; we bridge to async via channels)
// ---------------------------------------------------------------------------

fn kill_child_handle(child_killer: &ChildKillerHandle) -> Result<(), DriverError> {
    child_killer
        .lock()
        .expect("child killer mutex poisoned")
        .kill()
        .map_err(|e| DriverError::Io(std::io::Error::other(e.to_string())))
}

#[cfg(unix)]
fn signal_child_handle(
    child_killer: &ChildKillerHandle,
    process_id: Option<u32>,
    signal: libc::c_int,
) -> Result<(), DriverError> {
    let Some(pid) = process_id else {
        return kill_child_handle(child_killer);
    };
    let rc = unsafe { libc::kill(pid as libc::pid_t, signal) };
    if rc == 0 {
        return Ok(());
    }
    let e = std::io::Error::last_os_error();
    if e.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(DriverError::Io(e))
}

#[cfg(not(unix))]
fn signal_child_handle(
    child_killer: &ChildKillerHandle,
    _process_id: Option<u32>,
    _signal: i32,
) -> Result<(), DriverError> {
    kill_child_handle(child_killer)
}

async fn hard_cancel_session(
    child_killer: ChildKillerHandle,
    process_id: Option<u32>,
    exited: std::sync::Arc<std::sync::atomic::AtomicBool>,
    grace: Duration,
) -> Result<(), DriverError> {
    if exited.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(());
    }
    #[cfg(unix)]
    signal_child_handle(&child_killer, process_id, libc::SIGTERM)?;
    #[cfg(not(unix))]
    signal_child_handle(&child_killer, process_id, 0)?;

    tokio::time::sleep(grace).await;
    if !exited.load(std::sync::atomic::Ordering::Relaxed) {
        #[cfg(unix)]
        signal_child_handle(&child_killer, process_id, libc::SIGKILL)?;
        #[cfg(not(unix))]
        kill_child_handle(&child_killer)?;
    }
    Ok(())
}

/// Reader thread: blocking PTY reads, forwarding raw byte chunks to the parser
/// thread. Owns no parser and no event channel — its sole job is to drain the
/// PTY. Dropping `raw_tx` on EOF/error signals the parser thread to finalize.
fn spawn_reader_thread(
    mut reader: Box<dyn std::io::Read + Send>,
    raw_tx: std::sync::mpsc::Sender<Vec<u8>>,
) -> Result<(), DriverError> {
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
                        if raw_tx.send(buf[..n].to_vec()).is_err() {
                            trace!("PTY reader: parser thread gone, exiting");
                            return;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => {
                        warn!(error = %e, "PTY reader: read error");
                        break;
                    }
                }
            }
            // Dropping raw_tx here ends the parser thread's recv loop.
        })
        .map_err(|e| DriverError::Io(std::io::Error::other(e.to_string())))?;
    Ok(())
}

/// Parser thread: owns the [`AgentParser`] and the event channel. Consumes raw
/// byte chunks from the reader thread. When the parser advertises an
/// [`AgentParser::idle_timeout`], a quiescent stretch (`recv_timeout` elapses)
/// drives [`AgentParser::on_idle`] — the timer a blocking read loop can't give.
/// Channel disconnect (reader hit EOF) drives [`AgentParser::on_eof`].
fn spawn_parser_thread<P: AgentParser>(
    mut parser: P,
    raw_rx: std::sync::mpsc::Receiver<Vec<u8>>,
    tx: mpsc::Sender<AgentEvent>,
    input_tx: mpsc::Sender<Vec<u8>>,
    exited: std::sync::Arc<std::sync::atomic::AtomicBool>,
    include_raw_bytes: bool,
) -> Result<(), DriverError> {
    use std::sync::mpsc::RecvTimeoutError;
    std::thread::Builder::new()
        .name("cap-rs-pty-parser".into())
        .spawn(move || {
            let idle = parser.idle_timeout();
            // Forward any bytes the parser wants written back to the agent
            // (e.g. a re-sent Enter); returns false if the writer is gone.
            macro_rules! flush_injected {
                () => {{
                    let mut ok = true;
                    for b in parser.drain_input() {
                        if input_tx.blocking_send(b).is_err() {
                            ok = false;
                            break;
                        }
                    }
                    ok
                }};
            }
            loop {
                let recv = match idle {
                    Some(d) => raw_rx.recv_timeout(d),
                    None => raw_rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
                };
                let events = match recv {
                    Ok(bytes) => {
                        if include_raw_bytes
                            && tx
                                .blocking_send(AgentEvent::PtyRawBytes {
                                    bytes: Arc::from(bytes.clone().into_boxed_slice()),
                                })
                                .is_err()
                        {
                            trace!("PTY parser: receiver dropped, exiting");
                            return;
                        }
                        parser.on_bytes(&bytes)
                    }
                    Err(RecvTimeoutError::Timeout) => parser.on_idle(),
                    Err(RecvTimeoutError::Disconnected) => break,
                };
                for ev in events {
                    if tx.blocking_send(ev).is_err() {
                        trace!("PTY parser: receiver dropped, exiting");
                        return;
                    }
                }
                if !flush_injected!() {
                    trace!("PTY parser: input writer gone, exiting");
                    return;
                }
            }
            for ev in parser.on_eof() {
                let _ = tx.blocking_send(ev);
            }
            exited.store(true, std::sync::atomic::Ordering::Relaxed);
        })
        .map_err(|e| DriverError::Io(std::io::Error::other(e.to_string())))?;
    Ok(())
}

fn spawn_writer_thread(
    mut writer: Box<dyn std::io::Write + Send>,
    mut rx: mpsc::Receiver<Vec<u8>>,
) -> Result<(), DriverError> {
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
        .map_err(|e| DriverError::Io(std::io::Error::other(e.to_string())))?;
    Ok(())
}

fn spawn_child_waiter(
    mut child: Box<dyn portable_pty::Child + Send + Sync>,
    event_tx: mpsc::Sender<AgentEvent>,
    exited: std::sync::Arc<std::sync::atomic::AtomicBool>,
    exit_status: std::sync::Arc<std::sync::Mutex<Option<DriverExitStatus>>>,
) -> Result<(), DriverError> {
    std::thread::Builder::new()
        .name("cap-rs-pty-waiter".into())
        .spawn(move || {
            let status = child.wait();
            let mut slot = exit_status.lock().expect("exit_status mutex poisoned");
            let unexpected = slot.is_none();
            let final_status = match status {
                Ok(s) => DriverExitStatus::Exited {
                    code: i32::try_from(s.exit_code()).ok(),
                },
                Err(_) => DriverExitStatus::Disconnected,
            };
            if slot.is_none() {
                *slot = Some(final_status.clone());
            }
            drop(slot);
            exited.store(true, std::sync::atomic::Ordering::Relaxed);
            if unexpected {
                let _ = event_tx.blocking_send(AgentEvent::Error {
                    code: "pty_died".into(),
                    message: format!("PTY child exited unexpectedly: {final_status:?}"),
                    retryable: false,
                    details: None,
                });
            }
            drop(event_tx);
        })
        .map_err(|e| DriverError::Io(std::io::Error::other(e.to_string())))?;
    Ok(())
}

/// Returns true if the env var name matches a known sensitive pattern and
/// should be stripped before spawning a child agent process.
fn is_sensitive_env_var(name: &str) -> bool {
    const EXACT: &[&str] = &[
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "CODEX_API_KEY",
        "GITHUB_TOKEN",
        "GH_TOKEN",
        "NPM_TOKEN",
        "PYPI_TOKEN",
        "DOCKER_PASSWORD",
        "CODESPACES_TOKEN",
    ];
    const SUFFIXES: &[&str] = &[
        "_TOKEN", "_KEY", "_SECRET", "_PASSWORD", "_CREDENTIALS",
    ];
    const PREFIXES: &[&str] = &[
        "AWS_", "GCP_", "AZURE_", "GOOGLE_",
    ];

    if EXACT.contains(&name) {
        return true;
    }
    if SUFFIXES.iter().any(|s| name.ends_with(s)) {
        return true;
    }
    if PREFIXES.iter().any(|p| name.starts_with(p)) {
        return true;
    }
    false
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// First settle-at-prompt is the agent coming up: emit `Ready`, not `Done`.
    #[test]
    fn tui_first_idle_at_prompt_emits_ready() {
        let mut p = TuiParser::codex();
        // codex ready glyph is `›` (U+203A).
        assert!(
            p.on_bytes("welcome to codex\n\u{203a} ".as_bytes())
                .is_empty(),
            "intermediate frames are not streamed"
        );
        let evs = p.on_idle();
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], AgentEvent::Ready { .. }), "got {evs:?}");
    }

    /// Simulate the driver sending a prompt by arming the gate. No
    /// `pending_submit` → the parser treats the next settle as a clean turn
    /// boundary (submission already confirmed elsewhere).
    fn arm(p: &TuiParser) {
        let gate = p.prompt_gate().expect("TuiParser exposes a gate");
        let mut g = gate.lock().unwrap();
        g.armed = true;
        g.pending_submit = None;
    }

    #[tokio::test]
    async fn child_exit_emits_pty_died_error() {
        let mut driver = PtyDriver::builder("/bin/sh")
            .args(["-lc", "exit 7"])
            .spawn(RawParser)
            .unwrap();

        let mut saw_pty_died = false;
        for _ in 0..8 {
            let ev = tokio::time::timeout(Duration::from_secs(2), driver.next_event())
                .await
                .unwrap();
            let Some(ev) = ev else { break };
            if let AgentEvent::Error { code, .. } = ev {
                saw_pty_died = code == "pty_died";
                break;
            }
        }
        assert!(saw_pty_died);
    }

    #[tokio::test]
    async fn cancel_session_terminates_pty_child() {
        let mut driver = PtyDriver::builder("/bin/sh")
            .args(["-lc", "trap '' INT; while true; do sleep 1; done"])
            .hard_cancel_grace(Duration::from_millis(50))
            .spawn(RawParser)
            .unwrap();

        driver
            .send(ClientFrame::Cancel {
                scope: CancelScope::Session,
                reason: Some("test".into()),
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(3), async {
            while driver.is_alive() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            driver.exit_status(),
            Some(DriverExitStatus::Exited { .. } | DriverExitStatus::Disconnected)
        ));
    }

    /// A turn after Ready (and after a real prompt) settles to `TextChunk`
    /// (final screen) + `Done`.
    #[test]
    fn tui_second_idle_at_prompt_emits_textchunk_then_done() {
        let mut p = TuiParser::codex();
        p.on_bytes("\u{203a} ".as_bytes());
        assert!(matches!(p.on_idle().as_slice(), [AgentEvent::Ready { .. }]));

        arm(&p); // driver sent the prompt
        // New turn output, then the prompt returns.
        p.on_bytes("the answer is 42\n\u{203a} ".as_bytes());
        let evs = p.on_idle();
        assert_eq!(evs.len(), 2, "got {evs:?}");
        assert!(matches!(evs[0], AgentEvent::TextChunk { .. }));
        assert!(matches!(
            evs[1],
            AgentEvent::Done {
                stop_reason: StopReason::EndTurn,
                ..
            }
        ));
    }

    /// The gate's whole point: BEFORE a real prompt is sent, settles after the
    /// initial Ready are startup noise (boot frames, modals) and must NOT emit
    /// Done — otherwise the orchestrator routes empty output downstream. After
    /// the prompt (armed), the next settle is a real turn.
    #[test]
    fn tui_settles_before_prompt_emit_no_done() {
        let mut p = TuiParser::codex();
        // Boot: first settle → Ready.
        p.on_bytes("codex booting\n\u{203a} ".as_bytes());
        assert!(matches!(p.on_idle().as_slice(), [AgentEvent::Ready { .. }]));

        // More startup churn (MCP load, a modal) that settles at a prompt.
        p.on_bytes("Update available  1. Update  2. Skip\n\u{203a} ".as_bytes());
        assert!(
            p.on_idle().is_empty(),
            "settles before the first prompt must not fire Done"
        );

        // Driver sends the task prompt; now settles are real turns.
        arm(&p);
        p.on_bytes("done working\n\u{203a} ".as_bytes());
        let evs = p.on_idle();
        assert!(
            matches!(
                evs.as_slice(),
                [AgentEvent::TextChunk { .. }, AgentEvent::Done { .. }]
            ),
            "armed settle must fire a turn boundary; got {evs:?}"
        );
    }

    /// Submit verification: if the prompt is dropped (still sitting in the
    /// input box at the settle), the parser re-sends Enter instead of emitting
    /// Done, then fires the turn boundary once the box clears.
    #[test]
    fn tui_resends_enter_when_prompt_stuck_then_done_on_submit() {
        let mut p = TuiParser::codex();
        p.on_bytes("booting\n\u{203a} ".as_bytes());
        assert!(matches!(p.on_idle().as_slice(), [AgentEvent::Ready { .. }]));

        // Driver sent a prompt: arm + record the text to confirm submitted.
        {
            let gate = p.prompt_gate().unwrap();
            let mut g = gate.lock().unwrap();
            g.armed = true;
            g.pending_submit = Some("do the thing".into());
        }

        // codex echoed the prompt but dropped the Enter — text sits in the box.
        p.on_bytes("\u{203a} do the thing".as_bytes());
        let evs = p.on_idle();
        assert!(
            evs.is_empty(),
            "stuck prompt must not fire Done; got {evs:?}"
        );
        assert_eq!(
            p.drain_input(),
            vec![b"\r".to_vec()],
            "should re-send Enter"
        );

        // Enter took: screen repaints, box no longer holds the prompt text.
        p.on_bytes("\u{1b}[2J\u{1b}[H• done working\n\u{203a} ".as_bytes());
        let evs = p.on_idle();
        assert!(
            matches!(
                evs.as_slice(),
                [AgentEvent::TextChunk { .. }, AgentEvent::Done { .. }]
            ),
            "once submitted, the next settle is the turn boundary; got {evs:?}"
        );
        assert!(
            p.drain_input().is_empty(),
            "no further re-sends after submit"
        );
    }

    /// The parser tracks DECSET 2004 from the byte stream into the gate, so
    /// send(Prompt) knows whether to bracketed-paste-wrap the prompt.
    #[test]
    fn tui_tracks_bracketed_paste_mode() {
        let mut p = TuiParser::codex();
        let gate = p.prompt_gate().unwrap();
        assert!(!gate.lock().unwrap().bracketed_paste, "off by default");

        p.on_bytes(b"welcome \x1b[?2004h\xe2\x80\xba ");
        assert!(gate.lock().unwrap().bracketed_paste, "enable detected");

        p.on_bytes(b"bye \x1b[?2004l");
        assert!(!gate.lock().unwrap().bracketed_paste, "disable detected");
    }

    /// Bracketed-paste framing wraps the text in DECSET 2004 paste markers so
    /// the TUI treats embedded newlines as content, not submits.
    #[test]
    fn bracketed_wrap_frames_multiline_text() {
        let wrapped = bracketed_wrap("line1\nline2");
        assert!(wrapped.starts_with(b"\x1b[200~"));
        assert!(wrapped.ends_with(b"\x1b[201~"));
        // The newline is carried inside the paste verbatim.
        assert!(wrapped.windows(11).any(|w| w == b"line1\nline2"));
    }

    /// The prompt gate is opt-in: only TuiParser exposes one.
    #[test]
    fn only_tui_parser_exposes_prompt_gate() {
        assert!(RawParser.prompt_gate().is_none());
        assert!(VtPlainParser::new(50, 200).prompt_gate().is_none());
        assert!(ReplParser::generic_repl().prompt_gate().is_none());
        assert!(TuiParser::codex().prompt_gate().is_some());
    }

    /// Idling at the prompt with no NEW output must stay silent — otherwise
    /// every idle tick would re-fire Done while the agent waits for input.
    #[test]
    fn tui_idle_without_new_output_is_silent() {
        let mut p = TuiParser::codex();
        p.on_bytes("\u{203a} ".as_bytes());
        assert!(matches!(p.on_idle().as_slice(), [AgentEvent::Ready { .. }]));
        assert!(
            p.on_idle().is_empty(),
            "second idle with no new bytes must not re-fire"
        );
    }

    /// Quiet but NOT at the prompt (mid-turn lull, tool running) → wait.
    #[test]
    fn tui_idle_without_marker_is_silent() {
        let mut p = TuiParser::codex();
        p.on_bytes(b"thinking hard, no prompt on screen yet\n");
        assert!(
            p.on_idle().is_empty(),
            "no ready marker means the turn is not done"
        );
    }

    /// EOF always closes the turn with a `Done`.
    #[test]
    fn tui_eof_emits_done() {
        let mut p = TuiParser::codex();
        p.on_bytes(b"partial output, agent died\n");
        let evs = p.on_eof();
        assert!(
            matches!(evs.last(), Some(AgentEvent::Done { .. })),
            "got {evs:?}"
        );
    }

    /// Idle detection is opt-in: only TuiParser arms the timer. The byte-driven
    /// parsers must report no idle timeout so their behavior is unchanged.
    #[test]
    fn only_tui_parser_arms_idle_timer() {
        assert!(RawParser.idle_timeout().is_none());
        assert!(VtPlainParser::new(50, 200).idle_timeout().is_none());
        assert!(ReplParser::generic_repl().idle_timeout().is_none());
        assert!(TuiParser::codex().idle_timeout().is_some());
    }

    /// End-to-end through the real PTY thread plumbing: a process that prints a
    /// prompt then goes quiet must surface a `Ready` via the idle timer.
    #[tokio::test]
    async fn pty_tui_emits_ready_on_real_idle() {
        let parser = TuiParser::custom("test", &["\u{203a}"], Duration::from_millis(150));
        let mut driver = PtyDriver::builder("sh")
            .arg("-c")
            // Print a prompt, then sit idle so the idle timer fires.
            .arg("printf 'welcome\\n\u{203a} '; sleep 3")
            .spawn(parser)
            .expect("spawn PTY");

        let ev = tokio::time::timeout(Duration::from_secs(2), driver.next_event())
            .await
            .expect("timed out waiting for Ready")
            .expect("event stream closed unexpectedly");
        assert!(matches!(ev, AgentEvent::Ready { .. }), "got {ev:?}");

        let _ = driver.shutdown().await;
    }
}
