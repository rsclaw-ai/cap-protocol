# cap-protocol — status (2026-06-01)

Lives under `docs/` (not normative — overwrite on each work session).

## Latest (2026-06-01): CAP v1 conformance + full driver matrix on `main`

The reference implementation now covers the whole v1 driver matrix and
passes CAP core v1 conformance. Highlights since the orchestrator landed:

- **CAP core v1 conformance** — `manifest.rs` + `wire.rs` added; spec §7
  surface (SessionConfig, Usage, ToolCallDelta, risk_level, CancelScope,
  PermissionMode, AskKind::Schema, …) wired through core types. Latest
  `fix: resolve CAP v1 spec compliance gaps` closed the remaining drift.
- **A2A driver** (`A2aDriver`, `a2a:<url>`) — drive a remote agent over
  A2A HTTPS+SSE. Validated against a real A2A endpoint.
- **`cap chat`** — interactive multi-agent conversation; with no fleet
  file it auto-creates a single-agent fleet from `--driver`.
- **LLM / hybrid routing** — `cap run --mode static|llm|hybrid`; an LLM
  can pick the next session at each hand-off.
- **aider driver** (`DriverKind::Aider`) — first-class PTY agent via
  `ReplParser::aider` / `TuiParser::aider`.
- **codex defaults to `stream-json`** (Claude Code-compatible NDJSON).
  `pty:codex` stays as the screen-scraping fallback; `codex exec --json`
  remains as `DriverKind::Codex`.
- **`CodexAppServerDriver` removed** — the `codex app-server` websocket
  transport was never reliable from the operator network (see the
  obsolete blocker section below, now **RESOLVED** by removal). The
  stream-json path supersedes it.

Current driver matrix (`cap list-drivers`): `claude`, `openclaude`,
`codex`, `opencode`, `aider`, `a2a:<url>`, `acp:<cmd>`, `grpc:<addr>`,
`pty:<cmd>`.

**Next:** remote transport (WS/Tailscale) → push-based remote permission
approval → mobile approval app.

---

## `cap-rs-orchestrator` engine landed (sub-project 1 of the remote-control vision)

On branch `design/orchestrator-engine`. A headless multi-agent orchestration
engine: runs N collaborating CLI agents in one process from a declarative
`fleet.yaml`, driven locally by `cap run <fleet.yaml>`. Design:
`docs/cap-orchestrator-engine-design.md`; plan: `docs/cap-orchestrator-engine-plan.md`.

- **Architecture:** actor model — one tokio task per session owns a `Box<dyn Driver>`
  (Driver is Send-not-Sync; no `Mutex<Driver>`); a deterministic `executor` state
  machine drives a `SessionRegistry` from the validated fleet spec; an audit log
  records every cross-session route.
- **Works (vs `StubDriver`, zero LLM/network):** pipeline, lead-worker fan-out+join,
  parallel race, `by_subtask` split; per-session `ask`/`allow`/`deny` + fleet `bypass`;
  interactive `ask` decision round-trip; git-worktree isolation per session.
- **Agent fidelity tiers (key architecture):** prefer a native structured protocol per
  agent; PTY screen-scraping is the universal floor, not the goal.
  - **claude** → `stream-json` (structured, full-duplex). High fidelity.
  - **opencode** → **ACP** (`acp:opencode`, `AcpDriver`): Agent Client Protocol over
    JSON-RPC/stdio. Structured streaming — `agent_message_chunk`→`TextChunk`,
    `agent_thought_chunk`→`Thought`, `tool_call(_update)`→`ToolCall*`,
    `session/request_permission`→`PermissionRequest` (gated by the normal CAP permission
    flow). Turn boundary = the `session/prompt` **response** (`stopReason`+`usage`), not a
    notification. **Fleet-validated** (`acp:opencode → claude`): clean `Thought`+`hello`,
    no TUI chrome, none of the PTY hacks needed. Wire format verified against real
    `opencode acp` v1.14. This is the high-fidelity opencode path; `pty:opencode` remains
    as a fallback.
  - **codex** → `stream-json` (modified local Codex build, Claude Code-compatible
    NDJSON). This is the high-fidelity codex path for `driver: codex`; `pty:codex`
    remains available as the screen-scraping fallback, and `codex_mcp.rs` remains
    as a lower-level backend implementation but is not the default driver mapping.
- **PTY floor (universal fallback):** `pty:<cmd>` goes through the PTY path with a
  turn-completion heuristic — `TuiParser` (hybrid:
  **idle-settle + ready-marker + prompt-sent gate**). A TUI emits no structured `Done`,
  so the boundary is inferred: byte-silence for ~800ms AND the bottom of the rendered
  screen showing the agent's prompt glyph (codex `›`, captured live). First settle →
  `Ready`; settles AFTER a real prompt → final-screen `TextChunk` + `Done`. Three
  cross-cutting fixes make codex reliable under orchestration:
  1. **prompt-sent gate** (`AgentParser::prompt_gate` → shared `PromptGate`): no `Done`
     before the first real prompt, so boot frames / update+permission modals don't route
     empty output downstream.
  2. **submit verification**: `send(Prompt)` records the prompt text; if at the next
     settle that text is still sitting in the input box (a not-yet-ready TUI dropped the
     Enter), the parser re-sends `\r` (bounded, `MAX_RESUBMITS`) and waits instead of
     declaring the turn done. Parser→PTY write path via `AgentParser::drain_input`.
  3. **wait-for-ready** (`Driver::prompt_after_ready`, opt-in; `PtyDriver` → true): the
     session actor holds the first prompt until the agent emits `Ready`, so it isn't typed
     into a still-booting terminal. claude/stub opt out (prompted immediately).

  `send(Prompt)` also waits 150ms between text and Enter (a back-to-back `\r` races ahead
  of TUI ingestion). Mechanism: `crates/cap-rs/src/driver/pty.rs` (`AgentParser` gains
  `idle_timeout`/`on_idle`/`prompt_gate`/`drain_input`; reader thread split from a timed
  parser thread); session wait-for-ready in `orchestrator/src/session.rs`.
- **PTY validation (done, incl. fleet):** unit tests cover the state machine — gate
  (settles before prompt emit no `Done`), submit-verification (re-send Enter when stuck,
  then `Done` on submit), wait-for-ready (waits for `Ready`, fails loud if it never comes);
  a real-PTY integration test covers the idle-timer plumbing. **Live-validated against real
  codex**: single-agent smoke (`examples/codex_tui_smoke.rs`) gives `1 Ready, 1 Done`; and
  a real **`codex → claude` fleet via `cap run --bypass`** where codex actually ran the
  turn (answered `• hello`, input box cleared), fired one `Done`, routed to claude, and
  reached `== fleet complete ==`. `claude → claude` also clean.
- **Test gate:** `cargo test --all-features` = cap-rs 54 + orchestrator 25 unit + 6
  integration + 2 doctest, all green; clippy `-D warnings`, fmt, doc all clean.
- **Cleanup items (landed 2026-05-26):** `TurnResult` enum removed (no longer
  needed — result was discarded by caller); `by_subtask` routing failure now
  suppresses `SessionDone` (only `SessionFailed` emitted); `testing` module gated
  behind `#[cfg(any(test, feature = "testing"))]`; PTY fallback strips trailing
  prompt lines from boundary `TextChunk` (TUI chrome removed).
- **PTY follow-ups (low priority; only relevant for `pty:<cmd>` fallback):**
  everything else — all three first-class agents are on structured protocols.
- **Next:** sub-projects 2–5 (the remote-control vision proper) — remote transport
  (WS over Tailscale) → push-based remote permission approval → mobile app.

---

## Prior session (2026-05-19): v0.0.1 self-review + codex multi-turn

Working snapshot after the v0.0.1 self-review + codex multi-turn work.

## Session goal

Close the spec ↔ code drift surfaced in the v0.0.1 review, and add a
codex driver that actually supports multi-turn / mid-turn cancel rather
than respawning a process per prompt.

## Commits this session

```
424c8c6 fix(codex_app_server): surface real error + JSON-RPC error fallthrough
fde132d feat(cap-rs): CodexAppServerDriver — multi-turn JSON-RPC fast-path
50e0ba0 fix: rustdoc broken intra-link + spec §7.12 ordering
daf7b7b chore: CI workflow, CHANGELOG, README fixes
609c794 docs(cap-v1): tighten draft — _meta, lifecycle, regex, error tracks
0c7806c refactor(cap-rs): spec-align types, add serde, harden driver lifecycle
```

## Drivers

| Driver | Feature flag | Multi-turn | Streaming | Status |
|---|---|---|---|---|
| `ClaudeCodeDriver` (stream-json) | `stream-json` | ✅ | ✅ | works |
| `PtyDriver` (Raw/VtPlain/Repl parsers) | `pty` | ✅ | ✅ | works |
| `CodexExecDriver` | `codex` | one-shot/process | ✅ | works |
| `CodexAppServerDriver` | `codex` | ✅ | ✅ | ~~blocked on network~~ **removed 2026-05 (see top)** |

## Test gate

- `cargo test --all-features` — **27 unit + 2 doctest passed**
- `cargo clippy --all-features --all-targets -- -D warnings` — clean
- `cargo fmt --all -- --check` — clean
- `RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps` — clean
- `.github/workflows/ci.yml` runs all four on Ubuntu + macOS

## Spec ↔ code alignment (v0.0.1 review punch-list)

33 review items, **28 landed**, **5 deferred** by explicit choice:

1. `PtyDriver::send_bytes` stays inherent (not on trait) until ACP/A2A
   drivers land and force a unified keystroke abstraction.
2. `PermissionDecision::AllowAlways` orchestrator-side memory — the
   spec is intentionally silent on who persists this; revisit when
   the orchestrator crate exists.
3. `Driver: Send + Sync` API trade-off — left `Send`-only.
4. `cargo deny` supply-chain workflow not added.
5. Integration tests beyond the per-driver unit tests not yet written;
   live smoke is via the `examples/`.

Notable wins worth flagging:

- `ClientFrame::SessionConfig`, `AgentEvent::Usage` (mid-session),
  `ToolCallDelta`, `risk_level`, `TextChannel::Thought`,
  `AskKind::Schema(Value)`, `PermissionMode`, `CancelScope` —
  the whole spec §7 surface is now in core types.
- Serde derives for everything; lossless JSON round-trip including
  base64 image content. RFC 4648 vectors locked in by test.
- `Driver::is_alive()` / `exit_status()` + `DriverExitStatus`.

## Known blocker — `codex app-server` backend connection  ⟵ OBSOLETE / RESOLVED

> **Resolved (2026-05):** the `CodexAppServerDriver` was removed and codex
> now defaults to the `stream-json` driver. The websocket-transport
> analysis below is retained only as historical context.

The new `CodexAppServerDriver` correctly handshakes via stdio:
`examples/codex_smoke.rs` proves spawn + `initialize` +
`thread/start` in ~2s end-to-end, with a usable thread id and clean
shutdown.

But the first `turn/start` triggers codex's own connection to
`wss://chatgpt.com/backend-api/codex/responses`, which is reset on the
operator's network:

```
✗ codex_error: Reconnecting... 2/5
stream disconnected before completion: Connection reset by peer (os error 54)
(will retry)
```

The same network passes `codex exec --skip-git-repo-check "hi"`
(HTTPS POST, no websocket) → returns "Hi there", 15268 tokens.
So the issue is **specific to app-server's hardcoded websocket model-
inference transport**, not auth, not cap-rs.

`codex features list` notes:

- `responses_websockets` — `removed`
- `responses_websockets_v2` — `removed`
- `responses_websocket_response_processed` — `under development` / off

…yet app-server still opens `wss://chatgpt.com/backend-api/codex/responses`.
This is a codex-rs inconsistency. cap-rs can't fix it from the outside.

## Decision point

Three unblock paths for codex multi-turn on this network:

| Option | Effort | Notes |
|---|---|---|
| **B**: respawn-per-turn on `CodexExecDriver` | ½ day | works today; ~200-500ms gap between turns; turn-internal streaming preserved |
| **Wait** for codex to swap the app-server transport | unbounded | A2 driver auto-unblocks once upstream fixes it |
| Patch codex itself | high / fork | not realistic |

Recommendation: **B**. A2 stays in tree (tests + smoke green) — it just
can't serve actual turns from this network until upstream changes.

## Next session

- Decide B vs wait.
- If B: extend `CodexExecDriver` so `Driver::send` auto-respawns
  `codex exec resume <thread_id>` with the new prompt instead of
  returning `cap_queued_input_unsupported`. Keep the existing one-shot
  constructor untouched for callers that want it.
- Write integration tests that exercise each driver against a stub
  CLI binary so the test gate doesn't depend on a real LLM.
- Watch codex changelog for app-server going non-experimental.
