# cap-protocol — status (2026-05-21)

Lives under `docs/` (not normative — overwrite on each work session).

## Latest: `cap-rs-orchestrator` engine landed (sub-project 1 of the remote-control vision)

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
- **Real agents:** `claude` wired (stream-json). `codex` + `opencode` are **deferred** —
  both ride the PTY path and need a turn-completion heuristic (a TUI emits no
  structured `Done`); `RealDriverFactory` returns a loud "not wired yet" error for them.
- **Test gate:** `cargo test --all-features` = cap-rs 27 + orchestrator 17 unit + 5
  integration + 2 doctest, all green; clippy `-D warnings`, fmt, doc all clean.
- **Follow-on debt (non-blocking, from final review):** vestigial `TurnResult` enum;
  `by_subtask` failure emits `SessionFailed{lead}` after `SessionDone{lead}`; no
  cycle detection in `validate()`; `testing` module is unconditionally `pub`.
- **Next:** PTY turn-detection → wire codex/opencode; then sub-projects 2–5
  (remote transport, tunnel, push, mobile app).

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
| `CodexAppServerDriver` | `codex` | ✅ | ✅ | **blocked on network — see below** |

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

## Known blocker — `codex app-server` backend connection

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
