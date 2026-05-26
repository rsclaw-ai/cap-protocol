# CAP Core v1 Conformance Design

## Goal

Bring the Rust reference implementation into practical conformance with
`docs/cap-v1.md` for CAP core v1, without implementing the coding profile in
this pass.

This work should make the repository honestly support the core claims in the
spec: manifest-based discovery, binding selection, correct session lifecycle,
core event transport, multi-agent orchestration semantics, and A2A core
interoperability.

## Scope

In scope:

- CAP core manifest parsing, defaults, validation, and discovery.
- Binding selection from manifest fast-path declarations, with PTY fallback.
- Session lifecycle alignment: `cap.session.config` first, then
  `cap.session.ready`, then `cap.user_input.inject`.
- Driver conformance fixes for PTY, stream-json, ACP, gRPC, and A2A.
- A2A consume and publish support for core events.
- Multi-agent orchestration fixes for mid-turn injection, budget aggregation,
  workspace identity, auditability, and permission policy.
- Strict external wire behavior for core events.
- Tests that prove the conformance contract without requiring live LLM access.

Out of scope:

- Coding profile reverse RPC methods such as `cap.fs.*` and
  `cap.terminal.*`.
- Coding profile artifact types such as diffs, PR links, commits, test
  results, and transcripts.
- Production-grade A2A auth, TLS termination, or deployment packaging.
- A full rewrite of the public Rust API. Existing convenience APIs should keep
  working where possible.

## Architecture

The implementation should keep the existing crate split:

- `cap-rs` remains the core SDK and driver crate.
- `cap-rs-orchestrator` remains the multi-agent engine.
- `cap-cli` remains the operator-facing command line.

New CAP core infrastructure belongs in `cap-rs`:

- `manifest` module: strongly typed TOML manifest structs, defaults,
  validation, discovery, and binding preference calculation.
- `wire` or small helpers in `core`: strict event/frame helpers for external
  transport, including conversion between internal turn-completion convenience
  and spec-pure event streams.
- `driver::a2a`: minimal A2A HTTPS+SSE driver and publisher support for CAP
  core events.

The orchestrator should stop treating named drivers as the only source of
truth. It may keep the friendly `claude`, `codex`, `opencode`, and similar
short names, but these should resolve to manifests and then to selected
bindings. Hard-coded agent mappings can remain as a fallback only when no
manifest is available.

## Manifest Behavior

Manifest support must cover the distribution and resolution model from
`docs/cap-v1.md`:

1. Explicit path supplied by the caller.
2. In-package `cap-agent.toml` where applicable.
3. User-local `~/.config/cap/agents/<name>.toml`.
4. System-wide `/usr/share/cap-agents/<name>.toml`.
5. Probe-emitted manifest from `<binary> --cap-manifest`.
6. Repository examples as development fallback for first-party known agents.

Validation must enforce:

- Required fields from §5.2.
- Defaults from §5.3.
- Regex subset restrictions from §5.4, using Rust `regex-lite` as the
  effective acceptance boundary.
- Safe command shapes: argv arrays only, no shell interpolation.
- Fast-path declarations are well-typed.
- Profiles are recorded but not activated beyond CAP core in this pass.

Binding selection should follow the spec priority order:

1. gRPC
2. stream-json
3. ACP-stdio
4. A2A HTTPS+SSE
5. PTY

If a preferred binding fails to spawn or connect, the factory should try the
next declared binding and emit an auditable warning event.

## Session Lifecycle

The driver interface should support a spec-first lifecycle:

1. Orchestrator creates `SessionConfig` with cwd, model, budget, permission
   mode, resume id, and profile config.
2. Driver starts from that config or receives it as the first frame,
   depending on binding mechanics.
3. Driver emits `cap.session.ready`.
4. Orchestrator sends the initial task via `cap.user_input.inject`.
5. Later user messages are also `cap.user_input.inject`, even mid-turn.

Implementation detail: some existing drivers consume config at construction
because their underlying transport has no in-band config frame. That is
acceptable if the driver exposes a `start(config)` path and rejects any second
or late `SessionConfig` with `cap_session_config_missing` or
`cap_session_config_inline_unsupported` as appropriate.

The orchestrator should not send prompts before readiness. Structured drivers
that are immediately ready should synthesize `cap.session.ready` after their
handshake or process spawn, not silently skip the ready event.

## Driver Fixes

PTY:

- Build PTY sessions from manifest `startup`, `pty`, and `parse` fields.
- Keep the current tuned parsers for known TUI agents, but add a
  manifest-driven parser for generic CAP manifests.
- Honor manifest cancellation mode: graceful Ctrl+C, hard termination, or
  unsupported error.
- Add opt-in `cap.pty.raw_bytes` event support.
- Emit `cap.error { code: "pty_died" }` when the child exits unexpectedly.

stream-json:

- Fix shutdown so the spawned child can actually be terminated.
- Emit `cap.session.ready` from init frames and preserve the spec ordering.
- Do not silently drop malformed JSON when strict mode is enabled; emit
  `cap.error { code: "parse_failed" }`.
- Surface permission and ask-user frames only where the underlying stream
  supports them. Otherwise return the standard unsupported capability errors.

ACP:

- Keep the existing JSON-RPC handshake.
- Map `session/elicit` and `session/request_permission` to core events.
- Support `AskUserAnswer` for elicitation requests, not only permission
  responses.
- Include risk-level inference where ACP gives enough tool detail, defaulting
  to medium only when uncertain.

gRPC:

- Require `SessionConfig` before opening or beginning the chat stream.
- Preserve cwd, model, session resume id, prompts, permission responses, and
  cancel signals.
- Map server terminal usage into a `cap.usage` event and internal turn
  completion.

A2A:

- Implement consume mode: fetch AgentCard, verify `cap-protocol/v1`
  extension, send `message/send`, parse SSE stream responses into core events.
- Implement publish mode: expose AgentCard, `message/send`, and SSE stream
  endpoints for a locally driven CAP session.
- Use only CAP core mapping in this pass. Coding artifacts are out of scope.

Codex-specific drivers:

- Keep Codex MCP/app-server/exec helpers as non-normative high-fidelity
  drivers.
- Route their events through the same lifecycle and strict wire helpers so
  they behave like CAP drivers from the orchestrator's point of view.

## Orchestrator Behavior

The engine should enforce the CAP core orchestration rules:

- Agent identity is `cap://<session-id>` in emitted metadata and audit records.
- Cross-agent messages remain orchestrator-mediated and audit-visible.
- `UserMessage` control messages can be delivered while a turn is running.
- `cap.usage` events are aggregated per fleet.
- If aggregate cost exceeds `SessionConfig.budget_usd` or fleet budget, the
  orchestrator issues `cap.cancel`.
- High-risk permission requests are never auto-approved by `Allow` or
  `Bypass` unless an explicit policy flag says high-risk auto-approval is
  allowed.
- Worktree allocation remains per session for coding agents, but core
  conformance treats that as workspace isolation via `cwd`, not a coding
  profile implementation.

## Wire Compatibility

`AgentEvent::Done` may remain as an internal SDK convenience because the
orchestrator needs a turn boundary. It must not be presented as a CAP core
event in strict external transports.

External CAP wire streams should expose:

- `cap.session.ready`
- `cap.text_chunk`
- `cap.tool_call.*`
- `cap.plan`
- `cap.thought`
- `cap.ask_user`
- `cap.permission.request`
- `cap.usage`
- `cap.error`
- optional `cap.pty.raw_bytes`

When an internal driver produces `Done`, bridges should emit any final
`cap.usage` if present, then close or mark the transport task completed using
that transport's native terminal state.

## CLI Changes

`cap-cli` should gain core conformance commands:

- `cap manifest validate <path>`
- `cap manifest resolve <agent-name-or-path>`
- `cap run --agent <manifest-or-name> --task <text>` for single-agent core
  smoke tests.
- Existing `cap validate`, `cap list-drivers`, `cap init`, and `cap run
  fleet.yaml` should keep working.

The existing fleet YAML may keep `driver: claude` style entries, but it should
also accept manifest-backed entries such as `agent: claude-code` or
`manifest: examples/claude-code.toml`.

## Testing Strategy

Tests should not depend on live Claude, Codex, Opencode, OpenClaude, or A2A
services. Add local stub binaries and HTTP/SSE test servers where needed.

Required test coverage:

- Manifest parsing, defaults, validation, discovery, and binding preference.
- Regex validation rejects unsupported constructs.
- Driver factory chooses the highest available binding and falls back on
  failure.
- Session lifecycle emits ready before prompt injection.
- Mid-turn user injection is delivered while `pump_turn` is active.
- Stream-json shutdown terminates the child or closes it cleanly.
- gRPC requires and applies `SessionConfig`.
- A2A consume maps AgentCard + SSE events to CAP core events.
- A2A publish maps inbound `message/send` to local driver input and streams
  CAP events back.
- Budget aggregation cancels when the declared budget is exceeded.
- High-risk permission requests are not auto-approved without explicit policy.
- Strict wire mode excludes `cap.done`.

The final gate should include:

- `cargo fmt --all -- --check`
- `cargo clippy --all-features --all-targets -- -D warnings`
- `cargo test --all-features`
- `RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps`

## Migration Notes

This is a conformance pass, not an API reset. Existing examples should keep
working unless they relied on behavior that directly violates CAP core
lifecycle ordering.

The likely compatibility changes:

- Code that sends `Prompt` before `Ready` may need to wait for readiness or use
  a helper that performs the lifecycle.
- External consumers should stop treating `cap.done` as a CAP event.
- Fleet YAML can keep current short driver names, but manifest-backed config
  becomes the preferred path.

## Acceptance Criteria

The work is complete when:

- The implementation can resolve and validate a CAP manifest for each example
  agent.
- A single-agent session follows `SessionConfig -> Ready -> Inject`.
- A fleet run can route between manifest-backed agents.
- PTY remains available as universal fallback.
- stream-json, ACP, gRPC, and A2A have core event mappings tested by local
  stubs.
- Mid-turn injection, budget cancel, and high-risk permission policy are
  covered by tests.
- Strict external wire mode emits only CAP core events.
- The full verification gate passes.
