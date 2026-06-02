# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`cap-rs-orchestrator`** — headless multi-agent orchestration engine.
  Runs N collaborating CLI agents in one process from a declarative
  `fleet.yaml`: actor model (one tokio task per session owns a
  `Box<dyn Driver>`), deterministic executor state machine, audit log of
  every cross-session route, per-session `ask`/`allow`/`deny` + fleet
  `bypass`, and git-worktree isolation per session. Patterns: pipeline,
  lead-worker fan-out + join, parallel race, `by_subtask` split.
- **LLM and hybrid routing** — `cap run --mode static|llm|hybrid`. An LLM
  can pick the next session at each hand-off (`llm`), or fill gaps left
  by static routes (`hybrid`).
- **`AcpDriver`** (`acp:<cmd>`) — Agent Client Protocol over JSON-RPC/stdio.
  Structured streaming verified against real `opencode acp` v1.14.
- **`A2aDriver`** (`a2a:<url>`) — drive a remote agent over A2A HTTPS+SSE.
- **`GrpcDriver`** (`grpc:<addr>`) — OpenClaude gRPC fast-path.
- **aider driver** (`DriverKind::Aider`) — first-class PTY agent via
  `ReplParser::aider` / `TuiParser::aider`.
- **CAP core v1 conformance** — `manifest.rs` (agent Manifest loading +
  name discovery) and `wire.rs` (spec wire-frame round-trip), closing the
  remaining v1 spec ↔ code drift.
- **`cap` CLI** is now functional: `validate`, `list-drivers`, `init`,
  `manifest validate|resolve`, `chat`, and `run`.
- **`cap chat`** — interactive multi-agent conversation; auto-creates a
  single-agent fleet from `--driver` when no `fleet.yaml` is given.
- `ClientFrame::SessionConfig(SessionConfig)` — wire-frame form of spec
  §7.10 `cap.session.config`. Drivers can now accept session bootstrap
  either via the builder API or as an inbound frame.
- `AgentEvent::Usage { usage }` — standalone progress usage event (spec
  §7.9). Mid-session emission is now possible without bundling into the
  terminal `Done`.
- `AgentEvent::ToolCallDelta { call_id, output_chunk }` — streaming
  tool-call output chunks (spec §7.2).
- `RiskLevel { Low, Medium, High }` and `PermissionRequest.risk_level` —
  spec §7.6 normative field that orchestrators must consult before
  auto-approving privileged tool calls.
- `TextChannel::Thought` — third channel value documented in spec §7.1.
- `PlanPriority::Urgent` plus made `PlanEntry.priority` optional.
- `AskKind::Schema { schema }` — JSON-Schema escape hatch for §7.5
  elicitation, mapped to the `form` field on the wire.
- `CancelScope` and `ClientFrame::Cancel { scope, reason }` per spec §7.8.
- `PermissionMode` enum (`None`, `Confirm`, `Interactive`) on
  `SessionConfig` per spec §7.10.
- `Driver::is_alive()` and `Driver::exit_status()` plus `DriverExitStatus`
  enum — orchestrators can now distinguish clean exit, kill, and
  disconnection without polling the channel.
- `Serialize` + `Deserialize` for `AgentEvent`, `ClientFrame`,
  `SessionConfig`, `Usage`, and all dependent enums. Variants map to
  spec wire-names (`cap.text_chunk`, `cap.permission.request`, …) so
  values round-trip losslessly through JSON. Image content uses base64.
- `Content::text("…")` and `Content::image(mime, bytes)` ergonomic
  constructors.
- RFC 4648 base64 test vectors covering encoder + decoder.
- GitHub Actions CI (`fmt`, `clippy -D warnings`, `test`, `doc`, MSRV).
- CHANGELOG.md.

### Changed

- `Content::Image.data` is now `Arc<[u8]>` instead of `Vec<u8>` —
  `ClientFrame::Clone` no longer copies image payloads.
- `Content::Text(String)` is now `Content::Text { text: String }` —
  call sites move to `Content::text("…")`.
- `AgentEvent::AskUser.kind` renamed to `ask_kind` to avoid clash with
  the JSON `kind` discriminator.
- `Usage` now carries `stop_reason: Option<StopReason>` so the spec
  `cap.usage` frame round-trips without losing the terminator.
- `stream-json` driver's `dangerously_skip_permissions` default flipped
  to **`false`** — opt-in only. Examples updated to invoke
  `.dangerously_skip_permissions(true)` explicitly where needed.
- Codex now defaults to the `stream-json` driver (Claude Code-compatible
  NDJSON, multi-turn). `pty:codex` remains the screen-scraping fallback
  and `codex exec --json` remains available as `DriverKind::Codex`.
- Codex driver returns `cap_queued_input_unsupported` /
  `cap_cancel_unsupported` from `send`, matching spec §14.2 codes.
- `stream-json` driver returns `cap_cancel_unsupported` for
  `ClientFrame::Cancel` instead of writing a no-op control frame.
- `stream-json` `parse_usage.model_id` now selects the entry with the
  most output tokens from `modelUsage` instead of an insertion-order
  first key.
- PTY `ReplParser` scans the bottom of repainted screens for prompt /
  yes-no markers so turn boundaries are not lost on scroll.
- PTY child waiter no longer sleeps 50 ms before signalling exit.
- PTY codex `last_text_for` entries are released on `item.completed`,
  preventing per-session HashMap growth.
- `cap-rs/src/lib.rs` doc html_root_url updated to `0.0.1`.

### Fixed

- `pty.rs` `manual_pattern_char_comparison` clippy warning.
- `stream_json.rs` `manual_div_ceil` clippy warning.
- `codex.rs` unused `Content` import.

## [0.0.1] - 2026-05-18

First release with real code. Three drivers shipped:

- `stream-json` (Claude Code SDK / openclaude-compatible)
- `pty` (universal, with `RawParser`, `VtPlainParser`, and `ReplParser`)
- `codex` (OpenAI codex CLI via `exec --json`)

Plus the CAP v1 draft spec, coding profile draft, and the
`cap-protocol.org` website.

[Unreleased]: https://github.com/rsclaw-ai/cap-protocol/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/rsclaw-ai/cap-protocol/releases/tag/v0.0.1
