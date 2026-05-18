# CAP — CLI Agent Protocol (Core v1, draft)

> **Status**: `draft-2026-05-18`
> **Full name**: CLI Agent Protocol
> **Homepage**: <https://cap-protocol.org>
> **Editors**: rsclaw maintainers
> **Source of truth**: this file. Profiles live in sibling docs.
> **Profiles defined so far**: [coding](./cap-profile-coding-v1.md)

## Abstract

CAP is a protocol for **discovering, driving, and orchestrating
command-line AI agents**. It is transport-agnostic at its core:

- **PTY** is the REQUIRED universal substrate. Any agent that runs in a
  terminal can be driven via PTY, including agents that expose no
  structured protocol.
- **Fast-path transports** — stream-json, gRPC, ACP-stdio, A2A over
  HTTPS+SSE — are OPTIONAL. When an agent supports one, drivers SHOULD
  prefer it for cleaner event extraction.
- **Profiles** add vertical-specific extensions. The first profile is
  [coding](./cap-profile-coding-v1.md). Future profiles MAY cover
  devops, data analysis, security audit, research, and others.

CAP is **complementary, not competitive**, to existing agent protocols:

| Protocol | Layer |
|---|---|
| MCP | agent ↔ tools |
| A2A | agent ↔ agent (peer) |
| ACP (Zed) | agent ↔ editor (local stdio) |
| **CAP** | **orchestrator ↔ CLI agents (fleet-scale)** |

CAP composes with all three. An A2A-compliant agent MAY be driven by a
CAP orchestrator over the A2A binding (§6.5). An ACP-speaking agent
MAY be driven over the ACP binding (§6.4). MCP usage by the agent is
transparent to CAP.

## 1. Conventions

Keywords MUST, SHOULD, MAY follow [RFC 2119][rfc2119].

[rfc2119]: https://www.rfc-editor.org/rfc/rfc2119

Terminology:

- **Agent** — a command-line AI program (e.g. `claude`, `aider`,
  `terraform-agent`). Agents have a stable `name`, a binary on PATH,
  and a Manifest (§5).
- **Driver** — a CAP implementation that controls an Agent via one of
  the bindings in §6.
- **Orchestrator** — a CAP client coordinating one or more
  Drivers, typically on behalf of a human via chat or UI.
- **Session** — one logical conversation with one Agent. Backed by an
  OS process and (in PTY binding) a PTY.
- **Profile** — an optional vertical extension defining
  domain-specific Events, Reverse RPC methods, and Artifact types
  (e.g. `profile/coding`).

## 2. Position

```
┌────────────────────────────────────────────────────────────────┐
│  Layer                  │  Protocol  │  Scope                   │
│  ───────────────────────┼────────────┼────────────────────────  │
│  agent ↔ tools          │  MCP       │  tool calling             │
│  agent ↔ editor         │  ACP       │  local stdio, IDE         │
│  agent ↔ agent (peer)   │  A2A       │  peer coordination        │
│  orch  ↔ CLI agent      │  CAP core  │  THIS DOCUMENT            │
│  coding-specific add-on │  profile/  │  CAP coding profile       │
│                         │  coding    │                           │
└────────────────────────────────────────────────────────────────┘
```

A single Agent MAY participate in multiple protocols simultaneously:
expose ACP to a Zed editor, expose A2A to remote peers, consume MCP
for its tools, and be driven by a CAP orchestrator over PTY.

## 3. Conformance

### 3.1 Agent conformance

An Agent conforms to CAP v1 if:

1. It is invokable as a command-line program.
2. It publishes a **Manifest** (§5) describing its capabilities and
   parsing conventions.
3. Its behaviour under at least one binding (§6) matches the contract
   in §7-§9.

Profile conformance is independent. An Agent MAY conform to CAP core
without conforming to any profile. An Agent MUST NOT claim profile
conformance without conforming to core.

### 3.2 Orchestrator conformance

An Orchestrator conforms to CAP v1 if:

1. It can read Manifests (§5) and dispatch Agents accordingly.
2. It implements at least the PTY binding (§6.1).
3. It honours the multi-agent orchestration semantics (§10).
4. It implements all REQUIRED Reverse RPC methods (§8) for any profile
   it advertises supporting.

### 3.3 Driver conformance

A Driver conforms to CAP v1 if it:

1. Implements at least one binding (§6).
2. Emits Core Events (§7) faithfully to that binding's contract.
3. Accepts `cap.session.config` (§7.10) as the first frame and
   applies its `cwd`, `model`, `permission_mode`, and other fields
   before spawning or after a binding-specific handshake.
4. Surfaces `cap.permission.request` events with a non-omitted
   `risk_level` (§7.6) whenever the bound agent exposes a way to
   negotiate permission.
5. Authenticates the source of every inbound `cap.user_input.inject`
   per §13.1 — injected input MUST come from the same Orchestrator
   that owns the session, not from an external caller.

## 4. Identifiers and Versioning

- **Protocol identifier**: `cap-protocol/v1`
- **Manifest schema URI**: `https://cap-protocol.org/schema/manifest/v1.json`
  (placeholder; will be hosted alongside the spec repo).
- **Profile URIs**: `cap-protocol/v1/profile/<name>` (e.g.
  `cap-protocol/v1/profile/coding`).
- **Versioning**: major version in identifier (`v1`, `v2`).
  Backward-compatible additions use a `revision` field
  (`YYYY-MM-DD`) on the Manifest.

## 5. Agent Manifest

Each Agent publishes a Manifest. Distribution options:

1. **In-package**: `cap-agent.toml` at the agent's package root.
2. **System-wide**: `/usr/share/cap-agents/<name>.toml`.
3. **User-local**: `~/.config/cap/agents/<name>.toml`.
4. **Probe-emitted**: agent prints Manifest to stdout when invoked as
   `<binary> --cap-manifest`.

Resolution order: in-package → user-local → system-wide → probe.

### 5.1 Schema

```toml
# cap-agent.toml — example for claude-code

[agent]
name = "claude-code"
binary = "claude"                       # found via PATH
version_match = "^2\\."                 # regex on `--version` output
profiles = ["coding"]                   # declared profiles

[probe]
command = ["claude", "--version"]
version_pattern = "claude-code/(\\d+\\.\\d+)"

[startup]
command = ["claude"]                    # additional args appended by driver
# cwd_arg is OPTIONAL — when omitted (as is the case for claude), the
# Driver sets the child process's working directory directly. Provide
# only if the Agent's binary insists on a flag.
# cwd_arg = "--workspace"
model_arg = "--model"                   # how to inject model selection
session_id_env = "CLAUDE_SESSION_ID"    # how to inject session id (optional)
ready_when = { pattern = "Try \"how do I\\?\"" }
init_timeout_seconds = 60

[fast_path]
# Drivers MUST prefer the first fast-path the Agent supports, falling
# back to PTY if none are available or any fails.
stream_json = ["claude", "-p", "--input-format=stream-json", "--output-format=stream-json"]
grpc = false
acp_stdio = false                       # set true if binary speaks ACP natively
a2a_serve_at = false                    # set to a URL if agent runs A2A server

[pty]
cols = 200
rows = 50
bracketed_paste = true
sigint_cancels_turn = "graceful"        # graceful | hard | none
queued_input_supported = true           # agent accepts typed input mid-turn

[parse]
# Regular expressions for ANSI-stripped output. All are OPTIONAL but
# without them the Driver emits only cap.text_chunk events.
idle = ["^> $", "^❯ $"]
tool_call_start = "^◉ (?P<tool>\\w+)\\((?P<args>.*?)\\)$"
tool_call_end   = "^◉ (?P<tool>\\w+) (?P<status>completed|failed)$"
plan_section    = "^## Plan\\b"
thought_section = "^_thinking_\\b"
ask_yes_no      = "Do you want me to continue\\? \\(y/n\\)$"
ask_options     = "^\\? Select: "
error_lines     = ["rate_limit_error", "auth_error", "Please run /login"]

[capabilities]
streaming_output = true
queued_input = true
mid_turn_cancel = "graceful"
multi_session = true                    # agent supports parallel sessions
input_modalities = ["text"]
output_modalities = ["text"]

[capabilities.ask_user]
yes_no = true
options = false                         # TUI agent doesn't natively support
free_text = true                        # via plain typing

[cost]
metered = false                         # true if agent reports usage events
currency = "USD"
```

### 5.2 Required fields

| Field | Required | Notes |
|---|---|---|
| `agent.name` | yes | unique within an Orchestrator's registry |
| `agent.binary` | yes | resolved via PATH or absolute |
| `agent.profiles` | yes | array; empty allowed |
| `startup.command` | yes | argv prefix |
| `startup.ready_when` | yes | how the Driver knows initialization completed |
| `pty.cols`, `pty.rows` | yes | starting PTY size |
| `capabilities.streaming_output` | yes | reflects whether agent streams text |

All other fields are OPTIONAL; defaults are defined in §5.3.

### 5.3 Defaults

Unset fields take these values:

| Field | Default |
|---|---|
| `pty.bracketed_paste` | `false` |
| `pty.sigint_cancels_turn` | `"hard"` |
| `pty.queued_input_supported` | `false` |
| `capabilities.multi_session` | `false` |
| `capabilities.ask_user.*` | all `false` |
| `capabilities.streaming_tool_output` | `false` |
| `cost.metered` | `false` |
| `parse.idle` | `["^>\\s*$", "^❯\\s*$"]` |
| `parse.tool_call_start` | none (no tool-call events from PTY parse) |
| `parse.tool_call_end` | none |
| `parse.plan_section` | none |
| `parse.thought_section` | none |
| `parse.ask_yes_no` | none |
| `parse.ask_options` | none |
| `parse.error_lines` | empty |

### 5.4 Regex flavor

All `parse.*` patterns use a **POSIX ERE-compatible subset** equivalent
to Rust's `regex-lite` / Go's `RE2`. Specifically, backreferences and
look-around are NOT supported. Drivers MUST reject Manifests that
contain features outside the subset. Patterns are anchored to logical
lines (i.e. `^` matches start-of-line, `$` matches end-of-line), with
ANSI escapes stripped before matching.

## 6. Transport Bindings

A Driver implements one or more bindings. The Manifest declares which
fast-paths the Agent supports; the Driver picks the highest-priority
one available, falling back to PTY.

**Priority order** (recommended):

```
1. gRPC          (where available)         — fully structured, lowest overhead
2. stream-json   (where available)         — fully structured, line-oriented
3. ACP-stdio     (where available)         — fully structured, request/response
4. A2A HTTPS+SSE (where available, remote) — for cross-machine
5. PTY           (always)                  — universal fallback
```

### 6.1 PTY Binding (REQUIRED)

The Driver spawns the Agent under a master/slave PTY pair. All input
is written to the master; all output is read from the master and fed
to an ANSI-aware terminal emulator state machine before parsing.

Driver responsibilities:

1. Set initial size from `pty.cols`/`pty.rows`; honour `SIGWINCH`
   resize requests from the user.
2. Enable bracketed paste mode if `pty.bracketed_paste = true`.
3. Maintain an ANSI/VT100 screen buffer.
4. Run regular expressions from `[parse]` against the **rendered**
   (escape-stripped, full-grapheme) screen text, debouncing on full-
   line boundaries.
5. Translate matches to Core Events (§7).
6. Optionally emit `cap.pty.raw_bytes` (§7.12) for subscribers that
   need the raw screen state (e.g. a user-facing terminal mirror).
   Drivers MAY suppress this stream by default — it is a separate
   subscription, not part of the default event flow.

Cancellation:

- `mid_turn_cancel = "graceful"`: Driver sends `0x03` (Ctrl+C). Agent
  is expected to stop generation without process exit.
- `mid_turn_cancel = "hard"`: Driver sends `SIGTERM`; if Agent does
  not exit within 5s, `SIGKILL`. Session is destroyed.
- `mid_turn_cancel = "none"`: Cancellation not supported; Driver MUST
  reject `cap.cancel` requests.

Input injection:

- For short text: write directly, follow with `\n`.
- For multiline or text exceeding 1KB AND
  `bracketed_paste = true`: wrap in `\x1b[200~` / `\x1b[201~` and
  send in one syscall; follow with `\n`.

### 6.2 stream-json Binding (RECOMMENDED fast-path)

Used by Agents that expose a line-delimited JSON conversation over
stdio. Wire format compatible with Anthropic Claude SDK's
`--input-format=stream-json --output-format=stream-json`.

Frames (one JSON object per line):

```
client → agent: { "type": "user",     "message": { ... } }
agent  → client: { "type": "assistant", "message": { ... } }
agent  → client: { "type": "system",   "subtype": "init"|"stream_event", ... }
agent  → client: { "type": "user",     "message": { content: [{ type: "tool_result", ... }] } }
agent  → client: { "type": "result",   "subtype": "success"|"error", ... }
```

The Driver translates these frames into Core Events. Mapping is
defined in Appendix C.1.

### 6.3 gRPC Binding (OPTIONAL fast-path)

Used by Agents that ship a gRPC server (e.g. openclaude's
`AgentService.Chat`). Drivers connect to the configured address from
the Manifest's `[fast_path]` section.

The wire `service` MUST satisfy:

```proto
service CapAgent {
  rpc Chat(stream ClientMessage) returns (stream ServerMessage);
}
```

A schema-compatible default is published at `cap-protocol.org/proto/v1/`.
Agents MAY publish their own service that maps semantically to the
same Core Events (see Appendix C.2).

### 6.4 ACP-stdio Binding (OPTIONAL fast-path)

Used by Agents conforming to Zed's [Agent Client Protocol][acp]. The
Driver acts as an ACP client. Mapping from ACP frames to Core Events
is defined in Appendix C.3.

[acp]: https://agentclientprotocol.com/

This binding subsumes a large existing ecosystem at zero adapter cost
(claude-agent-acp, opencode acp, codex native ACP, gemini-cli).

### 6.5 A2A HTTPS+SSE Binding (OPTIONAL, for remote/peer)

Used when the Agent runs on a different host and is reachable via
[A2A][a2a]. The Driver:

[a2a]: https://a2a-protocol.org/

1. Fetches `.well-known/agent-card.json` from the configured base URL.
2. Verifies the AgentCard advertises CAP compatibility by including
   an `extensions` entry with URI `cap-protocol/v1`.
3. Sends `message/send` over HTTP, subscribes to SSE for streaming
   replies.

Mapping from A2A `StreamResponse` events to Core Events is in
Appendix C.4.

This binding is also used by Orchestrators that want to **expose** a
locally-driven CAP Agent as an A2A peer to remote callers. See §11.

## 7. Core Events

All Core Events are carried as **structured payloads** independent of
the binding. In bindings that natively use A2A DataParts (§6.5), each
Event is a DataPart with `_meta.cap.kind` set to the kind below. In
other bindings, each Event is a JSON object with a top-level `kind`
field.

### 7.0 `_meta` object

Every Core Event MAY carry a top-level `_meta` object containing
implementation-defined annotations. The `cap` namespace under `_meta`
is reserved for fields defined by this spec (e.g. `_meta.cap.kind`
when transported as A2A DataPart, `_meta.cap.assigned_to` and
`_meta.cap.depends_on` on Plan entries, `_meta.cap.message_to` for
cross-agent directives in §10.3). Other namespaces (`_meta.<vendor>.*`)
are reserved for implementers; receivers MUST ignore unknown
namespaces. The `_meta` field is OPTIONAL — events without it are
fully valid.

### 7.0.1 Direction-of-flow

The events in §7.1–§7.6, §7.9, §7.11 flow from Agent → Orchestrator.
§7.7 (`cap.user_input.inject`), §7.8 (`cap.cancel`), and §7.10
(`cap.session.config`) flow from Orchestrator → Agent. §7.10 is the
REQUIRED first frame at session start — see §9 lifecycle.

### 7.1 `cap.text_chunk`

```jsonc
{
  "kind": "cap.text_chunk",
  "message_id": "msg_01",
  "text": "...",
  "channel": "assistant"     // assistant | thought | system
}
```

### 7.2 `cap.tool_call.start` / `cap.tool_call.delta` / `cap.tool_call.end`

```jsonc
{ "kind": "cap.tool_call.start", "call_id": "c1", "name": "Bash", "input": {} }
{ "kind": "cap.tool_call.delta", "call_id": "c1", "output_chunk": "..." }
{ "kind": "cap.tool_call.end",   "call_id": "c1", "output": "...", "is_error": false, "duration_ms": 800 }
```

`tool_call.delta` is OPTIONAL; emit only if
`capabilities.streaming_tool_output = true`.

### 7.3 `cap.plan`

A full-state plan. Latest emission REPLACES previous.

```jsonc
{
  "kind": "cap.plan",
  "entries": [
    { "id": "p1", "content": "Design schema", "status": "in_progress", "priority": "high" }
  ]
}
```

`status` ∈ `pending | in_progress | completed | cancelled | blocked`.
`priority` is OPTIONAL; when present, it MUST be one of
`urgent | high | medium | low`. Orchestrators MUST treat an absent
priority as "unspecified" and MUST NOT infer a default — many
planners (Claude Code, codex, manus) do not assign priorities at all.

### 7.4 `cap.thought`

```jsonc
{ "kind": "cap.thought", "message_id": "msg_01", "text": "..." }
```

Orchestrators SHOULD NOT echo thoughts to end-users by default.

### 7.5 `cap.ask_user` / `cap.ask_user.answer`

Structured prompts whose answer the Agent needs to proceed.

```jsonc
// agent → orchestrator
{
  "kind": "cap.ask_user",
  "ask_id": "ask_01",
  "prompt": "Which DB?",
  "form": {                        // JSON Schema (informal subset)
    "type": "string",
    "oneOf": [
      { "const": "pg", "title": "PostgreSQL" },
      { "const": "sqlite", "title": "SQLite" }
    ]
  },
  "timeout_seconds": null
}

// orchestrator → agent
{ "kind": "cap.ask_user.answer", "ask_id": "ask_01", "value": "pg" }
```

The form schema follows ACP v2 Elicitation conventions. Drivers in
PTY binding synthesize `cap.ask_user` from regex matches in
`[parse].ask_yes_no` / `[parse].ask_options`.

### 7.6 `cap.permission.request` / `cap.permission.response`

Security decisions, distinct from `cap.ask_user`.

```jsonc
// agent → orchestrator
{
  "kind": "cap.permission.request",
  "req_id": "perm_01",
  "tool": "Bash",
  "intent": { "command": "rm -rf node_modules" },
  "scope": "write",                // read | write | execute | network
  "risk_level": "medium"           // low | medium | high
}

// orchestrator → agent
{
  "kind": "cap.permission.response",
  "req_id": "perm_01",
  "decision": "allow_once"          // allow_once | allow_always | deny
}
```

Orchestrators MUST NOT auto-approve `risk_level = high` without
explicit user policy.

### 7.7 `cap.user_input.inject`

Sent by the Orchestrator to push user input into a running session.

```jsonc
{
  "kind": "cap.user_input.inject",
  "content": [{ "type": "text", "text": "wait, use postcard" }]
}
```

Driver behaviour by binding:

- **PTY**: type into terminal immediately (Agent decides queuing).
- **stream-json**: write a new `user` frame to the SDK input stream.
- **ACP-stdio**: invoke `session/prompt` (queued by Agent).
- **gRPC / A2A**: send as new ClientMessage in the active stream.

If `capabilities.queued_input = false`, Drivers MUST reject with
error `cap_queued_input_unsupported`.

### 7.8 `cap.cancel`

```jsonc
{ "kind": "cap.cancel", "scope": "current_turn", "reason": "user" }
```

`scope` ∈ `current_turn | session`. Driver maps to the binding's
native cancel primitive (Ctrl+C in PTY, `session/cancel` in ACP,
`$/cancel_request` in ACP v2, `CancelSignal` in gRPC, `tasks/cancel`
in A2A).

### 7.9 `cap.usage`

REQUIRED at session termination if `cost.metered = true`. MAY be
emitted as progress.

```jsonc
{
  "kind": "cap.usage",
  "model_id": "claude-opus-4-7",
  "input_tokens": 12450,
  "output_tokens": 3120,
  "cache_read_tokens": 8200,
  "cache_creation_tokens": 4250,
  "thinking_tokens": 0,
  "cost_usd_estimate": 0.082,
  "duration_ms": 41200,
  "stop_reason": "end_turn"           // end_turn | max_tokens | tool_use | stop_sequence | cancelled | error
}
```

### 7.10 `cap.session.config`

REQUIRED as the first frame sent BY the Orchestrator to the Driver
when starting a session.

```jsonc
{
  "kind": "cap.session.config",
  "cwd": "/path/to/workspace",
  "model": "claude-opus-4-7",
  "system_prompt": "...",
  "max_turns": 50,
  "budget_usd": 5.00,
  "permission_mode": "interactive",   // none | confirm | interactive
  "session_resume_id": null,
  "profile_config": {                  // profile-specific config, opaque to core
    "coding": { "tools_allowed": ["Bash", "Edit"] }
  }
}
```

### 7.11 `cap.error`

For non-fatal errors. Fatal errors terminate the session.

```jsonc
{
  "kind": "cap.error",
  "code": "rate_limit",
  "message": "...",
  "retryable": true,
  "details": {}
}
```

Standard error codes are listed in §13.2.

### 7.12 `cap.pty.raw_bytes` (OPTIONAL, PTY binding only)

Raw byte stream from the agent's PTY master — the same bytes the
Driver feeds into its VT100 emulator. Drivers MUST NOT emit this
event unless a subscriber has explicitly opted in (via a transport-
specific subscription mechanism such as an `?include_raw=1` query
parameter on the A2A binding). When emitted, each event carries one
chunk of bytes; consumers are expected to maintain their own emulator
state.

```jsonc
{
  "kind": "cap.pty.raw_bytes",
  "bytes_b64": "G1szM21oZWxsbxtbMG0="
}
```

`bytes_b64` is the base64-encoded chunk. Order of events MUST match
the order in which bytes arrived from the PTY master.

## 8. Core Reverse RPC

Methods invoked by the Agent against the Orchestrator. These are part
of CAP core and are domain-neutral. Profile extensions (e.g. coding)
add more.

### 8.1 `cap.user_io.show`

Display a notification to the human user without expecting structured
input.

```
params: { title: string, body: string, level: "info"|"warn"|"error" }
result: { ok: true }
```

### 8.2 `cap.user_io.input`

Free-form text input prompt to the user. Prefer `cap.ask_user` for
structured prompts; use this only for unstructured chat input.

```
params: { prompt: string, default?: string }
result: { text: string }
```

### 8.3 `cap.notify`

Emit a notification that bypasses the chat stream (e.g. push,
mobile alert). Orchestrators MAY route to native OS notification.

```
params: { title: string, body: string, urgent: bool }
result: { ok: true }
```

## 9. Session Lifecycle

```
   client (cap.session.config)
              │
              ▼
       ┌──────────────┐
       │   starting   │   driver spawns binary, waits for ready_when
       └──────┬───────┘
              │ cap.session.ready
              ▼
       ┌──────────────┐
  ┌───►│   working    │◄─────────┐
  │    └──────┬───────┘          │
  │           │ ask_user / perm  │ ask_user.answer
  │           ▼                  │ / permission.response
  │    ┌───────────────┐         │
  │    │ input_required│─────────┘
  │    └───────────────┘
  │ Terminal:
  ▼
  completed | cancelled | failed
```

The Orchestrator MUST send `cap.session.config` (§7.10) as the first
frame to the Driver — before `starting`. The Driver replies with
`cap.session.ready` once the Agent reaches `ready_when` from the
Manifest. Subsequent prompts use `cap.user_input.inject` (§7.7).

## 10. Multi-Agent Orchestration

CAP makes multi-agent coordination a **first-class core feature**
(unlike A2A, which treats agents as anonymous peers).

### 10.1 Agent identity

Each Driver-managed Agent has a CAP URN:

```
cap://<orchestrator-scoped-id>
```

Stable within one Orchestrator. Mapping to physical bindings
(PTY/gRPC/A2A endpoint) is the Orchestrator's responsibility.

### 10.2 Plan propagation

The Orchestrator MAY publish a **master plan** whose `entries` carry
`_meta.cap.assigned_to` referencing sub-agent URNs:

```jsonc
{
  "kind": "cap.plan",
  "entries": [
    { "id": "t1", "content": "Architecture", "status": "in_progress",
      "_meta": { "cap.assigned_to": "cap://claude-1" } },
    { "id": "t2", "content": "Core impl",    "status": "pending",
      "_meta": { "cap.assigned_to": "cap://codex-1", "cap.depends_on": ["t1"] } }
  ]
}
```

Sub-agents MAY emit their own local plans; the Orchestrator merges
for the user-facing view.

### 10.3 Cross-agent communication

Sub-agents MUST NOT communicate directly. All inter-agent messages
are mediated by the Orchestrator and MUST be visible in the human
audit log.

The pattern:

1. Sub-agent A emits `cap.text_chunk` containing a directive like
   `@cap://codex-1 clarify IPC frame format` (or a structured
   `_meta.cap.message_to` field).
2. Orchestrator detects this, injects the directive as
   `cap.user_input.inject` into sub-agent B's session.
3. B's response is similarly routed back to A.

### 10.4 Workspace isolation

For Agents implementing the coding profile (§12), the Orchestrator
SHOULD provision isolated workspaces per sub-agent (e.g. git
worktrees). The `cap.session.config.cwd` field carries the
per-sub-agent path. Cross-sub-agent merging is the Orchestrator's
responsibility.

### 10.5 Budget aggregation

The Orchestrator MUST aggregate `cap.usage` across all sub-agents in
a logical project and enforce any declared total budget by issuing
`cap.cancel` when exceeded. Sub-agents MUST NOT be trusted to
self-enforce budget.

### 10.6 Cooperative interruption (advisory)

Most current LLMs do not support atomic mid-turn interruption. To
approximate "user @ to redirect" UX, Orchestrators SHOULD use:

```
1. cap.cancel { scope: "current_turn" }
2. (preserve partial output from cap.text_chunk so far)
3. cap.user_input.inject { content: [original_intent + user_correction] }
```

This pattern is profile-neutral.

## 11. A2A Interoperability

CAP composes with A2A in two directions:

### 11.1 Driving an A2A Agent (consume direction)

If the Manifest sets `fast_path.a2a_serve_at = <url>`, the Driver
uses §6.5 to talk to the Agent over A2A. The Agent appears in the
Orchestrator's registry exactly like a local PTY Agent.

A2A AgentCard MUST advertise CAP compatibility:

```json
{
  "name": "...",
  "extensions": [{ "uri": "cap-protocol/v1", "required": true }]
}
```

### 11.2 Exposing CAP Agents as A2A peers (publish direction)

An Orchestrator MAY expose any locally-driven Agent as an A2A peer:

1. Mount an HTTP server publishing
   `.well-known/agent-card.json`, `message/send`, SSE endpoints.
2. Synthesize the AgentCard from the Agent's Manifest +
   the `cap-protocol/v1` extension entry.
3. Translate incoming A2A `message/send` into
   `cap.user_input.inject` for the local Driver.
4. Translate Core Events from the Driver into A2A `StreamResponse`
   events.

This direction is the primary path to **fleet-scale deployment**:
1000 nodes each running their own Drivers, all exposed as A2A peers,
discovered by a higher-level Orchestrator.

### 11.3 CAP-to-A2A event mapping

| CAP Event | A2A frame |
|---|---|
| `cap.text_chunk` | TextPart in Message |
| `cap.tool_call.start/end` | DataPart with `_meta.cap.kind` |
| `cap.plan` | DataPart with `_meta.cap.kind = cap.plan` |
| `cap.ask_user` | Task transitions to `input-required` + DataPart |
| `cap.permission.request` | Same as above with different `_meta.cap.kind` |
| `cap.cancel` | `tasks/cancel` |
| `cap.usage` | DataPart attached to terminal Task state |
| `cap.user_input.inject` | `message/send` to active Task |

## 12. Profiles

A Profile is an OPTIONAL extension defining additional Events,
Reverse RPC methods, Artifact types, or Manifest fields for a
vertical domain. Profile identifiers are namespaced as
`cap-protocol/v1/profile/<name>`.

The Orchestrator and Driver negotiate which profiles to activate
based on `agent.profiles` in the Manifest.

Defined profiles:

- **coding** — filesystem and terminal access, code-specific artifact
  types. Defined in [cap-profile-coding-v1.md](./cap-profile-coding-v1.md).

Reserved profile names (future use): `devops`, `data`, `security`,
`research`, `content`, `sysadmin`, `network`.

Profiles MUST NOT redefine Core Events from §7 or Core Reverse RPC
from §8. Profiles MAY add new event kinds and methods within their
namespace (e.g. `cap.fs.read` belongs to `profile/coding`).

## 13. Security Considerations

1. **PTY input is privileged.** Driver-injected keystrokes are
   indistinguishable from a human's. Orchestrators MUST authenticate
   the source of every `cap.user_input.inject`.

2. **Manifest validation.** A malicious Manifest can spawn arbitrary
   binaries. Orchestrators SHOULD validate Manifests against a known
   schema and run unfamiliar Agents in sandboxed environments.

3. **Cross-agent messages are auditable.** Per §10.3, all inter-agent
   traffic flows through the Orchestrator and MUST appear in
   human-visible logs.

4. **Budget caps are advisory at the Agent level.** Hard enforcement
   is the Orchestrator's responsibility (§10.5).

5. **A2A binding security.** When using §6.5 or §11.2, follow A2A's
   authentication scheme. Never accept inbound A2A traffic without
   verifying the `extensions` declaration matches the expected
   `cap-protocol/v1`.

6. **PTY size leakage.** A non-default `pty.cols`/`pty.rows` may be
   used to fingerprint the Orchestrator. Use defaults when possible.

7. **Profile-specific security additions** are defined per profile.

## 14. Versioning and Errors

### 14.1 Versioning

- Major version in the protocol URI (`cap-protocol/v1`). Major bumps are
  not backward-compatible.
- Backward-compatible additions bump the Manifest `revision` field
  (`YYYY-MM-DD`).
- Experimental fields use `x-` prefix; promotion to normative
  requires a revision bump.

### 14.2 Error code registry

CAP defines two error vocabularies:

- **JSON-RPC numeric codes** (`-32099 ... -32000`) used by the
  ACP-stdio binding (§6.4), which is JSON-RPC native. When other
  bindings need to surface the same conditions (e.g. a Driver
  rejecting a frame), the corresponding **string code** (table below)
  is what appears on the wire in `cap.error` or `DriverError`
  payloads.
- **String codes** used everywhere else (stream-json, gRPC, A2A,
  in-memory SDK errors). Numeric and string codes share semantics
  one-to-one; receivers SHOULD treat them as equivalent.

| Code | Symbol | Meaning |
|---|---|---|
| -32001 | `cap_manifest_missing` | Agent has no resolvable Manifest |
| -32002 | `cap_session_config_missing` | First frame was not `cap.session.config` |
| -32003 | `cap_unsupported_capability` | Requested feature not declared in Manifest |
| -32004 | `cap_budget_exceeded` | Would exceed declared budget |
| -32005 | `cap_invalid_answer` | `ask_user.answer.value` failed form validation |
| -32006 | `cap_unknown_permission` | Permission response for unknown `req_id` |
| -32007 | `cap_queued_input_unsupported` | Inject requested but Agent doesn't support queued input |
| -32008 | `cap_cancel_unsupported` | Cancel requested but Manifest says `mid_turn_cancel = "none"` |
| -32009 | `cap_binding_unavailable` | All preferred bindings failed; PTY also unavailable |
| -32010 | `cap_profile_unsupported` | Required profile not implemented by Agent |

String error codes for `cap.error`:

| Code | Meaning |
|---|---|
| `rate_limit` | API rate limit |
| `auth_error` | Agent's auth failed |
| `tool_timeout` | Internal tool timed out |
| `context_overflow` | Context exceeded model limit |
| `model_unavailable` | Configured model is offline |
| `pty_died` | PTY child process exited unexpectedly |
| `parse_failed` | Driver could not parse Agent output |

## Appendix A. PTY Driver Conventions

A1. **ANSI handling.** Drivers MUST process the full VT100 escape
set including cursor moves, line erases, scroll regions, and
alternate screen buffer. Recommended Rust crate: `vt100`.

A2. **Line debouncing.** Events derived from `[parse]` regexes MUST
fire only on complete logical lines (terminated by `\n` or screen
clear), not on partial redraws.

A3. **Idle detection.** An Agent is considered idle when the regex in
`parse.idle` matches the last non-empty rendered line AND no output
has occurred for 200 ms.

A4. **Resize handling.** On receiving SIGWINCH, the Driver MAY pass
the new size through to the PTY child via the PTY's `set_size`
method. Some Agents handle resize gracefully (Claude Code, aider);
others may corrupt their TUI.

A5. **Scrollback.** Drivers SHOULD maintain at least 10,000 lines of
scrollback for replay and audit.

A6. **Process supervision.** If the PTY child exits unexpectedly, the
Driver MUST emit `cap.error { code: "pty_died" }` before marking the
session as terminated.

## Appendix B. Manifest Schema Reference

Full JSON Schema available at
`https://cap-protocol.org/schema/manifest/v1.json` (to be published).
For now, refer to the TOML example in §5.1; semantics are normative.

## Appendix C. Binding Mappings (informative)

### C.1 stream-json → Core Events

| stream-json frame | Core Event |
|---|---|
| `{ type: "system", subtype: "init" }` | `cap.session.ready` |
| `{ type: "assistant", message.content: [{ type: "text", text }] }` (delta) | `cap.text_chunk` |
| `{ type: "assistant", message.content: [{ type: "thinking", text }] }` | `cap.thought` |
| `{ type: "assistant", message.content: [{ type: "tool_use", id, name, input }] }` | `cap.tool_call.start` |
| `{ type: "user",      message.content: [{ type: "tool_result", tool_use_id, content }] }` | `cap.tool_call.end` |
| `{ type: "result",    subtype: "success", usage }` | `cap.usage` |
| `{ type: "result",    subtype: "error", error }` | `cap.error` |

### C.2 gRPC (openclaude-compatible) → Core Events

| ServerMessage variant | Core Event |
|---|---|
| `TextChunk` | `cap.text_chunk` |
| `ToolCallStart` | `cap.tool_call.start` |
| `ToolCallResult` | `cap.tool_call.end` |
| `ActionRequired` | `cap.ask_user` or `cap.permission.request` |
| `FinalResponse` | `cap.usage` |
| `ErrorResponse` | `cap.error` |

### C.3 ACP-stdio → Core Events

| ACP frame | Core Event |
|---|---|
| `session/update { agent_message_chunk }` | `cap.text_chunk` |
| `session/update { agent_thought_chunk }` | `cap.thought` |
| `session/update { tool_call }` | `cap.tool_call.start` |
| `session/update { tool_call_update completed }` | `cap.tool_call.end` |
| `session/update { plan }` | `cap.plan` |
| `session/request_permission` | `cap.permission.request` (or `cap.ask_user` for non-permission elicitation) |
| `session/elicit` (v2) | `cap.ask_user` |
| Final response with usage | `cap.usage` |

Reverse RPC mapping for coding profile is in
[cap-profile-coding-v1.md](./cap-profile-coding-v1.md) §6.

### C.4 A2A HTTPS+SSE → Core Events

| A2A frame | Core Event |
|---|---|
| `StreamResponse.message` (text part) | `cap.text_chunk` |
| `StreamResponse.statusUpdate { state: input-required }` + DataPart | `cap.ask_user` or `cap.permission.request` |
| `StreamResponse.artifactUpdate` | `cap.tool_call.end` or profile-specific artifact |
| `Task.status = completed` + usage DataPart | `cap.usage` |
| `Task.status = failed` + error | `cap.error` |

## Glossary

- **A2A** — Agent-to-Agent protocol; CAP composes with it (§11).
- **ACP** — Zed's Agent Client Protocol; CAP bridges to it (§6.4).
- **Agent** — a CLI AI program managed by CAP.
- **Binding** — a wire encoding for the Agent↔Driver channel.
- **Driver** — software implementing one or more bindings.
- **Manifest** — agent's self-description (§5).
- **MCP** — Model Context Protocol; orthogonal to CAP.
- **Orchestrator** — CAP client managing one or more Agents.
- **Profile** — a vertical extension (e.g. `coding`).
- **PTY** — pseudo-terminal; the universal CAP substrate.
- **Session** — one logical conversation with one Agent.

## Changelog

| Date | Notes |
|---|---|
| 2026-05-18 | Initial CLI-Agent-Protocol draft. Replaces earlier "Coding Agent Protocol" v1 working name. PTY promoted to REQUIRED substrate. Profiles introduced. A2A binding added. |
