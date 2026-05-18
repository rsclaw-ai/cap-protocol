# CAP Profile: Coding (v1, draft)

> **Status**: `draft-2026-05-18`
> **Profile URI**: `cap-protocol/v1/profile/coding`
> **Homepage**: <https://cap-protocol.org>
> **Depends on**: [CAP Core v1](./cap-v1.md)

## Abstract

The coding profile extends CAP core with capabilities specific to
**software engineering agents**: filesystem access against a project
workspace, terminal access for running build/test/git commands, and
artifact types for the code, diffs, PRs, and test results these
agents typically produce.

An Agent declares conformance by listing `"coding"` in its
Manifest `agent.profiles`.

## 1. Conventions

Same as CAP core (§1). Event kinds and Reverse RPC method names
defined here are prefixed `cap.fs.` and `cap.terminal.`. Artifact
types use the mediaType namespace `application/vnd.cap.coding.<type>`.

## 2. Conformance

An Agent conforms to `profile/coding` if it:

1. Declares `"coding"` in `agent.profiles` of its Manifest (CAP core
   §5).
2. Implements the capabilities advertised in its
   `[capabilities.coding]` Manifest section (§3).
3. Honours the security contract in §7.

An Orchestrator conforms to `profile/coding` if it:

1. Implements all Reverse RPC methods (§4, §5) the Agent declares as
   `required` in `[capabilities.coding]`.
2. Enforces the `fs.scope` declared in the Manifest (§7.1).
3. Surfaces coding-specific Artifacts (§6) to the user.

## 3. Manifest Additions

The coding profile extends the Manifest with a `[capabilities.coding]`
section:

```toml
[capabilities.coding]
revision = "2026-05-18"

# Filesystem capabilities — see §4
fs.read = true
fs.write = true
fs.list = true
fs.exists = true
fs.delete = false
fs.watch = false
fs.scope = "workspace_only"    # workspace_only | project_root | unrestricted

# Terminal capabilities — see §5
terminal.enabled = true
terminal.concurrent_max = 4

# Tooling expectations
tool_permission = "interactive" # none | confirm | interactive
tools_allowed = []              # empty = all permitted; else allow-list
tools_denied  = []              # deny-list, evaluated after allow-list

# Code-output capabilities
artifacts = ["diff", "plan_doc", "pr_link", "test_result", "commit", "transcript"]

# Optional integration hints
git_aware = true                # agent understands git context
language_servers = ["rust-analyzer", "tsserver"]  # if relevant
test_runners = ["cargo", "vitest"]
```

All fields are OPTIONAL except `fs.scope` if any `fs.*` is true, and
`tool_permission`.

## 4. Filesystem Reverse RPC

Methods invoked by the Agent against the Orchestrator. The
Orchestrator MUST enforce the declared `fs.scope`.

### 4.1 `cap.fs.read`

```
params: { path: string }
result: { content: string, encoding: "utf-8" | "base64", size: int }
errors: cap_scope_violation | cap_path_not_found | cap_io_error
```

All paths MUST be absolute. The Orchestrator MUST reject paths
outside `fs.scope` with error `-32001 cap_scope_violation` without
revealing whether the file exists.

### 4.2 `cap.fs.write`

```
params: { path: string, content: string, encoding?: "utf-8" | "base64", create?: bool, overwrite?: bool }
result: { bytes_written: int }
errors: cap_scope_violation | cap_io_error
```

Defaults: `create = true`, `overwrite = true`.

### 4.3 `cap.fs.list`

```
params: { path: string, recursive?: bool, follow_symlinks?: bool }
result: {
  entries: [{ name: string, kind: "file" | "dir" | "symlink", size: int, modified_ms?: int }]
}
```

### 4.4 `cap.fs.exists`

```
params: { path: string }
result: { exists: bool, kind?: "file" | "dir" | "symlink" }
```

### 4.5 `cap.fs.delete`

```
params: { path: string, recursive?: bool }
result: { ok: true }
errors: cap_scope_violation | cap_io_error
```

`recursive = true` MUST be honoured only for directories. Files
ignore the flag.

### 4.6 `cap.fs.watch` (OPTIONAL)

If `fs.watch = true`, the Agent MAY subscribe to filesystem changes:

```
params: { path: string, recursive?: bool }
result: { subscription_id: string }
```

Subsequent change events are pushed as `cap.fs.change` notifications:

```jsonc
{
  "kind": "cap.fs.change",
  "subscription_id": "...",
  "path": "/...",
  "event": "created" | "modified" | "deleted" | "renamed",
  "new_path": "/..."        // only on renamed
}
```

Orchestrators MAY rate-limit change events.

## 5. Terminal Reverse RPC

Used by Agents that need to spawn shell commands the Orchestrator
should mediate (logging, sandboxing, recording). Independent of the
Orchestrator's PTY binding to the Agent itself.

### 5.1 `cap.terminal.create`

```
params: { command: string, args?: string[], cwd?: string, env?: { [k]: v } }
result: { terminal_id: string }
errors: cap_terminal_unavailable | cap_scope_violation
```

The `cwd` MUST be within `fs.scope`. `env` is merged on top of the
Orchestrator's base environment.

### 5.2 `cap.terminal.output`

```
params: { terminal_id: string, since_byte?: int }
result: { output: string, next_byte: int, exit_code?: int }
```

Once `exit_code` is set, subsequent calls return the same data.

### 5.3 `cap.terminal.wait`

```
params: { terminal_id: string, timeout_ms?: int }
result: { exit_code: int, output: string }
```

`timeout_ms = null` means wait indefinitely.

### 5.4 `cap.terminal.kill`

```
params: { terminal_id: string, signal?: "SIGTERM" | "SIGKILL" | "SIGINT" }
result: { ok: true }
```

Default signal `SIGTERM`; escalates to `SIGKILL` after 5s if process
hasn't exited.

### 5.5 `cap.terminal.release`

```
params: { terminal_id: string }
result: { ok: true }
```

The Agent declares the terminal no longer needed. Orchestrator MAY
reclaim resources but MUST keep the buffer queryable for 60s.

## 6. Coding-Specific Artifact Types

Final outputs of a session attach to the Agent's terminal Event
stream as Artifacts (in A2A binding) or as `cap.artifact` events (in
other bindings). The latter form:

```jsonc
{
  "kind": "cap.artifact",
  "artifact_id": "art_01",
  "mime_type": "application/vnd.cap.coding.diff.v1+text",
  "name": "src/protocol.rs.patch",
  "size": 4820,
  "uri": "file:///tmp/...",         // OPTIONAL; if omitted, content inline
  "content": "..."                  // OPTIONAL; if omitted, fetch via uri
}
```

### 6.1 `application/vnd.cap.coding.diff.v1+text`

Unified diff (POSIX `diff -u` format). One file per artifact RECOMMENDED;
multi-file patches MUST use standard `diff --git` headers.

### 6.2 `application/vnd.cap.coding.pr_link.v1+json`

```jsonc
{
  "provider": "github",                     // github | gitlab | bitbucket | gitea | other
  "owner": "rsclaw",
  "repo": "github-rsclaw",
  "number": 142,
  "url": "https://github.com/rsclaw/github-rsclaw/pull/142",
  "title": "Add CAP profile",
  "state": "open",                          // open | merged | closed | draft
  "created_at": "2026-05-18T10:00:00Z"
}
```

### 6.3 `application/vnd.cap.coding.test_result.v1+json`

```jsonc
{
  "framework": "cargo-test",
  "passed": 41,
  "failed": 0,
  "skipped": 2,
  "duration_ms": 12480,
  "output": "...",                          // truncated to 64KB
  "suites": [
    { "name": "agent::registry", "passed": 8, "failed": 0, "duration_ms": 1200 }
  ]
}
```

### 6.4 `application/vnd.cap.coding.commit.v1+json`

```jsonc
{
  "sha": "2b769f8aabc",
  "message": "feat: ...",
  "author": "ai-agent",
  "committer": "ai-agent",
  "committed_at": "2026-05-18T10:30:00Z",
  "files_changed": 4,
  "insertions": 120,
  "deletions": 8,
  "parents": ["ba0101cdef"]
}
```

### 6.5 `application/vnd.cap.coding.plan_doc.v1+json`

Same schema as core `cap.plan` (§7.3 of core), but emitted as a
final document rather than a stream event. Useful for archival.

### 6.6 `application/vnd.cap.coding.transcript.v1+json`

```jsonc
{
  "session_id": "...",
  "agent": "claude-code",
  "model": "claude-opus-4-7",
  "started_at": "...",
  "ended_at": "...",
  "events": [ /* full Core Event log */ ]
}
```

OPTIONAL but RECOMMENDED for audit / replay.

## 7. Security Additions

### 7.1 `fs.scope` enforcement (REQUIRED)

The Orchestrator MUST enforce the declared `fs.scope`:

| Scope | Allowed paths |
|---|---|
| `workspace_only` | The `cwd` from `cap.session.config` and its subtree |
| `project_root` | The nearest ancestor of `cwd` containing a VCS root marker (`.git`, `.hg`, `.svn`) and its subtree |
| `unrestricted` | No filesystem restriction (DISCOURAGED) |

Symlink resolution MUST happen before scope check. The Orchestrator
MUST NOT follow symlinks that point outside the scope.

`unrestricted` Agents SHOULD ONLY be run inside hardware-isolated
sandboxes (VMs or containers with their own filesystem).

### 7.2 Terminal command vetting

When `tool_permission = "interactive"`, the Orchestrator MUST surface
every `cap.terminal.create` request as a `cap.permission.request` to
the user before spawning the command.

When `tool_permission = "confirm"`, the Orchestrator MAY auto-approve
commands matching a configured allow-list and prompt for others.

When `tool_permission = "none"`, the Orchestrator MUST log every
spawn but MAY auto-approve all. Users MUST be warned prominently
that this Agent runs unattended.

### 7.3 Diff vetting

Orchestrators SHOULD diff staged changes against the working tree
before allowing the Agent to commit, surfacing any unexpected file
modifications (e.g. credential leakage, large binary additions) to
the user.

### 7.4 Multi-sub-agent workspace isolation

Per CAP core §10.4, Orchestrators running multiple coding sub-agents
SHOULD provision separate git worktrees per sub-agent. Suggested
naming: `<repo>/.cap-worktrees/<sub-agent-urn>/`.

The Orchestrator MAY merge results via:

1. Three-way merge of worktree branches into a common integration
   branch.
2. PR-based review (each sub-agent's worktree becomes a draft PR).
3. Sequential cherry-pick with conflict resolution by a designated
   "merger" sub-agent.

## 8. Manifest Examples

### 8.1 Claude Code

```toml
[agent]
name = "claude-code"
binary = "claude"
profiles = ["coding"]

[capabilities.coding]
revision = "2026-05-18"
fs = { read = true, write = true, list = true, exists = true, scope = "workspace_only" }
terminal = { enabled = true, concurrent_max = 4 }
tool_permission = "interactive"
artifacts = ["diff", "plan_doc", "pr_link", "test_result"]
git_aware = true
```

### 8.2 aider (PTY-only)

```toml
[agent]
name = "aider"
binary = "aider"
profiles = ["coding"]

[fast_path]
stream_json = false
acp_stdio = false

[capabilities.coding]
revision = "2026-05-18"
# aider handles fs internally; doesn't expose reverse RPC
fs = { read = false, write = false, list = false, scope = "workspace_only" }
terminal = { enabled = false }
tool_permission = "interactive"
artifacts = ["diff", "commit"]
git_aware = true
```

For Agents like aider that don't expose Reverse RPC fs/terminal,
Orchestrators rely on PTY parsing of their TUI output for file
changes and command runs.

## 9. ACP Bridge Mapping (informative)

For Agents accessed via the ACP-stdio binding (CAP core §6.4),
coding profile Reverse RPC maps to ACP methods:

| CAP coding Reverse RPC | ACP method |
|---|---|
| `cap.fs.read` | `fs/read_text_file` |
| `cap.fs.write` | `fs/write_text_file` |
| `cap.fs.list` | NOT IN ACP — bridge SHOULD use `_cap/fs_list` extension method, or implement locally if Driver controls workspace |
| `cap.fs.exists` | NOT IN ACP — same handling as fs.list |
| `cap.fs.delete` | NOT IN ACP — same handling |
| `cap.terminal.create` | `terminal/create` |
| `cap.terminal.output` | `terminal/output` |
| `cap.terminal.wait` | `terminal/wait_for_exit` |
| `cap.terminal.kill` | `terminal/kill` |
| `cap.terminal.release` | `terminal/release` |

Where ACP lacks a method, the Driver uses ACP's `ExtRequest` /
`ExtNotification` mechanism with method names `_cap/<rpc>`.

## 10. Versioning and Errors

Profile-specific errors extend CAP core's error registry:

| Code | Symbol | Meaning |
|---|---|---|
| -32020 | `cap_scope_violation` | Path outside declared `fs.scope` |
| -32021 | `cap_path_not_found` | Filesystem operation on missing path |
| -32022 | `cap_io_error` | Generic filesystem I/O failure |
| -32023 | `cap_terminal_unavailable` | Terminal capability requested but disabled |
| -32024 | `cap_terminal_killed` | Terminal was killed by the Orchestrator |

String error codes for `cap.error` defined by this profile:

| Code | Meaning |
|---|---|
| `git_dirty` | Workspace has uncommitted changes when clean required |
| `test_failed` | Test suite reported failures |
| `merge_conflict` | Multi-agent merge could not be auto-resolved |
| `language_server_down` | Required LSP unavailable |

## Changelog

| Date | Notes |
|---|---|
| 2026-05-18 | Initial coding profile draft. |
