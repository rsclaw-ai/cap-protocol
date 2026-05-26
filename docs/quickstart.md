# CAP Quickstart

**CAP** (CLI Agent Protocol) orchestrates CLI-based AI agents like Claude Code,
OpenAI Codex, OpenCode, and OpenClaude in a single pipeline.

## Install

```bash
cargo build --package cap-cli
./target/debug/cap --help
```

## 1. Create a fleet

```bash
cap init                  # generates fleet.yaml in current dir
cap validate fleet.yaml   # verify it's correct
```

Or pick one of the ready-made examples:

```bash
ls docs/examples/
```

## 2. Run a pipeline

```bash
cap run fleet.yaml --task "Write hello world in Rust"
```

## 3. Supported agents

| Driver | Agent | Command |
|--------|-------|---------|
| `claude` | Claude Code | `claude` |
| `codex` | OpenAI Codex | `codex` |
| `acp:opencode` | OpenCode (ACP) | `opencode acp` |
| `grpc:localhost:50051` | OpenClaude (gRPC) | `openclaude grpc` |
| `pty:codex` | Codex (PTY fallback) | `codex` |

## 4. Fleet patterns

### Pipeline: A → B

```yaml
sessions:
  coder:    { driver: claude }
  reviewer: { driver: codex }
start: coder
routes:
  - { when: coder.done, route_to: reviewer }
```

### Fan-out + join

```yaml
sessions:
  lead:    { driver: claude }
  worker1: { driver: codex }
  worker2: { driver: acp:opencode }
  rev:     { driver: claude }
start: lead
routes:
  - when: lead.done
    fan_out: { to: [worker1, worker2], split: broadcast }
  - when: [worker1.done, worker2.done]
    route_to: rev
```

### Parallel + human collect

```yaml
sessions:
  a: { driver: claude }
  b: { driver: codex }
start: [a, b]
routes:
  - when: [a.done, b.done]
    collect: human
```

### By-subtask split

The lead agent outputs a ` ```cap-subtasks ` fence with a JSON array
of subtask strings. Each subtask is routed to one of the fan-out targets.

```yaml
sessions:
  lead: { driver: claude }
  dev1: { driver: codex }
  dev2: { driver: acp:opencode }
start: lead
routes:
  - when: lead.done
    fan_out: { to: [dev1, dev2], split: by_subtask }
```

## CLI reference

```
cap validate <fleet.yaml>   # Parse + validate without running
cap list-drivers            # Show all supported agent types
cap init                    # Generate fleet.yaml template
cap run  <fleet.yaml>       # Run the fleet
  --task "..."              # Override task
  --bypass                  # Auto-approve all permissions
```

## Worktree isolation

Each session runs in its own `git worktree` at `.cap/<session>/`.
Worktrees are cleaned up when the fleet finishes.
