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
| `openclaude` | OpenClaude (stream-json) | `openclaude` |
| `opencode` | OpenCode (stream-json) | `opencode` |
| `codex` | OpenAI Codex (stream-json) | `codex` |
| `aider` | Aider (PTY) | `aider` |
| `acp:opencode` | OpenCode (ACP) | `opencode acp` |
| `a2a:http://127.0.0.1:4000` | Any A2A agent (HTTPS+SSE) | remote endpoint |
| `grpc:localhost:50051` | OpenClaude (gRPC) | `openclaude grpc` |
| `pty:codex` | Codex (PTY fallback) | `codex` |

Run `cap list-drivers` to print the current set.

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

## Interactive chat

Talk to one or more agents directly. With no `fleet.yaml`, `cap chat`
auto-creates a single-agent fleet using `--driver`:

```bash
cap chat --driver claude --task "Write hello world in Rust"
cap chat fleet.yaml --task "..."        # multi-agent chat over a fleet
```

## CLI reference

```
cap validate <fleet.yaml>          # Parse + validate without running
cap list-drivers                   # Show all supported driver kinds
cap init                           # Generate fleet.yaml template
cap manifest validate <path>       # Validate a CAP agent manifest (TOML)
cap manifest resolve <name|path>   # Resolve a manifest by name or path
cap chat [fleet.yaml]              # Interactive chat (auto-fleet if omitted)
  --task "..."                       # Override task
  --bypass                           # Auto-approve all permissions
  --driver <kind>                    # Driver for the auto-created fleet (default: claude)
cap run  <fleet.yaml>              # Run the fleet
  --task "..."                       # Override task
  --bypass                           # Auto-approve all permissions
  --mode static|llm|hybrid           # Routing strategy (default: static)
```

### Routing strategies

- **`static`** (default) — follow the `routes:` block verbatim.
- **`llm`** — an LLM picks the next session at each hand-off.
- **`hybrid`** — static routes first, LLM fills the gaps.

## Worktree isolation

Each session runs in its own `git worktree` at `.cap/<session>/`.
Worktrees are cleaned up when the fleet finishes.
