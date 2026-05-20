# cap-rs-orchestrator — engine design

> Design spec for the first sub-project of the "remote multi-agent control"
> vision: a headless orchestration engine that runs N collaborating CLI
> agents in one process. Local-only in this phase — driven via `cap-cli`,
> no remote/mobile layer yet.

Status: draft — 2026-05-20 · Sits below `docs/cap-v1.md` (the protocol).

## 1. Context & scope

### The full vision (for reference, NOT this spec)

A mobile app to remotely **monitor + dispatch + approve** a fleet of
collaborating coding agents (Claude Code, Codex, opencode, …) running on
the user's dev machine. That decomposes into 5 layered sub-projects:

1. **Orchestration engine** — run N agents in one process, let them collaborate.
2. *(later)* Transport layer — WS+REST + auth + event replay over a tunnel.
3. *(later)* Push — APNs/FCM to wake approvals when the app is backgrounded.
4. *(later)* Mobile app — thin client.
5. *(later)* Connectivity — Tailscale / Cloudflare Tunnel.

**This spec covers sub-project 1 only.** Bottom-up: the mobile app is a thin
client over an engine that does not exist yet. The engine is the highest-risk,
highest-value, most reusable piece, so it goes first and is proven locally.

### In scope

- A new crate `cap-rs-orchestrator` (the reserved-but-empty name) depending on `cap-rs`.
- A declarative DSL (`fleet.yaml`) describing sessions + routing.
- A deterministic executor that drives four collaboration patterns from the DSL.
- Per-agent git worktree isolation.
- A per-session permission model plus a fleet-level `bypass` mode.
- An audit log of every cross-session route.
- A thin `cap run <fleet.yaml>` command in `cap-cli`.
- A `StubDriver` and a deterministic test gate that needs no real LLM/network.

### Out of scope (this phase)

- Any remote/network/mobile/push/auth layer.
- Dynamic peer-messaging topology (agents deciding recipients at runtime) — see §6.
- Auto-merge of worktrees — the engine surfaces branches + diffs; humans merge.

## 2. Approach

Chosen: **library crate + thin CLI driver** (over "build into cap-cli then
extract" and "standalone daemon now"). Rationale:

- The later remote layer embeds the same library behind WebSocket — matches the
  reserved `cap-rs-orchestrator` crate name.
- Independently testable with stub drivers, so CI never depends on a real LLM
  (carries forward the concern logged in `docs/STATUS.md`).
- A standalone daemon would prematurely introduce the transport layer that is
  explicitly deferred to sub-project 2.

## 3. Architecture & components

```
crates/cap-rs-orchestrator/src/
├── lib.rs        // public API: Orchestrator::run(FleetSpec) -> OrchestratorEvent stream
├── config.rs     // FleetSpec / SessionSpec / Route — serde DSL + validation, no IO
├── registry.rs   // SessionRegistry: holds inbox sender + handle per session id
├── session.rs    // one tokio task per session, owns a Box<dyn Driver>
├── worktree.rs   // WorktreeManager: git worktree allocation + cleanup
├── executor.rs   // deterministic state machine: interprets the DSL
└── audit.rs      // audit log + event multiplexer (tags each event with session id)
```

| Component | Responsibility | Depends on |
|---|---|---|
| `config` | YAML → typed `FleetSpec`; validate route refs point at declared sessions | serde, no IO |
| `WorktreeManager` | `git worktree add` per session off `base_branch`; cleanup on exit | git CLI |
| `session` actor | own one driver; inbox `ClientFrame` → `driver.send`; `driver.next_event` → tag with id → event bus | cap-rs `Driver` |
| `SessionRegistry` | by-id map of inbox senders + handles; spawn / cancel | session |
| `executor` | state machine: when to spawn, what to route a→b, when to join | registry, audit |
| `audit` | one immutable timestamped record per cross-session route; merge all session events into one stream | — |
| `lib` | assemble the above; expose `run()` returning the unified `OrchestratorEvent` stream | all |

**Key invariant:** `executor` never touches a driver. It only issues commands
to `registry` (spawn id / route a→b / join \[ids]) and records to `audit`. The
`Driver: Send`-not-`Sync` constraint is fully contained in the `session` actor,
leaving `executor` a pure state machine that is testable against stub drivers.

## 4. Concurrency & data flow

Actor model. One tokio task per session owns its driver; everything
communicates over `mpsc` channels — no shared mutable state, no `Mutex<Driver>`.

```
   fleet.yaml ─►config─►  executor (state machine, 1 task)
                            │  commands: spawn / route / join / cancel
                            ▼            ▲ join-complete signals
                       SessionRegistry (id → inbox mpsc::Sender)
                            │
            ClientFrame     ▼
                  session actor × N   (each owns Box<dyn Driver>)
                            │  AgentEvent tagged with session id
                            ▼
                  event bus (bounded mpsc) → audit → OrchestratorEvent stream
                                                       (out of lib, into cap-cli)
```

A prompt's journey (pipeline A→B example):

1. `executor` reads the DSL; tells registry to `spawn(A)`, `spawn(B)` (each
   starts an actor + a worktree).
2. `executor` sends the initial prompt to A's inbox → A's actor calls `driver.send`.
3. A's actor pumps `next_event()`, tagging each event `session=A` onto the bus
   (live stream to any consumer).
4. A emits a terminal event (`TurnComplete`); the actor signals `executor`.
5. `executor` consults route rule `A.done → B`, has `audit` record
   `Route { from: A, to: B, payload }`, then posts A's output to B's inbox.
6. B repeats 3–4. When all joins complete, `executor` finishes.

**Rationale (anchored in the cap-rs code):** `Driver: Send` (not `Sync`) with a
`next_event(&mut self)` poll model means a driver can only be mutably borrowed by
one task. One owning actor per session is therefore the only clean concurrency
model. The `executor↔consumer` channel boundary is also the **seam for the
future remote layer**: today it is an in-process channel; later it becomes a
WebSocket carrying the same serialized `OrchestratorEvent` / command types.

**Backpressure:** the event bus is a bounded channel. A session that floods
output cannot exhaust memory against a slow consumer (later: a phone); when the
bus is full, that session actor's `next_event` pump pauses.

## 5. The DSL (`fleet.yaml`)

Three primitives compose all four patterns: a trigger (`when`), an action
(`route_to` / `fan_out` / `collect`), and an entry point (`start`).

Fleet declaration:

```yaml
fleet:
  base_branch: main          # worktrees branch off this
  task: ./TASK.md            # initial task (or `cap run --task "..."`)
  permissions: ask           # fleet-level: ask | allow | deny | bypass (see §6)
  sessions:
    architect:
      driver: claude         # claude | codex | pty:opencode | pty:<any CLI>
      permissions: ask       # per-session override: ask | allow | deny
    worker-a: { driver: codex, permissions: allow }
    worker-b: { driver: codex, permissions: allow }
    reviewer: { driver: claude }
```

(Worktrees default to one-per-session, branched off `base_branch`. Omit unless overriding.)

**Pattern ① lead/worker** = fan_out + join:

```yaml
  start: architect
  routes:
    - when: architect.done
      fan_out: { to: [worker-a, worker-b], split: by_subtask }
    - when: [worker-a.done, worker-b.done]   # list = join: fires when all complete
      route_to: reviewer
```

**Pattern ② pipeline** = chained single edges:

```yaml
  start: coder
  routes:
    - { when: coder.done,    route_to: reviewer }
    - { when: reviewer.done, route_to: tester }
```

**Pattern ③ parallel race** = broadcast + collect:

```yaml
  start: [sol-a, sol-b, sol-c]              # same task to all three at once
  routes:
    - when: [sol-a.done, sol-b.done, sol-c.done]
      collect: human                        # diffs from 3 worktrees surfaced for a human pick
```

How `executor` reads it: `start` decides the first sessions to spawn; each
`route` is one state-machine edge; a list `when` is a join (fires only when all
listed sessions complete); `fan_out` triggers multiple spawns + dispatch;
`collect: human` emits `OrchestratorEvent::AwaitSelection` carrying the
candidate diffs and waits for a selection. Fully deterministic and replayable.

`fan_out.split` semantics:

- `broadcast` — the same payload goes to every target (used by the race pattern).
- `by_subtask` — the lead session must end its turn with a fenced
  ```` ```cap-subtasks ```` block: a JSON array of strings, one per subtask. The
  executor parses that block and assigns items to targets round-robin; the count
  need not match the target count. If the block is missing or unparseable, the
  route fails with `SessionFailed` on the lead (rather than guessing a split).
  This keeps the split deterministic and inspectable rather than inferred.

## 6. Permissions, errors, cancellation

### Permission model (three levels + bypass)

- **Per session:** `ask` (dangerous ops emit `OrchestratorEvent::Ask` and block
  for a decision) / `allow` (auto-approve) / `deny` (auto-reject).
- **Fleet-level `bypass`** — open everything. Two effects:
  1. the orchestrator auto-allows every `AskKind`, emitting no `Ask` events;
  2. each driver's native bypass flag is passed through — Claude
     `--dangerously-skip-permissions`, Codex `--full-auto` /
     `--dangerously-bypass-approvals-and-sandbox`, pty agents run unsandboxed.
- **CLI override:** `cap run fleet.yaml --bypass` forces bypass regardless of config.

> ⚠️ **`bypass` lets agents run arbitrary commands in their worktree with no
> human gate.** Worktree isolation still bounds the blast radius, but this mode
> is only for "I trust this batch of work." Default is `ask`.

`ask` resolution in local v1: `executor` emits an `Ask` event → `cap-cli`
prompts on stdin ("session X wants \<op\>, y/n") → sends a `Decision` back. This
ask/decision channel is the same interface the future mobile approval push reuses.

### Error handling

- Route referencing an undeclared session → `config` validation error before any
  spawn.
- Worktree creation failure → fleet start fails fast.
- A session's driver dies (`DriverError` / process exit) → mark that session
  failed, emit `SessionFailed`; its downstream routes are marked blocked, but
  sibling branches continue (no fleet-wide kill). An `on_error: abort | continue`
  knob is left for a later refinement.

### Cancellation

- `cap-cli` `Ctrl-C` → `executor` sends shutdown to all session actors →
  each driver `shutdown()` → worktrees kept by default for inspection
  (`--clean` to remove).
- Reuses the existing `CancelScope` in cap-rs core for per-session cancel.

## 7. Testing strategy

Carries forward the `docs/STATUS.md` requirement that tests not depend on a real LLM.

- **`StubDriver: Driver`** — emits scripted events, so `executor` + DSL run with
  zero LLM and zero network.
- **Unit:** `config` validation; `executor` state transitions; `audit` log ordering.
- **Integration:** three `fleet.yaml` files (lead/worker, pipeline, race) each
  run against stub drivers, asserting the event + audit sequence. This is the
  deterministic CI gate.

## 8. Deferred (explicit)

1. **Dynamic peer-messaging topology** — agents choosing recipients at runtime
   conflicts with declarative determinism. A v1.x compromise: declared allowed
   edges triggered by a marker in agent output (still auditable).
2. **Auto-merge of worktrees** — engine surfaces branch names + diffs only.
3. **`on_error: abort | continue`** routing policy.
4. **Budget aggregation** across sessions (cap-rs already emits `Usage`; summing
   is cheap to add later).
5. Everything in sub-projects 2–5 (remote, push, mobile, tunnel).
