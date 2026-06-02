# cap-cli

`cap` — command-line tool for discovering, driving, and orchestrating
CLI AI agents.

> **Status: functional.** `cap` validates manifests and fleet specs,
> lists supported drivers, scaffolds a `fleet.yaml`, runs interactive
> multi-agent chat, and executes fleets of collaborating agents with
> static, LLM, or hybrid routing. Public review of the spec is open;
> the API surface may still shift during the v0.x cycle.

CAP (CLI Agent Protocol) is an open protocol for discovering, driving,
and orchestrating any command-line AI agent.

- Homepage: <https://cap-protocol.org>
- Specification: <https://github.com/rsclaw-ai/cap-protocol/blob/main/docs/cap-v1.md>
- Repository: <https://github.com/rsclaw-ai/cap-protocol>
- Rust library: [`cap-rs`](https://crates.io/crates/cap-rs)

## Install

```bash
cargo install cap-cli
# or, from the repo:
cargo build --package cap-cli
./target/debug/cap --help
```

## Commands

```
cap validate <fleet.yaml>          Parse + validate a fleet.yaml without running it
cap list-drivers                   List every supported agent driver kind
cap init                           Scaffold a default fleet.yaml in the current dir
cap manifest validate <path>       Validate a CAP agent manifest (TOML)
cap manifest resolve <name|path>   Resolve a manifest by name (discovery) or path
cap chat [fleet.yaml]              Interactive chat with one or more agents
    --task "<text>"                  Task text (overrides fleet.task)
    --bypass                         Auto-approve every permission request
    --driver <kind>                  Driver for an auto-created single-agent fleet
                                     (default: claude)
cap run <fleet.yaml>               Run a fleet of collaborating agents
    --task "<text>"                  Task text (overrides fleet.task)
    --bypass                         Auto-approve every permission request
    --mode static|llm|hybrid         Routing strategy (default: static)
```

`cap chat` with no path auto-creates a single-agent fleet using
`--driver`, so the fastest way to talk to an agent is:

```bash
cap chat --driver claude --task "Write hello world in Rust"
```

## Supported drivers

`cap list-drivers` prints the current set:

| Driver | Agent | Transport |
|---|---|---|
| `claude` | Claude Code | stream-json |
| `openclaude` | OpenClaude | stream-json (Anthropic SDK-compatible) |
| `codex` | OpenAI Codex | stream-json (Claude Code-compatible NDJSON) |
| `opencode` | OpenCode | stream-json |
| `aider` | Aider | PTY |
| `a2a:<url>` | Any A2A agent | A2A HTTPS+SSE (e.g. `a2a:http://127.0.0.1:4000`) |
| `acp:<cmd>` | Any ACP agent | ACP-stdio (e.g. `acp:opencode`) |
| `grpc:<addr>` | OpenClaude | gRPC (e.g. `grpc:localhost:50051`) |
| `pty:<cmd>` | Any CLI agent | PTY fallback (e.g. `pty:opencode`) |

## Routing strategies (`cap run --mode`)

- **`static`** (default) — routes are taken verbatim from the `routes:`
  block in `fleet.yaml`.
- **`llm`** — an LLM decides the next session at each hand-off.
- **`hybrid`** — static routes first; the LLM fills in where static
  routing has no edge.

See [docs/quickstart.md](https://github.com/rsclaw-ai/cap-protocol/blob/main/docs/quickstart.md)
for fleet patterns (pipeline, fan-out + join, parallel + human collect,
by-subtask split) and [docs/cap-v1.md](https://github.com/rsclaw-ai/cap-protocol/blob/main/docs/cap-v1.md)
for the protocol spec.

## License

MIT. See [LICENSE-MIT](https://github.com/rsclaw-ai/cap-protocol/blob/main/LICENSE-MIT).
