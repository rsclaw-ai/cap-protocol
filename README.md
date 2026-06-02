# CAP — CLI Agent Protocol

> **An open protocol for discovering, driving, and orchestrating any command-line AI agent.**
>
> Homepage: <https://cap-protocol.org> · Spec: [v1 draft](docs/cap-v1.md) · Coding profile: [v1 draft](docs/cap-profile-coding-v1.md)

[![status: draft](https://img.shields.io/badge/status-draft--2026--05--18-yellow)](docs/cap-v1.md)
[![spec license: CC BY 4.0](https://img.shields.io/badge/spec-CC%20BY%204.0-blue)](LICENSE-CC-BY-4.0)
[![code license: MIT](https://img.shields.io/badge/code-MIT-green)](LICENSE-MIT)
[![website](https://img.shields.io/badge/site-cap--protocol.org-4ade80)](https://cap-protocol.org)

CAP — the **CLI Agent Protocol** — defines how a single orchestrator
can drive any AI agent that runs on the command line: Claude Code,
Codex, Opencode, aider, openclaude, Gemini CLI, and any future CLI
agent — whether they expose a structured API or not.

**Authored and currently maintained by [rsclaw][rsclaw-site]**, the
open-source agent fleet that uses CAP as its native orchestration
layer. Public review of the v1 draft is open.

[rsclaw-site]: https://rsclaw.ai

---

## Why CAP exists

Every major AI coding agent in 2026 ships as a CLI. They each have
their own protocol — or none at all. Trying to coordinate three of
them on the same project today means writing three different
adapters, debugging three different output formats, and giving up on
real-time interaction across fleet.

CAP fixes that:

- **PTY is the universal substrate.** Any program that runs in a
  terminal can be driven by a CAP driver. Zero protocol negotiation,
  day-1 compatibility, even for proprietary CLI agents.
- **Fast-path bindings** (`stream-json`, `gRPC`, `ACP-stdio`,
  `A2A HTTPS+SSE`) are used when an agent supports one — but they're
  optional optimizations, not gatekeepers.
- **Multi-agent orchestration is first-class.** Plan propagation,
  cross-agent message routing (always orchestrator-mediated and
  human-auditable), workspace isolation via git worktrees, budget
  aggregation with hard cancel.

## Position in the agent protocol stack

| Layer | Protocol | Maintainer |
|---|---|---|
| agent ↔ tools | [MCP](https://modelcontextprotocol.io) | Anthropic |
| agent ↔ editor | [ACP](https://agentclientprotocol.com) | Zed |
| agent ↔ agent (peer) | [A2A](https://a2a-protocol.org) | Google / LF |
| **orchestrator ↔ CLI agent** | **CAP** | **rsclaw / cap-protocol.org** |

CAP composes with all three. A single agent may speak all four at
once: ACP to a local editor, A2A to remote peers, MCP for tools, and
CAP for fleet orchestration.

## Repository layout

```
cap-protocol/
├── docs/
│   ├── cap-v1.md                       Core specification
│   └── cap-profile-coding-v1.md        Coding profile (first vertical)
├── website/                            cap-protocol.org official site
│   ├── index.html
│   ├── style.css
│   ├── favicon.svg
│   └── README.md                       Local preview + deploy instructions
├── crates/                             Reference Rust implementation
│   ├── cap-rs/                         Library — all bindings behind feature flags
│   └── cap-cli/                        `cap` CLI binary
│                                       (sub-crate names cap-rs-core / cap-rs-pty
│                                        / cap-rs-stream-json / cap-rs-acp /
│                                        cap-rs-a2a / cap-rs-grpc /
│                                        cap-rs-orchestrator are reserved on
│                                        crates.io at v0.0.0 for possible
│                                        future split — see crates/cap-rs/README.md)
├── examples/                           Reference manifests
│   ├── claude-code.toml
│   ├── aider.toml
│   ├── codex.toml
│   ├── opencode.toml
│   └── openclaude.toml
├── README.md                           You are here
├── LICENSE-MIT                         For all source code
└── LICENSE-CC-BY-4.0                   For all specification text
```

The reference implementation is **functional**: `cap-rs` ships drivers
for stream-json, PTY, ACP, A2A, and gRPC; `cap-rs-orchestrator` runs
multi-agent fleets; and the `cap` CLI exposes `validate`, `list-drivers`,
`init`, `manifest`, `chat`, and `run`. The spec remains the deliverable
that matters for v1 — the implementation is the proof it works.

## Status

**Current**: `draft-2026-05-25` · v1 · public review

| Milestone | Target | Status |
|---|---|---|
| v1 draft published | 2026-05-18 | ✅ done |
| Public review window | ~6 weeks | 🟡 open |
| Reference impl: PTY driver + Claude Code manifest | 2026-06 | ✅ done |
| Reference impl: Multi-agent orchestrator via `fleet.yaml` | 2026-06 | ✅ done |
| Remote transport: Tailscale + push-based approval | 2026-07 | 🟡 in progress |
| Mobile approval app | 2026-Q3 | ⚪ planned |
| First non-rsclaw implementation | when it happens | ⚪ |
| Stable v1 (no breaking changes for 6 months) | 2026-Q4 | ⚪ |
| Linux Foundation proposal (if applicable) | when criteria met | ⚪ |

See the [website timeline](https://cap-protocol.org#status) for the
public-facing roadmap.

## Quick start (for the curious)

The spec is human-readable and complete. Start here:

1. **Skim the [hero on cap-protocol.org](https://cap-protocol.org)** —
   30-second pitch + protocol stack diagram.
2. **Read [docs/cap-v1.md](docs/cap-v1.md)** — core spec, ~900 lines.
   Sections you'll want first:
   - §2 Position (vs MCP/A2A/ACP)
   - §5 Agent Manifest (TOML schema with example)
   - §6 Transport bindings (PTY + 4 fast-paths)
   - §10 Multi-agent orchestration
   - §11 A2A interoperability
3. **Then [docs/cap-profile-coding-v1.md](docs/cap-profile-coding-v1.md)**
   if you care about the coding vertical specifically.

If you're building a CLI agent and want it driveable by CAP:
write a [Manifest](docs/cap-v1.md#5-agent-manifest) for it. That's
the minimum.

**Want to run it?** The reference `cap` CLI works today —
see [docs/quickstart.md](docs/quickstart.md):

```bash
cargo build --package cap-cli
./target/debug/cap list-drivers
./target/debug/cap chat --driver claude --task "Write hello world in Rust"
```

## Contributing

CAP is in the **public review** phase. Useful contributions right now:

| If you want to… | Open a… |
|---|---|
| Suggest a wording / clarity change | PR against `docs/` |
| Question a design decision | Issue tagged `discussion` |
| Propose a new profile (devops / data / security / …) | Issue tagged `profile-proposal` |
| Report a gap in an existing binding | Issue tagged `binding:<name>` |
| Submit a reference Manifest for an existing CLI agent | PR adding `examples/<agent>.toml` |
| Implement a reference driver in a non-Rust language | Talk to us first via discussion |

**For substantive changes**, please open an issue before sending a
large PR — we'd rather align on direction than ask you to rewrite.

## Governance

CAP is **currently maintained by [rsclaw][rsclaw-site]** as the
originating author. The intent is to transition to neutral
governance — most likely under the [Linux
Foundation](https://www.linuxfoundation.org/) or
[Joint Development Foundation](https://www.jointdevelopment.org/) —
once **any of** the following are true:

- **3 or more independent (non-rsclaw) implementations exist**, OR
- **the spec text has been stable for 6+ months** without breaking
  changes, OR
- **community contributors have produced ≥ 30% of merged spec PRs**.

The neutral GitHub organisation is reserved at
[`github.com/cap-protocol`](https://github.com/cap-protocol). The
authoritative URL <https://cap-protocol.org> will remain stable
across any future repository transfer.

### Commitments

While rsclaw stewards CAP we commit to:

- **No patent encumbrance.** rsclaw does not hold, and will not
  pursue, patents on the protocol design.
- **Open spec under [CC BY 4.0](LICENSE-CC-BY-4.0).** Anyone may
  fork, redistribute, and reuse the spec text with attribution.
- **Open reference code under [MIT](LICENSE-MIT).** Anyone may use,
  modify, and redistribute the reference implementations.
- **Public review of breaking changes.** Any change that breaks
  conformance with an earlier published draft will be flagged in
  the changelog and given a minimum 2-week public review window
  before merge.
- **Outside contributions reviewed on merit, not affiliation.**

## Authors

CAP is created and maintained by the rsclaw team. See
<https://rsclaw.ai> for the parent project — the open-source agent
fleet that uses CAP as its native orchestration layer.

A reference implementation **using** CAP (driving Claude Code, Codex,
Opencode, aider, openclaude and other CLI agents at fleet scale)
ships as part of rsclaw — making rsclaw simultaneously the protocol's
author, its first major implementer, and its real-world stress test.

## License

CAP is dual-licensed by content type:

| Content | License |
|---|---|
| Specification text in `docs/` | [CC BY 4.0](LICENSE-CC-BY-4.0) |
| Website content in `website/` | [CC BY 4.0](LICENSE-CC-BY-4.0) |
| Source code in `crates/`, examples, and tooling | [MIT](LICENSE-MIT) |

This dual-license pattern matches LSP, DAP, A2A, and MCP: open spec
+ permissive code, with attribution preserved.

When in doubt, attribute the work to:
*"CAP — CLI Agent Protocol, cap-protocol.org, originally authored by rsclaw."*
