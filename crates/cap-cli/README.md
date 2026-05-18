# cap-cli

`cap` — command-line tool for discovering, driving, and orchestrating
CLI AI agents.

> 🚧 **Status: placeholder.** This crate is published to crates.io purely
> to reserve the `cap` binary name. The binary you get from
> `cargo install cap-cli` does **nothing useful** — it prints a notice
> and exits. The real CLI is being built; track progress in the spec
> repo before installing.

CAP (CLI Agent Protocol) is an open protocol for discovering, driving,
and orchestrating any command-line AI agent.

- Homepage: <https://cap-protocol.org>
- Specification: <https://github.com/rsclaw-ai/cap-protocol/blob/main/docs/cap-v1.md>
- Repository: <https://github.com/rsclaw-ai/cap-protocol>
- Rust library (functional today): [`cap-rs`](https://crates.io/crates/cap-rs)

## What this is NOT yet

- A working CLI: it is not.
- A working library: depend on [`cap-rs`](https://crates.io/crates/cap-rs)
  for the Rust SDK that already works.

## What this WILL be

A standalone binary that:

- Discovers locally-installed CAP-conformant agents via Manifests.
- Drives any of them with a uniform `cap run <agent> "<prompt>"` UX.
- Orchestrates multiple sub-agents for plan-driven work
  (spec §10 multi-agent orchestration).
- Bridges between PTY / stream-json / ACP / A2A bindings transparently.

Implementation lands as the protocol matures past its `draft-2026-05-18`
v1 milestone.

## License

MIT. See [LICENSE-MIT](https://github.com/rsclaw-ai/cap-protocol/blob/main/LICENSE-MIT).
