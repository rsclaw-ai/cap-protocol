# Contributing to CAP

CAP is in public review. All contributions welcome.

## Getting started

```shell
cargo build
cargo test --all-features
cargo clippy --all-features --all-targets -- -D warnings
cargo fmt --all -- --check
```

## What to work on

| Area | Where |
|---|---|
| Spec text | `docs/cap-v1.md` |
| Rust implementation | `crates/cap-rs/`, `crates/cap-rs-orchestrator/`, `crates/cap-cli/` |
| Website | `website/` |
| Agent manifests | `examples/*.toml` |

## Before opening a PR

1. Check existing issues/discussions for alignment.
2. For substantive changes, open an issue first.
3. Ensure `cargo test --all-features` passes.
4. Format with `cargo fmt`.

## Spec changes

Spec text lives in `docs/` under CC BY 4.0. Markdown with GFM tables.

## Code changes

Code lives in `crates/` under MIT. Rust 2024 edition, tokio async runtime.
