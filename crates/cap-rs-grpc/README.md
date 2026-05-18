# cap-rs-grpc

gRPC fast-path driver for the CAP (CLI Agent Protocol). For openclaude-style agents.

> 🚧 **Status**: Placeholder — implementation in progress.

CAP (CLI Agent Protocol) is an open protocol for discovering, driving,
and orchestrating any command-line AI agent.

- Homepage: <https://cap-protocol.org>
- Specification: <https://github.com/rsclaw-ai/cap-protocol/blob/main/docs/cap-v1.md>
- Repository: <https://github.com/rsclaw-ai/cap-protocol>

## Status

This crate currently exposes only build-time constants (`CRATE_NAME`,
`CRATE_VERSION`, `PROTOCOL_VERSION`). Real types and behaviour will
land as the protocol matures past its `draft-2026-05-18` v1 milestone.

## License

MIT. See [LICENSE-MIT](https://github.com/rsclaw-ai/cap-protocol/blob/main/LICENSE-MIT).
