//! Builds real `cap-rs` drivers. Each first-class agent name maps to its
//! highest-fidelity structured path:
//! - `claude` → `stream-json`
//! - `openclaude` → `stream-json` (Anthropic SDK-compatible)
//! - `opencode` → `stream-json` (Claude Code-compatible NDJSON)
//! - `codex` → `codex mcp-server` (stdio MCP)
//! - `acp:<cmd>` → ACP over stdio
//!
//! `pty:<cmd>` remains the universal screen-scraping fallback; `pty:codex`
//! still works (with the codex-tuned [`TuiParser::codex`]) if a caller needs
//! the old behavior. `pty:openclaude` uses a tuned parser with `>` prompt
//! markers from the reference manifest.
//!
//! `grpc:<addr>` is the alternative gRPC path with reduced event detail.

use std::path::Path;

use async_trait::async_trait;
use cap_rs::driver::Driver;
use cap_rs::driver::a2a::A2aDriver;
use cap_rs::driver::acp::AcpDriver;
use cap_rs::driver::codex_mcp::CodexMcpDriver;
use cap_rs::driver::grpc::GrpcDriver;
use cap_rs::driver::pty::{PtyDriver, TuiParser};
use cap_rs::driver::stream_json::ClaudeCodeDriver;

use crate::OrchestratorError;
use crate::config::{DriverKind, PermissionPolicy, SessionId};
use crate::factory::DriverFactory;

#[derive(Debug, Default)]
pub struct RealDriverFactory;

#[async_trait]
impl DriverFactory for RealDriverFactory {
    async fn build(
        &self,
        _session: &SessionId,
        kind: &DriverKind,
        cwd: &Path,
        policy: PermissionPolicy,
    ) -> Result<Box<dyn Driver>, OrchestratorError> {
        let bypass = policy == PermissionPolicy::Bypass;
        match kind {
            DriverKind::Claude => {
                let driver = ClaudeCodeDriver::builder(cwd)
                    .dangerously_skip_permissions(bypass)
                    .spawn()
                    .await?;
                Ok(Box::new(driver))
            }
            // openclaude: stream-json fast-path (Anthropic SDK-compatible).
            // Higher fidelity than gRPC: per-token streaming, structured
            // tool/permission events, full usage stats. Uses the same
            // ClaudeCodeDriver with `bin("openclaude")`.
            DriverKind::OpenClaude => {
                let driver = ClaudeCodeDriver::builder(cwd)
                    .bin("openclaude")
                    .dangerously_skip_permissions(bypass)
                    .spawn()
                    .await?;
                Ok(Box::new(driver))
            }
            // opencode: stream-json fast-path (Claude Code-compatible NDJSON).
            // Same fidelity as claude/openclaude: per-token streaming deltas,
            // structured tool/permission events, usage stats. Uses
            // `opencode run --output-format stream-json` with stdin prompt
            // delivery — the existing CAP send() flow works unchanged.
            // `acp:opencode` remains available as an alternative path.
            DriverKind::OpenCode => {
                let driver = ClaudeCodeDriver::opencode_builder(cwd).spawn().await?;
                Ok(Box::new(driver))
            }
            // codex: stdio MCP server (`codex mcp-server`). Structured streaming
            // — codex/event notifications mid-turn, clean structuredContent on
            // the tools/call response — no TUI chrome, no idle heuristics. Map
            // CAP's permission policy onto codex's approval-policy + sandbox.
            // The old PTY codex (with the tuned TuiParser) is still available
            // as `pty:codex`.
            DriverKind::Codex => {
                let (approval, sandbox) = match policy {
                    PermissionPolicy::Bypass => ("never", "danger-full-access"),
                    PermissionPolicy::Allow => ("never", "workspace-write"),
                    PermissionPolicy::Deny => ("never", "read-only"),
                    PermissionPolicy::Ask => ("on-request", "workspace-write"),
                };
                let driver = CodexMcpDriver::builder(cwd)
                    .approval_policy(approval)
                    .sandbox(sandbox)
                    .spawn()
                    .await?;
                Ok(Box::new(driver))
            }
            // a2a:<url> — remote A2A HTTPS+SSE CAP-compatible agent.
            // Permission policy is enforced by the remote peer.
            DriverKind::A2a(endpoint) => {
                let driver = A2aDriver::connect(endpoint.clone()).await?;
                Ok(Box::new(driver))
            }
            // grpc:<addr> — OpenClaude gRPC server (openclaude grpc).
            // Connects to a running openclaude gRPC server at the given address.
            // Permission policy is not passed to the gRPC server — it handles
            // its own permission prompts via the ActionRequired protocol.
            DriverKind::Grpc(addr) => {
                let driver = GrpcDriver::connect(addr).await?;
                Ok(Box::new(driver))
            }
            // acp:<cmd> — structured Agent Client Protocol agent (opencode,
            // …). Far higher fidelity than PTY: real streaming + structured
            // tool/permission events. Permission gating rides CAP's normal
            // PermissionRequest flow (so `bypass`/`allow` is honored by the
            // session actor), no agent-specific flag needed.
            DriverKind::Acp(cmd) => {
                let driver = if cmd.as_str() == "opencode" {
                    AcpDriver::opencode(cwd).await?
                } else {
                    AcpDriver::builder(cmd.clone(), cwd).spawn().await?
                };
                Ok(Box::new(driver))
            }
            // pty:<cmd> — universal interactive-CLI fallback. Known agents
            // get a tuned parser (codex's `›`, opencode's `❯`); unknown
            // commands fall back to the generic TUI parser. For codex via PTY
            // we still want bypass to pass the skip-all-prompts flag.
            DriverKind::Aider => {
                let driver = PtyDriver::builder("aider")
                    .cwd(cwd)
                    .spawn(TuiParser::aider())?;
                Ok(Box::new(driver))
            }
            DriverKind::Pty(cmd) => {
                let mut builder = PtyDriver::builder(cmd.clone()).cwd(cwd);
                if cmd.as_str() == "codex" && bypass {
                    builder = builder.arg("--dangerously-bypass-approvals-and-sandbox");
                }
                let parser = match cmd.as_str() {
                    "codex" => TuiParser::codex(),
                    "opencode" => TuiParser::opencode(),
                    "openclaude" => TuiParser::openclaude(),
                    _ => TuiParser::generic(),
                };
                let driver = builder.spawn(parser)?;
                Ok(Box::new(driver))
            }
        }
    }
}
