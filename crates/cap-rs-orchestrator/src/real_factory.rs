//! Builds real `cap-rs` drivers. `claude` rides the structured `stream-json`
//! fast-path; `codex` and `pty:<cmd>` agents (opencode, …) ride a PTY with the
//! [`TuiParser`] turn-completion heuristic (idle-settle + ready-marker).

use std::path::Path;

use async_trait::async_trait;
use cap_rs::driver::Driver;
use cap_rs::driver::acp::AcpDriver;
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
            // codex: interactive TUI under a PTY (NOT `codex exec` — PTY gives
            // multi-turn and dodges the app-server websocket blocker). Bypass
            // maps to codex's native skip-all-prompts flag.
            DriverKind::Codex => {
                let mut builder = PtyDriver::builder("codex").cwd(cwd);
                if bypass {
                    builder = builder.arg("--dangerously-bypass-approvals-and-sandbox");
                }
                let driver = builder.spawn(TuiParser::codex())?;
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
            // pty:<cmd> — any other interactive CLI agent. opencode gets a
            // tuned parser; unknown commands fall back to the generic TUI
            // parser. opencode has no CLI bypass flag (permissions are
            // config-driven), so `bypass` is a no-op for the PTY path here.
            DriverKind::Pty(cmd) => {
                let parser = if cmd.as_str() == "opencode" {
                    TuiParser::opencode()
                } else {
                    TuiParser::generic()
                };
                let driver = PtyDriver::builder(cmd.clone()).cwd(cwd).spawn(parser)?;
                Ok(Box::new(driver))
            }
        }
    }
}
