//! Builds real `cap-rs` drivers. v1 wires `claude` only; `codex`/`opencode`
//! (PTY) are deferred until the PTY turn-completion detection lands.

use std::path::Path;

use async_trait::async_trait;
use cap_rs::driver::Driver;
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
            // Deferred: codex + opencode ride the PTY path, which needs a
            // turn-completion heuristic not yet built. Fail loudly rather than
            // half-work.
            DriverKind::Codex | DriverKind::Pty(_) => {
                Err(OrchestratorError::UnknownDriver(format!(
                    "{kind:?} is not wired yet — this build supports `claude` only \
                 (codex/opencode via PTY are a follow-up: pty turn detection)"
                )))
            }
        }
    }
}
