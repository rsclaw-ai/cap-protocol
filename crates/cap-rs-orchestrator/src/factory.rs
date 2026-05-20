//! Constructs a concrete `Driver` for a session. Behind a trait so tests use
//! scripted stubs and the engine uses real CLI agents.

use std::path::Path;

use async_trait::async_trait;
use cap_rs::driver::Driver;

use crate::config::{DriverKind, PermissionPolicy, SessionId};
use crate::OrchestratorError;

#[async_trait]
pub trait DriverFactory: Send + Sync {
    /// Build a driver for `session` running in `cwd`. `policy` lets the factory
    /// pass each agent's native bypass flag when `policy == Bypass`.
    async fn build(
        &self,
        session: &SessionId,
        kind: &DriverKind,
        cwd: &Path,
        policy: PermissionPolicy,
    ) -> Result<Box<dyn Driver>, OrchestratorError>;
}
