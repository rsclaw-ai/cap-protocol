//! cap-rs-orchestrator — headless engine that runs N collaborating CLI agents
//! in one process, driven by a declarative `fleet.yaml`.
//!
//! See `docs/cap-orchestrator-engine-design.md`.
#![warn(missing_debug_implementations)]

pub mod audit;
pub mod config;
pub mod event;
pub mod executor;
pub mod factory;
pub mod real_factory;
pub mod registry;
pub mod session;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod worktree;

/// Errors surfaced by the orchestrator engine.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OrchestratorError {
    #[error("config error: {0}")]
    Config(String),
    #[error("worktree error: {0}")]
    Worktree(String),
    #[error("driver error: {0}")]
    Driver(#[from] cap_rs::driver::DriverError),
}

use crate::config::FleetSpec;
use crate::event::OrchestratorEvent;
use crate::executor::{Executor, ExecutorHandle};
use crate::real_factory::RealDriverFactory;
use crate::worktree::GitWorktreeManager;

/// Convenience façade: run a fleet against real CLI agents in `repo`.
pub async fn run(
    spec: FleetSpec,
    repo: impl AsRef<std::path::Path>,
    task: &str,
) -> Result<
    (
        ExecutorHandle,
        tokio::sync::mpsc::Receiver<OrchestratorEvent>,
    ),
    OrchestratorError,
> {
    let worktree = GitWorktreeManager::new(repo);
    Executor::start(spec, RealDriverFactory, worktree, task).await
}
