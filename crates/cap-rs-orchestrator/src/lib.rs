//! cap-rs-orchestrator — headless engine that runs N collaborating CLI agents
//! in one process, driven by a declarative `fleet.yaml`.
//!
//! See `docs/cap-orchestrator-engine-design.md`.
#![warn(missing_debug_implementations)]

pub mod config;
pub mod event;
pub mod session;
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
    #[error("unknown driver kind: {0}")]
    UnknownDriver(String),
}
