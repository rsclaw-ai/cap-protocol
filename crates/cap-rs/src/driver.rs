//! Driver backends — concrete implementations that drive a CLI agent.
//!
//! Each driver is gated behind a feature flag. The cross-driver API is the
//! [`Driver`] trait; all drivers emit the same [`crate::core::AgentEvent`]
//! stream regardless of wire format.

use crate::core::{AgentEvent, ClientFrame};

#[cfg(feature = "stream-json")]
pub mod stream_json;

// Future modules — gated on their respective features:
// #[cfg(feature = "pty")]          pub mod pty;
// #[cfg(feature = "acp")]          pub mod acp;
// #[cfg(feature = "a2a")]          pub mod a2a;
// #[cfg(feature = "grpc")]         pub mod grpc;
// #[cfg(feature = "orchestrator")] pub mod orchestrator;

/// A unified driver interface. Concrete drivers translate this to their
/// underlying wire format (PTY, stream-json, gRPC, ACP-stdio, A2A SSE).
#[cfg(feature = "stream-json")]
#[async_trait::async_trait]
pub trait Driver: Send {
    /// Send a frame to the agent. The frame is processed asynchronously;
    /// resulting events arrive via [`Driver::next_event`].
    async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError>;

    /// Await the next event from the agent. Returns `None` when the agent
    /// has exited cleanly.
    async fn next_event(&mut self) -> Option<AgentEvent>;

    /// Shut down the agent process and release resources.
    async fn shutdown(&mut self) -> Result<(), DriverError>;
}

/// Driver-level errors.
#[cfg(feature = "stream-json")]
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DriverError {
    #[error("agent binary not found on PATH: {0}")]
    BinaryNotFound(String),

    #[error("failed to spawn agent process: {0}")]
    SpawnFailed(#[source] std::io::Error),

    #[error("agent process exited unexpectedly")]
    AgentExited,

    #[error("io error while talking to agent: {0}")]
    Io(#[from] std::io::Error),

    #[error("failed to parse agent output: {0}")]
    Parse(#[source] serde_json::Error),

    #[error("agent reported error: {code} — {message}")]
    AgentError { code: String, message: String },
}
