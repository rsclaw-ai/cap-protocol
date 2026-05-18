//! Driver backends — concrete implementations that drive a CLI agent.
//!
//! Each driver is gated behind a feature flag. The cross-driver API is the
//! [`Driver`] trait; all drivers emit the same [`crate::core::AgentEvent`]
//! stream regardless of wire format.

#[cfg(feature = "stream-json")]
pub mod stream_json;

#[cfg(feature = "pty")]
pub mod pty;

#[cfg(feature = "codex")]
pub mod codex;

// Future modules — gated on their respective features:
// #[cfg(feature = "acp")]          pub mod acp;
// #[cfg(feature = "a2a")]          pub mod a2a;
// #[cfg(feature = "grpc")]         pub mod grpc;
// #[cfg(feature = "orchestrator")] pub mod orchestrator;

// The Driver trait and DriverError are shared across all driver backends.
// They're gated on `any(stream-json, pty, ...)` because their deps
// (async-trait, thiserror) come in via those features.

#[cfg(any(feature = "stream-json", feature = "pty", feature = "codex"))]
mod common {
    use crate::core::{AgentEvent, ClientFrame};

    /// A unified driver interface. Concrete drivers translate this to their
    /// underlying wire format (PTY, stream-json, gRPC, ACP-stdio, A2A SSE).
    #[async_trait::async_trait]
    pub trait Driver: Send {
        /// Send a frame to the agent. The frame is processed asynchronously;
        /// resulting events arrive via [`Driver::next_event`].
        async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError>;

        /// Await the next event from the agent. Returns `None` when the
        /// agent has exited cleanly.
        async fn next_event(&mut self) -> Option<AgentEvent>;

        /// Shut down the agent process and release resources.
        async fn shutdown(&mut self) -> Result<(), DriverError>;
    }

    /// Driver-level errors.
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
        Parse(String),

        #[error("agent reported error: {code} — {message}")]
        AgentError { code: String, message: String },
    }

    #[cfg(feature = "stream-json")]
    impl From<serde_json::Error> for DriverError {
        fn from(e: serde_json::Error) -> Self {
            DriverError::Parse(e.to_string())
        }
    }
}

#[cfg(any(feature = "stream-json", feature = "pty", feature = "codex"))]
pub use common::{Driver, DriverError};
