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

#[cfg(feature = "codex")]
pub mod codex_app_server;

#[cfg(feature = "codex")]
pub mod codex_mcp;

#[cfg(feature = "acp")]
pub mod acp;

#[cfg(feature = "grpc")]
pub mod grpc;

// Future modules — gated on their respective features:
// #[cfg(feature = "a2a")]          pub mod a2a;
// #[cfg(feature = "orchestrator")] pub mod orchestrator;

// The Driver trait and DriverError are shared across all driver backends.
// They're gated on `any(stream-json, pty, ...)` because their deps
// (async-trait, thiserror) come in via those features.

#[cfg(any(feature = "stream-json", feature = "pty", feature = "codex", feature = "acp", feature = "grpc"))]
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

        /// Whether the agent process is still running. Drivers SHOULD
        /// return `false` after the underlying process exits, regardless of
        /// whether `shutdown` was explicitly called.
        ///
        /// Default returns `true` for drivers that don't track liveness —
        /// callers should not rely on this default to mean "alive".
        fn is_alive(&self) -> bool {
            true
        }

        /// Terminal exit status, if the agent has exited. `None` means
        /// either the agent is still running, or the driver does not
        /// surface exit codes (e.g. remote A2A bindings).
        fn exit_status(&self) -> Option<DriverExitStatus> {
            None
        }

        /// Whether the caller must wait for an [`AgentEvent::Ready`] before
        /// sending the first [`ClientFrame::Prompt`]. Default `false`: a
        /// structured agent (claude stream-json) accepts a prompt the moment
        /// it spawns. PTY/TUI agents (codex, opencode) need ~seconds to boot
        /// to their input prompt — a prompt sent earlier is typed into a
        /// not-ready terminal and lost. Such drivers return `true`.
        fn prompt_after_ready(&self) -> bool {
            false
        }
    }

    /// How an agent session terminated. Roughly mirrors `std::process::ExitStatus`
    /// but is binding-agnostic so remote / non-process drivers (A2A) can use it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    #[non_exhaustive]
    pub enum DriverExitStatus {
        /// Process exited on its own. `code = None` when killed by signal
        /// on Unix without a numeric code.
        Exited { code: Option<i32> },
        /// Driver called shutdown / sent SIGKILL.
        Killed,
        /// Channel closed without an observable exit (network drop, pipe error).
        Disconnected,
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

#[cfg(any(feature = "stream-json", feature = "pty", feature = "codex", feature = "acp", feature = "grpc"))]
pub use common::{Driver, DriverError, DriverExitStatus};
