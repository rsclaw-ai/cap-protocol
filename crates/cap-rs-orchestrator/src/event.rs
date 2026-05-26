//! Types crossing the engine↔consumer boundary. This boundary is an in-process
//! `mpsc` channel today and the seam for the future remote (WebSocket) layer.

use cap_rs::core::{AgentEvent, ReverseRpc, RiskLevel, StopReason};

use crate::config::SessionId;

/// Everything the engine emits outward, tagged by session where applicable.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum OrchestratorEvent {
    SessionStarted {
        session: SessionId,
    },
    /// A raw agent event, tagged with its originating session.
    Agent {
        session: SessionId,
        event: AgentEvent,
    },
    /// A permission request awaiting a human decision (only under `ask` policy).
    Ask {
        session: SessionId,
        req_id: String,
        tool: String,
        risk_level: RiskLevel,
    },
    /// A Reverse RPC call from the agent to the orchestrator (§8).
    ReverseRpc {
        session: SessionId,
        rpc_id: String,
        rpc: ReverseRpc,
    },
    /// The engine routed one session's output into another's inbox.
    Routed {
        from: SessionId,
        to: SessionId,
    },
    SessionDone {
        session: SessionId,
        stop_reason: StopReason,
    },
    SessionFailed {
        session: SessionId,
        error: String,
    },
    /// A `collect: human` join completed; these candidate sessions await a pick.
    AwaitSelection {
        candidates: Vec<SessionId>,
    },
    FleetComplete,
}

/// Everything the consumer sends back in (decisions, selections, cancel).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum OrchestratorControl {
    /// Answer to an [`OrchestratorEvent::Ask`].
    Decision {
        session: SessionId,
        req_id: String,
        allow: bool,
    },
    /// Answer to an [`OrchestratorEvent::ReverseRpc`].
    ReverseRpcResult {
        session: SessionId,
        rpc_id: String,
        result: cap_rs::core::ReverseRpcResult,
    },
    /// Answer to an [`OrchestratorEvent::AwaitSelection`].
    Select { session: SessionId },
    /// Inject a user message into a running session's inbox.
    UserMessage {
        session: SessionId,
        text: String,
    },
    /// Hard-cancel the whole fleet.
    Cancel,
}
