//! Core protocol types — events, frames, content blocks, usage.
//!
//! These types are defined in the spec at <https://github.com/rsclaw-ai/cap-protocol/blob/main/docs/cap-v1.md>
//! sections §7 (events) and §10 (lifecycle).
//!
//! All enums are marked `#[non_exhaustive]` during v0.x so new variants can
//! be added without a semver-breaking change.

use std::time::Duration;

// ---------------------------------------------------------------------------
// ClientFrame — what the orchestrator sends to the agent.
// ---------------------------------------------------------------------------

/// A frame sent by the client (orchestrator) to the agent during a session.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ClientFrame {
    /// A user prompt with one or more content parts.
    Prompt { content: Vec<Content> },

    /// An answer to a previously emitted [`AgentEvent::AskUser`].
    AskUserAnswer { ask_id: String, value: serde_json::Value },

    /// An answer to a previously emitted [`AgentEvent::PermissionRequest`].
    PermissionResponse { req_id: String, decision: PermissionDecision },

    /// Cancel the current turn (graceful).
    Cancel,
}

/// A content part of a user prompt.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Content {
    Text(String),
    Image { mime: String, data: Vec<u8> },
    // File / ToolResult etc. land later
}

// ---------------------------------------------------------------------------
// AgentEvent — what the agent streams back to the client.
// ---------------------------------------------------------------------------

/// A streaming event from the agent. Subset of spec §7.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AgentEvent {
    /// Session has been initialized and is ready to accept prompts.
    Ready { session_id: String, model: Option<String> },

    /// Streaming text chunk from the assistant.
    TextChunk { msg_id: String, text: String, channel: TextChannel },

    /// Agent's internal reasoning (when exposed by the model).
    Thought { msg_id: String, text: String },

    /// Agent started a tool call.
    ToolCallStart { call_id: String, name: String, input: serde_json::Value },

    /// Agent's tool call returned a result.
    ToolCallEnd { call_id: String, output: String, is_error: bool },

    /// Agent published an updated plan (full state, replaces previous).
    Plan { entries: Vec<PlanEntry> },

    /// Agent asks the user a structured question.
    AskUser { ask_id: String, prompt: String, kind: AskKind, options: Vec<AskOption> },

    /// Agent requests permission for a security-sensitive action.
    PermissionRequest {
        req_id: String,
        tool: String,
        intent: serde_json::Value,
        scope: PermissionScope,
    },

    /// Turn complete. Usage stats included.
    Done { stop_reason: StopReason, usage: Usage },

    /// Non-fatal error during the turn.
    Error { code: String, message: String },
}

/// Channel hint for [`AgentEvent::TextChunk`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TextChannel {
    Assistant,
    System,
}

/// Plan entry from spec §7.3.
#[derive(Debug, Clone)]
pub struct PlanEntry {
    pub id: String,
    pub content: String,
    pub status: PlanStatus,
    pub priority: PlanPriority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PlanStatus { Pending, InProgress, Completed, Cancelled, Blocked }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PlanPriority { High, Medium, Low }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AskKind { YesNo, Options, FreeText }

#[derive(Debug, Clone)]
pub struct AskOption {
    pub id: String,
    pub label: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PermissionScope { Read, Write, Execute, Network }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PermissionDecision { AllowOnce, AllowAlways, Deny }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StopReason {
    EndTurn,
    MaxTokens,
    ToolUse,
    StopSequence,
    Cancelled,
    Error,
}

/// Token / cost accounting for a completed turn.
#[derive(Debug, Clone, Default)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub cost_usd_estimate: Option<f64>,
    pub duration: Option<Duration>,
    pub model_id: Option<String>,
}
