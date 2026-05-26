//! Core protocol types — events, frames, content blocks, usage.
//!
//! These types are defined in the spec at <https://github.com/rsclaw-ai/cap-protocol/blob/main/docs/cap-v1.md>
//! sections §7 (events) and §10 (lifecycle).
//!
//! All enums are marked `#[non_exhaustive]` during v0.x so new variants can
//! be added without a semver-breaking change.
//!
//! ## Wire format
//!
//! Both [`ClientFrame`] and [`AgentEvent`] derive `Serialize` and
//! `Deserialize`, mapped onto the spec's `kind` field with snake_case
//! variant names matching §7 (e.g. `cap.text_chunk`,
//! `cap.permission.request`). Round-tripping a value through JSON is
//! lossless except for [`Content::Image`] bytes, which are base64-encoded
//! on the wire.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// ClientFrame — what the orchestrator sends to the agent.
// ---------------------------------------------------------------------------

/// A frame sent by the client (orchestrator) to the agent during a session.
///
/// Spec mapping: `cap.session.config`, `cap.user_input.inject`,
/// `cap.ask_user.answer`, `cap.permission.response`, `cap.cancel`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "kind")]
pub enum ClientFrame {
    /// Session bootstrap. MUST be the first frame an Orchestrator sends to
    /// a Driver per spec §7.10. Drivers that accept config via builder
    /// arguments treat this as a no-op when the session is already running.
    #[serde(rename = "cap.session.config")]
    SessionConfig(SessionConfig),

    /// A user prompt — wire equivalent of `cap.user_input.inject` (spec §7.7).
    #[serde(rename = "cap.user_input.inject")]
    Prompt { content: Vec<Content> },

    /// An answer to a previously emitted [`AgentEvent::AskUser`].
    #[serde(rename = "cap.ask_user.answer")]
    AskUserAnswer {
        ask_id: String,
        value: serde_json::Value,
    },

    /// An answer to a previously emitted [`AgentEvent::PermissionRequest`].
    #[serde(rename = "cap.permission.response")]
    PermissionResponse {
        req_id: String,
        decision: PermissionDecision,
    },

    /// Cancel the current turn (spec §7.8).
    #[serde(rename = "cap.cancel")]
    Cancel {
        scope: CancelScope,
        reason: Option<String>,
    },

    /// Response to a previously emitted [`AgentEvent::ReverseRpc`].
    /// Carries the JSON value the Reverse RPC method returned.
    #[serde(rename = "cap.reverse_rpc.result")]
    ReverseRpcResult {
        rpc_id: String,
        #[serde(flatten)]
        result: ReverseRpcResult,
    },
}

/// Scope of a [`ClientFrame::Cancel`] (spec §7.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum CancelScope {
    CurrentTurn,
    Session,
}

/// Session bootstrap payload — spec §7.10 `cap.session.config`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionConfig {
    /// Working directory the agent should operate in. REQUIRED.
    pub cwd: std::path::PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turns: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_usd: Option<f64>,
    #[serde(default)]
    pub permission_mode: PermissionMode,
    /// Optional persisted-session UUID to resume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_resume_id: Option<String>,
    /// Profile-specific opaque config (e.g. `profile/coding` → tools_allowed).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub profile_config: BTreeMap<String, serde_json::Value>,
}

impl SessionConfig {
    pub fn new(cwd: impl Into<std::path::PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            ..Default::default()
        }
    }
}

/// Permission negotiation mode declared by the Orchestrator (spec §7.10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum PermissionMode {
    /// No prompting; agent may not request permission (all auto-denied if it does).
    None,
    /// Allow-list driven; orchestrator may prompt for unknowns.
    Confirm,
    /// Always surface permission requests to the user.
    #[default]
    Interactive,
}

/// A content part of a user prompt.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Content {
    Text {
        text: String,
    },
    /// Binary image data. Stored as `Arc<[u8]>` so cloning a [`ClientFrame`]
    /// is cheap even with large images. Serialized on the wire as base64.
    Image {
        mime: String,
        #[serde(with = "arc_b64")]
        data: Arc<[u8]>,
    },
    // File / ToolResult etc. land later
}

impl Content {
    /// Ergonomic constructor preserving the old `Content::Text(String)`
    /// shorthand. Use as `Content::Text("hello".into())`-equivalent calls
    /// don't fit anymore now that the variant has a named field — call
    /// `Content::text("hello")` instead.
    pub fn text(s: impl Into<String>) -> Self {
        Content::Text { text: s.into() }
    }

    /// Convenience constructor for images that copies a byte slice into
    /// the `Arc` storage.
    pub fn image(mime: impl Into<String>, data: impl Into<Arc<[u8]>>) -> Self {
        Content::Image {
            mime: mime.into(),
            data: data.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// AgentEvent — what the agent streams back to the client.
// ---------------------------------------------------------------------------

/// Valid protocol version string used in [`AgentEvent::Ready`].
pub const CAP_PROTOCOL_VERSION: &str = "cap-protocol/v1";

fn default_version() -> String {
    CAP_PROTOCOL_VERSION.to_string()
}

/// A streaming event from the agent. Subset of spec §7.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "kind")]
pub enum AgentEvent {
    /// Session has been initialized and is ready to accept prompts.
    #[serde(rename = "cap.session.ready")]
    Ready {
        session_id: String,
        #[serde(default = "default_version")]
        version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
    },

    /// Streaming text chunk from the assistant (spec §7.1).
    #[serde(rename = "cap.text_chunk")]
    TextChunk {
        #[serde(rename = "message_id")]
        msg_id: String,
        text: String,
        channel: TextChannel,
    },

    /// Agent's internal reasoning (when exposed by the model). Equivalent
    /// to `cap.text_chunk` with `channel = "thought"`; kept as a separate
    /// variant for ergonomic match-arms.
    #[serde(rename = "cap.thought")]
    Thought {
        #[serde(rename = "message_id")]
        msg_id: String,
        text: String,
    },

    /// Agent started a tool call (spec §7.2 `cap.tool_call.start`).
    #[serde(rename = "cap.tool_call.start")]
    ToolCallStart {
        call_id: String,
        name: String,
        input: serde_json::Value,
    },

    /// Streaming chunk of a tool call's output (spec §7.2 `cap.tool_call.delta`).
    /// OPTIONAL — only emitted when the agent declares
    /// `capabilities.streaming_tool_output = true`.
    #[serde(rename = "cap.tool_call.delta")]
    ToolCallDelta {
        call_id: String,
        output_chunk: String,
    },

    /// Agent's tool call returned a result (spec §7.2 `cap.tool_call.end`).
    #[serde(rename = "cap.tool_call.end")]
    ToolCallEnd {
        call_id: String,
        output: String,
        is_error: bool,
        /// Tool execution duration, serialized as `duration_ms` per spec §7.2.
        #[serde(
            default,
            rename = "duration_ms",
            skip_serializing_if = "Option::is_none",
            with = "duration_ms_opt"
        )]
        duration: Option<Duration>,
    },

    /// Agent published an updated plan (full state, REPLACES previous —
    /// spec §7.3).
    #[serde(rename = "cap.plan")]
    Plan { entries: Vec<PlanEntry> },

    /// Agent asks the user a structured question.
    #[serde(rename = "cap.ask_user")]
    AskUser {
        ask_id: String,
        prompt: String,
        /// What kind of input is expected. Flattened so the variant's
        /// `ask_kind` field appears at the top level of the JSON event.
        #[serde(flatten)]
        ask_kind: AskKind,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        options: Vec<AskOption>,
        /// Max seconds to wait for an answer. `None` = no timeout (spec §7.5).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_seconds: Option<u64>,
    },

    /// Agent requests permission for a security-sensitive action (spec §7.6).
    /// Orchestrators MUST NOT auto-approve `risk_level = High` without explicit
    /// user policy.
    #[serde(rename = "cap.permission.request")]
    PermissionRequest {
        req_id: String,
        tool: String,
        intent: serde_json::Value,
        scope: PermissionScope,
        risk_level: RiskLevel,
    },

    /// Standalone usage progress event (spec §7.9 `cap.usage`). MAY be emitted
    /// mid-session; MUST be emitted on terminal `Done` if the agent's
    /// Manifest declares `cost.metered = true`.
    #[serde(rename = "cap.usage")]
    Usage {
        #[serde(flatten)]
        usage: Usage,
    },

    /// Turn complete. Carries the terminal usage snapshot for convenience —
    /// orchestrators that prefer the spec-pure form can listen to
    /// [`AgentEvent::Usage`] instead. Not a spec-defined event; uses the
    /// `cap.done` kind on the wire and is filtered out by spec-strict
    /// receivers.
    #[serde(rename = "cap.done")]
    Done {
        stop_reason: StopReason,
        #[serde(flatten)]
        usage: Usage,
    },

    /// Non-fatal error during the turn (spec §7.11).
    #[serde(rename = "cap.error")]
    Error {
        code: String,
        message: String,
        #[serde(default)]
        retryable: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<serde_json::Value>,
    },

    /// Raw PTY byte chunk for explicit terminal mirror subscribers (§7.12).
    #[serde(rename = "cap.pty.raw_bytes")]
    PtyRawBytes {
        #[serde(rename = "bytes_b64", with = "arc_b64")]
        bytes: Arc<[u8]>,
    },

    /// A Reverse RPC call from the agent to the orchestrator (§8).
    /// The orchestrator MUST respond with [`ClientFrame::ReverseRpcResult`].
    #[serde(rename = "cap.reverse_rpc")]
    ReverseRpc {
        /// Unique identifier for matching the response.
        rpc_id: String,
        #[serde(flatten)]
        rpc: ReverseRpc,
    },
}

/// Channel hint for [`AgentEvent::TextChunk`] — spec §7.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum TextChannel {
    Assistant,
    Thought,
    System,
}

/// Severity level for `cap.user_io.show` notifications.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum NotificationLevel {
    #[default]
    Info,
    Warn,
    Error,
}

/// Spec §8 Reverse RPC — a method the agent invokes against the orchestrator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum ReverseRpc {
    /// Show a notification to the human user (§8.1 `cap.user_io.show`).
    #[serde(rename = "cap.user_io.show")]
    UserIoShow {
        title: String,
        body: String,
        #[serde(default)]
        level: NotificationLevel,
    },
    /// Request free-form text input from the user (§8.2 `cap.user_io.input`).
    #[serde(rename = "cap.user_io.input")]
    UserIoInput {
        prompt: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<String>,
    },
    /// Emit a notification that bypasses the chat stream (§8.3 `cap.notify`).
    #[serde(rename = "cap.notify")]
    Notify {
        title: String,
        body: String,
        #[serde(default)]
        urgent: bool,
    },
}

/// Result of a handled Reverse RPC call.
///
/// Uses `#[serde(untagged)]` — variant order matters for deserialization.
/// `Success` is tried first (matches `{ "ok": bool }`); `TextResult` second
/// (matches `{ "text": "..." }`). Both fields are required in their respective
/// variants to avoid ambiguity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(untagged)]
pub enum ReverseRpcResult {
    /// Boolean acknowledgement (e.g. `cap.user_io.show` response).
    Success { ok: bool },
    /// Free-text response (e.g. `cap.user_io.input` result).
    TextResult { text: String },
}

/// One entry in an agent's published plan (spec §7.3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PlanEntry {
    pub id: String,
    pub content: String,
    pub status: PlanStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<PlanPriority>,
    /// Opaque key-value metadata per spec §7.0. Carries `assigned_to` and
    /// `depends_on` for multi-agent orchestration (§10.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub _meta: Option<std::collections::BTreeMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    Pending,
    InProgress,
    Completed,
    Cancelled,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum PlanPriority {
    Urgent,
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "ask_kind")]
pub enum AskKind {
    #[serde(rename = "yes_no")]
    YesNo,
    #[serde(rename = "options")]
    Options,
    #[serde(rename = "free_text")]
    FreeText,
    /// Escape hatch — JSON-Schema form per spec §7.5. Bindings that natively
    /// carry a form schema (ACP v2 elicitation, A2A DataPart) use this.
    /// On the wire the schema is the `form` field per spec §7.5.
    #[serde(rename = "schema")]
    Schema {
        #[serde(rename = "form")]
        schema: serde_json::Value,
    },
}

/// One selectable option in an `AskUser` prompt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AskOption {
    /// Machine-readable option identifier.
    pub id: String,
    /// Human-readable display label.
    pub label: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum PermissionScope {
    Read,
    Write,
    Execute,
    Network,
}

/// Spec §7.6 `risk_level`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    #[default]
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum PermissionDecision {
    AllowOnce,
    AllowAlways,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    MaxTokens,
    ToolUse,
    StopSequence,
    Cancelled,
    Error,
}

// ---------------------------------------------------------------------------
// Base64 helper (avoiding an extra dep) + serde adapter for Arc<[u8]> image data.
// ---------------------------------------------------------------------------

pub(crate) mod base64 {
    /// RFC 4648 standard alphabet, with padding.
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub fn encode(data: &[u8]) -> String {
        let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
        let mut i = 0;
        while i + 3 <= data.len() {
            let b = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8) | (data[i + 2] as u32);
            out.push(T[((b >> 18) & 63) as usize] as char);
            out.push(T[((b >> 12) & 63) as usize] as char);
            out.push(T[((b >> 6) & 63) as usize] as char);
            out.push(T[(b & 63) as usize] as char);
            i += 3;
        }
        let rem = data.len() - i;
        if rem == 1 {
            let b = (data[i] as u32) << 16;
            out.push(T[((b >> 18) & 63) as usize] as char);
            out.push(T[((b >> 12) & 63) as usize] as char);
            out.push_str("==");
        } else if rem == 2 {
            let b = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8);
            out.push(T[((b >> 18) & 63) as usize] as char);
            out.push(T[((b >> 12) & 63) as usize] as char);
            out.push(T[((b >> 6) & 63) as usize] as char);
            out.push('=');
        }
        out
    }

    fn decode_char(c: u8) -> Result<u32, &'static str> {
        Ok(match c {
            b'A'..=b'Z' => (c - b'A') as u32,
            b'a'..=b'z' => (c - b'a' + 26) as u32,
            b'0'..=b'9' => (c - b'0' + 52) as u32,
            b'+' => 62,
            b'/' => 63,
            _ => return Err("invalid base64 character"),
        })
    }

    pub fn decode(s: &str) -> Result<Vec<u8>, &'static str> {
        let bytes: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
        if !bytes.len().is_multiple_of(4) {
            return Err("invalid base64 length");
        }
        let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
        let mut i = 0;
        while i < bytes.len() {
            let q: [u8; 4] = [bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]];
            let pad = q.iter().rev().take_while(|&&c| c == b'=').count();
            let mut v: u32 = 0;
            for (slot, &c) in q.iter().enumerate() {
                v <<= 6;
                if c != b'=' {
                    v |= decode_char(c)?;
                } else if slot < 2 {
                    return Err("invalid base64 padding");
                }
            }
            out.push(((v >> 16) & 0xff) as u8);
            if pad < 2 {
                out.push(((v >> 8) & 0xff) as u8);
            }
            if pad < 1 {
                out.push((v & 0xff) as u8);
            }
            i += 4;
        }
        Ok(out)
    }
}

/// Serde adapter — encodes `Arc<[u8]>` as a base64 string on the wire.
mod arc_b64 {
    use std::sync::Arc;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(data: &Arc<[u8]>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        s.serialize_str(&super::base64::encode(data))
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Arc<[u8]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(d)?;
        super::base64::decode(&s)
            .map(|v| Arc::from(v.into_boxed_slice()))
            .map_err(serde::de::Error::custom)
    }
}

/// Token / cost accounting for a completed turn — spec §7.9 `cap.usage`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_read_tokens: u64,
    #[serde(default)]
    pub cache_creation_tokens: u64,
    /// Reasoning / "thinking" tokens — separate from output for models that
    /// distinguish hidden reasoning from visible output (Codex's
    /// `reasoning_output_tokens`, Claude's `thinking_tokens`).
    #[serde(default)]
    pub thinking_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd_estimate: Option<f64>,
    /// Serialized as `duration_ms` per spec §7.9.
    #[serde(
        rename = "duration_ms",
        default,
        skip_serializing_if = "Option::is_none",
        with = "duration_ms_opt"
    )]
    pub duration: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    /// Stop reason if this is the terminal usage frame. `None` when emitted
    /// as mid-session progress.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<StopReason>,
}

mod duration_ms_opt {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(v: &Option<Duration>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match v {
            Some(d) => s.serialize_some(&(d.as_millis() as u64)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<u64>::deserialize(d).map(|v| v.map(Duration::from_millis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json_kind(v: &serde_json::Value) -> &str {
        v.get("kind").and_then(|v| v.as_str()).unwrap_or("")
    }

    #[test]
    fn agent_event_roundtrip_text_chunk() {
        let ev = AgentEvent::TextChunk {
            msg_id: "m1".into(),
            text: "hello".into(),
            channel: TextChannel::Assistant,
        };
        let j = serde_json::to_value(&ev).unwrap();
        assert_eq!(json_kind(&j), "cap.text_chunk");
        assert_eq!(j["message_id"], "m1");
        assert_eq!(j["channel"], "assistant");
        let back: AgentEvent = serde_json::from_value(j).unwrap();
        match back {
            AgentEvent::TextChunk { text, channel, .. } => {
                assert_eq!(text, "hello");
                assert_eq!(channel, TextChannel::Assistant);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn agent_event_roundtrip_permission_request() {
        let ev = AgentEvent::PermissionRequest {
            req_id: "p1".into(),
            tool: "Bash".into(),
            intent: serde_json::json!({"command": "rm -rf /"}),
            scope: PermissionScope::Write,
            risk_level: RiskLevel::High,
        };
        let j = serde_json::to_value(&ev).unwrap();
        assert_eq!(json_kind(&j), "cap.permission.request");
        assert_eq!(j["risk_level"], "high");
        assert_eq!(j["scope"], "write");
        let back: AgentEvent = serde_json::from_value(j).unwrap();
        assert!(matches!(
            back,
            AgentEvent::PermissionRequest {
                risk_level: RiskLevel::High,
                ..
            }
        ));
    }

    #[test]
    fn agent_event_roundtrip_ask_user_schema() {
        let ev = AgentEvent::AskUser {
            ask_id: "a1".into(),
            prompt: "Which DB?".into(),
            ask_kind: AskKind::Schema {
                schema: serde_json::json!({"type": "string"}),
            },
            options: vec![],
            timeout_seconds: None,
        };
        let j = serde_json::to_value(&ev).unwrap();
        assert_eq!(json_kind(&j), "cap.ask_user");
        assert_eq!(j["ask_kind"], "schema");
        assert_eq!(j["form"], serde_json::json!({"type": "string"}));
        let back: AgentEvent = serde_json::from_value(j).unwrap();
        assert!(matches!(
            back,
            AgentEvent::AskUser {
                ask_kind: AskKind::Schema { .. },
                ..
            }
        ));
    }

    #[test]
    fn agent_event_roundtrip_usage() {
        let ev = AgentEvent::Usage {
            usage: Usage {
                input_tokens: 100,
                output_tokens: 200,
                duration: Some(Duration::from_millis(1234)),
                stop_reason: Some(StopReason::EndTurn),
                ..Default::default()
            },
        };
        let j = serde_json::to_value(&ev).unwrap();
        assert_eq!(json_kind(&j), "cap.usage");
        assert_eq!(j["input_tokens"], 100);
        assert_eq!(j["duration_ms"], 1234);
        assert_eq!(j["stop_reason"], "end_turn");
        let _back: AgentEvent = serde_json::from_value(j).unwrap();
    }

    #[test]
    fn agent_event_roundtrip_tool_call_end_duration() {
        let ev = AgentEvent::ToolCallEnd {
            call_id: "call_1".into(),
            output: "done".into(),
            is_error: false,
            duration: Some(Duration::from_millis(42)),
        };
        let j = serde_json::to_value(&ev).unwrap();
        assert_eq!(json_kind(&j), "cap.tool_call.end");
        assert_eq!(j["duration_ms"], 42);
        let back: AgentEvent = serde_json::from_value(j).unwrap();
        assert!(matches!(
            back,
            AgentEvent::ToolCallEnd {
                duration: Some(d),
                ..
            } if d == Duration::from_millis(42)
        ));
    }

    #[test]
    fn client_frame_roundtrip_prompt_text() {
        let f = ClientFrame::Prompt {
            content: vec![Content::text("hello")],
        };
        let j = serde_json::to_value(&f).unwrap();
        assert_eq!(json_kind(&j), "cap.user_input.inject");
        assert_eq!(j["content"][0]["type"], "text");
        assert_eq!(j["content"][0]["text"], "hello");
        let _back: ClientFrame = serde_json::from_value(j).unwrap();
    }

    #[test]
    fn client_frame_roundtrip_session_config() {
        let f = ClientFrame::SessionConfig(SessionConfig {
            cwd: std::path::PathBuf::from("/tmp/x"),
            model: Some("claude-opus-4-7".into()),
            permission_mode: PermissionMode::Interactive,
            budget_usd: Some(5.0),
            ..Default::default()
        });
        let j = serde_json::to_value(&f).unwrap();
        assert_eq!(json_kind(&j), "cap.session.config");
        assert_eq!(j["model"], "claude-opus-4-7");
        assert_eq!(j["permission_mode"], "interactive");
        assert_eq!(j["budget_usd"], 5.0);
        let back: ClientFrame = serde_json::from_value(j).unwrap();
        match back {
            ClientFrame::SessionConfig(c) => {
                assert_eq!(c.model.as_deref(), Some("claude-opus-4-7"));
            }
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn client_frame_roundtrip_image_base64() {
        use std::sync::Arc;
        let bytes: Arc<[u8]> = Arc::from([0xde, 0xad, 0xbe, 0xefu8].as_slice());
        let f = ClientFrame::Prompt {
            content: vec![Content::Image {
                mime: "image/png".into(),
                data: Arc::clone(&bytes),
            }],
        };
        let j = serde_json::to_value(&f).unwrap();
        assert_eq!(j["content"][0]["type"], "image");
        assert_eq!(j["content"][0]["mime"], "image/png");
        assert_eq!(j["content"][0]["data"], "3q2+7w==");
        let back: ClientFrame = serde_json::from_value(j).unwrap();
        match back {
            ClientFrame::Prompt { content } => match &content[0] {
                Content::Image { data, .. } => assert_eq!(&data[..], &*bytes),
                _ => panic!(),
            },
            _ => panic!(),
        }
    }

    #[test]
    fn base64_roundtrip_inverse() {
        for v in [
            b"" as &[u8],
            b"a",
            b"ab",
            b"abc",
            b"abcd",
            b"hello world",
            &[0, 0, 0, 0xff, 0xff, 0xff][..],
        ] {
            let enc = base64::encode(v);
            let dec = base64::decode(&enc).unwrap();
            assert_eq!(dec.as_slice(), v, "roundtrip failed for {:?}", v);
        }
    }
}
