//! Minimal A2A core mapping helpers for CAP events.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::core::AgentEvent;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCard {
    pub name: String,
    #[serde(default)]
    pub extensions: Vec<AgentExtension>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentExtension {
    pub uri: String,
    #[serde(default)]
    pub required: bool,
}

impl AgentCard {
    pub fn supports_cap_v1(&self) -> bool {
        self.extensions.iter().any(|e| e.uri == "cap-protocol/v1")
    }
}

pub fn parse_sse_events(sse: &str) -> Result<Vec<AgentEvent>, serde_json::Error> {
    let mut events = Vec::new();
    let mut data = String::new();
    for line in sse.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.trim_start());
        } else if line.trim().is_empty() && !data.trim().is_empty() {
            events.push(serde_json::from_str::<AgentEvent>(&data)?);
            data.clear();
        }
    }
    if !data.trim().is_empty() {
        events.push(serde_json::from_str::<AgentEvent>(&data)?);
    }
    Ok(events)
}

pub fn cap_event_to_a2a_part(event: &AgentEvent) -> Option<Value> {
    match event {
        AgentEvent::TextChunk { text, .. } => Some(json!({
            "kind": "text",
            "text": text,
        })),
        other => Some(json!({
            "kind": "data",
            "data": other,
            "_meta": {
                "cap": {
                    "kind": event_kind(other),
                }
            }
        })),
    }
}

fn event_kind(event: &AgentEvent) -> &'static str {
    match event {
        AgentEvent::Ready { .. } => "cap.session.ready",
        AgentEvent::TextChunk { .. } => "cap.text_chunk",
        AgentEvent::Thought { .. } => "cap.thought",
        AgentEvent::ToolCallStart { .. } => "cap.tool_call.start",
        AgentEvent::ToolCallDelta { .. } => "cap.tool_call.delta",
        AgentEvent::ToolCallEnd { .. } => "cap.tool_call.end",
        AgentEvent::Plan { .. } => "cap.plan",
        AgentEvent::AskUser { .. } => "cap.ask_user",
        AgentEvent::PermissionRequest { .. } => "cap.permission.request",
        AgentEvent::Usage { .. } => "cap.usage",
        AgentEvent::Done { .. } => "cap.done",
        AgentEvent::Error { .. } => "cap.error",
        AgentEvent::ReverseRpc { .. } => "cap.reverse_rpc",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{AgentEvent, TextChannel};

    #[test]
    fn agent_card_must_advertise_cap_extension() {
        let card = AgentCard {
            name: "remote".into(),
            extensions: vec![AgentExtension {
                uri: "cap-protocol/v1".into(),
                required: true,
            }],
        };
        assert!(card.supports_cap_v1());
    }

    #[test]
    fn sse_data_part_maps_to_core_event() {
        let sse = r#"event: message
data: {"kind":"cap.text_chunk","message_id":"m1","text":"hi","channel":"assistant"}

"#;
        let events = parse_sse_events(sse).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::TextChunk {
                text,
                channel: TextChannel::Assistant,
                ..
            } => {
                assert_eq!(text, "hi");
            }
            other => panic!("expected text chunk, got {other:?}"),
        }
    }

    #[test]
    fn text_chunk_maps_to_a2a_text_part() {
        let part = cap_event_to_a2a_part(&AgentEvent::TextChunk {
            msg_id: "m1".into(),
            text: "hello".into(),
            channel: TextChannel::Assistant,
        })
        .unwrap();
        assert_eq!(part["kind"], "text");
        assert_eq!(part["text"], "hello");
    }
}
