//! Helpers for CAP core external wire compatibility.
//!
//! `AgentEvent::Done` is an SDK/orchestrator convenience, not a CAP core wire
//! event. Bridges that expose CAP externally should pass events through this
//! module before serialization.

use crate::core::{AgentEvent, Usage};

pub fn to_strict_events(event: AgentEvent) -> Vec<AgentEvent> {
    match event {
        AgentEvent::Done { stop_reason, usage } => {
            if usage_has_data(&usage) {
                let mut usage = usage;
                usage.stop_reason = Some(stop_reason);
                vec![AgentEvent::Usage { usage }]
            } else {
                Vec::new()
            }
        }
        other if is_core_wire_event(&other) => vec![other],
        _ => Vec::new(),
    }
}

pub fn is_core_wire_event(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::Ready { .. }
            | AgentEvent::TextChunk { .. }
            | AgentEvent::Thought { .. }
            | AgentEvent::ToolCallStart { .. }
            | AgentEvent::ToolCallDelta { .. }
            | AgentEvent::ToolCallEnd { .. }
            | AgentEvent::Plan { .. }
            | AgentEvent::AskUser { .. }
            | AgentEvent::PermissionRequest { .. }
            | AgentEvent::Usage { .. }
            | AgentEvent::Error { .. }
            | AgentEvent::PtyRawBytes { .. }
    )
}

fn usage_has_data(usage: &Usage) -> bool {
    usage.input_tokens != 0
        || usage.output_tokens != 0
        || usage.cache_read_tokens != 0
        || usage.cache_creation_tokens != 0
        || usage.thinking_tokens != 0
        || usage.cost_usd_estimate.is_some()
        || usage.duration.is_some()
        || usage.model_id.is_some()
        || usage.stop_reason.is_some()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::core::{AgentEvent, StopReason, Usage};

    #[test]
    fn strict_wire_omits_done_without_usage() {
        let events = to_strict_events(AgentEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: Usage::default(),
        });
        assert!(events.is_empty());
    }

    #[test]
    fn strict_wire_converts_done_with_usage_to_usage_event() {
        let events = to_strict_events(AgentEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: Usage {
                input_tokens: 10,
                output_tokens: 5,
                duration: Some(Duration::from_millis(250)),
                ..Default::default()
            },
        });
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::Usage { usage } => {
                assert_eq!(usage.input_tokens, 10);
                assert_eq!(usage.output_tokens, 5);
                assert_eq!(usage.stop_reason, Some(StopReason::EndTurn));
            }
            other => panic!("expected Usage, got {other:?}"),
        }
    }

    #[test]
    fn strict_wire_marks_done_as_non_core_wire_event() {
        assert!(!is_core_wire_event(&AgentEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: Usage::default(),
        }));
    }
}
