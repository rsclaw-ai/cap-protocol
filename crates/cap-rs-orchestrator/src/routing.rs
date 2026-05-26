//! Pluggable routing strategies for the orchestrator.
//!
//! The default [`StaticRouting`] interprets the declarative YAML `routes` array.
//! Custom strategies (e.g. LLM-based) implement [`RoutingStrategy`].

use std::collections::{HashMap, HashSet};

use cap_rs::core::StopReason;

use crate::config::{Action, FleetSpec, Route, SessionId, Split};

/// Context provided to a [`RoutingStrategy`] when making a decision.
#[derive(Debug)]
pub struct RoutingContext<'a> {
    pub spec: &'a FleetSpec,
    pub done: &'a HashSet<SessionId>,
    pub failed: &'a HashSet<SessionId>,
    pub spawned: &'a HashSet<SessionId>,
    pub buffers: &'a HashMap<SessionId, String>,
    pub task: &'a str,
}

/// A single decision produced by [`RoutingStrategy::on_session_done`].
#[derive(Debug, Clone)]
pub enum RouteDecision {
    Route { target: SessionId, payload: String },
    FanOut { targets: Vec<(SessionId, String)> },
    Select { candidates: Vec<SessionId> },
    Error(String),
    None,
}

/// Pluggable routing strategy.
///
/// Each time a session completes, the executor calls [`on_session_done`] and
/// carries out the returned decisions (spawning target sessions, routing frames,
/// emitting `OrchestratorEvent`s).
#[async_trait::async_trait]
pub trait RoutingStrategy: Send + Sync + 'static {
    async fn on_session_done(
        &self,
        ctx: &RoutingContext,
        session: &SessionId,
        stop_reason: StopReason,
    ) -> Vec<RouteDecision>;
}

/// The default strategy: interprets the YAML `routes` array from `FleetSpec`.
#[derive(Debug)]
pub struct StaticRouting {
    routes: Vec<Route>,
}

impl StaticRouting {
    pub fn new(routes: Vec<Route>) -> Self {
        Self { routes }
    }
}

#[async_trait::async_trait]
impl RoutingStrategy for StaticRouting {
    async fn on_session_done(
        &self,
        ctx: &RoutingContext,
        session: &SessionId,
        _stop_reason: StopReason,
    ) -> Vec<RouteDecision> {
        let mut decisions = Vec::new();

        for route in &self.routes {
            let triggers = route.trigger_sessions();
            if !triggers.iter().any(|t| t == session) {
                continue;
            }
            if !triggers.iter().all(|t| ctx.done.contains(t)) {
                continue;
            }

            match route.action() {
                Ok(Action::RouteTo(to)) => {
                    let payload = build_payload(ctx, &triggers);
                    decisions.push(RouteDecision::Route {
                        target: to,
                        payload,
                    });
                }
                Ok(Action::FanOut(f)) => match f.split {
                    Split::Broadcast => {
                        let payload = build_payload(ctx, &triggers);
                        let targets: Vec<_> =
                            f.to.iter().map(|t| (t.clone(), payload.clone())).collect();
                        decisions.push(RouteDecision::FanOut { targets });
                    }
                    Split::BySubtask => {
                        let buf = ctx.buffers.get(session).cloned().unwrap_or_default();
                        match parse_subtasks(&buf) {
                            Some(items) => {
                                let mut targets = Vec::new();
                                let mut sufficient = true;
                                for (i, to) in f.to.iter().enumerate() {
                                    if i >= items.len() {
                                        sufficient = false;
                                        break;
                                    }
                                    targets.push((to.clone(), items[i].clone()));
                                }
                                if sufficient {
                                    decisions.push(RouteDecision::FanOut { targets });
                                } else {
                                    if !targets.is_empty() {
                                        decisions.push(RouteDecision::FanOut { targets });
                                    }
                                    decisions.push(RouteDecision::Error(
                                        "fan_out by_subtask: lead emitted fewer \
                                         subtask items than targets"
                                            .into(),
                                    ));
                                }
                            }
                            None => {
                                decisions.push(RouteDecision::Error(
                                    "fan_out by_subtask: lead emitted no parseable \
                                     cap-subtasks JSON-array block"
                                        .into(),
                                ));
                            }
                        }
                    }
                },
                Ok(Action::Collect(_)) => {
                    decisions.push(RouteDecision::Select {
                        candidates: triggers.clone(),
                    });
                }
                Err(_) => {}
            }
        }

        decisions
    }
}

/// Build the prompt for a routed/fanned-out session: the original task plus
/// the accumulated output of each upstream (trigger) session.
fn build_payload(ctx: &RoutingContext, triggers: &[SessionId]) -> String {
    let mut parts = Vec::new();
    for t in triggers {
        if let Some(buf) = ctx.buffers.get(t) {
            if !buf.is_empty() {
                parts.push(format!("--- output from {t} ---\n{buf}"));
            }
        }
    }
    if parts.is_empty() {
        ctx.task.to_string()
    } else {
        format!("{}\n\n{}", ctx.task, parts.join("\n\n"))
    }
}

const MAX_SUBTASKS: usize = 256;

fn parse_subtasks(text: &str) -> Option<Vec<String>> {
    let fence = "`".repeat(3);
    let open = format!("{fence}cap-subtasks");
    let start = text.find(&open)? + open.len();
    let rest = &text[start..];
    let end = rest.find(&fence)?;
    let mut items: Vec<String> = serde_json::from_str(rest[..end].trim()).ok()?;
    if items.is_empty() {
        return None;
    }
    items.truncate(MAX_SUBTASKS);
    Some(items)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_subtasks_returns_json_array() {
        let fence = "`".repeat(3);
        let text = format!("prefix\n{fence}cap-subtasks\n[\"a\", \"b\"]\n{fence}\nsuffix");
        let items = parse_subtasks(&text).unwrap();
        assert_eq!(items, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn parse_subtasks_returns_none_when_missing() {
        assert!(parse_subtasks("no block here").is_none());
    }

    #[test]
    fn parse_subtasks_returns_none_for_empty_array() {
        let fence = "`".repeat(3);
        let text = format!("{fence}cap-subtasks\n[]\n{fence}");
        assert!(parse_subtasks(&text).is_none());
    }

    #[test]
    fn parse_subtasks_truncates_to_max() {
        let fence = "`".repeat(3);
        let items: Vec<String> = (0..300).map(|i| format!("x{i}")).collect();
        let json = serde_json::to_string(&items).unwrap();
        let text = format!("{fence}cap-subtasks\n{json}\n{fence}");
        let parsed = parse_subtasks(&text).unwrap();
        assert_eq!(parsed.len(), MAX_SUBTASKS);
    }
}
