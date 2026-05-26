//! Pluggable routing strategies for the orchestrator.
//!
//! The default [`StaticRouting`] interprets the declarative YAML `routes` array.
//! Custom strategies (e.g. LLM-based) implement [`RoutingStrategy`].

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use cap_rs::core::StopReason;

use crate::config::{Action, DriverKind, FleetSpec, PermissionPolicy, Route, SessionId, Split};

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
#[derive(Debug, Clone, PartialEq)]
pub enum RouteDecision {
    Route {
        target: SessionId,
        payload: String,
    },
    /// Route to a dynamically created session (not in the YAML spec).
    DynamicRoute {
        target: SessionId,
        payload: String,
        driver: DriverKind,
        permissions: PermissionPolicy,
    },
    FanOut {
        targets: Vec<(SessionId, String)>,
    },
    Select {
        candidates: Vec<SessionId>,
    },
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

// ---------------------------------------------------------------------------
// LLM-driven routing
// ---------------------------------------------------------------------------

/// Errors from LLM-based routing decisions.
#[derive(Debug, thiserror::Error)]
pub enum LlmError {
    #[error("{0}")]
    Message(String),
    #[error("LLM routing query timed out")]
    Timeout,
}

/// Abstraction for querying an LLM to make routing decisions.
#[async_trait::async_trait]
pub trait LlmClient: Send + Sync {
    /// Send a system prompt and user prompt, returning the LLM's text response.
    async fn query(&self, system: &str, prompt: &str) -> Result<String, LlmError>;
}

/// LLM client that shells out to a CLI command, piping the prompt to stdin.
#[derive(Debug)]
pub struct CliLlmClient {
    command: Vec<String>,
    timeout: Duration,
}

impl CliLlmClient {
    pub fn new(command: Vec<String>) -> Self {
        assert!(
            !command.is_empty(),
            "CliLlmClient command must have at least the binary"
        );
        Self {
            command,
            timeout: Duration::from_secs(120),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[async_trait::async_trait]
impl LlmClient for CliLlmClient {
    async fn query(&self, system: &str, prompt: &str) -> Result<String, LlmError> {
        let full = format!("{}\n\n{}", system, prompt);
        let mut child = tokio::process::Command::new(&self.command[0])
            .args(&self.command[1..])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| LlmError::Message(format!("spawn {} failed: {}", self.command[0], e)))?;

        // Write prompt to stdin and close to signal EOF.
        {
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| LlmError::Message("child has no stdin".into()))?;
            use tokio::io::AsyncWriteExt;
            stdin
                .write_all(full.as_bytes())
                .await
                .map_err(|e| LlmError::Message(format!("write stdin: {e}")))?;
        }

        let output = tokio::time::timeout(self.timeout, child.wait_with_output())
            .await
            .map_err(|_| LlmError::Timeout)?
            .map_err(|e| LlmError::Message(format!("wait: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(LlmError::Message(format!(
                "{} exited with {}: {}",
                self.command[0], output.status, stderr
            )));
        }

        let text = String::from_utf8_lossy(&output.stdout).to_string();
        if text.trim().is_empty() {
            return Err(LlmError::Message("empty response".into()));
        }
        Ok(text.trim().to_string())
    }
}

/// Describes one agent that the LLM orchestrator can route to.
#[derive(Debug, Clone)]
pub struct LlmSessionSpec {
    pub id: SessionId,
    pub role: Option<String>,
}

/// Configuration for [`LlmRouting`].
#[derive(Debug, Clone)]
pub struct LlmRoutingConfig {
    pub system_prompt: String,
    pub timeout: Duration,
    pub max_decisions: usize,
    pub max_context_chars: usize,
}

impl Default for LlmRoutingConfig {
    fn default() -> Self {
        Self {
            system_prompt: DEFAULT_ORCHESTRATOR_PROMPT.to_string(),
            timeout: Duration::from_secs(60),
            max_decisions: 5,
            max_context_chars: 4000,
        }
    }
}

const DEFAULT_ORCHESTRATOR_PROMPT: &str = r#"You are an orchestrator managing a fleet of AI coding agents working on a programming task.

Based on the completed agents' output and the available agents, decide what to do next.

Respond with a JSON object only:
{
  "actions": [
    {
      "type": "route",
      "target": "agent_name",
      "context": "specific instructions and context for this agent"
    }
  ],
  "reasoning": "brief explanation of your decision"
}

Action types:
- "route": Send instructions to an available agent. Must include "target" (agent name) and "context" (instructions). Optionally include "driver" to create a dynamic agent of a specific type (e.g. "claude", "codex", "grpc:localhost:50051").
- "collect": Present multiple outputs for human selection. Must include "candidates" array.
- "complete": The task is finished. Use an empty actions array: { "actions": [], "reasoning": "..." }

Only reference agents from the list below.
"#;

/// LLM-driven routing strategy. Calls an LLM to decide routing decisions
/// dynamically based on the task and completed sessions' output.
pub struct LlmRouting {
    client: Box<dyn LlmClient>,
    sessions: Vec<LlmSessionSpec>,
    config: LlmRoutingConfig,
}

impl std::fmt::Debug for LlmRouting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmRouting")
            .field("sessions", &self.sessions)
            .field("config", &self.config)
            .finish()
    }
}

impl LlmRouting {
    pub fn new(
        client: Box<dyn LlmClient>,
        sessions: Vec<LlmSessionSpec>,
        config: LlmRoutingConfig,
    ) -> Self {
        Self {
            client,
            sessions,
            config,
        }
    }
}

#[async_trait::async_trait]
impl RoutingStrategy for LlmRouting {
    async fn on_session_done(
        &self,
        ctx: &RoutingContext,
        _session: &SessionId,
        _stop_reason: StopReason,
    ) -> Vec<RouteDecision> {
        let prompt = build_llm_prompt(ctx, &self.sessions, self.config.max_context_chars);

        let response = match tokio::time::timeout(
            self.config.timeout,
            self.client.query(&self.config.system_prompt, &prompt),
        )
        .await
        {
            Ok(Ok(text)) => text,
            Ok(Err(e)) => {
                return vec![RouteDecision::Error(format!(
                    "LLM routing query failed: {e}"
                ))];
            }
            Err(_) => {
                return vec![RouteDecision::Error("LLM routing query timed out".into())];
            }
        };

        let valid_sessions: HashSet<SessionId> =
            self.sessions.iter().map(|s| s.id.clone()).collect();
        parse_llm_response(
            &response,
            &valid_sessions,
            self.config.max_decisions,
            ctx.task,
        )
    }
}

/// Structured summary of one agent session for LLM context.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionStatus {
    Completed,
    Failed,
    Running,
}

#[derive(Debug, Clone)]
pub struct SessionSummary {
    pub id: SessionId,
    pub status: SessionStatus,
    pub output_preview: String,
}

impl SessionSummary {
    /// Build summaries from routing context, covering all known sessions
    /// (done, failed, or spawned).
    pub fn build_all(ctx: &RoutingContext, max_chars_per_session: usize) -> Vec<Self> {
        let seen = ctx
            .done
            .iter()
            .chain(ctx.failed.iter())
            .chain(ctx.spawned.iter())
            .cloned()
            .collect::<HashSet<_>>();
        let mut summaries: Vec<Self> = seen
            .iter()
            .map(|id| {
                let output = ctx.buffers.get(id).cloned().unwrap_or_default();
                let preview = if output.len() > max_chars_per_session {
                    let cutoff = max_chars_per_session.saturating_sub(50);
                    format!(
                        "{}...\n[truncated {} chars]",
                        &output[..cutoff],
                        output.len() - cutoff
                    )
                } else {
                    output
                };
                let status = if ctx.failed.contains(id) {
                    SessionStatus::Failed
                } else if ctx.done.contains(id) {
                    SessionStatus::Completed
                } else {
                    SessionStatus::Running
                };
                Self {
                    id: id.clone(),
                    status,
                    output_preview: preview,
                }
            })
            .collect();
        summaries.sort_by(|a, b| a.id.cmp(&b.id));
        summaries
    }
}

/// Hybrid routing: YAML routes take priority; fall back to LLM when no static
/// route matches a session completion.
#[derive(Debug)]
pub struct HybridRouting {
    static_strategy: StaticRouting,
    llm_strategy: LlmRouting,
}

impl HybridRouting {
    pub fn new(routes: Vec<Route>, llm: LlmRouting) -> Self {
        Self {
            static_strategy: StaticRouting::new(routes),
            llm_strategy: llm,
        }
    }
}

#[async_trait::async_trait]
impl RoutingStrategy for HybridRouting {
    async fn on_session_done(
        &self,
        ctx: &RoutingContext,
        session: &SessionId,
        stop_reason: StopReason,
    ) -> Vec<RouteDecision> {
        let decisions = self
            .static_strategy
            .on_session_done(ctx, session, stop_reason)
            .await;

        // If any static route produced a concrete action, use it.
        let has_action = decisions.iter().any(|d| {
            matches!(
                d,
                RouteDecision::Route { .. }
                    | RouteDecision::DynamicRoute { .. }
                    | RouteDecision::FanOut { .. }
                    | RouteDecision::Select { .. }
            )
        });

        if has_action {
            return decisions;
        }

        // Fall back to LLM.
        self.llm_strategy
            .on_session_done(ctx, session, stop_reason)
            .await
    }
}

/// Build the user prompt for the orchestrator LLM, using structured
/// session summaries for context window management.
fn build_llm_prompt(ctx: &RoutingContext, sessions: &[LlmSessionSpec], max_chars: usize) -> String {
    let task = ctx.task;

    let session_desc: Vec<String> = sessions
        .iter()
        .map(|s| {
            let role = s.role.as_deref().unwrap_or("general agent");
            format!("- {}: {}", s.id, role)
        })
        .collect();

    let summaries = SessionSummary::build_all(ctx, max_chars);
    let completed: Vec<String> = summaries
        .into_iter()
        .filter(|s| !s.output_preview.trim().is_empty())
        .map(|s| {
            let status_tag = match s.status {
                SessionStatus::Completed => "✓ completed",
                SessionStatus::Failed => "✗ failed",
                SessionStatus::Running => "▶ running",
            };
            format!(
                "--- {id} ({status_tag}) ---\n{output}",
                id = s.id,
                output = s.output_preview
            )
        })
        .collect();

    let agents_str = session_desc.join("\n");
    let completed_str = if completed.is_empty() {
        "(none yet)".to_string()
    } else {
        completed.join("\n\n")
    };
    format!(
        "TASK:\n{task}\n\nAVAILABLE AGENTS:\n{agents_str}\n\nAGENT OUTPUTS:\n{completed_str}\n\nWhat should happen next? Respond with a JSON object."
    )
}

/// Parse a JSON routing decision from the LLM's text response.
/// Supports optional `driver` field for dynamic session creation.
fn parse_llm_response(
    text: &str,
    valid_sessions: &HashSet<SessionId>,
    max_decisions: usize,
    task: &str,
) -> Vec<RouteDecision> {
    let json_str = match extract_json(text) {
        Some(s) => s,
        None => {
            return vec![RouteDecision::Error(
                "LLM response contains no JSON object".into(),
            )];
        }
    };

    let json: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(e) => {
            return vec![RouteDecision::Error(format!(
                "LLM response is not valid JSON: {e}"
            ))];
        }
    };

    let Some(actions) = json.get("actions").and_then(|a| a.as_array()) else {
        return vec![RouteDecision::None];
    };

    if actions.is_empty() {
        return vec![RouteDecision::None];
    }

    let mut decisions = Vec::new();
    for action in actions.iter().take(max_decisions) {
        let Some(typ) = action.get("type").and_then(|t| t.as_str()) else {
            continue;
        };
        match typ {
            "route" => {
                let Some(target) = action.get("target").and_then(|t| t.as_str()) else {
                    decisions.push(RouteDecision::Error("route action missing 'target'".into()));
                    continue;
                };
                let context = action
                    .get("context")
                    .and_then(|c| c.as_str())
                    .unwrap_or(task);

                // Check for driver field → dynamic session
                let driver_str = action.get("driver").and_then(|d| d.as_str());
                if let Some(ds) = driver_str {
                    // Dynamic session: driver specified
                    let driver_kind = match DriverKind::parse(ds) {
                        Some(d) => d,
                        None => {
                            decisions.push(RouteDecision::Error(format!(
                                "LLM used unknown driver '{ds}'"
                            )));
                            continue;
                        }
                    };
                    let permissions = if valid_sessions.contains(target) {
                        // Target exists in spec; we still route with original session's policy.
                        // For dynamic we default to Allow.
                        PermissionPolicy::Allow
                    } else {
                        PermissionPolicy::Allow
                    };
                    decisions.push(RouteDecision::DynamicRoute {
                        target: target.to_string(),
                        payload: context.to_string(),
                        driver: driver_kind,
                        permissions,
                    });
                } else if valid_sessions.contains(target) {
                    decisions.push(RouteDecision::Route {
                        target: target.to_string(),
                        payload: context.to_string(),
                    });
                } else {
                    decisions.push(RouteDecision::Error(format!(
                        "LLM referenced unknown session '{target}' (use 'driver' field for dynamic sessions)"
                    )));
                }
            }
            "collect" => {
                let candidates: Vec<SessionId> = action
                    .get("candidates")
                    .and_then(|c| c.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                decisions.push(RouteDecision::Select { candidates });
            }
            "complete" => {
                decisions.push(RouteDecision::None);
            }
            other => {
                decisions.push(RouteDecision::Error(format!(
                    "LLM used unknown action type '{other}'"
                )));
            }
        }
    }

    decisions
}

/// Extract the first top-level JSON object `{...}` from text (handles markdown
/// code fences and surrounding prose).
fn extract_json(text: &str) -> Option<&str> {
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    if end <= start {
        return None;
    }
    Some(&text[start..=end])
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

/// A stub LLM client that returns canned responses (for testing).
#[cfg(any(test, feature = "testing"))]
#[derive(Debug)]
pub struct StubLlmClient {
    responses: std::sync::Mutex<Vec<String>>,
}

#[cfg(any(test, feature = "testing"))]
impl StubLlmClient {
    pub fn new(responses: Vec<&str>) -> Self {
        Self {
            responses: std::sync::Mutex::new(responses.into_iter().map(String::from).collect()),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
#[async_trait::async_trait]
impl LlmClient for StubLlmClient {
    async fn query(&self, _system: &str, _prompt: &str) -> Result<String, LlmError> {
        self.responses
            .lock()
            .unwrap()
            .pop()
            .ok_or_else(|| LlmError::Message("no more stubs".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FanOut, Trigger};

    // -- parse_subtasks tests ------------------------------------------------

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

    // -- LlmRouting / parse_llm_response tests ------------------------------

    fn valid_sessions() -> HashSet<SessionId> {
        ["coder", "reviewer"]
            .into_iter()
            .map(String::from)
            .collect()
    }

    #[test]
    fn parse_llm_route_to_known_session() {
        let json = r#"{"actions": [{"type": "route", "target": "reviewer", "context": "review this"}], "reasoning": "ok"}"#;
        let decisions = parse_llm_response(json, &valid_sessions(), 5, "task");
        assert_eq!(decisions.len(), 1);
        match &decisions[0] {
            RouteDecision::Route { target, payload } => {
                assert_eq!(target, "reviewer");
                assert_eq!(payload, "review this");
            }
            other => panic!("expected Route, got {other:?}"),
        }
    }

    #[test]
    fn parse_llm_complete_returns_none() {
        let json = r#"{"actions": [], "reasoning": "done"}"#;
        let decisions = parse_llm_response(json, &valid_sessions(), 5, "task");
        assert_eq!(decisions, vec![RouteDecision::None]);
    }

    #[test]
    fn parse_llm_invalid_target_returns_error() {
        let json = r#"{"actions": [{"type": "route", "target": "ghost", "context": "hi"}]}"#;
        let decisions = parse_llm_response(json, &valid_sessions(), 5, "task");
        assert!(
            decisions
                .iter()
                .any(|d| matches!(d, RouteDecision::Error(_)))
        );
    }

    #[test]
    fn parse_llm_garbage_text_returns_error() {
        let decisions = parse_llm_response("not json at all", &valid_sessions(), 5, "task");
        assert!(
            decisions
                .iter()
                .any(|d| matches!(d, RouteDecision::Error(_)))
        );
    }

    #[test]
    fn parse_llm_empty_actions_returns_none() {
        let json = r#"{"actions": []}"#;
        let decisions = parse_llm_response(json, &valid_sessions(), 5, "task");
        assert_eq!(decisions, vec![RouteDecision::None]);
    }

    #[test]
    fn parse_llm_with_markdown_fence() {
        let md = format!(
            "Some text\n```json\n{}\n```\nmore text",
            r#"{"actions": [{"type": "route", "target": "coder", "context": "fix"}]}"#
        );
        let decisions = parse_llm_response(&md, &valid_sessions(), 5, "task");
        match &decisions[0] {
            RouteDecision::Route { target, payload } => {
                assert_eq!(target, "coder");
                assert_eq!(payload, "fix");
            }
            other => panic!("expected Route, got {other:?}"),
        }
    }

    #[test]
    fn parse_llm_missing_actions_object_falls_back_to_none() {
        let json = r#"{"reasoning": "done"}"#;
        let decisions = parse_llm_response(json, &valid_sessions(), 5, "task");
        assert_eq!(decisions, vec![RouteDecision::None]);
    }

    #[test]
    fn parse_llm_collect_returns_select() {
        let json = r#"{"actions": [{"type": "collect", "candidates": ["coder", "reviewer"]}]}"#;
        let decisions = parse_llm_response(json, &valid_sessions(), 5, "task");
        match &decisions[0] {
            RouteDecision::Select { candidates } => {
                assert_eq!(
                    candidates,
                    &vec!["coder".to_string(), "reviewer".to_string()]
                );
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parse_llm_respects_max_decisions() {
        let json = r#"{"actions": [
            {"type": "route", "target": "coder", "context": "a"},
            {"type": "route", "target": "reviewer", "context": "b"},
            {"type": "route", "target": "coder", "context": "c"}
        ]}"#;
        let decisions = parse_llm_response(json, &valid_sessions(), 2, "task");
        assert_eq!(decisions.len(), 2);
    }

    #[test]
    fn parse_llm_unknown_action_type_returns_error() {
        let json = r#"{"actions": [{"type": "fly", "target": "moon"}]}"#;
        let decisions = parse_llm_response(json, &valid_sessions(), 5, "task");
        assert!(
            decisions
                .iter()
                .any(|d| matches!(d, RouteDecision::Error(_)))
        );
    }

    #[test]
    fn build_llm_prompt_includes_sections() {
        let mut done = HashSet::new();
        done.insert("coder".into());
        let mut buffers = HashMap::new();
        buffers.insert("coder".into(), "my output".into());
        let ctx = RoutingContext {
            spec: &FleetSpec::from_yaml("fleet:\n  base_branch: main\n  sessions:\n    coder: { driver: claude }\n    reviewer: { driver: codex }\n  start: coder\n").unwrap(),
            done: &done,
            failed: &HashSet::new(),
            spawned: &HashSet::new(),
            buffers: &buffers,
            task: &"write code",
        };
        let sessions = vec![
            LlmSessionSpec {
                id: "coder".into(),
                role: Some("writer".into()),
            },
            LlmSessionSpec {
                id: "reviewer".into(),
                role: None,
            },
        ];
        let prompt = build_llm_prompt(&ctx, &sessions, 4000);
        assert!(prompt.contains("TASK:"));
        assert!(prompt.contains("write code"));
        assert!(prompt.contains("writer"));
        assert!(prompt.contains("my output"));
    }

    #[tokio::test]
    async fn llm_routing_basic_route() {
        let client = Box::new(StubLlmClient::new(vec![
            r#"{"actions": [{"type": "route", "target": "reviewer", "context": "review it"}], "reasoning": "ok"}"#,
        ]));
        let sessions = vec![
            LlmSessionSpec {
                id: "coder".into(),
                role: None,
            },
            LlmSessionSpec {
                id: "reviewer".into(),
                role: None,
            },
        ];
        let strategy = LlmRouting::new(client, sessions, LlmRoutingConfig::default());

        let mut done = HashSet::new();
        done.insert("coder".into());
        let ctx = RoutingContext {
            spec: &FleetSpec::from_yaml("fleet:\n  base_branch: main\n  sessions:\n    coder: { driver: claude }\n    reviewer: { driver: codex }\n  start: coder\n").unwrap(),
            done: &done,
            failed: &HashSet::new(),
            spawned: &HashSet::new(),
            buffers: &HashMap::new(),
            task: &"write code",
        };

        let decisions = strategy
            .on_session_done(&ctx, &"coder".into(), StopReason::EndTurn)
            .await;
        assert_eq!(decisions.len(), 1);
        match &decisions[0] {
            RouteDecision::Route { target, payload } => {
                assert_eq!(target, "reviewer");
                assert!(payload.contains("review it"));
            }
            other => panic!("expected Route, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn llm_routing_timeout_returns_error() {
        let client = Box::new(StubLlmClient::new(vec!["slow"]));
        let sessions = vec![LlmSessionSpec {
            id: "x".into(),
            role: None,
        }];
        let mut config = LlmRoutingConfig::default();
        config.timeout = Duration::from_nanos(1); // immediate timeout
        let strategy = LlmRouting::new(client, sessions, config);

        let mut done = HashSet::new();
        done.insert("x".into());
        let ctx = RoutingContext {
            spec: &FleetSpec::from_yaml(
                "fleet:\n  base_branch: main\n  sessions:\n    x: { driver: claude }\n  start: x\n",
            )
            .unwrap(),
            done: &done,
            failed: &HashSet::new(),
            spawned: &HashSet::new(),
            buffers: &HashMap::new(),
            task: &"test",
        };
        let decisions = strategy
            .on_session_done(&ctx, &"x".into(), StopReason::EndTurn)
            .await;
        assert!(
            decisions
                .iter()
                .any(|d| matches!(d, RouteDecision::Error(_)))
        );
    }

    #[test]
    fn extract_json_handles_fenced_block() {
        let text = "prefix\n```json\n{\"key\": \"value\"}\n```\nsuffix";
        let extracted = extract_json(text).unwrap();
        assert_eq!(extracted, "{\"key\": \"value\"}");
    }

    #[test]
    fn extract_json_returns_none_when_no_brace() {
        assert!(extract_json("no braces here").is_none());
    }

    // -- SessionSummary tests -------------------------------------------------

    #[test]
    fn session_summary_build_all_lists_all_sessions() {
        let mut done = HashSet::new();
        done.insert("a".into());
        let mut failed = HashSet::new();
        failed.insert("b".into());
        let mut spawned = HashSet::new();
        spawned.insert("c".into());
        let ctx = RoutingContext {
            spec: &FleetSpec::from_yaml(
                "fleet:\n  base_branch: main\n  sessions:\n    a: { driver: claude }\n    b: { driver: codex }\n    c: { driver: claude }\n  start: a\n",
            )
            .unwrap(),
            done: &done,
            failed: &failed,
            spawned: &spawned,
            buffers: &HashMap::new(),
            task: &"test",
        };
        let summaries = SessionSummary::build_all(&ctx, 100);
        assert_eq!(summaries.len(), 3);
        let ids: HashSet<&str> = summaries.iter().map(|s| s.id.as_str()).collect();
        assert!(ids.contains("a"));
        assert!(ids.contains("b"));
        assert!(ids.contains("c"));
    }

    #[test]
    fn session_summary_status_reflects_state() {
        let mut done = HashSet::new();
        done.insert("ok".into());
        let mut failed = HashSet::new();
        failed.insert("fail".into());
        let spawned = HashSet::from(["ok".into(), "fail".into(), "running".into()]);
        let ctx = RoutingContext {
            spec: &FleetSpec::from_yaml(
                "fleet:\n  base_branch: main\n  sessions:\n    ok: { driver: claude }\n    fail: { driver: codex }\n    running: { driver: claude }\n  start: ok\n",
            )
            .unwrap(),
            done: &done,
            failed: &failed,
            spawned: &spawned,
            buffers: &HashMap::new(),
            task: &"test",
        };
        let summaries = SessionSummary::build_all(&ctx, 100);
        let get = |id: &str| summaries.iter().find(|s| s.id == id).unwrap();
        assert_eq!(get("ok").status, SessionStatus::Completed);
        assert_eq!(get("fail").status, SessionStatus::Failed);
        assert_eq!(get("running").status, SessionStatus::Running);
    }

    // -- DynamicRoute tests ---------------------------------------------------

    #[test]
    fn parse_llm_dynamic_route_with_driver_field() {
        let json = r#"{"actions": [{"type": "route", "target": "spy", "driver": "codex", "context": "sneak"}], "reasoning": "ok"}"#;
        // "spy" is NOT in valid_sessions, but `driver` field should allow it
        let decisions = parse_llm_response(json, &valid_sessions(), 5, "task");
        assert_eq!(decisions.len(), 1);
        match &decisions[0] {
            RouteDecision::DynamicRoute {
                target,
                payload,
                driver,
                permissions,
            } => {
                assert_eq!(target, "spy");
                assert_eq!(payload, "sneak");
                assert_eq!(*driver, DriverKind::Codex);
                assert_eq!(*permissions, PermissionPolicy::Allow);
            }
            other => panic!("expected DynamicRoute, got {other:?}"),
        }
    }

    #[test]
    fn parse_llm_dynamic_route_with_grpc_driver() {
        let json = r#"{"actions": [{"type": "route", "target": "remote", "driver": "grpc:localhost:50051", "context": "do stuff"}], "reasoning": "ok"}"#;
        let decisions = parse_llm_response(json, &valid_sessions(), 5, "task");
        assert_eq!(decisions.len(), 1);
        match &decisions[0] {
            RouteDecision::DynamicRoute {
                target, driver, ..
            } => {
                assert_eq!(target, "remote");
                assert_eq!(*driver, DriverKind::Grpc("localhost:50051".into()));
            }
            other => panic!("expected DynamicRoute, got {other:?}"),
        }
    }

    #[test]
    fn parse_llm_unknown_driver_returns_error() {
        let json = r#"{"actions": [{"type": "route", "target": "spy", "driver": "nonexistent", "context": "x"}], "reasoning": "bad"}"#;
        let decisions = parse_llm_response(json, &valid_sessions(), 5, "task");
        assert_eq!(decisions.len(), 1);
        assert!(matches!(&decisions[0], RouteDecision::Error(msg) if msg.contains("unknown driver")));
    }

    #[test]
    fn parse_llm_unknown_target_without_driver_mentions_driver_field() {
        let json = r#"{"actions": [{"type": "route", "target": "ghost", "context": "x"}], "reasoning": "bad"}"#;
        let decisions = parse_llm_response(json, &valid_sessions(), 5, "task");
        assert_eq!(decisions.len(), 1);
        assert!(matches!(&decisions[0], RouteDecision::Error(msg) if msg.contains("driver")));
    }

    // -- HybridRouting tests --------------------------------------------------

    #[tokio::test]
    async fn hybrid_routing_uses_static_when_route_matches() {
        let routes = vec![Route {
            when: Trigger::Single("coder.done".into()),
            route_to: Some("reviewer".into()),
            fan_out: None,
            collect: None,
        }];
        let client = Box::new(StubLlmClient::new(vec![
            r#"{"actions": [{"type": "complete"}], "reasoning": "should not be called"}"#.into(),
        ]));
        let llm = LlmRouting::new(
            client,
            vec![
                LlmSessionSpec { id: "coder".into(), role: None },
                LlmSessionSpec { id: "reviewer".into(), role: None },
            ],
            LlmRoutingConfig::default(),
        );
        let hybrid = HybridRouting::new(routes, llm);

        let mut done = HashSet::new();
        done.insert("coder".into());
        let mut spawned = HashSet::new();
        spawned.insert("coder".into());
        let mut buffers = HashMap::new();
        buffers.insert("coder".into(), "my code".into());
        let ctx = RoutingContext {
            spec: &FleetSpec::from_yaml(
                "fleet:\n  base_branch: main\n  sessions:\n    coder: { driver: claude }\n    reviewer: { driver: codex }\n  start: coder\n",
            )
            .unwrap(),
            done: &done,
            failed: &HashSet::new(),
            spawned: &spawned,
            buffers: &buffers,
            task: &"build it",
        };

        let decisions = hybrid.on_session_done(&ctx, &"coder".into(), StopReason::EndTurn).await;
        // Static route should match before LLM is consulted
        assert_eq!(decisions.len(), 1);
        assert!(matches!(&decisions[0], RouteDecision::Route { target, .. } if target == "reviewer"));
    }

    #[tokio::test]
    async fn hybrid_routing_falls_back_to_llm_when_no_static_match() {
        // Route that won't match (different trigger session)
        let routes = vec![Route {
            when: Trigger::Single("other-session.done".into()),
            route_to: Some("reviewer".into()),
            fan_out: None,
            collect: None,
        }];
        let client = Box::new(StubLlmClient::new(vec![
            r#"{"actions": [{"type": "route", "target": "reviewer", "context": "review this"}], "reasoning": "llm decision"}"#.into(),
        ]));
        let llm = LlmRouting::new(
            client,
            vec![
                LlmSessionSpec { id: "coder".into(), role: None },
                LlmSessionSpec { id: "reviewer".into(), role: None },
            ],
            LlmRoutingConfig::default(),
        );
        let hybrid = HybridRouting::new(routes, llm);

        let mut done = HashSet::new();
        done.insert("coder".into());
        let mut spawned = HashSet::new();
        spawned.insert("coder".into());
        let mut buffers = HashMap::new();
        buffers.insert("coder".into(), "some output".into());
        let ctx = RoutingContext {
            spec: &FleetSpec::from_yaml(
                "fleet:\n  base_branch: main\n  sessions:\n    coder: { driver: claude }\n    reviewer: { driver: codex }\n  start: coder\n",
            )
            .unwrap(),
            done: &done,
            failed: &HashSet::new(),
            spawned: &spawned,
            buffers: &buffers,
            task: &"build it",
        };

        let decisions = hybrid.on_session_done(&ctx, &"coder".into(), StopReason::EndTurn).await;
        assert_eq!(decisions.len(), 1);
        assert!(matches!(&decisions[0], RouteDecision::Route { target, .. } if target == "reviewer"));
    }

    #[tokio::test]
    async fn hybrid_routing_fan_out_not_overridden_by_llm() {
        let routes = vec![Route {
            when: Trigger::Single("lead.done".into()),
            route_to: None,
            fan_out: Some(FanOut {
                to: vec!["worker1".into(), "worker2".into()],
                split: Split::Broadcast,
            }),
            collect: None,
        }];
        let client = Box::new(StubLlmClient::new(vec![
            r#"{"actions": [{"type": "complete"}], "reasoning": "should not be called"}"#.into(),
        ]));
        let llm = LlmRouting::new(
            client,
            vec![LlmSessionSpec { id: "lead".into(), role: None }],
            LlmRoutingConfig::default(),
        );
        let hybrid = HybridRouting::new(routes, llm);

        let mut done = HashSet::new();
        done.insert("lead".into());
        let mut spawned = HashSet::new();
        spawned.insert("lead".into());
        let ctx = RoutingContext {
            spec: &FleetSpec::from_yaml(
                "fleet:\n  base_branch: main\n  sessions:\n    lead: { driver: claude }\n    worker1: { driver: codex }\n    worker2: { driver: codex }\n  start: lead\n",
            )
            .unwrap(),
            done: &done,
            failed: &HashSet::new(),
            spawned: &spawned,
            buffers: &HashMap::new(),
            task: &"parallel work",
        };

        let decisions = hybrid.on_session_done(&ctx, &"lead".into(), StopReason::EndTurn).await;
        assert_eq!(decisions.len(), 1);
        assert!(matches!(&decisions[0], RouteDecision::FanOut { targets } if targets.len() == 2));
    }
}
