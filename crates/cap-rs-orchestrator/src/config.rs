//! Declarative `fleet.yaml` schema + validation.

use std::collections::BTreeMap;
use std::collections::HashMap;

use serde::Deserialize;
use url::Url;

use crate::OrchestratorError;

pub type SessionId = String;

/// Top-level document: `{ fleet: { ... } }`.
#[derive(Debug, Clone, Deserialize)]
pub struct FleetSpec {
    pub fleet: Fleet,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutingMode {
    #[default]
    Static,
    Llm,
    Hybrid,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LlmConfig {
    #[serde(default)]
    pub command: Option<Vec<String>>,
    pub system_prompt: Option<String>,
    pub timeout_secs: Option<u64>,
    pub max_decisions: Option<usize>,
    pub max_context_chars: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Fleet {
    pub base_branch: String,
    #[serde(default)]
    pub task: Option<String>,
    /// Fleet-level permission default; per-session may override.
    #[serde(default)]
    pub permissions: PermissionPolicy,
    /// Optional total budget for all sessions in the fleet.
    #[serde(default)]
    pub budget_usd: Option<f64>,
    /// Routing mode: static (default), llm, or hybrid.
    #[serde(default)]
    pub mode: RoutingMode,
    /// LLM orchestrator configuration (required when mode == llm or hybrid).
    pub llm: Option<LlmConfig>,
    pub sessions: BTreeMap<SessionId, SessionSpec>,
    pub start: Start,
    #[serde(default)]
    pub routes: Vec<Route>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionSpec {
    #[serde(default)]
    pub driver: Option<DriverKind>,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub manifest: Option<String>,
    /// `None` means "inherit the fleet-level policy".
    #[serde(default)]
    pub permissions: Option<PermissionPolicy>,
    /// Human-readable role description for LLM-driven orchestration (e.g. "code reviewer").
    pub role: Option<String>,
}

impl SessionSpec {
    pub fn driver_kind(&self) -> Option<DriverKind> {
        self.driver
            .clone()
            .or_else(|| self.agent.as_deref().and_then(DriverKind::parse))
            .or_else(|| self.manifest.as_deref().and_then(driver_kind_from_manifest))
    }

    pub fn descriptor(&self) -> String {
        if let Some(driver) = &self.driver {
            format!("{driver:?}")
        } else if let Some(agent) = &self.agent {
            format!("agent:{agent}")
        } else if let Some(manifest) = &self.manifest {
            format!("manifest:{manifest}")
        } else {
            "unconfigured".into()
        }
    }
}

fn driver_kind_from_manifest(path: &str) -> Option<DriverKind> {
    use cap_rs::manifest::AgentManifest;

    let manifest = AgentManifest::from_path(path)
        .or_else(|_| {
            AgentManifest::from_path(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("..")
                    .join("..")
                    .join(path),
            )
        })
        .ok()?;
    driver_kind_from_agent_manifest(&manifest)
}

fn driver_kind_from_agent_manifest(
    manifest: &cap_rs::manifest::AgentManifest,
) -> Option<DriverKind> {
    use cap_rs::manifest::BindingKind;

    for binding in manifest.binding_preferences() {
        match binding {
            BindingKind::Grpc => {
                if let Some(url) = manifest.fast_path.grpc.url() {
                    return Some(DriverKind::Grpc(
                        url.trim_start_matches("http://").to_string(),
                    ));
                }
                if manifest.agent.name == "openclaude" || manifest.agent.binary == "openclaude" {
                    return Some(DriverKind::OpenClaude);
                }
            }
            BindingKind::StreamJson => match manifest.agent.name.as_str() {
                "claude-code" => return Some(DriverKind::Claude),
                "openclaude" => return Some(DriverKind::OpenClaude),
                "opencode" => return Some(DriverKind::OpenCode),
                _ => return Some(DriverKind::Pty(manifest.agent.binary.clone())),
            },
            BindingKind::AcpStdio => {
                return Some(DriverKind::Acp(manifest.agent.binary.clone()));
            }
            BindingKind::A2aHttpsSse => {
                if let Some(url) = manifest.fast_path.a2a_serve_at.url() {
                    return Some(DriverKind::A2a(url.to_string()));
                }
            }
            BindingKind::Pty => {
                return manifest
                    .startup
                    .command
                    .first()
                    .cloned()
                    .map(DriverKind::Pty);
            }
        }
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionPolicy {
    #[default]
    Ask,
    Allow,
    Deny,
    Bypass,
}

/// `claude` | `openclaude` | `codex` | `opencode` | `aider` | `grpc:<addr>` | `acp:<command>` | `pty:<command>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverKind {
    Claude,
    OpenClaude,
    Codex,
    /// OpenCode via stream-json (Claude Code-compatible NDJSON frames).
    /// Higher fidelity than ACP: token-level deltas, no handshake overhead.
    OpenCode,
    /// Aider chat via PTY (<https://github.com/paul-gauthier/aider>).
    Aider,
    /// Structured Agent Client Protocol agent (e.g. `acp:opencode`).
    Acp(String),
    /// A2A HTTPS+SSE endpoint (e.g. `a2a:http://127.0.0.1:4000`).
    A2a(String),
    /// OpenClaude gRPC server (e.g. `grpc:localhost:50051`).
    Grpc(String),
    Pty(String),
}

impl DriverKind {
    /// Parse a driver kind from its string representation (reverse of the
    /// display/deserialization format).
    pub fn parse(s: &str) -> Option<Self> {
        parse_driver_kind(s).ok()
    }
}

fn parse_driver_kind(s: &str) -> Result<DriverKind, String> {
    match s {
        "claude" => Ok(DriverKind::Claude),
        "openclaude" => Ok(DriverKind::OpenClaude),
        "codex" => Ok(DriverKind::Codex),
        "opencode" => Ok(DriverKind::OpenCode),
        "aider" => Ok(DriverKind::Aider),
        other => {
            if let Some(addr) = other.strip_prefix("grpc:") {
                if !valid_grpc_address(addr) {
                    return Err(format!(
                        "invalid grpc address '{addr}' — expected host:port (e.g. 'localhost:50051')"
                    ));
                }
                Ok(DriverKind::Grpc(addr.to_string()))
            } else if let Some(url) = other.strip_prefix("a2a:") {
                if !valid_a2a_url(url) {
                    return Err(format!(
                        "invalid a2a url '{url}' — expected http(s)://host[:port][/path]"
                    ));
                }
                Ok(DriverKind::A2a(url.to_string()))
            } else if let Some(cmd) = other.strip_prefix("acp:") {
                if cmd.is_empty() || !valid_binary_name(cmd) {
                    return Err(format!(
                        "invalid acp command '{cmd}' — expected a binary name"
                    ));
                }
                Ok(DriverKind::Acp(cmd.to_string()))
            } else if let Some(cmd) = other.strip_prefix("pty:") {
                if cmd.is_empty() || !valid_binary_name(cmd) {
                    return Err(format!(
                        "invalid pty command '{cmd}' — expected a binary name"
                    ));
                }
                Ok(DriverKind::Pty(cmd.to_string()))
            } else {
                Err(format!(
                    "unknown driver kind '{other}' (expected claude | openclaude | codex | opencode | aider | grpc:<host:port> | a2a:<http-url> | acp:<cmd> | pty:<cmd>)"
                ))
            }
        }
    }
}

impl<'de> Deserialize<'de> for DriverKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        parse_driver_kind(&s).map_err(serde::de::Error::custom)
    }
}

/// Return a list of all supported driver kind descriptors (for `cap list-drivers`).
pub fn list_driver_kinds() -> Vec<&'static str> {
    vec![
        "claude       Claude Code CLI (stream-json)",
        "openclaude   OpenClaude CLI (stream-json, Anthropic SDK-compatible)",
        "codex        OpenAI Codex CLI (MCP)",
        "opencode     OpenCode CLI (stream-json, Claude Code-compatible)",
        "aider        Aider chat via PTY (https://github.com/paul-gauthier/aider)",
        "a2a:<url>    A2A HTTPS+SSE endpoint (e.g. a2a:http://127.0.0.1:4000)",
        "acp:<cmd>    Any ACP-compatible agent (e.g. acp:opencode)",
        "grpc:<addr>  OpenClaude gRPC server (e.g. grpc:localhost:50051)",
        "pty:<cmd>    PTY fallback for any CLI agent (e.g. pty:opencode)",
    ]
}

/// Return a default fleet.yaml template string (for `cap init`).
pub fn default_fleet_yaml() -> String {
    r#"# CAP fleet configuration — see docs/quickstart.md
fleet:
  # Git branch for worktree isolation
  base_branch: main

  # Default task (override with --task)
  task: "Write a hello world Rust program and compile it"

  # Permission policy: ask | allow | deny | bypass (default: ask)
  permissions: ask

  # Define your agent sessions
  sessions:
    coder:    { driver: claude, permissions: allow }
    reviewer: { driver: codex,  permissions: allow }

  # Start here
  start: coder

  # Route definitions
  routes:
    - { when: coder.done, route_to: reviewer }
"#
    .to_string()
}

/// Entry point: one session or several launched at once.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Start {
    One(SessionId),
    Many(Vec<SessionId>),
}

impl Start {
    pub fn sessions(&self) -> Vec<SessionId> {
        match self {
            Start::One(s) => vec![s.clone()],
            Start::Many(v) => v.clone(),
        }
    }
}

/// A `when:` trigger — a single `X.done` or a list (a join).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Trigger {
    Single(String),
    Join(Vec<String>),
}

/// One routing edge. Exactly one of `route_to` / `fan_out` / `collect` must be set.
#[derive(Debug, Clone, Deserialize)]
pub struct Route {
    pub when: Trigger,
    #[serde(default)]
    pub route_to: Option<SessionId>,
    #[serde(default)]
    pub fan_out: Option<FanOut>,
    #[serde(default)]
    pub collect: Option<Collect>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FanOut {
    pub to: Vec<SessionId>,
    #[serde(default)]
    pub split: Split,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Split {
    #[default]
    Broadcast,
    BySubtask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Collect {
    Human,
}

/// The resolved action of a [`Route`].
#[derive(Debug, Clone)]
pub enum Action {
    RouteTo(SessionId),
    FanOut(FanOut),
    Collect(Collect),
}

impl Trigger {
    /// The raw tokens (e.g. `"coder.done"`) referenced by this trigger.
    fn raw_tokens(&self) -> Vec<&str> {
        match self {
            Trigger::Single(s) => vec![s.as_str()],
            Trigger::Join(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }
}

impl Route {
    /// Session ids this route fires on (the `.done` suffix removed).
    ///
    /// Assumes the spec has been validated; each token must end in `.done`.
    /// Call `validate()` first.
    pub fn trigger_sessions(&self) -> Vec<String> {
        self.when
            .raw_tokens()
            .iter()
            .map(|t| {
                debug_assert!(
                    t.ends_with(".done"),
                    "trigger token '{t}' missing .done suffix; validate() not run?"
                );
                t.strip_suffix(".done").unwrap_or(t).to_string()
            })
            .collect()
    }

    /// Resolve the single action, erroring if zero or more than one is set.
    pub fn action(&self) -> Result<Action, OrchestratorError> {
        let count = self.route_to.is_some() as u8
            + self.fan_out.is_some() as u8
            + self.collect.is_some() as u8;
        if count != 1 {
            return Err(OrchestratorError::Config(format!(
                "route on {:?} must have exactly one of route_to/fan_out/collect (found {count})",
                self.trigger_sessions()
            )));
        }
        if let Some(to) = &self.route_to {
            Ok(Action::RouteTo(to.clone()))
        } else if let Some(f) = &self.fan_out {
            Ok(Action::FanOut(f.clone()))
        } else if let Some(c) = self.collect {
            Ok(Action::Collect(c))
        } else {
            Err(OrchestratorError::Config(
                "route must have exactly one of route_to/fan_out/collect".into(),
            ))
        }
    }
}

/// Safe session id: non-empty, ASCII alphanumeric / `_` / `-`, no leading `-`.
pub fn valid_session_id(id: &str) -> bool {
    !id.is_empty()
        && !id.starts_with('-')
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Safe binary name: non-empty, alphanumeric / `_` / `-` / `.` (e.g. `codex`, `opencode`, `my-agent`).
/// Rejects paths, args, shell metacharacters.
fn valid_binary_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains(' ')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

/// Safe gRPC address: `host:port` where host is a valid hostname or IP, and
/// port is numeric. Rejects paths, schemes, and other URI components to
/// prevent SSRF and config injection.
fn valid_grpc_address(addr: &str) -> bool {
    if addr.is_empty() {
        return false;
    }
    // Reject scheme prefixes (http://, https://, unix:, etc.)
    if addr.contains("://") || addr.starts_with("unix:") {
        return false;
    }
    // Must have exactly one colon separating host and port.
    let Some((host, port)) = addr.rsplit_once(':') else {
        return false;
    };
    if host.is_empty() || port.is_empty() {
        return false;
    }
    // Port must be numeric.
    if !port.chars().all(|c| c.is_ascii_digit()) {
        return false;
    }
    // Host: alphanumeric, dots, hyphens, underscores, or IPv6 brackets.
    host.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '[' | ']' | ':'))
}

fn valid_a2a_url(url: &str) -> bool {
    let Ok(parsed) = Url::parse(url) else {
        return false;
    };
    matches!(parsed.scheme(), "http" | "https")
        && parsed
            .host_str()
            .is_some_and(|host| !host.is_empty() && !host.starts_with('-') && !host.contains(".."))
        && parsed.password().is_none()
        && parsed.username().is_empty()
}

/// Safe git ref: non-empty, no `..`, no leading `-`, chars limited to
/// alphanumeric / `_` `-` `.` `/`.
fn valid_git_ref(r: &str) -> bool {
    !r.is_empty()
        && !r.starts_with('-')
        && !r.contains("..")
        && r.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/'))
}

fn validate_agent_name(name: &str) -> Result<(), OrchestratorError> {
    if valid_binary_name(name) {
        Ok(())
    } else {
        Err(OrchestratorError::Config(format!(
            "invalid agent name '{name}'"
        )))
    }
}

fn validate_manifest_path(path: &str) -> Result<(), OrchestratorError> {
    if path.is_empty()
        || path.starts_with('-')
        || path.contains('\0')
        || path.contains("..")
        || path.contains('\\')
    {
        return Err(OrchestratorError::Config(format!(
            "invalid manifest path '{path}'"
        )));
    }
    Ok(())
}

impl FleetSpec {
    pub fn from_yaml(s: &str) -> Result<Self, OrchestratorError> {
        serde_yaml::from_str(s).map_err(|e| OrchestratorError::Config(e.to_string()))
    }

    /// Static validation: every referenced session exists, every trigger uses
    /// the `.done` form, and every route has exactly one action.
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        if !valid_git_ref(&self.fleet.base_branch) {
            return Err(OrchestratorError::Config(format!(
                "invalid base_branch '{}'",
                self.fleet.base_branch
            )));
        }
        for id in self.fleet.sessions.keys() {
            if !valid_session_id(id) {
                return Err(OrchestratorError::Config(format!(
                    "invalid session id '{id}' (allowed: letters, digits, '_', '-'; no leading '-')"
                )));
            }
        }
        for (id, session) in &self.fleet.sessions {
            let configured = session.driver.is_some() as u8
                + session.agent.is_some() as u8
                + session.manifest.is_some() as u8;
            if configured != 1 {
                return Err(OrchestratorError::Config(format!(
                    "session '{id}' must set exactly one of driver, agent, or manifest"
                )));
            }
            if let Some(agent) = &session.agent {
                validate_agent_name(agent)?;
            }
            if let Some(manifest) = &session.manifest {
                validate_manifest_path(manifest)?;
            }
        }

        let known = |id: &str| self.fleet.sessions.contains_key(id);
        let bad = |what: &str, id: &str| {
            Err(OrchestratorError::Config(format!(
                "{what} references unknown session '{id}'"
            )))
        };

        for s in self.fleet.start.sessions() {
            if !known(&s) {
                return bad("start", &s);
            }
        }
        for route in &self.fleet.routes {
            if route.when.raw_tokens().is_empty() {
                return Err(OrchestratorError::Config(
                    "route trigger must reference at least one session".into(),
                ));
            }
            for token in route.when.raw_tokens() {
                let id = token.strip_suffix(".done").ok_or_else(|| {
                    OrchestratorError::Config(format!("trigger '{token}' must end in '.done'"))
                })?;
                if !known(id) {
                    return bad("trigger", id);
                }
            }
            match route.action()? {
                Action::RouteTo(to) => {
                    if !known(&to) {
                        return bad("route_to", &to);
                    }
                }
                Action::FanOut(f) => {
                    if f.to.is_empty() {
                        return Err(OrchestratorError::Config(
                            "fan_out must have at least one target".into(),
                        ));
                    }
                    for to in &f.to {
                        if !known(to) {
                            return bad("fan_out target", to);
                        }
                    }
                    if matches!(f.split, Split::BySubtask) && route.trigger_sessions().len() > 1 {
                        return Err(OrchestratorError::Config(
                            "fan_out split: by_subtask requires a single-session trigger".into(),
                        ));
                    }
                }
                Action::Collect(_) => {}
            }
        }
        self.detect_route_cycles()?;
        Ok(())
    }

    /// DFS cycle detection on the route graph. Every edge goes from a trigger
    /// session to a target session (route_to / fan_out). `collect: human` is
    /// terminal and creates no edges. A cycle would loop forever at runtime.
    fn detect_route_cycles(&self) -> Result<(), OrchestratorError> {
        let ids: Vec<String> = self.fleet.sessions.keys().cloned().collect();
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for id in &ids {
            adj.entry(id.as_str()).or_default();
        }

        // Build adjacency list from routes. We iterate in two passes so the
        // borrows of `triggers` / `targets` do not outlive their scope.
        let edges: Vec<(Vec<String>, Vec<String>)> = self
            .fleet
            .routes
            .iter()
            .map(|route| {
                let triggers = route.trigger_sessions();
                let targets: Vec<String> = match route.action()? {
                    Action::RouteTo(to) => vec![to],
                    Action::FanOut(ref f) => f.to.clone(),
                    Action::Collect(_) => Vec::new(),
                };
                Ok::<_, OrchestratorError>((triggers, targets))
            })
            .collect::<Result<Vec<_>, _>>()?;

        for (triggers, targets) in &edges {
            for t in triggers {
                for target in targets {
                    adj.entry(t.as_str()).or_default().push(target.as_str());
                }
            }
        }

        enum Color {
            White,
            Gray,
            Black,
        }
        let mut color: HashMap<&str, Color> =
            ids.iter().map(|k| (k.as_str(), Color::White)).collect();
        let mut path: Vec<&str> = Vec::new();

        fn visit<'a>(
            node: &'a str,
            adj: &HashMap<&'a str, Vec<&'a str>>,
            color: &mut HashMap<&'a str, Color>,
            path: &mut Vec<&'a str>,
        ) -> Result<(), OrchestratorError> {
            match color[node] {
                Color::Black => return Ok(()),
                Color::Gray => {
                    let cycle_start = path
                        .iter()
                        .position(|n| *n == node)
                        .expect("cycle node must be on current DFS path");
                    let cycle: Vec<&str> = path[cycle_start..].to_vec();
                    return Err(OrchestratorError::Config(format!(
                        "route cycle detected: {}",
                        cycle.join(" → ")
                    )));
                }
                Color::White => {}
            }
            color.insert(node, Color::Gray);
            path.push(node);
            if let Some(neighbors) = adj.get(node) {
                for neighbor in neighbors {
                    visit(neighbor, adj, color, path)?;
                }
            }
            path.pop();
            color.insert(node, Color::Black);
            Ok(())
        }

        for node in &ids {
            if matches!(color[node.as_str()], Color::White) {
                visit(node.as_str(), &adj, &mut color, &mut path)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PIPELINE: &str = r#"
fleet:
  base_branch: main
  task: "do the thing"
  sessions:
    coder: { driver: claude }
    reviewer: { driver: codex, permissions: allow }
  start: coder
  routes:
    - { when: coder.done,    route_to: reviewer }
"#;

    #[test]
    fn parses_pipeline() {
        let spec = FleetSpec::from_yaml(PIPELINE).unwrap();
        assert_eq!(spec.fleet.base_branch, "main");
        assert_eq!(spec.fleet.sessions.len(), 2);
        assert_eq!(spec.fleet.permissions, PermissionPolicy::Ask); // default
        assert_eq!(
            spec.fleet.sessions["reviewer"].permissions,
            Some(PermissionPolicy::Allow)
        );
        match &spec.fleet.start {
            Start::One(s) => assert_eq!(s, "coder"),
            other => panic!("wrong start: {other:?}"),
        }
        spec.validate().unwrap();
    }

    #[test]
    fn parses_manifest_backed_session() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    coder: { manifest: examples/claude-code.toml, permissions: allow }
    reviewer: { agent: codex, permissions: allow }
  start: coder
  routes:
    - { when: coder.done, route_to: reviewer }
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert_eq!(
            spec.fleet.sessions["coder"].manifest.as_deref(),
            Some("examples/claude-code.toml")
        );
        assert_eq!(
            spec.fleet.sessions["reviewer"].agent.as_deref(),
            Some("codex")
        );
        assert_eq!(
            spec.fleet.sessions["coder"].driver_kind(),
            Some(DriverKind::Claude)
        );
        assert_eq!(
            spec.fleet.sessions["reviewer"].driver_kind(),
            Some(DriverKind::Codex)
        );
        spec.validate().unwrap();
    }

    #[test]
    fn parses_fan_out_and_join() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    lead: { driver: claude }
    a: { driver: codex }
    b: { driver: codex }
    rev: { driver: claude }
  start: lead
  routes:
    - when: lead.done
      fan_out: { to: [a, b], split: by_subtask }
    - when: [a.done, b.done]
      route_to: rev
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        let r0 = &spec.fleet.routes[0];
        assert_eq!(r0.trigger_sessions(), vec!["lead"]);
        match r0.action().unwrap() {
            Action::FanOut(f) => {
                assert_eq!(f.to, vec!["a", "b"]);
                assert_eq!(f.split, Split::BySubtask);
            }
            other => panic!("wrong action: {other:?}"),
        }
        let r1 = &spec.fleet.routes[1];
        assert_eq!(r1.trigger_sessions(), vec!["a", "b"]); // join
        spec.validate().unwrap();
    }

    #[test]
    fn rejects_route_to_unknown_session() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    coder: { driver: claude }
  start: coder
  routes:
    - { when: coder.done, route_to: ghost }
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        let err = spec.validate().unwrap_err();
        assert!(format!("{err}").contains("ghost"), "got: {err}");
    }

    #[test]
    fn rejects_route_with_two_actions() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    a: { driver: claude }
    b: { driver: claude }
  start: a
  routes:
    - when: a.done
      route_to: b
      collect: human
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn parses_openclaude_driver_kind() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    oc: { driver: openclaude }
  start: oc
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert_eq!(
            spec.fleet.sessions["oc"].driver,
            Some(DriverKind::OpenClaude)
        );
    }

    #[test]
    fn parses_opencode_driver_kind() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    oc: { driver: opencode }
  start: oc
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert_eq!(spec.fleet.sessions["oc"].driver, Some(DriverKind::OpenCode));
    }

    #[test]
    fn parses_pty_driver_kind() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    oc: { driver: "pty:opencode" }
  start: oc
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert_eq!(
            spec.fleet.sessions["oc"].driver,
            Some(DriverKind::Pty("opencode".into()))
        );
    }

    #[test]
    fn rejects_empty_join_trigger() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    a: { driver: claude }
  start: a
  routes:
    - when: []
      route_to: a
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_empty_fan_out() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    a: { driver: claude }
  start: a
  routes:
    - when: a.done
      fan_out: { to: [] }
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_path_escaping_session_id() {
        for bad in ["../evil", "/tmp/evil", "a/b", "a.b", "-x"] {
            let yaml = format!(
                "fleet:\n  base_branch: main\n  sessions:\n    \"{bad}\": {{ driver: claude }}\n  start: \"{bad}\"\n"
            );
            let spec = FleetSpec::from_yaml(&yaml).unwrap();
            assert!(spec.validate().is_err(), "id '{bad}' should be rejected");
        }
    }

    #[test]
    fn rejects_bad_base_branch() {
        let yaml = "fleet:\n  base_branch: \"../../etc\"\n  sessions:\n    a: { driver: claude }\n  start: a\n";
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_self_loop_route() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    a: { driver: claude }
  start: a
  routes:
    - { when: a.done, route_to: a }
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        let err = spec.validate().unwrap_err();
        assert!(format!("{err}").contains("cycle"), "got: {err}");
    }

    #[test]
    fn rejects_two_node_cycle() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    a: { driver: claude }
    b: { driver: claude }
  start: a
  routes:
    - { when: a.done, route_to: b }
    - { when: b.done, route_to: a }
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        let err = spec.validate().unwrap_err();
        assert!(format!("{err}").contains("cycle"), "got: {err}");
    }

    #[test]
    fn rejects_fan_out_cycle() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    a: { driver: claude }
    b: { driver: claude }
    c: { driver: claude }
  start: a
  routes:
    - when: a.done
      fan_out: { to: [b, c] }
    - when: [b.done, c.done]
      route_to: a
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        let err = spec.validate().unwrap_err();
        assert!(format!("{err}").contains("cycle"), "got: {err}");
    }

    #[test]
    fn accepts_dag_route() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    a: { driver: claude }
    b: { driver: claude }
    c: { driver: claude }
  start: a
  routes:
    - { when: a.done, route_to: b }
    - { when: b.done, route_to: c }
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        spec.validate().unwrap();
    }

    #[test]
    fn rejects_by_subtask_with_join_trigger() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    a: { driver: claude }
    b: { driver: claude }
    c: { driver: claude }
  start: [a, b]
  routes:
    - when: [a.done, b.done]
      fan_out: { to: [c], split: by_subtask }
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn parses_grpc_driver_with_valid_address() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    agent: { driver: "grpc:localhost:50051" }
  start: agent
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert_eq!(
            spec.fleet.sessions["agent"].driver,
            Some(DriverKind::Grpc("localhost:50051".into()))
        );
        spec.validate().unwrap();
    }

    #[test]
    fn rejects_grpc_with_invalid_address() {
        for bad in [
            "",
            "localhost",
            "http://localhost:50051",
            "localhost:abc",
            ":50051",
        ] {
            let yaml = format!(
                "fleet:\n  base_branch: main\n  sessions:\n    a: {{ driver: \"grpc:{bad}\" }}\n  start: a\n"
            );
            let result = FleetSpec::from_yaml(&yaml);
            assert!(result.is_err(), "address '{bad}' should be rejected");
        }
    }

    #[test]
    fn parses_a2a_driver_with_valid_url() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    agent: { driver: "a2a:http://127.0.0.1:4000/agent" }
  start: agent
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert_eq!(
            spec.fleet.sessions["agent"].driver,
            Some(DriverKind::A2a("http://127.0.0.1:4000/agent".into()))
        );
        spec.validate().unwrap();
    }

    #[test]
    fn rejects_a2a_driver_with_invalid_url() {
        for bad in [
            "",
            "localhost:4000",
            "ftp://localhost",
            "http://user@host",
            "http://..",
        ] {
            let yaml = format!(
                "fleet:\n  base_branch: main\n  sessions:\n    a: {{ driver: \"a2a:{bad}\" }}\n  start: a\n"
            );
            let result = FleetSpec::from_yaml(&yaml);
            assert!(result.is_err(), "url '{bad}' should be rejected");
        }
    }

    #[test]
    fn manifest_a2a_fast_path_resolves_to_a2a_driver() {
        let manifest = cap_rs::manifest::AgentManifest::from_toml_str(
            r#"
[cap]
protocol_version = "1.0"

[agent]
name = "remote"
binary = "remote-agent"
args = []
profiles = []

[startup]
command = ["remote-agent"]
ready_when = { pattern = "ready" }

[agent.io]
transport = "a2a_https_sse"

[fast_path]
a2a_serve_at = "https://agent.example.test/a2a"

[pty]
cols = 80
rows = 24

[capabilities]
streaming_output = true
input_modalities = ["text"]
output_modalities = ["text"]
"#,
        )
        .unwrap();

        assert_eq!(
            driver_kind_from_agent_manifest(&manifest),
            Some(DriverKind::A2a("https://agent.example.test/a2a".into()))
        );
    }
    #[test]
    fn accepts_grpc_with_ip_address() {
        let yaml = r#"
fleet:
  base_branch: main
  sessions:
    agent: { driver: "grpc:127.0.0.1:8080" }
  start: agent
"#;
        let spec = FleetSpec::from_yaml(yaml).unwrap();
        assert_eq!(
            spec.fleet.sessions["agent"].driver,
            Some(DriverKind::Grpc("127.0.0.1:8080".into()))
        );
    }
}
