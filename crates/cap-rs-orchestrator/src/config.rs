//! Declarative `fleet.yaml` schema + validation.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::OrchestratorError;

pub type SessionId = String;

/// Top-level document: `{ fleet: { ... } }`.
#[derive(Debug, Clone, Deserialize)]
pub struct FleetSpec {
    pub fleet: Fleet,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Fleet {
    pub base_branch: String,
    #[serde(default)]
    pub task: Option<String>,
    /// Fleet-level permission default; per-session may override.
    #[serde(default)]
    pub permissions: PermissionPolicy,
    pub sessions: BTreeMap<SessionId, SessionSpec>,
    pub start: Start,
    #[serde(default)]
    pub routes: Vec<Route>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionSpec {
    pub driver: DriverKind,
    /// `None` means "inherit the fleet-level policy".
    #[serde(default)]
    pub permissions: Option<PermissionPolicy>,
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

/// `claude` | `codex` | `pty:<command>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverKind {
    Claude,
    Codex,
    Pty(String),
}

impl<'de> Deserialize<'de> for DriverKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(match s.as_str() {
            "claude" => DriverKind::Claude,
            "codex" => DriverKind::Codex,
            other => match other.strip_prefix("pty:") {
                Some(cmd) if !cmd.is_empty() => DriverKind::Pty(cmd.to_string()),
                _ => return Err(serde::de::Error::custom(format!(
                    "unknown driver kind '{other}' (expected claude | codex | pty:<cmd>)"
                ))),
            },
        })
    }
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
        } else {
            Ok(Action::Collect(self.collect.unwrap()))
        }
    }
}

impl FleetSpec {
    pub fn from_yaml(s: &str) -> Result<Self, OrchestratorError> {
        serde_yaml::from_str(s).map_err(|e| OrchestratorError::Config(e.to_string()))
    }

    /// Static validation: every referenced session exists, every trigger uses
    /// the `.done` form, and every route has exactly one action.
    pub fn validate(&self) -> Result<(), OrchestratorError> {
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
                    OrchestratorError::Config(format!(
                        "trigger '{token}' must end in '.done'"
                    ))
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
            DriverKind::Pty("opencode".into())
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
}
