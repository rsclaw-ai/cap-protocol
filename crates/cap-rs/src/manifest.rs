//! CAP agent manifest parsing and validation.
//!
//! This module implements the core manifest shape from `docs/cap-v1.md` §5.
//! It intentionally records profile names without implementing profile-specific
//! behavior.

use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Deserialize;

#[derive(Debug)]
pub enum ManifestError {
    Io(std::io::Error),
    Toml(toml::de::Error),
    Invalid(String),
    NotFound(String),
}

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ManifestError::Io(e) => write!(f, "{e}"),
            ManifestError::Toml(e) => write!(f, "{e}"),
            ManifestError::Invalid(msg) => write!(f, "{msg}"),
            ManifestError::NotFound(name) => write!(f, "manifest not found for {name}"),
        }
    }
}

impl std::error::Error for ManifestError {}

impl From<std::io::Error> for ManifestError {
    fn from(e: std::io::Error) -> Self {
        ManifestError::Io(e)
    }
}

impl From<toml::de::Error> for ManifestError {
    fn from(e: toml::de::Error) -> Self {
        ManifestError::Toml(e)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentManifest {
    pub agent: AgentSection,
    #[serde(default)]
    pub probe: Option<ProbeSection>,
    pub startup: StartupSection,
    #[serde(default)]
    pub fast_path: FastPathSection,
    pub pty: PtySection,
    #[serde(default)]
    pub parse: ParseSection,
    pub capabilities: CapabilitiesSection,
    #[serde(default)]
    pub cost: CostSection,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentSection {
    pub name: String,
    pub binary: String,
    #[serde(default)]
    pub version_match: Option<String>,
    pub profiles: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProbeSection {
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub version_pattern: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StartupSection {
    pub command: Vec<String>,
    #[serde(default)]
    pub cwd_arg: Option<String>,
    #[serde(default)]
    pub model_arg: Option<String>,
    #[serde(default)]
    pub session_id_env: Option<String>,
    pub ready_when: ReadyWhen,
    #[serde(default)]
    pub init_timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReadyWhen {
    pub pattern: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct FastPathSection {
    #[serde(default)]
    pub stream_json: FastPathValue,
    #[serde(default)]
    pub grpc: FastPathValue,
    #[serde(default)]
    pub acp_stdio: FastPathValue,
    #[serde(default)]
    pub a2a_serve_at: FastPathValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum FastPathValue {
    Bool(bool),
    Command(Vec<String>),
    Url(String),
}

impl Default for FastPathValue {
    fn default() -> Self {
        FastPathValue::Bool(false)
    }
}

impl FastPathValue {
    pub fn enabled(&self) -> bool {
        match self {
            FastPathValue::Bool(v) => *v,
            FastPathValue::Command(v) => !v.is_empty(),
            FastPathValue::Url(v) => !v.is_empty() && v != "false",
        }
    }

    pub fn command(&self) -> Option<&[String]> {
        match self {
            FastPathValue::Command(v) => Some(v.as_slice()),
            _ => None,
        }
    }

    pub fn url(&self) -> Option<&str> {
        match self {
            FastPathValue::Url(v) if v != "false" => Some(v),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PtySection {
    pub cols: u16,
    pub rows: u16,
    #[serde(default)]
    pub bracketed_paste: bool,
    #[serde(default)]
    pub sigint_cancels_turn: CancelMode,
    #[serde(default)]
    pub queued_input_supported: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelMode {
    Graceful,
    #[default]
    Hard,
    None,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParseSection {
    #[serde(default = "default_idle_patterns")]
    pub idle: Vec<String>,
    #[serde(default)]
    pub tool_call_start: Option<String>,
    #[serde(default)]
    pub tool_call_end: Option<String>,
    #[serde(default)]
    pub plan_section: Option<String>,
    #[serde(default)]
    pub thought_section: Option<String>,
    #[serde(default)]
    pub ask_yes_no: Option<String>,
    #[serde(default)]
    pub ask_options: Option<String>,
    #[serde(default)]
    pub error_lines: Vec<String>,
}

impl Default for ParseSection {
    fn default() -> Self {
        Self {
            idle: default_idle_patterns(),
            tool_call_start: None,
            tool_call_end: None,
            plan_section: None,
            thought_section: None,
            ask_yes_no: None,
            ask_options: None,
            error_lines: Vec::new(),
        }
    }
}

fn default_idle_patterns() -> Vec<String> {
    vec!["^>\\s*$".to_string(), "^❯\\s*$".to_string()]
}

#[derive(Debug, Clone, Deserialize)]
pub struct CapabilitiesSection {
    pub streaming_output: bool,
    #[serde(default)]
    pub queued_input: bool,
    #[serde(default)]
    pub mid_turn_cancel: CancelMode,
    #[serde(default)]
    pub multi_session: bool,
    #[serde(default)]
    pub input_modalities: Vec<String>,
    #[serde(default)]
    pub output_modalities: Vec<String>,
    #[serde(default)]
    pub streaming_tool_output: bool,
    #[serde(default)]
    pub ask_user: AskUserCapabilities,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AskUserCapabilities {
    #[serde(default)]
    pub yes_no: bool,
    #[serde(default)]
    pub options: bool,
    #[serde(default)]
    pub free_text: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CostSection {
    #[serde(default)]
    pub metered: bool,
    #[serde(default = "default_currency")]
    pub currency: String,
}

impl Default for CostSection {
    fn default() -> Self {
        Self {
            metered: false,
            currency: default_currency(),
        }
    }
}

fn default_currency() -> String {
    "USD".to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingKind {
    Grpc,
    StreamJson,
    AcpStdio,
    A2aHttpsSse,
    Pty,
}

impl AgentManifest {
    pub fn from_toml_str(s: &str) -> Result<Self, ManifestError> {
        let manifest: Self = toml::from_str(s)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let text = std::fs::read_to_string(path)?;
        Self::from_toml_str(&text)
    }

    pub fn validate(&self) -> Result<(), ManifestError> {
        validate_non_empty("agent.name", &self.agent.name)?;
        validate_non_empty("agent.binary", &self.agent.binary)?;
        validate_command("startup.command", &self.startup.command)?;
        validate_non_empty(
            "startup.ready_when.pattern",
            &self.startup.ready_when.pattern,
        )?;
        validate_regex(
            "startup.ready_when.pattern",
            &self.startup.ready_when.pattern,
        )?;

        if self.pty.cols == 0 || self.pty.rows == 0 {
            return Err(ManifestError::Invalid(
                "pty.cols and pty.rows must be greater than zero".into(),
            ));
        }
        if self.fast_path.stream_json.enabled()
            && let Some(cmd) = self.fast_path.stream_json.command()
        {
            validate_command("fast_path.stream_json", cmd)?;
        }
        for (name, value) in [
            ("agent.version_match", self.agent.version_match.as_deref()),
            (
                "probe.version_pattern",
                self.probe
                    .as_ref()
                    .and_then(|p| p.version_pattern.as_deref()),
            ),
        ] {
            if let Some(pattern) = value {
                validate_regex(name, pattern)?;
            }
        }
        if let Some(probe) = &self.probe
            && !probe.command.is_empty()
        {
            validate_command("probe.command", &probe.command)?;
        }
        for pattern in self.all_parse_patterns() {
            validate_regex(pattern.0, pattern.1)?;
        }
        Ok(())
    }

    pub fn binding_preferences(&self) -> Vec<BindingKind> {
        let mut out = Vec::new();
        if self.fast_path.grpc.enabled() {
            out.push(BindingKind::Grpc);
        }
        if self.fast_path.stream_json.enabled() {
            out.push(BindingKind::StreamJson);
        }
        if self.fast_path.acp_stdio.enabled() {
            out.push(BindingKind::AcpStdio);
        }
        if self.fast_path.a2a_serve_at.enabled() {
            out.push(BindingKind::A2aHttpsSse);
        }
        out.push(BindingKind::Pty);
        out
    }

    pub fn discover_by_name(name: &str) -> Result<Self, ManifestError> {
        if name.is_empty() || name.contains('\0') {
            return Err(ManifestError::Invalid(
                "manifest name must not be empty or contain NUL".into(),
            ));
        }
        if name.contains('/') || name.contains('\\') || name.contains("..") {
            return Err(ManifestError::Invalid(
                "manifest name must not contain path separators".into(),
            ));
        }
        let candidates = manifest_candidates(name);
        for path in candidates {
            if path.exists() {
                return Self::from_path(path);
            }
        }
        // Only attempt --cap-manifest discovery for safe binary names
        // (alphanumeric, '_', '-', '.'), matching the orchestrator's
        // valid_binary_name contract.
        let is_safe_name = name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
        if !is_safe_name {
            return Err(ManifestError::NotFound(name.to_string()));
        }
        match Command::new(name).arg("--cap-manifest").output() {
            Ok(output) if output.status.success() => {
                let text = String::from_utf8_lossy(&output.stdout);
                return Self::from_toml_str(&text);
            }
            _ => {}
        }
        Err(ManifestError::NotFound(name.to_string()))
    }

    fn all_parse_patterns(&self) -> Vec<(&'static str, &str)> {
        let mut patterns = Vec::new();
        for p in &self.parse.idle {
            patterns.push(("parse.idle", p.as_str()));
        }
        for (name, p) in [
            (
                "parse.tool_call_start",
                self.parse.tool_call_start.as_deref(),
            ),
            ("parse.tool_call_end", self.parse.tool_call_end.as_deref()),
            ("parse.plan_section", self.parse.plan_section.as_deref()),
            (
                "parse.thought_section",
                self.parse.thought_section.as_deref(),
            ),
            ("parse.ask_yes_no", self.parse.ask_yes_no.as_deref()),
            ("parse.ask_options", self.parse.ask_options.as_deref()),
        ] {
            if let Some(p) = p {
                patterns.push((name, p));
            }
        }
        for p in &self.parse.error_lines {
            patterns.push(("parse.error_lines", p.as_str()));
        }
        patterns
    }
}

fn manifest_candidates(name: &str) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let direct = PathBuf::from(name);
    if direct.components().count() > 1 || direct.extension().is_some() {
        paths.push(direct);
    }
    if let Some(home) = std::env::var_os("HOME") {
        paths.push(
            PathBuf::from(home)
                .join(".config")
                .join("cap")
                .join("agents")
                .join(format!("{name}.toml")),
        );
    }
    paths.push(PathBuf::from("/usr/share/cap-agents").join(format!("{name}.toml")));
    paths.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("examples")
            .join(format!("{name}.toml")),
    );
    paths
}

fn validate_non_empty(field: &str, value: &str) -> Result<(), ManifestError> {
    if value.trim().is_empty() {
        return Err(ManifestError::Invalid(format!("{field} must not be empty")));
    }
    Ok(())
}

fn validate_command(field: &str, cmd: &[String]) -> Result<(), ManifestError> {
    if cmd.is_empty() {
        return Err(ManifestError::Invalid(format!("{field} must not be empty")));
    }
    for part in cmd {
        if part.is_empty() {
            return Err(ManifestError::Invalid(format!(
                "{field} contains an empty argv segment"
            )));
        }
        if part.contains('\0') {
            return Err(ManifestError::Invalid(format!("{field} contains NUL")));
        }
    }
    Ok(())
}

fn validate_regex(field: &str, pattern: &str) -> Result<(), ManifestError> {
    for needle in [
        "(?=", "(?!", "(?<=", "(?<!", "\\1", "\\2", "\\3", "\\4", "\\5",
    ] {
        if pattern.contains(needle) {
            return Err(ManifestError::Invalid(format!(
                "unsupported regex feature in {field}: {pattern}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_example_manifest_and_applies_defaults() {
        let toml = r#"
[agent]
name = "demo"
binary = "demo"
profiles = []

[startup]
command = ["demo"]
ready_when = { pattern = "^> $" }

[pty]
cols = 120
rows = 40

[capabilities]
streaming_output = true
"#;
        let manifest = AgentManifest::from_toml_str(toml).unwrap();
        assert_eq!(manifest.agent.name, "demo");
        assert!(!manifest.pty.bracketed_paste);
        assert_eq!(manifest.pty.sigint_cancels_turn, CancelMode::Hard);
        assert_eq!(manifest.parse.idle, vec!["^>\\s*$", "^❯\\s*$"]);
    }

    #[test]
    fn rejects_missing_required_fields() {
        let err = AgentManifest::from_toml_str("[agent]\nname = \"bad\"\n").unwrap_err();
        assert!(err.to_string().contains("missing field"));
    }

    #[test]
    fn rejects_regex_lookaround() {
        let toml = r#"
[agent]
name = "demo"
binary = "demo"
profiles = []

[startup]
command = ["demo"]
ready_when = { pattern = "(?=bad)" }

[pty]
cols = 120
rows = 40

[capabilities]
streaming_output = true
"#;
        let err = AgentManifest::from_toml_str(toml).unwrap_err();
        assert!(err.to_string().contains("unsupported regex"));
    }

    #[test]
    fn binding_preferences_follow_spec_priority() {
        let toml = r#"
[agent]
name = "demo"
binary = "demo"
profiles = []

[startup]
command = ["demo"]
ready_when = { pattern = "^> $" }

[fast_path]
stream_json = ["demo", "--json"]
grpc = true
acp_stdio = true
a2a_serve_at = "http://localhost:4000"

[pty]
cols = 120
rows = 40

[capabilities]
streaming_output = true
"#;
        let manifest = AgentManifest::from_toml_str(toml).unwrap();
        assert_eq!(
            manifest.binding_preferences(),
            vec![
                BindingKind::Grpc,
                BindingKind::StreamJson,
                BindingKind::AcpStdio,
                BindingKind::A2aHttpsSse,
                BindingKind::Pty,
            ]
        );
    }
}
