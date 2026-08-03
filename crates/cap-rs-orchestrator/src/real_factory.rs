//! Builds real `cap-rs` drivers. Each first-class agent name maps to its
//! highest-fidelity structured path:
//! - `claude` → `stream-json`
//! - `openclaude` → `stream-json` (Anthropic SDK-compatible)
//! - `opencode` → try stream-json optimistically; fallback to `acp:opencode`
//! - `codex` → try stream-json optimistically; fallback to `codex_mcp`
//! - `qoder` → `stream-json` (Claude Code-compatible NDJSON)
//! - `acp:<cmd>` → ACP over stdio
//!
//! For `opencode` and `codex`, stream-json is optional. We check `--help`
//! before spawning and use the native driver (ACP / MCP) when the installed
//! CLI does not advertise stream-json support. An early-exit check remains as
//! a guard for forks whose help output is inaccurate.
//!
//! `pty:<cmd>` remains the universal screen-scraping fallback; `pty:codex`
//! still works (with the codex-tuned [`TuiParser::codex`]) if a caller needs
//! the old behavior. `pty:openclaude` uses a tuned parser with `>` prompt
//! markers from the reference manifest.
//!
//! `grpc:<addr>` is the alternative gRPC path with reduced event detail.

use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use cap_rs::driver::Driver;
use cap_rs::driver::a2a::A2aDriver;
use cap_rs::driver::acp::AcpDriver;
use cap_rs::driver::codex_mcp::CodexMcpDriver;
use cap_rs::driver::grpc::GrpcDriver;
use cap_rs::driver::pty::{PtyDriver, TuiParser};
use cap_rs::driver::stream_json::ClaudeCodeDriver;
use tracing::{info, warn};

use crate::OrchestratorError;
use crate::config::{DriverKind, PermissionPolicy, SessionId};
use crate::factory::DriverFactory;

static PROBE_CACHE: Mutex<Option<std::collections::HashMap<String, bool>>> = Mutex::new(None);

fn default_codex_stream_bin() -> String {
    std::env::var("CODEX_STREAM_BIN")
        .or_else(|_| std::env::var("CODEX_BIN"))
        .unwrap_or_else(|_| "codex".into())
}

fn default_codex_mcp_bin() -> String {
    codex_mcp_bin(std::env::var("CODEX_MCP_BIN").ok())
}

fn codex_mcp_bin(configured: Option<String>) -> String {
    configured.unwrap_or_else(|| "codex".into())
}

fn claude_settings(policy: PermissionPolicy) -> (Option<&'static str>, bool) {
    match policy {
        PermissionPolicy::Ask => (Some("manual"), false),
        PermissionPolicy::Allow => (Some("acceptEdits"), false),
        PermissionPolicy::Deny => (Some("dontAsk"), false),
        PermissionPolicy::Bypass => (None, true),
    }
}

fn qoder_settings(policy: PermissionPolicy) -> (Option<&'static str>, bool) {
    match policy {
        PermissionPolicy::Ask => (Some("default"), false),
        PermissionPolicy::Allow => (Some("accept_edits"), false),
        PermissionPolicy::Deny => (Some("dont_ask"), false),
        PermissionPolicy::Bypass => (None, true),
    }
}

fn codex_settings(policy: PermissionPolicy) -> (&'static str, &'static str, bool) {
    match policy {
        PermissionPolicy::Ask => ("on-request", "workspace-write", false),
        PermissionPolicy::Allow => ("never", "workspace-write", false),
        PermissionPolicy::Deny => ("never", "read-only", false),
        PermissionPolicy::Bypass => ("never", "danger-full-access", true),
    }
}

fn codebuddy_settings(policy: PermissionPolicy) -> (Option<&'static str>, bool) {
    match policy {
        PermissionPolicy::Ask => (Some("default"), false),
        PermissionPolicy::Allow => (Some("acceptEdits"), false),
        PermissionPolicy::Deny => (Some("dontAsk"), false),
        PermissionPolicy::Bypass => (None, true),
    }
}

fn auto_approve(policy: PermissionPolicy) -> bool {
    matches!(policy, PermissionPolicy::Allow | PermissionPolicy::Bypass)
}

fn pty_permission_args(cmd: &str, policy: PermissionPolicy) -> Vec<&'static str> {
    match cmd {
        "claude" | "openclaude" => match claude_settings(policy) {
            (Some(mode), false) => vec!["--permission-mode", mode],
            (None, true) => vec!["--dangerously-skip-permissions"],
            _ => Vec::new(),
        },
        "codex" => match policy {
            PermissionPolicy::Ask => vec![
                "--ask-for-approval",
                "on-request",
                "--sandbox",
                "workspace-write",
            ],
            PermissionPolicy::Allow => vec![
                "--ask-for-approval",
                "never",
                "--sandbox",
                "workspace-write",
            ],
            PermissionPolicy::Deny => {
                vec!["--ask-for-approval", "never", "--sandbox", "read-only"]
            }
            PermissionPolicy::Bypass => {
                vec!["--dangerously-bypass-approvals-and-sandbox"]
            }
        },
        "opencode" => auto_approve(policy)
            .then_some(vec!["--dangerously-skip-permissions"])
            .unwrap_or_default(),
        "qodercli" => match qoder_settings(policy) {
            (Some(mode), false) => vec!["--permission-mode", mode],
            (None, true) => vec!["--dangerously-skip-permissions"],
            _ => Vec::new(),
        },
        "codebuddy" | "cbc" => match codebuddy_settings(policy) {
            (Some(mode), false) => vec!["--permission-mode", mode],
            (None, true) => vec!["--dangerously-skip-permissions"],
            _ => Vec::new(),
        },
        "rscode" => auto_approve(policy)
            .then_some(vec!["--yes"])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn codex_mcp_builder(
    cwd: &Path,
    policy: PermissionPolicy,
    bin: String,
) -> cap_rs::driver::codex_mcp::CodexMcpBuilder {
    let (approval, sandbox, _) = codex_settings(policy);
    CodexMcpDriver::builder(cwd)
        .bin(bin)
        .approval_policy(approval)
        .sandbox(sandbox)
}

/// Probe whether a binary supports stream-json by running `<bin> <subcmd> --help`
/// and checking if the output contains `keyword`. Results are cached per
/// `(bin, subcmd)` pair to avoid redundant process spawns across sessions.
///
/// The production path probes before spawning, so unsupported CLIs select
/// their native fallback without first creating a doomed stream-json process.
async fn probe_stream_json_support(bin: &str, subcmd: &[&str], keyword: &str) -> bool {
    let cache_key = format!("{}:{}", bin, subcmd.join(","));

    if let Ok(cache) = PROBE_CACHE.lock()
        && let Some(ref map) = *cache
        && let Some(&result) = map.get(&cache_key)
    {
        return result;
    }

    let result = match tokio::time::timeout(Duration::from_secs(5), async {
        let output = tokio::process::Command::new(bin)
            .args(subcmd)
            .arg("--help")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .await?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        Ok::<bool, std::io::Error>(stdout.contains(keyword) || stderr.contains(keyword))
    })
    .await
    {
        Ok(Ok(supported)) => supported,
        _ => false,
    };

    if let Ok(mut cache) = PROBE_CACHE.lock() {
        cache
            .get_or_insert_with(Default::default)
            .insert(cache_key, result);
    }

    result
}

async fn build_codex_driver(
    cwd: &Path,
    policy: PermissionPolicy,
    stream_bin: String,
    mcp_bin: String,
) -> Result<Box<dyn Driver>, OrchestratorError> {
    let (approval, sandbox, bypass) = codex_settings(policy);
    if !probe_stream_json_support(&stream_bin, &["exec"], "stream-json").await {
        info!(bin = %stream_bin, mcp_bin = %mcp_bin, "codex: stream-json unsupported, using codex-mcp");
        let driver = codex_mcp_builder(cwd, policy, mcp_bin).spawn().await?;
        return Ok(Box::new(driver));
    }

    match ClaudeCodeDriver::codex_builder(cwd)
        .bin(stream_bin.clone())
        .codex_permissions(approval, sandbox)
        .dangerously_skip_permissions(bypass)
        .spawn()
        .await
    {
        Ok(mut driver) => {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if driver.is_alive() {
                info!(bin = %stream_bin, "codex: using stream-json driver");
                Ok(Box::new(driver))
            } else {
                warn!(bin = %stream_bin, mcp_bin = %mcp_bin, "codex: stream-json spawn exited early, falling back to codex-mcp");
                let _ = driver.shutdown().await;
                let _ = probe_stream_json_support(&stream_bin, &["exec"], "stream-json").await;
                let driver = codex_mcp_builder(cwd, policy, mcp_bin).spawn().await?;
                Ok(Box::new(driver))
            }
        }
        Err(cap_rs::driver::DriverError::BinaryNotFound(_)) => {
            info!(bin = %stream_bin, "codex: binary not found");
            Err(OrchestratorError::Driver(
                cap_rs::driver::DriverError::BinaryNotFound(stream_bin),
            ))
        }
        Err(e) => {
            warn!(bin = %stream_bin, mcp_bin = %mcp_bin, error = %e, "codex: stream-json spawn failed, falling back to codex-mcp");
            let driver = codex_mcp_builder(cwd, policy, mcp_bin).spawn().await?;
            Ok(Box::new(driver))
        }
    }
}

async fn spawn_opencode_acp(cwd: &Path, bin: String) -> Result<Box<dyn Driver>, OrchestratorError> {
    let driver = AcpDriver::builder(bin, cwd).arg("acp").spawn().await?;
    Ok(Box::new(driver))
}

async fn build_opencode_driver(
    cwd: &Path,
    policy: PermissionPolicy,
    bin: String,
) -> Result<Box<dyn Driver>, OrchestratorError> {
    if !probe_stream_json_support(&bin, &["run"], "stream-json").await {
        info!(bin = %bin, "opencode: stream-json unsupported, using ACP");
        return spawn_opencode_acp(cwd, bin).await;
    }

    match ClaudeCodeDriver::opencode_builder(cwd)
        .bin(bin.clone())
        .dangerously_skip_permissions(auto_approve(policy))
        .spawn()
        .await
    {
        Ok(mut driver) => {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if driver.is_alive() {
                info!(bin = %bin, "opencode: using stream-json driver");
                Ok(Box::new(driver))
            } else {
                warn!(bin = %bin, "opencode: stream-json spawn exited early, falling back to ACP");
                let _ = driver.shutdown().await;
                spawn_opencode_acp(cwd, bin).await
            }
        }
        Err(cap_rs::driver::DriverError::BinaryNotFound(_)) => {
            info!(bin = %bin, "opencode: binary not found");
            Err(OrchestratorError::Driver(
                cap_rs::driver::DriverError::BinaryNotFound(bin),
            ))
        }
        Err(e) => {
            warn!(bin = %bin, error = %e, "opencode: stream-json spawn failed, falling back to ACP");
            spawn_opencode_acp(cwd, bin).await
        }
    }
}

#[derive(Debug, Default)]
pub struct RealDriverFactory;

#[async_trait]
impl DriverFactory for RealDriverFactory {
    async fn build(
        &self,
        _session: &SessionId,
        kind: &DriverKind,
        cwd: &Path,
        policy: PermissionPolicy,
    ) -> Result<Box<dyn Driver>, OrchestratorError> {
        match kind {
            DriverKind::Claude => {
                let (mode, bypass) = claude_settings(policy);
                let mut builder =
                    ClaudeCodeDriver::builder(cwd).dangerously_skip_permissions(bypass);
                if let Some(mode) = mode {
                    builder = builder.permission_mode(mode);
                }
                Ok(Box::new(builder.spawn().await?))
            }
            DriverKind::OpenClaude => {
                let (mode, bypass) = claude_settings(policy);
                let mut builder = ClaudeCodeDriver::builder(cwd)
                    .bin("openclaude")
                    .dangerously_skip_permissions(bypass);
                if let Some(mode) = mode {
                    builder = builder.permission_mode(mode);
                }
                Ok(Box::new(builder.spawn().await?))
            }
            // OpenCode falls back to ACP when its installed CLI does not
            // advertise stream-json. Keep the early-exit check for forks
            // whose advertised capability disagrees with their parser.
            DriverKind::OpenCode => {
                let bin = std::env::var("OPENCODE_BIN").unwrap_or_else(|_| "opencode".into());
                build_opencode_driver(cwd, policy, bin).await
            }
            // codex: try stream-json optimistically, fall back to codex-mcp.
            // Fork versions add `--input-format stream-json` to `codex exec`;
            // vanilla codex rejects the flag and exits immediately. We
            // spawn and check for early exit rather than probing `--help`
            // first — faster on the happy path (no extra process spawn).
            DriverKind::Codex => {
                build_codex_driver(
                    cwd,
                    policy,
                    default_codex_stream_bin(),
                    default_codex_mcp_bin(),
                )
                .await
            }
            DriverKind::Rscode => {
                let driver = ClaudeCodeDriver::rscode_builder(cwd)
                    // RSCode's stream input owns stdin, so Allow/Bypass must
                    // use its non-interactive approval switch. Ask/Deny remain
                    // fail-closed when a tool needs approval.
                    .dangerously_skip_permissions(auto_approve(policy))
                    .spawn()
                    .await?;
                Ok(Box::new(driver))
            }
            DriverKind::Codebuddy => {
                let bin = std::env::var("CODEBUDDY_BIN").unwrap_or_else(|_| "codebuddy".into());
                let (permission_mode, skip_permissions) = codebuddy_settings(policy);
                let mut builder = ClaudeCodeDriver::builder(cwd)
                    .bin(bin)
                    .prompt_after_ready(false)
                    .dangerously_skip_permissions(skip_permissions);
                if let Some(mode) = permission_mode {
                    builder = builder.permission_mode(mode);
                }
                Ok(Box::new(builder.spawn().await?))
            }
            DriverKind::Qoder => {
                let (mode, bypass) = qoder_settings(policy);
                let mut builder = ClaudeCodeDriver::builder(cwd)
                    .bin("qodercli")
                    .dangerously_skip_permissions(bypass);
                if let Some(mode) = mode {
                    builder = builder.permission_mode(mode);
                }
                Ok(Box::new(builder.spawn().await?))
            }
            DriverKind::A2a(endpoint) => {
                let driver = A2aDriver::connect(endpoint.clone()).await?;
                Ok(Box::new(driver))
            }
            DriverKind::Grpc(addr) => {
                let driver = GrpcDriver::connect(addr).await?;
                Ok(Box::new(driver))
            }
            DriverKind::Acp(cmd) => {
                let driver = if cmd.as_str() == "opencode" {
                    AcpDriver::opencode(cwd).await?
                } else {
                    AcpDriver::builder(cmd.clone(), cwd).spawn().await?
                };
                Ok(Box::new(driver))
            }
            DriverKind::Aider => {
                let driver = PtyDriver::builder("aider")
                    .cwd(cwd)
                    .spawn(TuiParser::aider())?;
                Ok(Box::new(driver))
            }
            DriverKind::Pty(cmd) => {
                let mut builder = PtyDriver::builder(cmd.clone()).cwd(cwd);
                for arg in pty_permission_args(cmd, policy) {
                    builder = builder.arg(arg);
                }
                let parser = match cmd.as_str() {
                    "codex" => TuiParser::codex(),
                    "opencode" => TuiParser::opencode(),
                    "openclaude" => TuiParser::openclaude(),
                    _ => TuiParser::generic(),
                };
                let driver = builder.spawn(parser)?;
                Ok(Box::new(driver))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn write_executable(path: &Path, contents: &str) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::write(path, contents).unwrap();
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).unwrap();
    }

    #[tokio::test]
    async fn probe_returns_false_for_nonexistent_binary() {
        let result =
            probe_stream_json_support("definitely-not-a-real-binary-xyz", &["run"], "stream-json")
                .await;
        assert!(!result);
    }

    #[tokio::test]
    async fn probe_caches_results() {
        let r1 = probe_stream_json_support("probe-cache-test-bin", &["run"], "stream-json").await;
        let r2 = probe_stream_json_support("probe-cache-test-bin", &["run"], "stream-json").await;
        assert_eq!(r1, r2);
    }

    #[test]
    fn codex_mcp_fallback_has_an_independent_binary() {
        assert_eq!(codex_mcp_bin(None), "codex");
        assert_eq!(
            codex_mcp_bin(Some("/custom/codex-mcp".into())),
            "/custom/codex-mcp"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_unsupported_stream_uses_independent_mcp_binary() {
        let dir = tempfile::tempdir().unwrap();
        let stream_bin = dir.path().join("codex-stream");
        let mcp_bin = dir.path().join("codex-mcp");

        write_executable(
            &stream_bin,
            "#!/bin/sh\ncase \" $* \" in *\" --help \"*) echo 'usage: fake'; exit 0;; esac\nexit 2\n",
        );
        write_executable(
            &mcp_bin,
            "#!/bin/sh\nIFS= read -r line || exit 1\nprintf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}'\nwhile IFS= read -r line; do :; done\n",
        );

        let mut driver = build_codex_driver(
            dir.path(),
            PermissionPolicy::Deny,
            stream_bin.display().to_string(),
            mcp_bin.display().to_string(),
        )
        .await
        .expect("fallback MCP driver should complete its handshake");

        assert!(driver.is_alive());
        driver.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn opencode_unsupported_stream_uses_its_native_acp_binary() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("opencode");

        write_executable(
            &bin,
            "#!/bin/sh\ncase \" $* \" in *\" run --help \"*) echo 'usage: fake'; exit 0;; esac\nIFS= read -r line || exit 1\nprintf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}'\nIFS= read -r line || exit 1\nprintf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"sessionId\":\"fake-session\"}}'\nwhile IFS= read -r line; do :; done\n",
        );

        let mut driver = build_opencode_driver(
            dir.path(),
            PermissionPolicy::Deny,
            bin.display().to_string(),
        )
        .await
        .expect("fallback ACP driver should complete its handshake");

        assert!(matches!(
            driver.next_event().await,
            Some(cap_rs::core::AgentEvent::Ready { session_id: Some(id), .. }) if id == "fake-session"
        ));
        driver.shutdown().await.unwrap();
    }

    #[test]
    fn native_permission_mappings_cover_every_policy() {
        assert_eq!(
            claude_settings(PermissionPolicy::Ask),
            (Some("manual"), false)
        );
        assert_eq!(
            claude_settings(PermissionPolicy::Allow),
            (Some("acceptEdits"), false)
        );
        assert_eq!(
            claude_settings(PermissionPolicy::Deny),
            (Some("dontAsk"), false)
        );
        assert_eq!(claude_settings(PermissionPolicy::Bypass), (None, true));

        assert_eq!(
            qoder_settings(PermissionPolicy::Allow),
            (Some("accept_edits"), false)
        );
        assert_eq!(
            qoder_settings(PermissionPolicy::Deny),
            (Some("dont_ask"), false)
        );
        assert_eq!(qoder_settings(PermissionPolicy::Bypass), (None, true));

        assert_eq!(
            codex_settings(PermissionPolicy::Ask),
            ("on-request", "workspace-write", false)
        );
        assert_eq!(
            codex_settings(PermissionPolicy::Allow),
            ("never", "workspace-write", false)
        );
        assert_eq!(
            codex_settings(PermissionPolicy::Deny),
            ("never", "read-only", false)
        );
        assert_eq!(
            codex_settings(PermissionPolicy::Bypass),
            ("never", "danger-full-access", true)
        );

        assert_eq!(
            codebuddy_settings(PermissionPolicy::Ask),
            (Some("default"), false)
        );
        assert_eq!(
            codebuddy_settings(PermissionPolicy::Allow),
            (Some("acceptEdits"), false)
        );
        assert_eq!(
            codebuddy_settings(PermissionPolicy::Deny),
            (Some("dontAsk"), false)
        );
        assert_eq!(codebuddy_settings(PermissionPolicy::Bypass), (None, true));

        assert!(!auto_approve(PermissionPolicy::Ask));
        assert!(auto_approve(PermissionPolicy::Allow));
        assert!(!auto_approve(PermissionPolicy::Deny));
        assert!(auto_approve(PermissionPolicy::Bypass));

        assert_eq!(
            pty_permission_args("claude", PermissionPolicy::Allow),
            vec!["--permission-mode", "acceptEdits"]
        );
        assert_eq!(
            pty_permission_args("codex", PermissionPolicy::Deny),
            vec!["--ask-for-approval", "never", "--sandbox", "read-only"]
        );
        assert_eq!(
            pty_permission_args("opencode", PermissionPolicy::Allow),
            vec!["--dangerously-skip-permissions"]
        );
        assert!(pty_permission_args("unknown", PermissionPolicy::Bypass).is_empty());
    }
}
