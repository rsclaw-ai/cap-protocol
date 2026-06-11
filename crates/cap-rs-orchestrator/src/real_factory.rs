//! Builds real `cap-rs` drivers. Each first-class agent name maps to its
//! highest-fidelity structured path:
//! - `claude` → `stream-json`
//! - `openclaude` → `stream-json` (Anthropic SDK-compatible)
//! - `opencode` → probe stream-json support; fallback to `acp:opencode`
//! - `codex` → probe stream-json support; fallback to `codex_mcp`
//! - `qoder` → `stream-json` (Claude Code-compatible NDJSON)
//! - `acp:<cmd>` → ACP over stdio
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
use tracing::info;

use crate::OrchestratorError;
use crate::config::{DriverKind, PermissionPolicy, SessionId};
use crate::factory::DriverFactory;

static PROBE_CACHE: Mutex<Option<std::collections::HashMap<String, bool>>> = Mutex::new(None);

/// Probe whether a binary supports stream-json by running `<bin> <subcmd> --help`
/// and checking if the output contains `keyword`. Results are cached per
/// `(bin, subcmd)` pair to avoid redundant process spawns across sessions.
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
        let bypass = policy == PermissionPolicy::Bypass;
        match kind {
            DriverKind::Claude => {
                let driver = ClaudeCodeDriver::builder(cwd)
                    .dangerously_skip_permissions(bypass)
                    .spawn()
                    .await?;
                Ok(Box::new(driver))
            }
            DriverKind::OpenClaude => {
                let driver = ClaudeCodeDriver::builder(cwd)
                    .bin("openclaude")
                    .dangerously_skip_permissions(bypass)
                    .spawn()
                    .await?;
                Ok(Box::new(driver))
            }
            // opencode: probe stream-json support at spawn time.
            // Fork versions add `--output-format stream-json` to `opencode run`;
            // vanilla opencode does not. Fall back to ACP (opencode's native
            // structured protocol) when stream-json is unavailable.
            DriverKind::OpenCode => {
                let bin = std::env::var("OPENCODE_BIN").unwrap_or_else(|_| "opencode".into());
                if probe_stream_json_support(&bin, &["run"], "stream-json").await {
                    info!(bin = %bin, "opencode: using stream-json driver");
                    let driver = ClaudeCodeDriver::opencode_builder(cwd).spawn().await?;
                    Ok(Box::new(driver))
                } else {
                    info!(bin = %bin, "opencode: stream-json not detected, falling back to ACP");
                    let driver = AcpDriver::opencode(cwd).await?;
                    Ok(Box::new(driver))
                }
            }
            // codex: probe stream-json support at spawn time.
            // Fork versions add `--input-format stream-json` to `codex exec`;
            // vanilla codex does not. Fall back to CodexMcpDriver (codex's
            // native MCP server mode) when stream-json is unavailable.
            DriverKind::Codex => {
                let bin = std::env::var("CODEX_BIN").unwrap_or_else(|_| "codex".into());
                if probe_stream_json_support(&bin, &["exec"], "stream-json").await {
                    info!(bin = %bin, "codex: using stream-json driver");
                    let driver = ClaudeCodeDriver::builder(cwd)
                        .bin(bin)
                        .dangerously_skip_permissions(bypass)
                        .spawn()
                        .await?;
                    Ok(Box::new(driver))
                } else {
                    info!(bin = %bin, "codex: stream-json not detected, falling back to codex-mcp");
                    let mut builder = CodexMcpDriver::builder(cwd);
                    if bypass {
                        builder = builder.approval_policy("never");
                    }
                    let driver = builder.spawn().await?;
                    Ok(Box::new(driver))
                }
            }
            DriverKind::Qoder => {
                let driver = ClaudeCodeDriver::builder(cwd)
                    .bin("qodercli")
                    .dangerously_skip_permissions(bypass)
                    .spawn()
                    .await?;
                Ok(Box::new(driver))
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
                if cmd.as_str() == "codex" && bypass {
                    builder = builder.arg("--dangerously-bypass-approvals-and-sandbox");
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
    use cap_rs::driver::DriverError;

    use super::*;

    #[tokio::test]
    async fn codex_defaults_to_stream_json_driver_family() {
        let temp = tempfile::tempdir().unwrap();
        let factory = RealDriverFactory;
        let result = factory
            .build(
                &"codex".to_string(),
                &DriverKind::Codex,
                temp.path(),
                PermissionPolicy::Ask,
            )
            .await;

        match result {
            Err(OrchestratorError::Driver(DriverError::BinaryNotFound(_))) => {}
            Ok(mut driver) => {
                driver.shutdown().await.unwrap();
            }
            Err(err) => panic!("unexpected error: {err}"),
        }
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
        let r1 =
            probe_stream_json_support("probe-cache-test-bin", &["run"], "stream-json").await;
        let r2 =
            probe_stream_json_support("probe-cache-test-bin", &["run"], "stream-json").await;
        assert_eq!(r1, r2);
    }
}

