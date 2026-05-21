//! Live smoke: runs a single real `claude` session in the current git repo.
//! Requires the `claude` CLI on PATH + valid auth. Build-only in CI.
//!
//! Run: cargo run -p cap-rs-orchestrator --example claude_smoke

use cap_rs_orchestrator::config::FleetSpec;
use cap_rs_orchestrator::event::OrchestratorEvent;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let spec = FleetSpec::from_yaml(
        r#"
fleet:
  base_branch: HEAD
  sessions:
    coder: { driver: claude, permissions: bypass }
  start: coder
"#,
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    let repo = std::env::current_dir()?;
    let (_handle, mut events) =
        cap_rs_orchestrator::run(spec, repo, "Say hello in one short sentence.")
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

    while let Some(ev) = events.recv().await {
        match ev {
            OrchestratorEvent::Agent { session, event } => println!("[{session}] {event:?}"),
            OrchestratorEvent::FleetComplete => {
                println!("== fleet complete ==");
                break;
            }
            other => println!(":: {other:?}"),
        }
    }
    Ok(())
}
