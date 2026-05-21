//! `cap` — drive and orchestrate CLI AI agents.

use std::path::PathBuf;

use cap_rs_orchestrator::config::{FleetSpec, PermissionPolicy};
use cap_rs_orchestrator::event::{OrchestratorControl, OrchestratorEvent};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Parser)]
#[command(
    name = "cap",
    version,
    about = "Discover, drive, and orchestrate CLI AI agents."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a fleet of collaborating agents described by a fleet.yaml.
    Run {
        /// Path to the fleet.yaml file.
        path: PathBuf,
        /// Task text (overrides `fleet.task` in the file).
        #[arg(long)]
        task: Option<String>,
        /// Force fleet-wide bypass: auto-approve every permission request.
        #[arg(long)]
        bypass: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Run { path, task, bypass } => cmd_run(path, task, bypass).await,
    }
}

async fn cmd_run(path: PathBuf, task: Option<String>, bypass: bool) -> anyhow::Result<()> {
    let yaml = std::fs::read_to_string(&path)?;
    let mut spec = FleetSpec::from_yaml(&yaml).map_err(|e| anyhow::anyhow!("{e}"))?;
    if bypass {
        // Force fleet-wide bypass: set the fleet default AND clear every
        // per-session override so no `permissions:` in the file can opt out.
        spec.fleet.permissions = PermissionPolicy::Bypass;
        for session in spec.fleet.sessions.values_mut() {
            session.permissions = None;
        }
    }
    spec.validate().map_err(|e| anyhow::anyhow!("{e}"))?;

    let effective_task = task
        .or_else(|| spec.fleet.task.clone())
        .ok_or_else(|| anyhow::anyhow!("no task: pass --task or set fleet.task"))?;

    let repo = std::env::current_dir()?;
    let (handle, mut events) = cap_rs_orchestrator::run(spec, repo, &effective_task)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Cancel the fleet on Ctrl-C via a cloned control sender (keep `handle`
    // in the main loop so it can answer `ask` prompts via `decide`).
    let control = handle.control_sender();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("\n^C — cancelling fleet…");
            let _ = control.send(OrchestratorControl::Cancel).await;
        }
    });

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();

    while let Some(ev) = events.recv().await {
        match ev {
            OrchestratorEvent::SessionStarted { session } => println!("▶ {session} started"),
            OrchestratorEvent::Agent { session, event } => println!("[{session}] {event:?}"),
            OrchestratorEvent::Routed { from, to } => println!("→ routed {from} → {to}"),
            OrchestratorEvent::SessionDone {
                session,
                stop_reason,
            } => {
                println!("✓ {session} done ({stop_reason:?})")
            }
            OrchestratorEvent::SessionFailed { session, error } => {
                println!("✗ {session} failed: {error}")
            }
            OrchestratorEvent::Ask {
                session,
                req_id,
                tool,
                risk_level,
            } => {
                println!("⚠ {session} wants to use {tool} (risk: {risk_level:?}) — allow? [y/N]");
                let line = stdin.next_line().await?.unwrap_or_default();
                let allow = matches!(line.trim(), "y" | "Y" | "yes");
                handle.decide(session, req_id, allow).await;
            }
            OrchestratorEvent::AwaitSelection { candidates } => {
                println!("⊙ pick a winner among: {}", candidates.join(", "));
            }
            OrchestratorEvent::FleetComplete => {
                println!("== fleet complete ==");
                break;
            }
            // OrchestratorEvent is #[non_exhaustive]; ignore any future variants.
            _ => {}
        }
    }
    Ok(())
}
