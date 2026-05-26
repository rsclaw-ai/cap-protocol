//! `cap` — drive and orchestrate CLI AI agents.

use std::path::PathBuf;

use cap_rs_orchestrator::config::{FleetSpec, PermissionPolicy};
use cap_rs_orchestrator::event::{OrchestratorControl, OrchestratorEvent};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Debug, Parser)]
#[command(
    name = "cap",
    version,
    about = "Discover, drive, and orchestrate CLI AI agents."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Validate a fleet.yaml without running it.
    Validate {
        /// Path to the fleet.yaml file.
        path: PathBuf,
    },
    /// List all supported agent driver kinds.
    ListDrivers,
    /// Generate a default fleet.yaml template in the current directory.
    Init,
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
        Command::Validate { path } => cmd_validate(path),
        Command::ListDrivers => cmd_list_drivers(),
        Command::Init => cmd_init(),
        Command::Run { path, task, bypass } => cmd_run(path, task, bypass).await,
    }
}

fn cmd_validate(path: PathBuf) -> anyhow::Result<()> {
    let yaml = std::fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", path.display()))?;
    let spec = FleetSpec::from_yaml(&yaml).map_err(|e| anyhow::anyhow!("parse error: {e}"))?;
    spec.validate()
        .map_err(|e| anyhow::anyhow!("validation error: {e}"))?;
    println!("✓ {} is valid", path.display());
    let task = spec.fleet.task.as_deref().unwrap_or("(none)");
    println!("  sessions: {}", spec.fleet.sessions.len());
    for (id, s) in &spec.fleet.sessions {
        println!("    {id}: {:?}", s.driver);
    }
    println!("  task: {task}");
    println!("  routes: {}", spec.fleet.routes.len());
    Ok(())
}

fn cmd_list_drivers() -> anyhow::Result<()> {
    println!("Supported agent drivers:");
    for d in cap_rs_orchestrator::config::list_driver_kinds() {
        println!("  {d}");
    }
    Ok(())
}

fn cmd_init() -> anyhow::Result<()> {
    let path = PathBuf::from("fleet.yaml");
    if path.exists() {
        anyhow::bail!("fleet.yaml already exists in current directory");
    }
    std::fs::write(&path, cap_rs_orchestrator::config::default_fleet_yaml())?;
    println!("✓ Created fleet.yaml");
    println!("  Edit it, then run: cap run fleet.yaml");
    Ok(())
}

async fn cmd_run(path: PathBuf, task: Option<String>, bypass: bool) -> anyhow::Result<()> {
    let yaml = std::fs::read_to_string(&path)?;
    let mut spec = FleetSpec::from_yaml(&yaml).map_err(|e| anyhow::anyhow!("{e}"))?;
    if bypass {
        eprintln!(
            "⚠ --bypass: every agent will run with no permission gate.\n  \
             Worktree isolation still applies. Continue? [y/N]"
        );
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        if !matches!(line.trim(), "y" | "Y" | "yes") {
            eprintln!("Aborted.");
            std::process::exit(0);
        }
        spec.fleet.permissions = PermissionPolicy::Bypass;
        for session in spec.fleet.sessions.values_mut() {
            session.permissions = None;
        }
    }
    spec.validate().map_err(|e| anyhow::anyhow!("{e}"))?;

    let effective_task = task
        .or_else(|| spec.fleet.task.clone())
        .ok_or_else(|| anyhow::anyhow!("no task: pass --task or set fleet.task"))?;

    let repo = path
        .parent()
        .map(|p| {
            if p.as_os_str().is_empty() {
                std::env::current_dir().unwrap_or_default()
            } else {
                p.to_path_buf()
            }
        })
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());
    let (handle, mut events) = cap_rs_orchestrator::run(spec, repo, &effective_task)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let control = handle.control_sender();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("\n^C — cancelling fleet…");
            let _ = control.send(OrchestratorControl::Cancel).await;
        }
    });

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();

    let mut saw_fleet_complete = false;
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
                println!(
                    "⊙ candidates ready for manual review (one git worktree each): {}",
                    candidates.join(", ")
                );
            }
            OrchestratorEvent::FleetComplete => {
                println!("== fleet complete ==");
                saw_fleet_complete = true;
                break;
            }
            _ => {}
        }
    }

    if !saw_fleet_complete {
        eprintln!("✗ fleet ended without FleetComplete — executor may have crashed");
        std::process::exit(1);
    }
    Ok(())
}
