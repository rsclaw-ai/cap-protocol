//! `cap` — drive and orchestrate CLI AI agents.

use std::path::PathBuf;

use cap_rs_orchestrator::config::{FleetSpec, PermissionPolicy};
use cap_rs_orchestrator::event::{OrchestratorControl, OrchestratorEvent};
use cap_rs_orchestrator::routing::{
    CliLlmClient, HybridRouting, LlmRouting, LlmRoutingConfig, LlmSessionSpec,
};
use clap::{Parser, Subcommand, ValueEnum};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ModeArg {
    Static,
    Llm,
    Hybrid,
}

impl std::fmt::Display for ModeArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModeArg::Static => write!(f, "static"),
            ModeArg::Llm => write!(f, "llm"),
            ModeArg::Hybrid => write!(f, "hybrid"),
        }
    }
}

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
        /// Routing strategy: static (default), llm, or hybrid.
        #[arg(long, default_value = "static")]
        mode: ModeArg,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber for debug logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("cap_rs=debug".parse().unwrap()),
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    match Cli::parse().command {
        Command::Validate { path } => cmd_validate(path),
        Command::ListDrivers => cmd_list_drivers(),
        Command::Init => cmd_init(),
        Command::Run {
            path,
            task,
            bypass,
            mode,
        } => cmd_run(path, task, bypass, mode).await,
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

/// Build an `LlmRoutingConfig` + `CliLlmClient` from the fleet config, applying overrides.
fn llm_routing_from_spec(
    spec: &FleetSpec,
) -> (Vec<LlmSessionSpec>, LlmRoutingConfig, Box<CliLlmClient>) {
    let llm_sessions: Vec<LlmSessionSpec> = spec
        .fleet
        .sessions
        .iter()
        .map(|(id, s)| LlmSessionSpec {
            id: id.clone(),
            role: s.role.clone(),
        })
        .collect();
    let command = spec
        .fleet
        .llm
        .as_ref()
        .and_then(|c| c.command.clone())
        .unwrap_or_else(|| vec!["claude".into(), "-p".into()]);
    let mut config = LlmRoutingConfig::default();
    if let Some(ref llm) = spec.fleet.llm {
        if let Some(ref sp) = llm.system_prompt {
            config.system_prompt = sp.clone();
        }
        if let Some(t) = llm.timeout_secs {
            config.timeout = std::time::Duration::from_secs(t);
        }
        if let Some(m) = llm.max_decisions {
            config.max_decisions = m;
        }
        if let Some(c) = llm.max_context_chars {
            config.max_context_chars = c;
        }
    }
    (llm_sessions, config, Box::new(CliLlmClient::new(command)))
}

async fn cmd_run(
    path: PathBuf,
    task: Option<String>,
    bypass: bool,
    mode: ModeArg,
) -> anyhow::Result<()> {
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
    let (handle, mut events) = match mode {
        ModeArg::Static => cap_rs_orchestrator::run(spec, repo, &effective_task)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?,
        ModeArg::Llm => {
            let (llm_sessions, config, client) = llm_routing_from_spec(&spec);
            let strategy = LlmRouting::new(client, llm_sessions, config);
            cap_rs_orchestrator::run_with_strategy(spec, repo, &effective_task, strategy)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
        }
        ModeArg::Hybrid => {
            let routes = spec.fleet.routes.clone();
            let (llm_sessions, config, client) = llm_routing_from_spec(&spec);
            let llm = LlmRouting::new(client, llm_sessions, config);
            let strategy = HybridRouting::new(routes, llm);
            cap_rs_orchestrator::run_with_strategy(spec, repo, &effective_task, strategy)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
        }
    };

    let control = handle.control_sender();
    let ctrl_c_control = control.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("\n^C — cancelling fleet…");
            let _ = ctrl_c_control.send(OrchestratorControl::Cancel).await;
        }
    });

    let (stdin_tx, mut stdin_rx) = mpsc::channel::<String>(256);
    tokio::spawn(async move {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if stdin_tx.send(line).await.is_err() {
                break;
            }
        }
    });

    let mut saw_fleet_complete = false;
    loop {
        tokio::select! {
            ev = events.recv() => {
                let Some(ev) = ev else { break };
                match ev {
                    OrchestratorEvent::SessionStarted { session } => {
                        println!("▶ {session} started");
                    }
                    OrchestratorEvent::Agent { session, event } => {
                        println!("[{session}] {event:?}");
                    }
                    OrchestratorEvent::Routed { from, to } => {
                        println!("→ routed {from} → {to}");
                    }
                    OrchestratorEvent::SessionDone { session, stop_reason } => {
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
                        // Read permission response from stdin channel
                        let line = stdin_rx.recv().await.unwrap_or_default();
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
            line = stdin_rx.recv() => {
                let Some(line) = line else { continue };
                let line = line.trim().to_string();
                if line.is_empty() { continue; }
                if line == "/q" || line == "/quit" {
                    println!("quitting…");
                    let _ = control.send(OrchestratorControl::Cancel).await;
                    break;
                }
                // @session_name: message or @session_name message
                if let Some(rest) = line.strip_prefix('@') {
                    if let Some((session, msg)) = rest.split_once([' ', ':']) {
                        let msg = msg.trim();
                        if !msg.is_empty() {
                            let _ = control.send(OrchestratorControl::UserMessage {
                                session: session.trim().to_string(),
                                text: msg.to_string(),
                            }).await;
                        }
                    }
                }
            }
        }
    }

    if !saw_fleet_complete {
        eprintln!("✗ fleet ended without FleetComplete — executor may have crashed");
        std::process::exit(1);
    }
    Ok(())
}
