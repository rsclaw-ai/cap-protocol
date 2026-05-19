//! Smoke test for `CodexAppServerDriver`: spawn, initialize, thread/start,
//! collect any unsolicited events for a few seconds, shut down. Does NOT
//! issue a model turn — costs zero tokens.

use std::time::Duration;

use cap_rs::core::AgentEvent;
use cap_rs::driver::Driver;
use cap_rs::driver::codex_app_server::CodexAppServerDriver;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let cwd = std::env::current_dir()?;
    eprintln!("│ codex_smoke");
    eprintln!("│   cwd: {}", cwd.display());

    let started = std::time::Instant::now();
    let mut driver = CodexAppServerDriver::builder(&cwd).spawn().await?;
    let elapsed = started.elapsed();
    eprintln!("● spawned + thread/start in {:?}", elapsed);
    eprintln!(
        "● thread: {}",
        driver.thread_id().unwrap_or_else(|| "?".into())
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                eprintln!("● drain window done");
                break;
            }
            ev = driver.next_event() => {
                match ev {
                    Some(AgentEvent::Ready { session_id, model }) => {
                        eprintln!("· Ready session={} model={}", session_id, model.as_deref().unwrap_or("?"));
                    }
                    Some(other) => {
                        eprintln!("· {other:?}");
                    }
                    None => {
                        eprintln!("✗ event stream closed");
                        break;
                    }
                }
            }
        }
    }

    eprintln!("● shutting down");
    driver.shutdown().await?;
    eprintln!("● exit_status: {:?}", driver.exit_status());
    Ok(())
}
