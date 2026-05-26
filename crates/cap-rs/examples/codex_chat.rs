//! Real-time multi-turn chat with codex via the `app-server` JSON-RPC binding.
//!
//! One `codex app-server` process serves the whole conversation. Each
//! prompt issues `turn/start`; events stream back via CAP. Type `exit`,
//! `/quit`, or close stdin (Ctrl+D) to end the session.
//!
//! Usage:
//!     cargo run --example codex_chat --features codex
//!
//! Env vars:
//!   CODEX_BIN  Override binary path (default: `codex` on PATH)
//!   RUST_LOG   Enable tracing (e.g. `cap_rs=debug`)

use std::io::{BufRead, Write};

use cap_rs::core::{AgentEvent, ClientFrame, Content, TextChannel};
use cap_rs::driver::Driver;
use cap_rs::driver::codex_app_server::CodexAppServerDriver;
use tokio::sync::mpsc;

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

    eprintln!("│ cap-rs · CodexAppServerDriver · session mode");
    eprintln!("│   cwd: {}", cwd.display());
    eprintln!("│   type 'exit' or '/quit' or Ctrl+D to end");
    eprintln!();

    let mut driver = CodexAppServerDriver::builder(&cwd).spawn().await?;
    eprintln!(
        "● thread: {}",
        driver.thread_id().unwrap_or_else(|| "?".into())
    );

    let (stdin_tx, mut stdin_rx) = mpsc::channel::<String>(8);
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut handle = stdin.lock();
        let mut line = String::new();
        loop {
            line.clear();
            match handle.read_line(&mut line) {
                Ok(0) => {
                    drop(stdin_tx);
                    return;
                }
                Ok(_) => {
                    if stdin_tx.blocking_send(line.trim().to_string()).is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });

    let mut awaiting_response = false;
    eprint!("you: ");
    std::io::stderr().flush().ok();

    loop {
        tokio::select! {
            line = stdin_rx.recv() => {
                let Some(line) = line else { break };
                if line.is_empty() {
                    eprint!("you: ");
                    std::io::stderr().flush().ok();
                    continue;
                }
                if line == "exit" || line == "/quit" {
                    break;
                }
                driver.send(ClientFrame::Prompt {
                    content: vec![Content::text(line)],
                }).await?;
                awaiting_response = true;
                eprint!("codex: ");
                std::io::stderr().flush().ok();
            }
            event = driver.next_event() => {
                let Some(event) = event else { break };
                match event {
                    AgentEvent::Ready { session_id, model, .. } => {
                        eprintln!("● ready  session={} model={}",
                                  short(&session_id, 8),
                                  model.as_deref().unwrap_or("?"));
                    }
                    AgentEvent::TextChunk { text, channel, .. } => {
                        if channel == TextChannel::Assistant {
                            print!("{text}");
                            std::io::stdout().flush().ok();
                        }
                    }
                    AgentEvent::Thought { text, .. } => {
                        if std::env::var("CAP_SHOW_THOUGHTS").is_ok() {
                            eprintln!("\n◌ {}", short(&text, 80));
                        }
                    }
                    AgentEvent::ToolCallStart { name, input, .. } => {
                        eprintln!("\n⚙  {} {}", name, short(&input.to_string(), 80));
                    }
                    AgentEvent::ToolCallEnd { is_error, .. } => {
                        eprintln!("   → {}", if is_error { "ERR" } else { "ok" });
                    }
                    AgentEvent::PermissionRequest { tool, intent, req_id, .. } => {
                        eprintln!("\n? approve {} {} (auto-denying for demo)", tool, short(&intent.to_string(), 60));
                        driver.send(ClientFrame::PermissionResponse {
                            req_id,
                            decision: cap_rs::core::PermissionDecision::Deny,
                        }).await.ok();
                    }
                    AgentEvent::Usage { usage } => {
                        eprintln!(
                            "\n  · usage in/out: {}/{} (cache_r: {}, thinking: {})",
                            usage.input_tokens, usage.output_tokens,
                            usage.cache_read_tokens, usage.thinking_tokens
                        );
                    }
                    AgentEvent::Done { stop_reason, usage } => {
                        if awaiting_response {
                            println!();
                            eprintln!(
                                "● done stop={:?} in/out: {}/{} (dur: {:?})",
                                stop_reason, usage.input_tokens, usage.output_tokens, usage.duration
                            );
                            awaiting_response = false;
                            eprint!("you: ");
                            std::io::stderr().flush().ok();
                        }
                    }
                    AgentEvent::Error { code, message, .. } => {
                        eprintln!("\n✗ {code}: {message}");
                    }
                    _ => {}
                }
            }
        }
    }

    driver.shutdown().await?;
    Ok(())
}

fn short(s: &str, n: usize) -> String {
    let s = s.trim().replace('\n', " ");
    if s.chars().count() <= n {
        s
    } else {
        format!("{}…", s.chars().take(n).collect::<String>())
    }
}
