//! End-to-end smoke test: drive OpenAI Codex CLI via `codex exec --json`.
//!
//! Usage:
//!     cargo run --example codex_hello --features codex -- "your prompt"
//!     cargo run --example codex_hello --features codex -- --resume <thread-id> "follow-up"
//!
//! Env vars:
//!   CODEX_BIN  Override binary path (default: `codex` on PATH)
//!   RUST_LOG   Enable tracing (e.g. `cap_rs=debug`)
//!
//! Codex `exec` is one-shot per process — multi-turn means re-running this
//! binary with `--resume <thread-id>`. The thread_id is printed in the
//! Ready event.

use std::time::Instant;

use cap_rs::core::{AgentEvent, TextChannel};
use cap_rs::driver::Driver;
use cap_rs::driver::codex::CodexExecDriver;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let mut resume: Option<String> = None;
    if let Some(pos) = args.iter().position(|a| a == "--resume") {
        if pos + 1 < args.len() {
            resume = Some(args.remove(pos + 1));
            args.remove(pos);
        }
    }
    let prompt = if args.is_empty() {
        "what is 2 + 2? answer in one short sentence".to_string()
    } else {
        args.join(" ")
    };

    let cwd = std::env::current_dir()?;
    println!("│ cap-rs · CodexExecDriver");
    println!("│   cwd:    {}", cwd.display());
    println!("│   prompt: {prompt}");
    if let Some(t) = &resume {
        println!("│   resume: {t}");
    }
    println!();

    let started = Instant::now();
    let mut builder = CodexExecDriver::builder(&cwd).prompt(prompt);
    if let Some(t) = resume {
        builder = builder.resume(t);
    }
    let mut driver = builder.spawn().await?;

    let mut last_was_text = false;
    while let Some(event) = driver.next_event().await {
        match event {
            AgentEvent::Ready { session_id, .. } => {
                println!("● ready  thread={}", session_id);
            }
            AgentEvent::TextChunk { text, channel, .. } => {
                if !last_was_text {
                    print!("│ ");
                }
                match channel {
                    TextChannel::Assistant => print!("{text}"),
                    TextChannel::System => print!("[sys: {text}]"),
                    _ => print!("{text}"),
                }
                use std::io::Write;
                std::io::stdout().flush().ok();
                last_was_text = !text.ends_with('\n');
            }
            AgentEvent::Thought { text, .. } => {
                if last_was_text {
                    println!();
                    last_was_text = false;
                }
                println!("◌ thought: {}", short(&text, 80));
            }
            AgentEvent::ToolCallStart { name, input, .. } => {
                if last_was_text {
                    println!();
                    last_was_text = false;
                }
                println!("⚙  tool   {name}({})", short(&input.to_string(), 80));
            }
            AgentEvent::ToolCallEnd {
                output, is_error, ..
            } => {
                let status = if is_error { "ERR" } else { "ok" };
                println!("   → {status}: {}", short(&output, 80));
            }
            AgentEvent::Plan { entries } => {
                println!("□ plan ({} items)", entries.len());
                for e in entries {
                    println!("    [{:?}] {}", e.status, e.content);
                }
            }
            AgentEvent::Done { stop_reason, usage } => {
                if last_was_text {
                    println!();
                }
                println!();
                println!("● done   stop={:?}", stop_reason);
                println!(
                    "         tokens in/out/think: {}/{}/{}  (cache_read: {})",
                    usage.input_tokens,
                    usage.output_tokens,
                    usage.thinking_tokens,
                    usage.cache_read_tokens,
                );
                println!("         wall: {:?}", started.elapsed());
                if let Some(tid) = driver.thread_id().await {
                    println!("         resume with: --resume {tid}");
                }
                break;
            }
            AgentEvent::Error { code, message } => {
                eprintln!("✗ error  {code}: {message}");
            }
            other => {
                eprintln!("· event  {other:?}");
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
        let truncated: String = s.chars().take(n).collect();
        format!("{truncated}…")
    }
}
