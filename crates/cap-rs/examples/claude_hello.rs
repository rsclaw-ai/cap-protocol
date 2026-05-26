//! End-to-end smoke test: drive Claude Code via stream-json.
//!
//! Usage:
//!   cargo run --example claude_hello --features stream-json -- "your prompt"
//!
//! Env vars:
//!   CLAUDE_BIN  Override binary path (default: `claude` on PATH)
//!   RUST_LOG    Enable tracing logs (e.g. `cap_rs=debug,info`)

use std::time::Instant;

use cap_rs::core::{AgentEvent, ClientFrame, Content, TextChannel};
use cap_rs::driver::Driver;
use cap_rs::driver::stream_json::ClaudeCodeDriver;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let prompt = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "what is 2 + 2? Answer in one sentence.".into());

    let cwd = std::env::current_dir()?;
    println!("│ cap-rs · ClaudeCodeDriver");
    println!("│   cwd:    {}", cwd.display());
    println!("│   prompt: {prompt}");
    println!();

    let started = Instant::now();
    // Smoke test runs unattended — opt in to claude's permission bypass.
    // Production callers SHOULD leave this off (the default) and route
    // permission prompts through CAP.
    let mut driver = ClaudeCodeDriver::builder(&cwd)
        .dangerously_skip_permissions(true)
        .spawn()
        .await?;

    driver
        .send(ClientFrame::Prompt {
            content: vec![Content::text(prompt.clone())],
        })
        .await?;

    // One-shot demo: signal no more user input so claude processes the
    // pending prompt, emits its terminal `result` frame, and exits.
    // For a multi-turn session we would NOT call this — claude would
    // keep stdin open and wait for follow-up prompts.
    driver.finish_input();

    let mut last_was_text = false;
    while let Some(event) = driver.next_event().await {
        match event {
            AgentEvent::Ready { session_id, model, .. } => {
                println!(
                    "● ready  session={} model={}",
                    short(&session_id, 8),
                    model.as_deref().unwrap_or("?")
                );
            }
            AgentEvent::TextChunk { text, channel, .. } => {
                if !last_was_text {
                    print!("│ ");
                }
                match channel {
                    TextChannel::Assistant => print!("{text}"),
                    TextChannel::System => print!("[system: {text}]"),
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
                    "         tokens in/out: {}/{}  (cache r/w: {}/{})",
                    usage.input_tokens,
                    usage.output_tokens,
                    usage.cache_read_tokens,
                    usage.cache_creation_tokens
                );
                if let Some(cost) = usage.cost_usd_estimate {
                    println!("         cost: ${cost:.6}");
                }
                println!("         wall: {:?}", started.elapsed());
                break;
            }
            AgentEvent::Error { code, message, .. } => {
                eprintln!("✗ error  {code}: {message}");
                break;
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
