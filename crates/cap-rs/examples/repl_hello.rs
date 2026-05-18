//! End-to-end demo of the PTY driver + ReplParser combination.
//!
//! Drives a prompt-delimited REPL (default: `python3 -i`) and emits
//! structured CAP events for each turn — Ready when the first prompt
//! appears, TextChunks for output, Done when the prompt re-appears.
//!
//! Usage:
//!     # Default: python3 REPL
//!     cargo run --example repl_hello --features pty
//!
//!     # Aider (when installed)
//!     cargo run --example repl_hello --features pty -- aider
//!
//!     # Generic > / ❯ prompt agent
//!     cargo run --example repl_hello --features pty -- some-other-repl

use std::time::Duration;

use cap_rs::core::{AgentEvent, ClientFrame, Content};
use cap_rs::driver::Driver;
use cap_rs::driver::pty::{AgentParser, PtyDriver, ReplParser};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let arg = std::env::args().nth(1).unwrap_or_else(|| "python3".into());
    let (binary, parser) = match arg.as_str() {
        "python3" | "python" => ("python3", ReplParser::python_repl()),
        "aider" => ("aider", ReplParser::aider()),
        other => (other, ReplParser::generic_repl()),
    };

    eprintln!("│ cap-rs · PtyDriver + ReplParser");
    eprintln!("│   command: {binary}");
    eprintln!("│   parser:  {}", parser.name());
    eprintln!();

    let mut builder = PtyDriver::builder(binary)
        .cwd(std::env::current_dir()?)
        .env_remove("CLAUDECODE")
        .env_remove("CLAUDE_CODE_ENTRYPOINT")
        .size(50, 200);

    if binary == "python3" {
        // Make python's prompts predictable: line-buffered stdout, no banner.
        builder = builder.env("PYTHONUNBUFFERED", "1");
    }

    let mut driver = builder.spawn(parser)?;

    // Wait briefly for Ready event before sending input.
    let mut ready = false;
    let probes: Vec<(&str, &str)> = if binary == "python3" {
        vec![("2 + 2", "4"), ("'hello'.upper()", "'HELLO'")]
    } else if binary == "aider" {
        vec![("/help", ""), ("/exit", "")]
    } else {
        vec![("hi", "")]
    };
    let mut probe_iter = probes.into_iter();
    let mut deadline = std::time::Instant::now() + Duration::from_secs(20);

    loop {
        tokio::select! {
            ev = driver.next_event() => {
                match ev {
                    Some(AgentEvent::Ready { session_id, .. }) => {
                        eprintln!("● ready  ({session_id})");
                        ready = true;
                        if let Some((line, expected)) = probe_iter.next() {
                            eprintln!("→ send: {line}");
                            driver.send(ClientFrame::Prompt {
                                content: vec![Content::Text(line.into())],
                            }).await?;
                            eprint!("  (expect ~ \"{expected}\"): ");
                        }
                    }
                    Some(AgentEvent::TextChunk { text, .. }) => {
                        let t = text.trim();
                        if !t.is_empty() {
                            eprintln!("│ {t}");
                        }
                    }
                    Some(AgentEvent::AskUser { prompt, kind, .. }) => {
                        eprintln!("? ask ({kind:?}): {prompt}");
                        // For demo, auto-yes.
                        driver.send_bytes(b"y\r").await?;
                    }
                    Some(AgentEvent::Done { .. }) => {
                        eprintln!("● turn done");
                        if let Some((line, expected)) = probe_iter.next() {
                            eprintln!("→ send: {line}");
                            driver.send(ClientFrame::Prompt {
                                content: vec![Content::Text(line.into())],
                            }).await?;
                            eprint!("  (expect ~ \"{expected}\"): ");
                        } else {
                            eprintln!("● probes exhausted — closing");
                            driver.close_input();
                            driver.send_bytes(b"\x04").await.ok(); // Ctrl+D
                            break;
                        }
                    }
                    Some(other) => {
                        eprintln!("· {other:?}");
                    }
                    None => break,
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(500)) => {
                if std::time::Instant::now() >= deadline {
                    eprintln!("[timeout 20s] giving up");
                    break;
                }
                if !ready {
                    deadline = std::time::Instant::now() + Duration::from_secs(20);
                }
            }
        }
    }

    driver.shutdown().await?;
    Ok(())
}
