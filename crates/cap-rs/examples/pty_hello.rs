//! End-to-end smoke test: drive ANY CLI agent (or just `bash`) via PTY.
//!
//! Usage examples:
//!
//!     # smoke-test the PTY plumbing with bash
//!     cargo run --example pty_hello --features pty -- bash -c 'echo hello; sleep 1; echo world'
//!
//!     # drive Claude Code's interactive TUI through PTY
//!     cargo run --example pty_hello --features pty -- claude
//!
//!     # drive aider
//!     cargo run --example pty_hello --features pty -- aider
//!
//! After the agent is spawned this example becomes a tiny relay:
//! - bytes typed at our stdin are forwarded to the PTY (so you can chat).
//! - all PTY output is streamed back to our stdout via [`RawParser`].
//! - Ctrl+D on stdin closes input; Ctrl+C exits.

use std::io::{Read, Write};
use std::time::Duration;

use cap_rs::core::AgentEvent;
use cap_rs::driver::Driver;
use cap_rs::driver::pty::{PtyDriver, RawParser};

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

    let mut argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.is_empty() {
        eprintln!("usage: pty_hello <command> [args...]");
        eprintln!("example: pty_hello bash -c 'echo hi'");
        std::process::exit(2);
    }
    let cmd = argv.remove(0);

    eprintln!("│ cap-rs · PtyDriver");
    eprintln!("│   command: {cmd} {}", argv.join(" "));
    eprintln!("│   cwd:     {}", std::env::current_dir()?.display());
    eprintln!("│   ── (output below, ANSI passes through) ─────────────");
    eprintln!();

    let cwd = std::env::current_dir()?;
    let mut driver = PtyDriver::builder(&cmd)
        .args(argv)
        .cwd(&cwd)
        .env_remove("CLAUDECODE")
        .env_remove("CLAUDE_CODE_ENTRYPOINT")
        .env_remove("CLAUDE_CODE_SSE_PORT")
        .env_remove("CLAUDE_CODE_SESSION_ID")
        .size(40, 160)
        .spawn(RawParser)?;

    // Forward our stdin → PTY in a background blocking thread.
    let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(32);
    std::thread::spawn(move || {
        let mut buf = [0u8; 1024];
        let stdin = std::io::stdin();
        let mut stdin = stdin.lock();
        loop {
            match stdin.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if stdin_tx.blocking_send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let mut out = std::io::stdout().lock();
    let started = std::time::Instant::now();

    loop {
        tokio::select! {
            ev = driver.next_event() => {
                match ev {
                    Some(AgentEvent::TextChunk { text, .. }) => {
                        out.write_all(text.as_bytes()).ok();
                        out.flush().ok();
                    }
                    Some(AgentEvent::Done { stop_reason, .. }) => {
                        out.flush().ok();
                        eprintln!();
                        eprintln!("│ ── done — stop={stop_reason:?}, wall={:?}", started.elapsed());
                        break;
                    }
                    Some(other) => {
                        eprintln!();
                        eprintln!("· event {other:?}");
                    }
                    None => break,
                }
            }
            Some(bytes) = stdin_rx.recv() => {
                driver.send_bytes(&bytes).await.ok();
            }
            _ = tokio::time::sleep(Duration::from_secs(120)) => {
                eprintln!();
                eprintln!("│ ── 120s idle, exiting");
                break;
            }
        }
    }

    driver.shutdown().await.ok();
    Ok(())
}
