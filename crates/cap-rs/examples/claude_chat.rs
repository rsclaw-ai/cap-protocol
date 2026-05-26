//! Real-time multi-turn chat with Claude Code via stream-json session mode.
//!
//! One `claude` process serves the entire conversation — no per-turn
//! spawn tax. This is the same trick claude-agent-acp uses internally,
//! exposed through CAP.
//!
//! Usage:
//!     cargo run --example claude_chat --features stream-json
//!
//! Type a prompt and hit Enter. Claude streams the answer back. Repeat
//! as many times as you want. Send `exit`, `/quit`, or close stdin
//! (Ctrl+D) to end the session.
//!
//! Env vars:
//!   CLAUDE_BIN  Override binary path (default: `claude` on PATH)
//!   RUST_LOG    Enable tracing (e.g. `cap_rs=debug`)

use std::io::{BufRead, Write};

use cap_rs::core::{AgentEvent, ClientFrame, Content, TextChannel};
use cap_rs::driver::Driver;
use cap_rs::driver::stream_json::ClaudeCodeDriver;
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

    eprintln!("│ cap-rs · ClaudeCodeDriver · session mode");
    eprintln!("│   cwd: {}", cwd.display());
    eprintln!("│   type 'exit' or '/quit' or Ctrl+D to end");
    eprintln!();

    // Interactive demo — opt in to claude's permission bypass so tool
    // calls in the middle of the chat don't hang the REPL. Real CAP
    // orchestrators should leave this off and forward permission events.
    let mut driver = ClaudeCodeDriver::builder(&cwd)
        .dangerously_skip_permissions(true)
        .spawn()
        .await?;

    // Read stdin lines from a background thread so we can select between
    // events from claude and user input. Tokio's stdin is awkward for
    // interactive reads, so we use blocking std::io::stdin in a thread.
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
                    if stdin_tx.blocking_send(line.trim_end().to_string()).is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });

    let mut turn = 0u32;
    let mut pending_turns: u32 = 0;
    let mut want_exit = false;
    let mut stdin_open = true;
    let mut waiting_for_prompt = true;
    let mut current_msg_id = String::new();
    let mut session_id = String::new();

    // Prompt for the first turn.
    print_prompt(turn);

    loop {
        // Don't poll stdin once we've decided to exit and stdin is done —
        // otherwise the closed receiver fires repeatedly and starves the
        // event branch in tokio::select!.
        let read_stdin = stdin_open && !want_exit;

        tokio::select! {
            event = driver.next_event() => {
                match event {
                    Some(AgentEvent::Ready { session_id: sid, model, .. }) => {
                        session_id = sid.clone();
                        eprintln!("\n● ready session={} model={}",
                            short(&sid, 8),
                            model.as_deref().unwrap_or("?"));
                    }
                    Some(AgentEvent::TextChunk { msg_id, text, channel }) => {
                        if waiting_for_prompt {
                            print!("\n│ ");
                            waiting_for_prompt = false;
                        }
                        if msg_id != current_msg_id && !current_msg_id.is_empty() {
                            print!("\n│ ");
                        }
                        current_msg_id = msg_id;
                        match channel {
                            TextChannel::Assistant => print!("{text}"),
                            TextChannel::System => print!("[sys: {text}]"),
                            _ => print!("{text}"),
                        }
                        std::io::stdout().flush().ok();
                    }
                    Some(AgentEvent::ToolCallStart { name, input, .. }) => {
                        print!("\n⚙  {} {}", name, short(&input.to_string(), 60));
                        std::io::stdout().flush().ok();
                    }
                    Some(AgentEvent::ToolCallEnd { output, is_error, .. }) => {
                        let status = if is_error { "ERR" } else { "ok" };
                        print!("\n   → {status}: {}", short(&output, 60));
                        std::io::stdout().flush().ok();
                    }
                    Some(AgentEvent::Done { stop_reason, usage }) => {
                        println!();
                        println!(
                            "● turn {} done · stop={:?} · in/out {}/{} · ${:.6}",
                            turn,
                            stop_reason,
                            usage.input_tokens,
                            usage.output_tokens,
                            usage.cost_usd_estimate.unwrap_or(0.0),
                        );
                        println!();
                        turn += 1;
                        pending_turns = pending_turns.saturating_sub(1);
                        current_msg_id.clear();
                        waiting_for_prompt = true;

                        // If user already asked to exit and we've drained
                        // all pending turns, signal stdin EOF now.
                        if want_exit && pending_turns == 0 {
                            driver.finish_input();
                        } else if !want_exit {
                            print_prompt(turn);
                        }
                    }
                    Some(AgentEvent::Error { code, message, .. }) => {
                        eprintln!("\n✗ {code}: {message}");
                    }
                    Some(other) => {
                        eprintln!("· {other:?}");
                    }
                    None => {
                        println!("● claude exited (session={})", short(&session_id, 8));
                        break;
                    }
                }
            }
            line = stdin_rx.recv(), if read_stdin => {
                match line {
                    Some(prompt) if prompt == "exit" || prompt == "/quit" || prompt == "quit" => {
                        want_exit = true;
                        if pending_turns == 0 {
                            eprintln!("[bye] closing session…");
                            driver.finish_input();
                        } else {
                            eprintln!("[bye] waiting for {} pending turn(s)…", pending_turns);
                        }
                    }
                    Some(prompt) if prompt.is_empty() => {
                        print_prompt(turn);
                    }
                    Some(prompt) => {
                        pending_turns += 1;
                        driver.send(ClientFrame::Prompt {
                            content: vec![Content::text(prompt)],
                        }).await?;
                    }
                    None => {
                        // Stdin EOF without explicit exit. Treat as a
                        // soft request to end after current work drains.
                        stdin_open = false;
                        want_exit = true;
                        if pending_turns == 0 {
                            eprintln!("[stdin closed] closing session…");
                            driver.finish_input();
                        } else {
                            eprintln!("[stdin closed] waiting for {} pending turn(s)…", pending_turns);
                        }
                    }
                }
            }
        }
    }

    driver.shutdown().await?;
    Ok(())
}

fn print_prompt(turn: u32) {
    print!("you ({turn})> ");
    std::io::stdout().flush().ok();
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
