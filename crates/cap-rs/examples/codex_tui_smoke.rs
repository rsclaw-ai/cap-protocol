//! Live smoke for the PTY turn-completion heuristic against **real codex**.
//!
//! Drives codex's interactive TUI through [`PtyDriver`] + [`TuiParser`] and
//! reports, in clean text, when the heuristic fires `Ready` / `Done` and what
//! the screen held at each turn boundary. This is the manual validation the
//! deterministic unit tests can't give: does idle-settle + the `›` ready-marker
//! (plus the prompt-sent gate) fire at the right moment on a real codex turn?
//!
//! Prompts go through the real [`Driver::send`] `Prompt` path — which arms the
//! parser's gate and applies the settle delay before Enter — so this exercises
//! exactly what the orchestrator does. Modal-dismiss keystrokes (arrow keys)
//! still go raw via `send_bytes`.
//!
//! It does **not** mirror codex's raw byte stream: codex is a full-screen TUI
//! (absolute cursor moves, alternate screen, kitty-keyboard escapes); piping
//! those raw bytes to your terminal while also printing markers turns into
//! garbage. Instead we print the ANSI-stripped screen `TuiParser` captures at
//! each boundary.
//!
//! ```text
//! # auto-drive: dismiss the update modal, send a prompt, watch for Done
//! cargo run -p cap-rs --example codex_tui_smoke -- "Reply with exactly one word: hello"
//!
//! # interactive: each line you type is sent as a prompt; turns print on Done
//! cargo run -p cap-rs --example codex_tui_smoke
//! ```
//!
//! With the prompt-sent gate, codex's startup settles (update/permission
//! modals, MCP-server boot frames) no longer fire a spurious `Done` — only a
//! `Ready`. The first `Done` you see is the first real turn.

use std::io::Read;
use std::time::{Duration, Instant};

use cap_rs::core::{AgentEvent, ClientFrame, Content};
use cap_rs::driver::Driver;
use cap_rs::driver::pty::{PtyDriver, TuiParser};

/// What the input channel carries: raw keystrokes (modal navigation) vs a
/// prompt to submit through the real `Driver::send` path.
enum Input {
    Raw(Vec<u8>),
    Prompt(String),
}

/// Print the last few non-blank lines of a captured screen, indented, so the
/// reader can see codex's answer without the full 50-row dump.
fn print_screen_tail(screen: &str) {
    let tail: Vec<&str> = screen.lines().filter(|l| !l.trim().is_empty()).collect();
    let start = tail.len().saturating_sub(12);
    println!(
        "        ┌─ turn output (last {} lines) ─",
        tail.len() - start
    );
    for line in &tail[start..] {
        println!("        │ {}", line.trim_end());
    }
    println!("        └─");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let prompt = std::env::args().nth(1);
    let cwd = std::env::current_dir()?;

    println!("codex TUI smoke · PtyDriver + TuiParser (prompt-gated)");
    println!("  cwd: {}", cwd.display());
    match &prompt {
        Some(p) => println!("  mode: auto-drive · prompt = {p:?}"),
        None => println!("  mode: interactive (each line = a prompt; Ctrl-D to exit)"),
    }
    println!();

    let mut driver = PtyDriver::builder("codex")
        .cwd(&cwd)
        .size(50, 200)
        .spawn(TuiParser::codex())?;

    let (in_tx, mut in_rx) = tokio::sync::mpsc::channel::<Input>(32);

    if let Some(p) = prompt.clone() {
        // codex boot + MCP load runs ~10-15s; timings are deliberately loose.
        // Dismiss the update modal (Down → "Skip", Enter) with raw keystrokes,
        // then submit the prompt through the real Prompt path.
        let tx = in_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(2500)).await;
            let _ = tx.send(Input::Raw(b"\x1b[B".to_vec())).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = tx.send(Input::Raw(b"\r".to_vec())).await;
            tokio::time::sleep(Duration::from_secs(14)).await;
            let _ = tx.send(Input::Prompt(p)).await;
        });
    } else {
        let tx = in_tx.clone();
        std::thread::spawn(move || {
            let mut buf = [0u8; 1024];
            let mut stdin = std::io::stdin().lock();
            let mut line = String::new();
            loop {
                match stdin.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        for &b in &buf[..n] {
                            if b == b'\n' || b == b'\r' {
                                if tx
                                    .blocking_send(Input::Prompt(std::mem::take(&mut line)))
                                    .is_err()
                                {
                                    return;
                                }
                            } else {
                                line.push(b as char);
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }
    drop(in_tx);

    let started = Instant::now();
    let auto = prompt.is_some();
    let mut prompt_sent = false;
    let mut ready_count = 0u32;
    let mut done_count = 0u32;

    loop {
        tokio::select! {
            ev = driver.next_event() => {
                match ev {
                    Some(AgentEvent::Ready { session_id, .. }) => {
                        ready_count += 1;
                        println!(
                            "[CAP] ◀ Ready #{ready_count}  session={session_id}  @{:.2?}",
                            started.elapsed()
                        );
                    }
                    Some(AgentEvent::TextChunk { text, .. }) => print_screen_tail(&text),
                    Some(AgentEvent::Done { stop_reason, .. }) => {
                        done_count += 1;
                        println!(
                            "[CAP] ■ Done #{done_count}  stop={stop_reason:?}  @{:.2?}",
                            started.elapsed()
                        );
                        if auto && prompt_sent {
                            println!("── auto-drive turn complete, exiting");
                            break;
                        }
                    }
                    Some(other) => println!("[CAP] · {other:?}"),
                    None => {
                        println!("── codex exited");
                        break;
                    }
                }
            }
            Some(input) = in_rx.recv() => {
                match input {
                    Input::Raw(bytes) => {
                        driver.send_bytes(&bytes).await.ok();
                    }
                    Input::Prompt(text) => {
                        prompt_sent = true;
                        println!("[CAP] → prompt sent (gate armed)  @{:.2?}", started.elapsed());
                        driver
                            .send(ClientFrame::Prompt {
                                content: vec![Content::Text { text }],
                            })
                            .await
                            .ok();
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(60)) => {
                println!("── 60s timeout, exiting");
                break;
            }
        }
    }

    println!(
        "summary: {ready_count} Ready, {done_count} Done over {:.2?}",
        started.elapsed()
    );
    driver.shutdown().await.ok();
    Ok(())
}
