//! One tokio task per session, owning a `Box<dyn Driver>`. Communicates only
//! over channels — no shared mutable state, no `Mutex<Driver>`.

use cap_rs::core::{AgentEvent, ClientFrame, PermissionDecision, ReverseRpcResult};
use cap_rs::driver::Driver;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::{PermissionPolicy, SessionId};
use crate::event::OrchestratorEvent;

/// A live session: its inbox sender + the task handle.
#[derive(Debug)]
pub struct SessionHandle {
    pub inbox: mpsc::Sender<ClientFrame>,
    pub join: JoinHandle<()>,
}

/// Spawn the actor task. Returns immediately; the task runs until its driver
/// exits, the inbox closes, or the cancel token fires.
pub fn spawn_session(
    id: SessionId,
    mut driver: Box<dyn Driver>,
    policy: PermissionPolicy,
    bus: mpsc::Sender<OrchestratorEvent>,
    cancel: CancellationToken,
) -> SessionHandle {
    let (inbox_tx, mut inbox_rx) = mpsc::channel::<ClientFrame>(256);

    let join = tokio::spawn(async move {
        bus_send(
            &bus,
            OrchestratorEvent::SessionStarted {
                session: id.clone(),
            },
            &cancel,
        )
        .await;

        // Wait for the first (and only) frame to drive the turn.
        let frame = tokio::select! {
            biased;
            _ = cancel.cancelled() => { let _ = driver.shutdown().await; return; }
            maybe = inbox_rx.recv() => match maybe {
                Some(f) => f,
                None => { let _ = driver.shutdown().await; return; }
            }
        };

        // PTY/TUI agents need to boot to their input prompt before they can
        // receive a prompt — sending earlier loses it into a not-ready
        // terminal. Such drivers ask us to wait for the agent's `Ready`. We
        // forward any boot events to the bus meanwhile. Structured drivers
        // (claude) opt out and are prompted immediately.
        if driver.prompt_after_ready() {
            loop {
                let ev = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => { let _ = driver.shutdown().await; return; }
                    ev = driver.next_event() => ev,
                };
                match ev {
                    Some(AgentEvent::Ready { .. }) => break,
                    Some(event) => {
                        bus_send(
                            &bus,
                            OrchestratorEvent::Agent {
                                session: id.clone(),
                                event,
                            },
                            &cancel,
                        )
                        .await;
                    }
                    None => {
                        bus_send(
                            &bus,
                            OrchestratorEvent::SessionFailed {
                                session: id.clone(),
                                error: "driver exited before becoming ready".into(),
                            },
                            &cancel,
                        )
                        .await;
                        return;
                    }
                }
            }
        }

        tracing::info!(session = %id, "sending prompt to driver");
        if let Err(e) = driver.send(frame).await {
            tracing::error!(session = %id, error = %e, "failed to send prompt");
            bus_send(
                &bus,
                OrchestratorEvent::SessionFailed {
                    session: id.clone(),
                    error: e.to_string(),
                },
                &cancel,
            )
            .await;
            return;
        }
        tracing::info!(session = %id, "prompt sent, entering pump_turn");

        // Pump events until this turn ends (Done or error/cancel).
        let _ = pump_turn(&id, &mut driver, policy, &bus, &mut inbox_rx, &cancel).await;
    });

    SessionHandle {
        inbox: inbox_tx,
        join,
    }
}

/// Send to bus with cancel awareness — never blocks if cancelled.
async fn bus_send(
    bus: &mpsc::Sender<OrchestratorEvent>,
    event: OrchestratorEvent,
    cancel: &CancellationToken,
) {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {},
        _ = bus.send(event) => {},
    }
}

/// Pump events until `Done`. The actor task exits after the turn completes,
/// regardless of whether the session ended cleanly, by error, or cancellation.
async fn pump_turn(
    id: &SessionId,
    driver: &mut Box<dyn Driver>,
    policy: PermissionPolicy,
    bus: &mpsc::Sender<OrchestratorEvent>,
    inbox_rx: &mut mpsc::Receiver<ClientFrame>,
    cancel: &CancellationToken,
) {
    loop {
        tracing::debug!(session = %id, "waiting for next event");
        let ev = tokio::select! {
            biased;
            _ = cancel.cancelled() => { let _ = driver.shutdown().await; return; }
            ev = driver.next_event() => ev,
        };

        let Some(ev) = ev else {
            tracing::warn!(session = %id, "driver.next_event() returned None");
            bus_send(
                bus,
                OrchestratorEvent::SessionFailed {
                    session: id.clone(),
                    error: "driver exited before completing the turn".into(),
                },
                cancel,
            )
            .await;
            return;
        };
        tracing::debug!(session = %id, event = ?ev, "received event");

        match ev {
            AgentEvent::Done { stop_reason, .. } => {
                bus_send(
                    bus,
                    OrchestratorEvent::SessionDone {
                        session: id.clone(),
                        stop_reason,
                    },
                    cancel,
                )
                .await;
                // A `Done` event is terminal for this session: the driver has
                // finished its work. Exit the actor so callers awaiting `join`
                // are not blocked. Multi-turn use-cases open a fresh session.
                return;
            }
            AgentEvent::PermissionRequest {
                ref req_id,
                ref tool,
                risk_level,
                ..
            } => {
                let req_id = req_id.clone();
                let tool = tool.clone();
                bus_send(
                    bus,
                    OrchestratorEvent::Agent {
                        session: id.clone(),
                        event: ev.clone(),
                    },
                    cancel,
                )
                .await;

                let decision = match policy {
                    PermissionPolicy::Allow | PermissionPolicy::Bypass => {
                        PermissionDecision::AllowOnce
                    }
                    PermissionPolicy::Deny => PermissionDecision::Deny,
                    PermissionPolicy::Ask => {
                        bus_send(
                            bus,
                            OrchestratorEvent::Ask {
                                session: id.clone(),
                                req_id: req_id.clone(),
                                tool,
                                risk_level,
                            },
                            cancel,
                        )
                        .await;
                        // Q7: Already cancel-safe — the nested select! below
                        // also checks cancellation before blocking on inbox_rx.
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => { let _ = driver.shutdown().await; return; }
                            maybe = inbox_rx.recv() => match maybe {
                                Some(ClientFrame::PermissionResponse { decision, .. }) => decision,
                                _ => PermissionDecision::Deny,
                            }
                        }
                    }
                };

                if let Err(e) = driver
                    .send(ClientFrame::PermissionResponse { req_id, decision })
                    .await
                {
                    bus_send(
                        bus,
                        OrchestratorEvent::SessionFailed {
                            session: id.clone(),
                            error: e.to_string(),
                        },
                        cancel,
                    )
                    .await;
                    return;
                }
            }
            AgentEvent::ReverseRpc {
                ref rpc_id,
                ref rpc,
            } => {
                let rpc_id = rpc_id.clone();
                let rpc = rpc.clone();
                bus_send(
                    bus,
                    OrchestratorEvent::ReverseRpc {
                        session: id.clone(),
                        rpc_id: rpc_id.clone(),
                        rpc,
                    },
                    cancel,
                )
                .await;
                // Wait for the consumer to respond.
                let result = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => { let _ = driver.shutdown().await; return; }
                    maybe = inbox_rx.recv() => match maybe {
                        Some(ClientFrame::ReverseRpcResult { result, .. }) => result,
                        _ => ReverseRpcResult::Success { ok: false },
                    }
                };
                if let Err(e) = driver
                    .send(ClientFrame::ReverseRpcResult { rpc_id, result })
                    .await
                {
                    bus_send(
                        bus,
                        OrchestratorEvent::SessionFailed {
                            session: id.clone(),
                            error: e.to_string(),
                        },
                        cancel,
                    )
                    .await;
                    return;
                }
            }
            other => {
                bus_send(
                    bus,
                    OrchestratorEvent::Agent {
                        session: id.clone(),
                        event: other,
                    },
                    cancel,
                )
                .await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PermissionPolicy;
    use crate::event::OrchestratorEvent;
    use crate::testing::StubDriver;
    use cap_rs::core::{ClientFrame, Content, PermissionDecision, RiskLevel, StopReason};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    fn prompt(s: &str) -> ClientFrame {
        ClientFrame::Prompt {
            content: vec![Content::text(s)],
        }
    }

    #[tokio::test]
    async fn pumps_events_and_signals_done() {
        let driver = Box::new(StubDriver::new("a").text("hi").done(StopReason::EndTurn));
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let token = CancellationToken::new();
        let handle = spawn_session("a".into(), driver, PermissionPolicy::Allow, bus_tx, token);

        handle.inbox.send(prompt("go")).await.unwrap();

        let mut kinds = Vec::new();
        while let Some(ev) = bus_rx.recv().await {
            match ev {
                OrchestratorEvent::SessionStarted { .. } => kinds.push("started"),
                OrchestratorEvent::Agent { .. } => kinds.push("agent"),
                OrchestratorEvent::SessionDone { stop_reason, .. } => {
                    assert_eq!(stop_reason, StopReason::EndTurn);
                    kinds.push("done");
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(kinds, vec!["started", "agent", "done"]);
        handle.join.await.unwrap();
    }

    #[tokio::test]
    async fn await_ready_driver_waits_for_ready_then_prompts() {
        // A PTY-like driver: boot noise, then Ready, then the turn. The prompt
        // must be held until Ready and then delivered exactly once.
        let sink = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let driver = Box::new(
            StubDriver::new("a")
                .await_ready()
                .text("booting")
                .ready()
                .text("answer")
                .done(StopReason::EndTurn)
                .capture(sink.clone()),
        );
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let token = CancellationToken::new();
        let handle = spawn_session("a".into(), driver, PermissionPolicy::Allow, bus_tx, token);
        handle.inbox.send(prompt("go")).await.unwrap();

        let mut done = false;
        while let Some(ev) = bus_rx.recv().await {
            if let OrchestratorEvent::SessionDone { .. } = ev {
                done = true;
                break;
            }
        }
        assert!(done, "session should complete after Ready");
        assert_eq!(
            *sink.lock().unwrap(),
            vec!["go".to_string()],
            "prompt delivered once, only after Ready"
        );
        handle.join.await.unwrap();
    }

    #[tokio::test]
    async fn await_ready_driver_fails_if_never_ready() {
        // No Ready is ever scripted; the driver exits while we wait → fail loud
        // rather than send a prompt into a not-ready agent or hang forever.
        let driver = Box::new(StubDriver::new("a").await_ready().text("noise"));
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let token = CancellationToken::new();
        let handle = spawn_session("a".into(), driver, PermissionPolicy::Allow, bus_tx, token);
        handle.inbox.send(prompt("go")).await.unwrap();

        let mut failed = false;
        while let Some(ev) = bus_rx.recv().await {
            if let OrchestratorEvent::SessionFailed { error, .. } = ev {
                assert!(error.contains("before becoming ready"), "got {error}");
                failed = true;
                break;
            }
        }
        assert!(
            failed,
            "must surface SessionFailed when Ready never arrives"
        );
        handle.join.await.unwrap();
    }

    #[tokio::test]
    async fn allow_policy_auto_approves_permission() {
        let driver = Box::new(
            StubDriver::new("a")
                .permission("Bash", RiskLevel::Medium)
                .done(StopReason::EndTurn),
        );
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let token = CancellationToken::new();
        let handle = spawn_session("a".into(), driver, PermissionPolicy::Allow, bus_tx, token);
        handle.inbox.send(prompt("go")).await.unwrap();

        let mut saw_ask = false;
        while let Some(ev) = bus_rx.recv().await {
            match ev {
                OrchestratorEvent::Ask { .. } => saw_ask = true,
                OrchestratorEvent::SessionDone { .. } => break,
                _ => {}
            }
        }
        assert!(!saw_ask, "Allow policy must not surface an Ask");
    }

    #[tokio::test]
    async fn ask_policy_surfaces_ask_and_awaits_decision() {
        let driver = Box::new(
            StubDriver::new("a")
                .permission("Bash", RiskLevel::High)
                .done(StopReason::EndTurn),
        );
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let token = CancellationToken::new();
        let handle = spawn_session("a".into(), driver, PermissionPolicy::Ask, bus_tx, token);
        handle.inbox.send(prompt("go")).await.unwrap();

        loop {
            if let OrchestratorEvent::Ask { req_id, .. } = bus_rx.recv().await.unwrap() {
                handle
                    .inbox
                    .send(ClientFrame::PermissionResponse {
                        req_id,
                        decision: PermissionDecision::AllowOnce,
                    })
                    .await
                    .unwrap();
                break;
            }
        }
        let mut saw_done = false;
        while let Some(ev) = bus_rx.recv().await {
            if let OrchestratorEvent::SessionDone { .. } = ev {
                saw_done = true;
                break;
            }
        }
        assert!(saw_done);
    }
}
