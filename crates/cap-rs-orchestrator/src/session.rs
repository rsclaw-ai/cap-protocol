//! One tokio task per session, owning a `Box<dyn Driver>`. Communicates only
//! over channels — no shared mutable state, no `Mutex<Driver>`.

use cap_rs::core::{AgentEvent, ClientFrame, PermissionDecision};
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
    let (inbox_tx, mut inbox_rx) = mpsc::channel::<ClientFrame>(32);

    let join = tokio::spawn(async move {
        let _ = bus
            .send(OrchestratorEvent::SessionStarted {
                session: id.clone(),
            })
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

        if let Err(e) = driver.send(frame).await {
            let _ = bus
                .send(OrchestratorEvent::SessionFailed {
                    session: id.clone(),
                    error: e.to_string(),
                })
                .await;
            return;
        }

        // Pump events until this turn ends (Done or error/cancel).
        let _ = pump_turn(&id, &mut driver, policy, &bus, &mut inbox_rx, &cancel).await;
    });

    SessionHandle {
        inbox: inbox_tx,
        join,
    }
}

/// Result of one call to [`pump_turn`].
enum TurnResult {
    /// The driver emitted `Done`; the actor should exit cleanly.
    SessionEnded,
    /// An error or cancellation occurred; the actor should stop.
    Stop,
}

/// Pump events until `Done`. Returns a [`TurnResult`] telling the outer loop
/// what to do next.
async fn pump_turn(
    id: &SessionId,
    driver: &mut Box<dyn Driver>,
    policy: PermissionPolicy,
    bus: &mpsc::Sender<OrchestratorEvent>,
    inbox_rx: &mut mpsc::Receiver<ClientFrame>,
    cancel: &CancellationToken,
) -> TurnResult {
    loop {
        let ev = tokio::select! {
            biased;
            _ = cancel.cancelled() => { let _ = driver.shutdown().await; return TurnResult::Stop; }
            ev = driver.next_event() => ev,
        };

        let Some(ev) = ev else {
            let _ = bus
                .send(OrchestratorEvent::SessionFailed {
                    session: id.clone(),
                    error: "driver exited before completing the turn".into(),
                })
                .await;
            return TurnResult::Stop;
        };

        match ev {
            AgentEvent::Done { stop_reason, .. } => {
                let _ = bus
                    .send(OrchestratorEvent::SessionDone {
                        session: id.clone(),
                        stop_reason,
                    })
                    .await;
                // A `Done` event is terminal for this session: the driver has
                // finished its work. Exit the actor so callers awaiting `join`
                // are not blocked. Multi-turn use-cases open a fresh session.
                return TurnResult::SessionEnded;
            }
            AgentEvent::PermissionRequest {
                ref req_id,
                ref tool,
                risk_level,
                ..
            } => {
                let req_id = req_id.clone();
                let tool = tool.clone();
                let _ = bus
                    .send(OrchestratorEvent::Agent {
                        session: id.clone(),
                        event: ev.clone(),
                    })
                    .await;

                let decision = match policy {
                    PermissionPolicy::Allow | PermissionPolicy::Bypass => {
                        PermissionDecision::AllowOnce
                    }
                    PermissionPolicy::Deny => PermissionDecision::Deny,
                    PermissionPolicy::Ask => {
                        let _ = bus
                            .send(OrchestratorEvent::Ask {
                                session: id.clone(),
                                req_id: req_id.clone(),
                                tool,
                                risk_level,
                            })
                            .await;
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => { let _ = driver.shutdown().await; return TurnResult::Stop; }
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
                    let _ = bus
                        .send(OrchestratorEvent::SessionFailed {
                            session: id.clone(),
                            error: e.to_string(),
                        })
                        .await;
                    return TurnResult::Stop;
                }
            }
            other => {
                let _ = bus
                    .send(OrchestratorEvent::Agent {
                        session: id.clone(),
                        event: other,
                    })
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
