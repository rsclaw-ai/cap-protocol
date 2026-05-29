//! One tokio task per session, owning a `Box<dyn Driver>`. Communicates only
//! over channels — no shared mutable state, no `Mutex<Driver>`.

use std::path::PathBuf;

use cap_rs::core::{
    AgentEvent, CancelScope, ClientFrame, PermissionDecision, PermissionMode, ReverseRpcResult,
    SessionConfig,
};
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

/// Per-session config to thread from orchestrator → session actor.
#[derive(Debug, Clone, Default)]
pub struct SessionSpawnConfig {
    pub model: Option<String>,
    pub system_prompt: Option<String>,
    pub max_turns: Option<u32>,
    pub budget_usd: Option<f64>,
}

/// Spawn the actor task. Returns immediately; the task runs until its driver
/// exits, the inbox closes, or the cancel token fires.
pub fn spawn_session(
    id: SessionId,
    driver: Box<dyn Driver>,
    policy: PermissionPolicy,
    cwd: PathBuf,
    bus: mpsc::Sender<OrchestratorEvent>,
    cancel: CancellationToken,
    spawn_cfg: SessionSpawnConfig,
) -> SessionHandle {
    spawn_session_with_options(id, driver, policy, cwd, bus, cancel, false, spawn_cfg)
}

pub fn spawn_chat_session(
    id: SessionId,
    driver: Box<dyn Driver>,
    policy: PermissionPolicy,
    cwd: PathBuf,
    bus: mpsc::Sender<OrchestratorEvent>,
    cancel: CancellationToken,
    spawn_cfg: SessionSpawnConfig,
) -> SessionHandle {
    spawn_session_with_options(id, driver, policy, cwd, bus, cancel, true, spawn_cfg)
}

#[allow(clippy::too_many_arguments)]
fn spawn_session_with_options(
    id: SessionId,
    mut driver: Box<dyn Driver>,
    policy: PermissionPolicy,
    cwd: PathBuf,
    bus: mpsc::Sender<OrchestratorEvent>,
    cancel: CancellationToken,
    keep_alive_after_done: bool,
    spawn_cfg: SessionSpawnConfig,
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

        // Wait for the first frame to drive the turn. This is the task prompt
        // supplied by the orchestrator; CAP session config is synthesized from
        // the session actor's launch context and sent first.
        let frame = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                let _ = driver.send(ClientFrame::Cancel { scope: CancelScope::Session, reason: Some("orchestrator_cancel".into()) }).await;
                let _ = driver.shutdown().await;
                return;
            }
            maybe = inbox_rx.recv() => match maybe {
                Some(f) => f,
                None => { let _ = driver.shutdown().await; return; }
            }
        };

        let permission_mode = match policy {
            PermissionPolicy::Ask => PermissionMode::Interactive,
            PermissionPolicy::Allow | PermissionPolicy::Bypass => PermissionMode::Confirm,
            PermissionPolicy::Deny => PermissionMode::None,
        };
        let mut config = SessionConfig::new(cwd);
        config.permission_mode = permission_mode;
        config.model = spawn_cfg.model;
        config.system_prompt = spawn_cfg.system_prompt;
        config.max_turns = spawn_cfg.max_turns;
        config.budget_usd = spawn_cfg.budget_usd;
        if let Err(e) = driver.send(ClientFrame::SessionConfig(config)).await {
            tracing::error!(session = %id, error = %e, "failed to send session config");
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

        // PTY/TUI agents need to boot to their input prompt before they can
        // receive a prompt — sending earlier loses it into a not-ready
        // terminal. Such drivers ask us to wait for the agent's `Ready`. We
        // forward any boot events to the bus meanwhile. Structured drivers
        // (claude) opt out and are prompted immediately.
        if driver.prompt_after_ready() {
            loop {
                let ev = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        let _ = driver.send(ClientFrame::Cancel { scope: CancelScope::Session, reason: Some("orchestrator_cancel".into()) }).await;
                        let _ = driver.shutdown().await;
                        return;
                    }
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
        let _ = pump_turn(
            &id,
            &mut driver,
            policy,
            &bus,
            &mut inbox_rx,
            &cancel,
            keep_alive_after_done,
        )
        .await;
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
    keep_alive_after_done: bool,
) {
    let mut awaiting_prompt = false;
    loop {
        tracing::debug!(session = %id, "waiting for next event");
        enum PumpInput {
            Cancelled,
            DriverEvent(Option<AgentEvent>),
            Inbox(Option<ClientFrame>),
        }
        let input = tokio::select! {
            biased;
            _ = cancel.cancelled() => PumpInput::Cancelled,
            frame = inbox_rx.recv() => PumpInput::Inbox(frame),
            ev = driver.next_event(), if !awaiting_prompt => PumpInput::DriverEvent(ev),
        };

        let ev = match input {
            PumpInput::Cancelled => {
                let _ = driver.send(ClientFrame::Cancel { scope: CancelScope::Session, reason: Some("orchestrator_cancel".into()) }).await;
                let _ = driver.shutdown().await;
                return;
            }
            PumpInput::Inbox(Some(frame)) => {
                if let ClientFrame::SessionConfig(_) = frame {
                    continue;
                }
                awaiting_prompt = false;
                if let Err(e) = driver.send(frame).await {
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
                continue;
            }
            PumpInput::Inbox(None) => {
                let _ = driver.shutdown().await;
                return;
            }
            PumpInput::DriverEvent(ev) => ev,
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
                if keep_alive_after_done {
                    awaiting_prompt = true;
                    continue;
                }
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
                    PermissionPolicy::Allow | PermissionPolicy::Bypass
                        if risk_level != cap_rs::core::RiskLevel::High =>
                    {
                        PermissionDecision::AllowOnce
                    }
                    PermissionPolicy::Deny => PermissionDecision::Deny,
                    PermissionPolicy::Ask | PermissionPolicy::Allow | PermissionPolicy::Bypass => {
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
                            _ = cancel.cancelled() => {
                                let _ = driver.send(ClientFrame::Cancel { scope: CancelScope::Session, reason: Some("orchestrator_cancel".into()) }).await;
                                let _ = driver.shutdown().await;
                                return;
                            }
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
                    _ = cancel.cancelled() => {
                        let _ = driver.send(ClientFrame::Cancel { scope: CancelScope::Session, reason: Some("orchestrator_cancel".into()) }).await;
                        let _ = driver.shutdown().await;
                        return;
                    }
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
            AgentEvent::AskUser {
                ref ask_id,
                ref prompt,
                ref ask_kind,
                ref options,
                ..
            } => {
                let ask_id = ask_id.clone();
                bus_send(
                    bus,
                    OrchestratorEvent::AskUser {
                        session: id.clone(),
                        ask_id: ask_id.clone(),
                        prompt: prompt.clone(),
                        ask_kind: ask_kind.clone(),
                        options: options.clone(),
                    },
                    cancel,
                )
                .await;
                // Wait for the consumer to answer.
                let value = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        let _ = driver.send(ClientFrame::Cancel { scope: CancelScope::Session, reason: Some("orchestrator_cancel".into()) }).await;
                        let _ = driver.shutdown().await;
                        return;
                    }
                    maybe = inbox_rx.recv() => match maybe {
                        Some(ClientFrame::AskUserAnswer { value, .. }) => value,
                        _ => serde_json::Value::Null,
                    }
                };
                if let Err(e) = driver
                    .send(ClientFrame::AskUserAnswer { ask_id, value })
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

    fn spawn_test_session(
        id: SessionId,
        driver: Box<dyn Driver>,
        policy: PermissionPolicy,
        cwd: PathBuf,
        bus: mpsc::Sender<OrchestratorEvent>,
        cancel: CancellationToken,
    ) -> SessionHandle {
        spawn_session(id, driver, policy, cwd, bus, cancel, SessionSpawnConfig::default())
    }
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

    fn test_cwd() -> std::path::PathBuf {
        std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
    }

    #[tokio::test]
    async fn pumps_events_and_signals_done() {
        let driver = Box::new(StubDriver::new("a").text("hi").done(StopReason::EndTurn));
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let token = CancellationToken::new();
        let handle = spawn_test_session(
            "a".into(),
            driver,
            PermissionPolicy::Allow,
            test_cwd(),
            bus_tx,
            token,
        );

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
        let handle = spawn_test_session(
            "a".into(),
            driver,
            PermissionPolicy::Allow,
            test_cwd(),
            bus_tx,
            token,
        );
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
    async fn sends_session_config_before_prompt() {
        let frame_kinds = std::sync::Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));
        let driver = Box::new(
            StubDriver::new("a")
                .ready()
                .done(StopReason::EndTurn)
                .capture_frame_kinds(frame_kinds.clone()),
        );
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let token = CancellationToken::new();
        let handle = spawn_test_session(
            "a".into(),
            driver,
            PermissionPolicy::Allow,
            test_cwd(),
            bus_tx,
            token,
        );
        handle.inbox.send(prompt("go")).await.unwrap();

        while let Some(ev) = bus_rx.recv().await {
            if let OrchestratorEvent::SessionDone { .. } = ev {
                break;
            }
        }

        assert_eq!(
            *frame_kinds.lock().unwrap(),
            vec!["SessionConfig", "Prompt"]
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
        let handle = spawn_test_session(
            "a".into(),
            driver,
            PermissionPolicy::Allow,
            test_cwd(),
            bus_tx,
            token,
        );
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
        let handle = spawn_test_session(
            "a".into(),
            driver,
            PermissionPolicy::Allow,
            test_cwd(),
            bus_tx,
            token,
        );
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
    async fn high_risk_is_not_auto_approved() {
        let driver = Box::new(
            StubDriver::new("a")
                .permission("Bash", RiskLevel::High)
                .done(StopReason::EndTurn),
        );
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let token = CancellationToken::new();
        let handle = spawn_test_session(
            "a".into(),
            driver,
            PermissionPolicy::Allow,
            test_cwd(),
            bus_tx,
            token,
        );
        handle.inbox.send(prompt("go")).await.unwrap();

        let mut saw_ask = false;
        loop {
            match bus_rx.recv().await.unwrap() {
                OrchestratorEvent::Ask {
                    req_id, risk_level, ..
                } => {
                    assert_eq!(risk_level, RiskLevel::High);
                    saw_ask = true;
                    handle
                        .inbox
                        .send(ClientFrame::PermissionResponse {
                            req_id,
                            decision: PermissionDecision::Deny,
                        })
                        .await
                        .unwrap();
                }
                OrchestratorEvent::SessionDone { .. } => break,
                _ => {}
            }
        }
        assert!(saw_ask, "high-risk permission must ask even under Allow");
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
        let handle = spawn_test_session(
            "a".into(),
            driver,
            PermissionPolicy::Ask,
            test_cwd(),
            bus_tx,
            token,
        );
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

    #[tokio::test]
    async fn chat_session_waits_for_next_prompt_after_done() {
        let driver = Box::new(StubDriver::new("a").done(StopReason::EndTurn));
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let token = CancellationToken::new();
        let handle = spawn_chat_session(
            "a".into(),
            driver,
            PermissionPolicy::Allow,
            test_cwd(),
            bus_tx,
            token.clone(),
            SessionSpawnConfig::default(),
        );
        handle.inbox.send(prompt("go")).await.unwrap();

        let mut saw_done = false;
        while let Some(ev) = bus_rx.recv().await {
            if let OrchestratorEvent::SessionDone { .. } = ev {
                saw_done = true;
                break;
            }
        }
        assert!(saw_done);

        let next = tokio::time::timeout(std::time::Duration::from_millis(100), bus_rx.recv()).await;
        assert!(
            next.is_err(),
            "chat session should wait silently after Done"
        );

        token.cancel();
        handle.join.await.unwrap();
    }

    #[tokio::test]
    async fn mid_turn_user_message_is_forwarded() {
        let frame_kinds = std::sync::Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));
        let driver = Box::new(
            StubDriver::new("a")
                .delay_events(std::time::Duration::from_millis(100))
                .done(StopReason::EndTurn)
                .capture_frame_kinds(frame_kinds.clone()),
        );
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let token = CancellationToken::new();
        let handle = spawn_test_session(
            "a".into(),
            driver,
            PermissionPolicy::Allow,
            test_cwd(),
            bus_tx,
            token,
        );
        handle.inbox.send(prompt("first")).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        handle.inbox.send(prompt("second")).await.unwrap();

        while let Some(ev) = bus_rx.recv().await {
            if let OrchestratorEvent::SessionDone { .. } = ev {
                break;
            }
        }

        assert_eq!(
            *frame_kinds.lock().unwrap(),
            vec!["SessionConfig", "Prompt", "Prompt"]
        );
        handle.join.await.unwrap();
    }
}
