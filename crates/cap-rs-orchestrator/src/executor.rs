//! Deterministic state machine. Owns the registry + audit log; interprets the
//! DSL to drive fan-out, joins, and routing. Runs in its own task; the consumer
//! reads `OrchestratorEvent`s from the returned channel.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use cap_rs::core::{AgentEvent, ClientFrame, Content, PermissionDecision, StopReason, TextChannel};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::OrchestratorError;
use crate::audit::AuditLog;
use crate::config::{FleetSpec, PermissionPolicy, SessionId};
use crate::event::{OrchestratorControl, OrchestratorEvent};
use crate::factory::DriverFactory;
use crate::registry::SessionRegistry;
use crate::routing::{RouteDecision, RoutingContext, RoutingStrategy, StaticRouting};
use crate::worktree::WorktreeManager;

/// A handle to a running fleet: query the audit log, answer asks, cancel.
#[derive(Debug)]
pub struct ExecutorHandle {
    cancel: CancellationToken,
    control: mpsc::Sender<OrchestratorControl>,
    audit: Arc<Mutex<AuditLog>>,
}

impl ExecutorHandle {
    /// Snapshot the audit log as `(from, to)` pairs in order. Readable even
    /// after the fleet completes — the log is shared, not message-passed.
    pub fn audit_pairs(&self) -> Vec<(SessionId, SessionId)> {
        use crate::audit::AuditEvent;
        self.audit
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .records()
            .iter()
            .filter_map(|r| match &r.event {
                AuditEvent::Route { from, to } => Some((from.clone(), to.clone())),
                _ => None,
            })
            .collect()
    }

    /// Answer an [`OrchestratorEvent::Ask`] (only needed under `ask` policy).
    pub async fn decide(&self, session: SessionId, req_id: String, allow: bool) {
        let _ = self
            .control
            .send(OrchestratorControl::Decision {
                session,
                req_id,
                allow,
            })
            .await;
    }

    /// A cloneable control sender — e.g. for a Ctrl-C task to send `Cancel`.
    pub fn control_sender(&self) -> mpsc::Sender<OrchestratorControl> {
        self.control.clone()
    }

    /// Hard-cancel the whole fleet.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

/// Build the initial prompt frame for a session.
fn task_prompt(task: &str) -> ClientFrame {
    ClientFrame::Prompt {
        content: vec![Content::text(task)],
    }
}

#[derive(Debug)]
pub struct Executor;

impl Executor {
    /// Start the fleet with the default static YAML routing strategy.
    pub async fn start<F, W>(
        spec: FleetSpec,
        factory: F,
        worktree: W,
        task: &str,
    ) -> Result<(ExecutorHandle, mpsc::Receiver<OrchestratorEvent>), OrchestratorError>
    where
        F: DriverFactory + 'static,
        W: WorktreeManager + 'static,
    {
        let strategy = StaticRouting::new(spec.fleet.routes.clone());
        Self::start_with_strategy(spec, factory, worktree, task, strategy).await
    }

    /// Start the fleet with a custom routing strategy.
    pub async fn start_with_strategy<F, W, S>(
        spec: FleetSpec,
        factory: F,
        worktree: W,
        task: &str,
        strategy: S,
    ) -> Result<(ExecutorHandle, mpsc::Receiver<OrchestratorEvent>), OrchestratorError>
    where
        F: DriverFactory + 'static,
        W: WorktreeManager + 'static,
        S: RoutingStrategy,
    {
        spec.validate()?;

        let (out_tx, out_rx) = mpsc::channel::<OrchestratorEvent>(256);
        let (bus_tx, bus_rx) = mpsc::channel::<OrchestratorEvent>(256);
        let (control_tx, control_rx) = mpsc::channel::<OrchestratorControl>(32);
        let cancel = CancellationToken::new();
        let audit = Arc::new(Mutex::new(AuditLog::new()));

        let handle = ExecutorHandle {
            cancel: cancel.clone(),
            control: control_tx,
            audit: Arc::clone(&audit),
        };

        let task = task.to_string();
        tokio::spawn(async move {
            let mut run = Run {
                spec,
                strategy: Box::new(strategy),
                factory,
                worktree,
                task,
                registry: SessionRegistry::new(),
                audit,
                done: HashSet::new(),
                spawned: HashSet::new(),
                failed: HashSet::new(),
                buffers: HashMap::new(),
                out: out_tx,
                bus_tx,
                cancel,
            };
            run.drive(bus_rx, control_rx).await;
        });

        Ok((handle, out_rx))
    }
}

struct Run<F: DriverFactory, W: WorktreeManager> {
    spec: FleetSpec,
    strategy: Box<dyn RoutingStrategy>,
    factory: F,
    worktree: W,
    task: String,
    registry: SessionRegistry,
    audit: Arc<Mutex<AuditLog>>,
    done: HashSet<SessionId>,
    /// Sessions successfully spawned (so we know what must still settle).
    spawned: HashSet<SessionId>,
    /// Sessions that failed (driver crash, or reported SessionFailed).
    failed: HashSet<SessionId>,
    /// Accumulated assistant text per session, used to parse `by_subtask` blocks.
    buffers: HashMap<SessionId, String>,
    out: mpsc::Sender<OrchestratorEvent>,
    bus_tx: mpsc::Sender<OrchestratorEvent>,
    cancel: CancellationToken,
}

impl<F: DriverFactory, W: WorktreeManager> Run<F, W> {
    /// Effective permission policy for a session (per-session override or fleet default).
    fn policy_for(&self, id: &str) -> Result<PermissionPolicy, OrchestratorError> {
        let s = self.spec.fleet.sessions.get(id).ok_or_else(|| {
            OrchestratorError::Config(format!("session '{id}' not found in fleet spec"))
        })?;
        Ok(s.permissions.unwrap_or(self.spec.fleet.permissions))
    }

    async fn spawn(&mut self, id: &SessionId) -> bool {
        let kind = match self.spec.fleet.sessions.get(id) {
            Some(s) => s.driver.clone(),
            None => return false,
        };
        let policy = match self.policy_for(id) {
            Ok(p) => p,
            Err(e) => {
                let _ = self
                    .out
                    .send(OrchestratorEvent::SessionFailed {
                        session: id.clone(),
                        error: e.to_string(),
                    })
                    .await;
                return false;
            }
        };
        let base = &self.spec.fleet.base_branch;
        match self
            .registry
            .spawn(
                id.clone(),
                &kind,
                policy,
                base,
                &self.factory,
                &self.worktree,
                &self.bus_tx,
                &self.cancel,
            )
            .await
        {
            Ok(()) => {
                self.spawned.insert(id.clone());
                true
            }
            Err(e) => {
                let _ = self
                    .out
                    .send(OrchestratorEvent::SessionFailed {
                        session: id.clone(),
                        error: e.to_string(),
                    })
                    .await;
                false
            }
        }
    }

    /// Deliver the initial task prompt to a freshly spawned session.
    async fn kick(&self, id: &SessionId) {
        let _ = self.registry.route(id, task_prompt(&self.task)).await;
    }

    async fn drive(
        &mut self,
        mut bus_rx: mpsc::Receiver<OrchestratorEvent>,
        mut control_rx: mpsc::Receiver<OrchestratorControl>,
    ) {
        // Spawn + kick the start sessions.
        for id in self.spec.fleet.start.sessions() {
            if self.spawn(&id).await {
                self.kick(&id).await;
            }
        }

        // If nothing is pending (e.g. all start sessions failed to spawn), finish now.
        if self.fleet_complete() {
            let _ = self.out.send(OrchestratorEvent::FleetComplete).await;
            self.registry.shutdown().await;
            return;
        }

        // Clone the token so the select! arm does NOT borrow `self`, leaving the
        // handlers free to take `&mut self`. (Required for borrowck.)
        let cancel = self.cancel.clone();

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                Some(ctrl) = control_rx.recv() => self.on_control(ctrl).await,
                maybe = bus_rx.recv() => {
                    let Some(ev) = maybe else { break };

                    // Accumulate assistant text so `by_subtask` can parse the lead's output.
                    if let OrchestratorEvent::Agent {
                        session,
                        event: AgentEvent::TextChunk { text, channel: TextChannel::Assistant, .. },
                    } = &ev
                    {
                        self.buffers.entry(session.clone()).or_default().push_str(text);
                    }

                    // Record failed sessions BEFORE checking fleet_complete,
                    // otherwise the completion check won't see the failure and
                    // the fleet will hang waiting for a session that already died.
                    if let OrchestratorEvent::SessionFailed { ref session, .. } = ev {
                        self.failed.insert(session.clone());
                    }
                    match ev {
                        OrchestratorEvent::SessionDone { session, stop_reason } => {
                            // Don't forward SessionDone before routing — the consumer
                            // may see SessionFailed instead if routing (e.g. by_subtask)
                            // fails. on_session_done sends the appropriate event.
                            if self.on_session_done(&session, stop_reason).await {
                                let _ = self.out.send(OrchestratorEvent::FleetComplete).await;
                                break;
                            }
                        }
                        ev @ OrchestratorEvent::SessionFailed { .. } => {
                            let _ = self.out.send(ev).await;
                            if self.fleet_complete() {
                                let _ = self.out.send(OrchestratorEvent::FleetComplete).await;
                                break;
                            }
                        }
                        other => {
                            let _ = self.out.send(other).await;
                        }
                    }
                }
            }
        }

        // Ensure all session actors see the cancellation and stop promptly.
        self.cancel.cancel();

        self.registry.shutdown().await;

        // Always clean up worktrees. On cancellation the user can still inspect
        // the audit log and event stream; orphaned git worktrees are worse than
        // losing the filesystem state of a cancelled run.
        for id in self.spawned.drain() {
            if let Err(e) = self.worktree.cleanup(&id) {
                tracing::warn!(session = %id, error = %e, "worktree cleanup failed");
            }
        }
    }

    /// Handle a control message from the consumer (decision / cancel / select).
    async fn on_control(&mut self, ctrl: OrchestratorControl) {
        match ctrl {
            OrchestratorControl::Decision {
                session,
                req_id,
                allow,
            } => {
                let decision = if allow {
                    PermissionDecision::AllowOnce
                } else {
                    PermissionDecision::Deny
                };
                let _ = self
                    .registry
                    .route(
                        &session,
                        ClientFrame::PermissionResponse { req_id, decision },
                    )
                    .await;
            }
            OrchestratorControl::Cancel => self.cancel.cancel(),
            OrchestratorControl::ReverseRpcResult {
                session,
                rpc_id,
                result,
            } => {
                let _ = self
                    .registry
                    .route(&session, ClientFrame::ReverseRpcResult { rpc_id, result })
                    .await;
            }
            // v1: selection is informational; the human merges the chosen worktree.
            OrchestratorControl::Select { .. } => {}
        }
    }

    /// React to a session finishing. Returns `true` when the fleet is complete.
    /// Delegates routing decisions to the [`RoutingStrategy`].
    async fn on_session_done(&mut self, session: &SessionId, stop: StopReason) -> bool {
        debug!(%session, ?stop, "session done");
        self.done.insert(session.clone());

        let ctx = RoutingContext {
            spec: &self.spec,
            done: &self.done,
            failed: &self.failed,
            spawned: &self.spawned,
            buffers: &self.buffers,
            task: &self.task,
        };

        let decisions = self.strategy.on_session_done(&ctx, session, stop).await;

        enum Fire {
            Route(SessionId, ClientFrame),
            Select(Vec<SessionId>),
        }

        let mut fires: Vec<Fire> = Vec::new();
        let mut routing_failed = false;

        for d in decisions {
            match d {
                RouteDecision::Route { target, payload } => {
                    fires.push(Fire::Route(target, task_prompt(&payload)));
                }
                RouteDecision::FanOut { targets } => {
                    for (target, payload) in targets {
                        fires.push(Fire::Route(target, task_prompt(&payload)));
                    }
                }
                RouteDecision::Select { candidates } => {
                    fires.push(Fire::Select(candidates));
                }
                RouteDecision::Error(_) => {
                    routing_failed = true;
                }
                RouteDecision::None => {}
            }
        }

        // Forward SessionDone before processing routes (done → routed ordering).
        // When routing fails, send SessionFailed instead (no done).
        let from = session.clone();
        if routing_failed {
            self.done.remove(&from);
            self.failed.insert(from.clone());
            let _ = self
                .out
                .send(OrchestratorEvent::SessionFailed {
                    session: from.clone(),
                    error: "fan_out by_subtask: lead emitted no parseable \
                             cap-subtasks JSON-array block"
                        .into(),
                })
                .await;
        } else {
            let _ = self
                .out
                .send(OrchestratorEvent::SessionDone {
                    session: from.clone(),
                    stop_reason: stop,
                })
                .await;
        }

        for fire in fires {
            match fire {
                Fire::Route(to, frame) => {
                    if !self.registry.is_live(&to) && !self.spawn(&to).await {
                        continue;
                    }
                    self.audit
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .record_route(&from, &to);
                    let _ = self
                        .out
                        .send(OrchestratorEvent::Routed {
                            from: from.clone(),
                            to: to.clone(),
                        })
                        .await;
                    let _ = self.registry.route(&to, frame).await;
                }
                Fire::Select(candidates) => {
                    let _ = self
                        .out
                        .send(OrchestratorEvent::AwaitSelection { candidates })
                        .await;
                }
            }
        }

        self.fleet_complete()
    }

    /// The fleet is complete once every spawned session has settled — i.e.
    /// completed (`done`) or failed. Sessions that were never spawned (e.g. the
    /// targets of a route that never fired because its lead failed) do not block
    /// completion; this is how failed branches terminate without hanging while
    /// sibling branches still finish.
    fn fleet_complete(&self) -> bool {
        self.spawned
            .iter()
            .all(|s| self.done.contains(s) || self.failed.contains(s))
    }
}
