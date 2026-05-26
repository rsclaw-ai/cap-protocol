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
use crate::config::{Action, FleetSpec, PermissionPolicy, SessionId, Split};
use crate::event::{OrchestratorControl, OrchestratorEvent};
use crate::factory::DriverFactory;
use crate::registry::SessionRegistry;
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
    /// Start the fleet. Returns a handle plus the outbound event stream.
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
    /// Build the prompt for a routed/fanned-out session: the original task plus
    /// the accumulated output of each upstream (trigger) session. Falls back to
    /// just the task if no upstream produced text.
    fn routed_payload(&self, triggers: &[String]) -> String {
        let mut parts = Vec::new();
        for t in triggers {
            if let Some(buf) = self.buffers.get(t) {
                if !buf.is_empty() {
                    parts.push(format!("--- output from {t} ---\n{buf}"));
                }
            }
        }
        if parts.is_empty() {
            self.task.clone()
        } else {
            format!("{}\n\n{}", self.task, parts.join("\n\n"))
        }
    }

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
    /// Forwards `SessionDone` to the consumer before processing routes (so the
    /// consumer sees done-then-routed ordering), or `SessionFailed` when routing
    /// (e.g. `by_subtask`) fails (no `SessionDone` emitted).
    async fn on_session_done(&mut self, session: &SessionId, stop: StopReason) -> bool {
        debug!(%session, ?stop, "session done");
        self.done.insert(session.clone());

        // A join fires only once — when its last member completes — because
        // every member is in `done` only then.
        enum Fire {
            Route(SessionId, ClientFrame),
            Select(Vec<SessionId>),
        }

        let mut fires: Vec<Fire> = Vec::new();
        let mut routing_failed = false;

        for route in &self.spec.fleet.routes {
            let triggers = route.trigger_sessions();
            if !triggers.iter().any(|t| t == session) {
                continue;
            }
            if !triggers.iter().all(|t| self.done.contains(t)) {
                continue; // join not yet complete
            }
            match route.action().expect("validated") {
                Action::RouteTo(to) => {
                    let payload = self.routed_payload(&triggers);
                    fires.push(Fire::Route(to, task_prompt(&payload)));
                }
                Action::FanOut(f) => match f.split {
                    Split::Broadcast => {
                        let payload = self.routed_payload(&triggers);
                        for to in f.to {
                            fires.push(Fire::Route(to, task_prompt(&payload)));
                        }
                    }
                    Split::BySubtask => {
                        let buf = self.buffers.get(session).cloned().unwrap_or_default();
                        match parse_subtasks(&buf) {
                            Some(items) if !items.is_empty() => {
                                for (i, to) in f.to.iter().enumerate() {
                                    // Guard against more targets than subtasks:
                                    // stop assigning rather than wrapping around.
                                    if i >= items.len() {
                                        routing_failed = true;
                                        break;
                                    }
                                    let sub = items[i].clone();
                                    fires.push(Fire::Route(to.clone(), task_prompt(&sub)));
                                }
                            }
                            _ => {
                                routing_failed = true;
                            }
                        }
                    }
                },
                Action::Collect(_) => fires.push(Fire::Select(triggers.clone())),
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

/// Parse a fenced `cap-subtasks` block — a JSON array of strings — out of agent
/// text. The fence is three backticks; the delimiter is built at runtime so
/// this source stays free of literal triple-backticks.
fn parse_subtasks(text: &str) -> Option<Vec<String>> {
    let fence = "`".repeat(3);
    let open = format!("{fence}cap-subtasks");
    let start = text.find(&open)? + open.len();
    let rest = &text[start..];
    let end = rest.find(&fence)?;
    serde_json::from_str::<Vec<String>>(rest[..end].trim()).ok()
}
