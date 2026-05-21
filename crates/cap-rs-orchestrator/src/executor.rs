//! Deterministic state machine. Owns the registry + audit log; interprets the
//! DSL to drive fan-out, joins, and routing. Runs in its own task; the consumer
//! reads `OrchestratorEvent`s from the returned channel.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use cap_rs::core::{AgentEvent, ClientFrame, Content, PermissionDecision, StopReason, TextChannel};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

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
        self.audit
            .lock()
            .unwrap()
            .records()
            .iter()
            .map(|r| (r.from.clone(), r.to.clone()))
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
    fn policy_for(&self, id: &str) -> PermissionPolicy {
        self.spec.fleet.sessions[id]
            .permissions
            .unwrap_or(self.spec.fleet.permissions)
    }

    async fn spawn(&mut self, id: &SessionId) -> bool {
        let kind = self.spec.fleet.sessions[id].driver.clone();
        let policy = self.policy_for(id);
        let base = self.spec.fleet.base_branch.clone();
        match self
            .registry
            .spawn(
                id.clone(),
                &kind,
                policy,
                &base,
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

                    // Forward every engine event to the consumer.
                    let _ = self.out.send(ev.clone()).await;

                    match ev {
                        OrchestratorEvent::SessionDone { session, stop_reason } => {
                            if self.on_session_done(&session, stop_reason).await {
                                let _ = self.out.send(OrchestratorEvent::FleetComplete).await;
                                break;
                            }
                        }
                        OrchestratorEvent::SessionFailed { session, .. } => {
                            self.failed.insert(session);
                            if self.fleet_complete() {
                                let _ = self.out.send(OrchestratorEvent::FleetComplete).await;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        self.registry.shutdown().await;
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
            // v1: selection is informational; the human merges the chosen worktree.
            OrchestratorControl::Select { .. } => {}
        }
    }

    /// React to a session finishing. Returns `true` when the fleet is complete.
    async fn on_session_done(&mut self, session: &SessionId, _stop: StopReason) -> bool {
        self.done.insert(session.clone());

        // A join fires only once — when its last member completes — because
        // every member is in `done` only then.
        enum Fire {
            Route(SessionId, ClientFrame),
            Select(Vec<SessionId>),
            FailLead(String),
        }

        let mut fires: Vec<Fire> = Vec::new();

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
                                    let sub = items[i % items.len()].clone();
                                    fires.push(Fire::Route(to.clone(), task_prompt(&sub)));
                                }
                            }
                            _ => fires.push(Fire::FailLead(
                                "fan_out by_subtask: lead emitted no parseable \
                                 cap-subtasks JSON-array block"
                                    .into(),
                            )),
                        }
                    }
                },
                Action::Collect(_) => fires.push(Fire::Select(triggers.clone())),
            }
        }

        let from = session.clone();
        for fire in fires {
            match fire {
                Fire::Route(to, frame) => {
                    if !self.registry.is_live(&to) && !self.spawn(&to).await {
                        continue;
                    }
                    self.audit.lock().unwrap().record_route(&from, &to);
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
                Fire::FailLead(error) => {
                    let _ = self
                        .out
                        .send(OrchestratorEvent::SessionFailed {
                            session: from.clone(),
                            error,
                        })
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
