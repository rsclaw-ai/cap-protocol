//! Owns all live sessions: maps id → inbox sender + task handle.

use std::collections::HashMap;

use cap_rs::core::ClientFrame;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::OrchestratorError;
use crate::config::{DriverKind, PermissionPolicy, SessionId};
use crate::event::OrchestratorEvent;
use crate::factory::DriverFactory;
use crate::session::{SessionHandle, SessionSpawnConfig, spawn_chat_session, spawn_session};
use crate::worktree::WorktreeManager;

#[derive(Debug, Default)]
pub struct SessionRegistry {
    sessions: HashMap<SessionId, SessionHandle>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_live(&self, id: &str) -> bool {
        self.sessions.contains_key(id)
    }

    /// Allocate a worktree, build the driver, and spawn the actor.
    #[allow(clippy::too_many_arguments)]
    pub async fn spawn(
        &mut self,
        id: SessionId,
        kind: &DriverKind,
        policy: PermissionPolicy,
        base_branch: &str,
        factory: &dyn DriverFactory,
        worktree: &dyn WorktreeManager,
        bus: &mpsc::Sender<OrchestratorEvent>,
        cancel: &CancellationToken,
        spawn_cfg: SessionSpawnConfig,
    ) -> Result<(), OrchestratorError> {
        self.spawn_with_mode(
            id,
            kind,
            policy,
            base_branch,
            factory,
            worktree,
            bus,
            cancel,
            false,
            spawn_cfg,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn spawn_chat(
        &mut self,
        id: SessionId,
        kind: &DriverKind,
        policy: PermissionPolicy,
        base_branch: &str,
        factory: &dyn DriverFactory,
        worktree: &dyn WorktreeManager,
        bus: &mpsc::Sender<OrchestratorEvent>,
        cancel: &CancellationToken,
        spawn_cfg: SessionSpawnConfig,
    ) -> Result<(), OrchestratorError> {
        self.spawn_with_mode(
            id,
            kind,
            policy,
            base_branch,
            factory,
            worktree,
            bus,
            cancel,
            true,
            spawn_cfg,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn spawn_with_mode(
        &mut self,
        id: SessionId,
        kind: &DriverKind,
        policy: PermissionPolicy,
        base_branch: &str,
        factory: &dyn DriverFactory,
        worktree: &dyn WorktreeManager,
        bus: &mpsc::Sender<OrchestratorEvent>,
        cancel: &CancellationToken,
        chat_mode: bool,
        spawn_cfg: SessionSpawnConfig,
    ) -> Result<(), OrchestratorError> {
        let cwd = worktree.create(&id, base_branch)?;
        let driver = match factory.build(&id, kind, &cwd, policy).await {
            Ok(d) => d,
            Err(e) => {
                let _ = worktree.cleanup(&id);
                return Err(e);
            }
        };
        let handle = if chat_mode {
            spawn_chat_session(id.clone(), driver, policy, cwd, bus.clone(), cancel.clone(), spawn_cfg)
        } else {
            spawn_session(id.clone(), driver, policy, cwd, bus.clone(), cancel.clone(), spawn_cfg)
        };
        self.sessions.insert(id, handle);
        Ok(())
    }

    /// Deliver a frame to a session's inbox.
    pub async fn route(&self, to: &str, frame: ClientFrame) -> Result<(), OrchestratorError> {
        let handle = self.sessions.get(to).ok_or_else(|| {
            OrchestratorError::Config(format!("route to unknown/dead session '{to}'"))
        })?;
        handle
            .inbox
            .send(frame)
            .await
            .map_err(|_| OrchestratorError::Config(format!("session '{to}' inbox is closed")))
    }

    /// Drop all inboxes and await every task to finish.
    pub async fn shutdown(&mut self) {
        let handles: Vec<SessionHandle> = self.sessions.drain().map(|(_, h)| h).collect();
        // Separate inboxes (senders) from join handles.
        let (inboxes, joins): (Vec<_>, Vec<_>) =
            handles.into_iter().map(|h| (h.inbox, h.join)).unzip();
        // Drop all senders first so session actors see a closed channel
        // and exit their recv loops cleanly.
        drop(inboxes);
        // Await all tasks to ensure no leaked tokio tasks.
        for join in joins {
            if let Err(e) = join.await {
                tracing::warn!(error = %e, "session task panicked during shutdown");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DriverKind, PermissionPolicy};
    use crate::event::OrchestratorEvent;
    use crate::testing::{StubDriver, StubDriverFactory};
    use crate::worktree::NoopWorktreeManager;
    use cap_rs::core::{ClientFrame, Content, StopReason};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn spawn_then_route_a_frame_to_a_session() {
        let factory = StubDriverFactory::new().with(
            "w",
            StubDriver::new("w").text("done").done(StopReason::EndTurn),
        );
        let wt = NoopWorktreeManager::new();
        let (bus_tx, mut bus_rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let mut reg = SessionRegistry::new();

        reg.spawn(
            "w".into(),
            &DriverKind::Claude,
            PermissionPolicy::Allow,
            "main",
            &factory,
            &wt,
            &bus_tx,
            &cancel,
            SessionSpawnConfig::default(),
        )
        .await
        .unwrap();

        reg.route(
            "w",
            ClientFrame::Prompt {
                content: vec![Content::text("hi")],
            },
        )
        .await
        .unwrap();

        let mut saw_done = false;
        while let Some(ev) = bus_rx.recv().await {
            if let OrchestratorEvent::SessionDone { session, .. } = ev {
                assert_eq!(session, "w");
                saw_done = true;
                break;
            }
        }
        assert!(saw_done);
        reg.shutdown().await;
    }

    #[tokio::test]
    async fn route_to_unknown_session_errors() {
        let reg = SessionRegistry::new();
        let err = reg
            .route("nope", ClientFrame::Prompt { content: vec![] })
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("nope"));
    }
}
