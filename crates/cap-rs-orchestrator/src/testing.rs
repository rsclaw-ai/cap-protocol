//! Test doubles: a `Driver` and (later) a driver factory that emit scripted
//! events, so the engine can be tested with zero real LLM / network.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cap_rs::core::{
    AgentEvent, ClientFrame, PermissionScope, RiskLevel, StopReason, TextChannel, Usage,
};
use cap_rs::driver::{Driver, DriverError};

use crate::OrchestratorError;
use crate::config::{DriverKind, PermissionPolicy, SessionId};
use crate::factory::DriverFactory;

/// A scripted driver. Build it with chained helpers, then it replays the queued
/// events on successive `next_event()` calls and returns `None` afterwards.
#[derive(Debug, Default)]
pub struct StubDriver {
    name: String,
    queue: VecDeque<AgentEvent>,
    alive: bool,
    /// Set when a permission request is scripted; the next `send` of a
    /// `PermissionResponse` records the decision here for assertions.
    pub last_decision: Option<cap_rs::core::PermissionDecision>,
    captured: Option<Arc<Mutex<Vec<String>>>>,
}

impl StubDriver {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            queue: VecDeque::new(),
            alive: true,
            last_decision: None,
            captured: None,
        }
    }

    pub fn text(mut self, t: &str) -> Self {
        self.queue.push_back(AgentEvent::TextChunk {
            msg_id: format!("{}-m", self.name),
            text: t.to_string(),
            channel: TextChannel::Assistant,
        });
        self
    }

    /// Script a permission request the engine must resolve before `done`.
    pub fn permission(mut self, tool: &str, risk: RiskLevel) -> Self {
        self.queue.push_back(AgentEvent::PermissionRequest {
            req_id: format!("{}-req", self.name),
            tool: tool.to_string(),
            intent: serde_json::json!({}),
            scope: PermissionScope::Execute,
            risk_level: risk,
        });
        self
    }

    pub fn done(mut self, stop: StopReason) -> Self {
        self.queue.push_back(AgentEvent::Done {
            stop_reason: stop,
            usage: Usage::default(),
        });
        self
    }

    /// Record the text of every Prompt frame this driver receives, for assertions.
    pub fn capture(mut self, sink: Arc<Mutex<Vec<String>>>) -> Self {
        self.captured = Some(sink);
        self
    }
}

#[async_trait::async_trait]
impl Driver for StubDriver {
    async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError> {
        match frame {
            ClientFrame::PermissionResponse { decision, .. } => {
                self.last_decision = Some(decision);
            }
            ClientFrame::Prompt { content } => {
                if let Some(sink) = &self.captured {
                    let text: String = content
                        .iter()
                        .filter_map(|c| match c {
                            cap_rs::core::Content::Text { text } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect();
                    sink.lock().unwrap().push(text);
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn next_event(&mut self) -> Option<AgentEvent> {
        let ev = self.queue.pop_front();
        if ev.is_none() {
            self.alive = false;
        }
        ev
    }

    async fn shutdown(&mut self) -> Result<(), DriverError> {
        self.alive = false;
        Ok(())
    }

    fn is_alive(&self) -> bool {
        self.alive
    }
}

/// A factory that hands out pre-scripted `StubDriver`s by session id.
#[derive(Debug, Default)]
pub struct StubDriverFactory {
    scripts: Mutex<HashMap<SessionId, StubDriver>>,
}

impl StubDriverFactory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register the driver a given session id should receive.
    pub fn with(self, session: &str, driver: StubDriver) -> Self {
        self.scripts
            .lock()
            .unwrap()
            .insert(session.to_string(), driver);
        self
    }
}

#[async_trait]
impl DriverFactory for StubDriverFactory {
    async fn build(
        &self,
        session: &SessionId,
        _kind: &DriverKind,
        _cwd: &Path,
        _policy: PermissionPolicy,
    ) -> Result<Box<dyn cap_rs::driver::Driver>, OrchestratorError> {
        self.scripts
            .lock()
            .unwrap()
            .remove(session)
            .map(|d| Box::new(d) as Box<dyn cap_rs::driver::Driver>)
            .ok_or_else(|| OrchestratorError::Config(format!("no stub for session '{session}'")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cap_rs::core::{ClientFrame, Content, StopReason};
    use cap_rs::driver::Driver;

    #[tokio::test]
    async fn stub_emits_scripted_events_then_done() {
        let mut d = StubDriver::new("s1")
            .text("hello ")
            .text("world")
            .done(StopReason::EndTurn);

        // Driving a prompt in is a no-op for the stub but must not error.
        d.send(ClientFrame::Prompt {
            content: vec![Content::text("hi")],
        })
        .await
        .unwrap();

        let mut texts = String::new();
        let mut saw_done = false;
        while let Some(ev) = d.next_event().await {
            match ev {
                cap_rs::core::AgentEvent::TextChunk { text, .. } => texts.push_str(&text),
                cap_rs::core::AgentEvent::Done { .. } => saw_done = true,
                _ => {}
            }
        }
        assert_eq!(texts, "hello world");
        assert!(saw_done);
    }
}
