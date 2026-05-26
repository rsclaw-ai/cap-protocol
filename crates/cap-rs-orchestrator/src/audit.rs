//! Immutable, ordered record of every cross-session route the engine performs.
//! Human-auditable per CAP's "orchestrator-mediated, human-auditable" rule.

use std::time::{SystemTime, UNIX_EPOCH};

use tracing::warn;

use crate::config::SessionId;

/// One auditable event in the fleet lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditEvent {
    /// Cross-session route.
    Route { from: SessionId, to: SessionId },
    /// Session started.
    SessionStarted { session: SessionId },
    /// Session completed normally.
    SessionDone { session: SessionId },
    /// Session failed.
    SessionFailed { session: SessionId, error: String },
    /// Fleet cancelled by user or error.
    FleetCancelled,
}

/// One audit record with metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    /// Monotonic sequence number, starting at 0.
    pub seq: u64,
    /// Milliseconds since the Unix epoch when the event happened.
    pub at: u128,
    /// The event that occurred.
    pub event: AuditEvent,
}

#[derive(Debug, Default)]
pub struct AuditLog {
    records: Vec<AuditRecord>,
}

impl AuditLog {
    pub fn new() -> Self {
        Self::default()
    }

    fn now_millis() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or_else(|_| {
                warn!("system clock is before Unix epoch; using timestamp 0");
                0
            })
    }

    fn push(&mut self, event: AuditEvent) -> &AuditRecord {
        self.records.push(AuditRecord {
            seq: self.records.len() as u64,
            at: Self::now_millis(),
            event,
        });
        self.records.last().unwrap()
    }

    pub fn record_route(&mut self, from: &str, to: &str) -> &AuditRecord {
        self.push(AuditEvent::Route {
            from: from.to_string(),
            to: to.to_string(),
        })
    }

    pub fn record_session_started(&mut self, session: &str) -> &AuditRecord {
        self.push(AuditEvent::SessionStarted {
            session: session.to_string(),
        })
    }

    pub fn record_session_done(&mut self, session: &str) -> &AuditRecord {
        self.push(AuditEvent::SessionDone {
            session: session.to_string(),
        })
    }

    pub fn record_session_failed(&mut self, session: &str, error: &str) -> &AuditRecord {
        self.push(AuditEvent::SessionFailed {
            session: session.to_string(),
            error: error.to_string(),
        })
    }

    pub fn record_cancelled(&mut self) -> &AuditRecord {
        self.push(AuditEvent::FleetCancelled)
    }

    pub fn records(&self) -> &[AuditRecord] {
        &self.records
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_routes_in_order_with_increasing_seq() {
        let mut log = AuditLog::new();
        log.record_route("a", "b");
        log.record_route("b", "c");
        let records = log.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].seq, 0);
        assert_eq!(records[1].seq, 1);
        assert_eq!(
            records[0].event,
            AuditEvent::Route {
                from: "a".into(),
                to: "b".into()
            }
        );
        assert!(records[1].at >= records[0].at);
    }

    #[test]
    fn records_lifecycle_events() {
        let mut log = AuditLog::new();
        log.record_session_started("a");
        log.record_session_done("a");
        log.record_session_failed("b", "driver crashed");
        log.record_cancelled();
        let records = log.records();
        assert_eq!(records.len(), 4);
        assert!(matches!(
            records[0].event,
            AuditEvent::SessionStarted { .. }
        ));
        assert!(matches!(records[1].event, AuditEvent::SessionDone { .. }));
        assert!(matches!(records[2].event, AuditEvent::SessionFailed { .. }));
        assert!(matches!(records[3].event, AuditEvent::FleetCancelled));
    }
}
