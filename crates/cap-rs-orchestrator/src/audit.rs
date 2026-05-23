//! Immutable, ordered record of every cross-session route the engine performs.
//! Human-auditable per CAP's "orchestrator-mediated, human-auditable" rule.

use std::time::{SystemTime, UNIX_EPOCH};

use tracing::warn;

use crate::config::SessionId;

/// One routing event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    /// Monotonic sequence number, starting at 0.
    pub seq: u64,
    /// Milliseconds since the Unix epoch when the route happened.
    pub at: u128,
    pub from: SessionId,
    pub to: SessionId,
}

#[derive(Debug, Default)]
pub struct AuditLog {
    records: Vec<AuditRecord>,
}

impl AuditLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_route(&mut self, from: &str, to: &str) -> &AuditRecord {
        let at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or_else(|_| {
                warn!("system clock is before Unix epoch; using timestamp 0");
                0
            });
        self.records.push(AuditRecord {
            seq: self.records.len() as u64,
            at,
            from: from.to_string(),
            to: to.to_string(),
        });
        self.records.last().unwrap()
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
        assert_eq!(records[0].from, "a");
        assert_eq!(records[0].to, "b");
        assert!(records[1].at >= records[0].at);
    }
}
