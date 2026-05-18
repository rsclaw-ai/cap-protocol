//! Placeholder. Real implementation is in progress.
//!
//! See <https://cap-protocol.org> for the protocol specification and
//! <https://github.com/rsclaw-ai/cap-protocol> for the source repository.

/// Name of this crate at build time.
pub const CRATE_NAME: &str = env!("CARGO_PKG_NAME");

/// Version of this crate at build time.
pub const CRATE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// CAP protocol version targeted by this crate.
pub const PROTOCOL_VERSION: &str = "cap-protocol/v1";
