//! # cap-rs — Rust reference implementation of the CAP (CLI Agent Protocol).
//!
//! See <https://cap-protocol.org> for the protocol specification.
//!
//! ## Layout
//!
//! - [`core`] — protocol types (events, frames, capabilities, manifest).
//!   Always available, no IO dependencies.
//! - [`driver`] — backends that drive an actual CLI agent.
//!   Each backend is gated behind a feature flag.
//!
//! ## Quick start
//!
//! ```toml
//! [dependencies]
//! cap-rs = { version = "0.1", features = ["stream-json"] }
//! ```
//!
//! ```no_run
//! # #[cfg(feature = "stream-json")]
//! # async fn run() -> anyhow::Result<()> {
//! use cap_rs::driver::stream_json::ClaudeCodeDriver;
//! use cap_rs::core::{ClientFrame, Content};
//!
//! let mut driver = ClaudeCodeDriver::spawn(std::env::current_dir()?).await?;
//! driver.send(ClientFrame::Prompt {
//!     content: vec![Content::Text("What is 2 + 2?".into())],
//! }).await?;
//!
//! while let Some(event) = driver.next_event().await {
//!     println!("{event:?}");
//! }
//! # Ok(())
//! # }
//! ```

#![doc(html_root_url = "https://docs.rs/cap-rs/0.0.0")]
#![warn(missing_debug_implementations)]

pub mod core;
pub mod driver;

/// Crate name (build-time constant).
pub const CRATE_NAME: &str = env!("CARGO_PKG_NAME");

/// Crate version (build-time constant).
pub const CRATE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// CAP protocol version targeted by this crate.
pub const PROTOCOL_VERSION: &str = "cap-protocol/v1";
