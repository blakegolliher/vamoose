//! Operator TUI for `vamoose coord`.
//!
//! Live dashboard that subscribes to a running coord via REST + SSE
//! and renders the cluster state to the terminal. The crate is
//! split along three planes:
//!
//! - [`client`] — HTTP layer (REST snapshot fetch, SSE consumer with
//!   `Last-Event-ID` resume). Hand-rolled over `reqwest` byte stream
//!   so the TUI stays on the same TLS family as the rest of vamoose.
//!
//! - [`state`] — in-memory state derived from events. Reuses the
//!   coord's own [`migration_coord::schema::Snapshot::apply`] reducer
//!   so server and client converge on the same wire semantics.
//!   Wraps it with TUI-only state (connection status, selected job,
//!   filter string).
//!
//! - rendering (Phase 4b) — ratatui widgets. Not yet present.
//!
//! The library is composed in `vamoose tui` (Phase 4c) which owns
//! the terminal and event loop. Headless callers can drive
//! [`state::AppState`] directly for tests.

pub mod app;
pub mod client;
pub mod format;
pub mod palette;
pub mod render;
pub mod state;
pub mod theme;
