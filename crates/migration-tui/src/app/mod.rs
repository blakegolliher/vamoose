//! Input reduction, runtime orchestration, streaming, commands, and terminal
//! lifecycle for the TUI.
//!
//! [`handle_input`] is the synchronous reducer: every state-changing runtime
//! input flows through it. [`run`] owns the I/O shell, while the REST bootstrap
//! and reconnect lifecycle remain in [`sse_driver`].

mod commands;
mod keys;
mod reducer;
mod runtime;
mod stream;
mod terminal;

pub use reducer::{handle_input, AppAction, Input};
pub use runtime::{run, RunOpts};
pub use stream::{fetch_bootstrap_snapshot, sse_driver};
pub(crate) use terminal::{install_panic_hook, restore_terminal};

#[cfg(test)]
use terminal::panic_after_ms;

#[cfg(test)]
mod tests;
