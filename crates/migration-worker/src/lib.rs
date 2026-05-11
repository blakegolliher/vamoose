//! Library surface of `migration-worker` for in-process composition
//! by other crates (notably `vamoose-cli`). The existing `mig-worker`
//! binary continues to declare its own module tree in `main.rs` and
//! is not affected by this lib.

pub mod backpressure;
pub mod caps;
pub mod config;
pub mod heartbeat;
pub mod orchestrator;
pub mod shard_processor;
pub mod throughput;
