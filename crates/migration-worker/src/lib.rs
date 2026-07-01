//! Library surface of `migration-worker` for in-process composition
//! by other crates (notably `vamoose-cli`) and by the `mig-worker`
//! binary, which is a thin `main.rs` over this lib.

pub mod backpressure;
pub mod caps;
pub mod config;
pub mod coord_client;
pub mod coord_driver;
pub mod heartbeat;
pub mod orchestrator;
pub mod run_control;
pub mod shard_processor;
pub mod throughput;
