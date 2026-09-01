//! mongoose — single-host NFS-to-NFS data mover.
//!
//! A focused front-end over the vamoose engine, shipped as one
//! binary: the nfs-walker scan and `mig-walker-rewrite` canonical
//! conversion are compiled in as libraries (the scanner pinned to the
//! same commit `packaging/nfs-walker.lock.json` pins for vamoose),
//! and the copy path is the same libnfs mover and `ShardProcessor`
//! dispatch the worker uses — but everything on local disk. No S3, no
//! claims, no coordinator, no worker fleet, no TUI, no external tools.
//!
//! ```text
//! mongoose prepare   scan + rewrite -> local canonical shards + manifest.json
//! mongoose copy      process the local shards with the libnfs mover
//! mongoose run       prepare, then copy
//! ```
//!
//! ## Work-dir layout
//!
//! ```text
//! <work-dir>/
//!   run.json                     run identity (sticky across resumes)
//!   scan/attempt-NNNN/           nfs-walker output + progress log
//!   scan.json                    scan checkpoint
//!   canonical/part-NNNN.parquet  canonical shards (mig-walker-rewrite)
//!   rewrite.json                 mig-walker-rewrite's own checkpoint
//!   manifest.json                local run plan (shard paths are
//!                                work-dir-relative, never S3 keys)
//!   progress.json                copy progress + completed-shard list
//!   failures/part-NNNN.jsonl     per-file failures, per shard
//!   downgrades/part-NNNN.jsonl   per-file metadata downgrades, per shard
//! ```
//!
//! ## Scope (v1 limitations, by design)
//!
//! - One-pass migration only; no rsync-style delta or incremental
//!   comparison, and no automatic torn-copy remediation.
//! - Single host; no distributed execution, no S3 claims.
//! - Hardlink fidelity is micro-batch/shard scoped, and directory
//!   metadata convergence is shard-scoped (same as the vamoose worker).
//! - NFSv3 via libnfs only; Linux, root, and reserved-port
//!   expectations are unchanged from vamoose.

pub mod cli;
pub mod copy;
pub mod delta;
pub mod manifest;
pub mod prepare;
pub mod progress;
pub mod scan;
pub mod sync;
pub mod util;
pub mod workdir;
