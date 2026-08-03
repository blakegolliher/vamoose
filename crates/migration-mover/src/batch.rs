//! Byte-budgeted micro-batch builder.
//!
//! Within a shard, we walk rows in `row_id` order and accumulate them
//! into a batch until a budget hits:
//!
//! - `bytes_budget` (default 8 GiB of source data), OR
//! - `files_budget` (default 100,000 rows), OR
//! - end of shard.
//!
//! This is what makes the system wire-rate-correct for both 1KB files
//! (where you want huge counts to amortize syscall overhead) and 1GB
//! files (where you want few-file batches to bound memory).
//!
//! See DESIGN.md "Batching and backpressure".

use migration_core::shard::RowView;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone, Copy)]
pub struct BatchBudget {
    pub bytes: u64,
    pub files: u64,
}

impl Default for BatchBudget {
    fn default() -> Self {
        Self {
            bytes: 8 * 1024 * 1024 * 1024,
            files: 100_000,
        }
    }
}

#[derive(Debug, Default)]
pub struct Batch {
    pub rows: Vec<RowView>,
    pub bytes: u64,
}

impl Batch {
    /// Returns true if the row should *not* be added because the batch
    /// is already full. The caller is responsible for flushing the
    /// current batch and starting a new one.
    pub fn would_overflow(&self, row: &RowView, budget: &BatchBudget) -> bool {
        if self.rows.is_empty() {
            return false; // never refuse the first row in an empty batch
        }
        self.rows.len() as u64 >= budget.files || self.bytes.saturating_add(row.size) > budget.bytes
    }

    pub fn push(&mut self, row: RowView) {
        self.bytes = self.bytes.saturating_add(row.size);
        self.rows.push(row);
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn take(&mut self) -> Self {
        std::mem::take(self)
    }
}

/// Adaptive in-flight concurrency by file-size class. See DESIGN.md
/// "Batching and backpressure".
#[derive(Debug, Clone, Copy)]
pub struct InflightProfile {
    pub small: usize,  // files < 1 MiB
    pub medium: usize, // 1 MiB – 1 GiB
    pub large: usize,  // > 1 GiB
    pub large_stripe_size: u64,
    pub large_stripe_depth: usize,
}

impl Default for InflightProfile {
    fn default() -> Self {
        Self {
            small: 256,
            medium: 16,
            large: 4,
            large_stripe_size: 4 * 1024 * 1024,
            large_stripe_depth: 32,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeClass {
    Small,
    Medium,
    Large,
}

impl SizeClass {
    pub fn classify(size: u64) -> Self {
        const MIB: u64 = 1024 * 1024;
        const GIB: u64 = 1024 * MIB;
        if size < MIB {
            Self::Small
        } else if size < GIB {
            Self::Medium
        } else {
            Self::Large
        }
    }
}

/// Three semaphores — one per size class — that gate the M3 concurrent
/// shard dispatch. Each row acquires a permit from the semaphore for
/// its class before the mover does any work; the permit is held for
/// the full mover call (acquire pair → spawn_blocking → release).
///
/// Cheap clone (Arc-of-Semaphore). Cloned into every dispatched task.
#[derive(Clone)]
pub struct InflightLimiter {
    small: Arc<Semaphore>,
    medium: Arc<Semaphore>,
    large: Arc<Semaphore>,
}

impl InflightLimiter {
    pub fn new(profile: &InflightProfile) -> Self {
        Self {
            small: Arc::new(Semaphore::new(profile.small.max(1))),
            medium: Arc::new(Semaphore::new(profile.medium.max(1))),
            large: Arc::new(Semaphore::new(profile.large.max(1))),
        }
    }

    /// Acquire the appropriate permit for `size`. The returned guard
    /// holds the permit for its lifetime; drop it after the mover
    /// returns to release the slot.
    pub async fn acquire(&self, size: u64) -> OwnedSemaphorePermit {
        let sem = match SizeClass::classify(size) {
            SizeClass::Small => self.small.clone(),
            SizeClass::Medium => self.medium.clone(),
            SizeClass::Large => self.large.clone(),
        };
        // Semaphore can only be closed by an explicit close() — we
        // never call it, so unwrap is sound.
        sem.acquire_owned().await.expect("semaphore not closed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limiter_distinct_semaphores_per_class() {
        let p = InflightProfile {
            small: 2,
            medium: 1,
            large: 1,
            large_stripe_size: 0,
            large_stripe_depth: 0,
        };
        let lim = InflightLimiter::new(&p);

        // Two small permits available; we can hold both.
        let s1 = lim.acquire(100).await;
        let s2 = lim.acquire(200).await;
        // Medium has its own count (1) — acquiring it doesn't block on
        // the small semaphore being full.
        let m1 = lim.acquire(2 * 1024 * 1024).await;
        // Large semaphore likewise independent.
        let l1 = lim.acquire(2 * 1024 * 1024 * 1024).await;
        drop((s1, s2, m1, l1));
    }
}
