//! Deterministic sampled-content selection.
//!
//! Selection is a pure function of `(run_id, sample_seed, raw_path)` plus the
//! mandatory-risk reasons derived from the independent scans and the risk
//! evidence artifact. Nothing here depends on scan order, insertion order, or
//! process lifetime, which is what makes a resumed selection byte-identical.

use crate::model::RiskReason;
use migration_mover::BUCKETS;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, BinaryHeap};

/// Domain separator for the versioned ranking algorithm
/// (`SAMPLE_ALGORITHM = "sha256-smallest-v1"`).
const RANK_DOMAIN: &[u8] = b"vamoose-sample-v1\0";

/// Computes the versioned rank digest:
///
/// ```text
/// SHA256("vamoose-sample-v1\0" || u64_be(len(run_id)) || run_id ||
///        u64_be(sample_seed) || u64_be(len(raw_path)) || raw_path)
/// ```
///
/// The run/seed prefix is hashed once and cloned per path.
#[derive(Clone)]
pub(crate) struct Ranker {
    prefix: Sha256,
}

impl Ranker {
    pub(crate) fn new(run_id: &str, sample_seed: u64) -> Self {
        let mut prefix = Sha256::new();
        prefix.update(RANK_DOMAIN);
        prefix.update((run_id.len() as u64).to_be_bytes());
        prefix.update(run_id.as_bytes());
        prefix.update(sample_seed.to_be_bytes());
        Self { prefix }
    }

    pub(crate) fn rank(&self, path: &[u8]) -> [u8; 32] {
        let mut hasher = self.prefix.clone();
        hasher.update((path.len() as u64).to_be_bytes());
        hasher.update(path);
        hasher.finalize().into()
    }
}

/// Additive set of [`RiskReason`]s persisted as a bitmask. Iteration order
/// is the declaration order of `RiskReason::ALL`, so the stored reasons for
/// a path render identically no matter how they were accumulated.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReasonSet(u32);

impl ReasonSet {
    pub(crate) const EMPTY: Self = Self(0);

    pub(crate) fn only(reason: RiskReason) -> Self {
        Self(reason.bit())
    }

    pub(crate) fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    pub(crate) fn bits(self) -> u32 {
        self.0
    }

    pub(crate) fn insert(&mut self, reason: RiskReason) {
        self.0 |= reason.bit();
    }

    pub(crate) fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub(crate) fn contains(self, reason: RiskReason) -> bool {
        self.0 & reason.bit() != 0
    }

    pub(crate) fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// True when any mandatory (non-seeded) reason is present.
    pub(crate) fn is_risk(self) -> bool {
        self.0 & !RiskReason::Seeded.bit() != 0
    }

    pub(crate) fn iter(self) -> impl Iterator<Item = RiskReason> {
        RiskReason::ALL
            .into_iter()
            .filter(move |reason| self.contains(*reason))
    }
}

/// Every mover bucket transition, derived from `migration_mover::BUCKETS`
/// rather than duplicated: the lowest size of each bucket other than the
/// smallest one.
pub(crate) fn bucket_thresholds() -> Vec<u64> {
    BUCKETS
        .iter()
        .map(|bucket| bucket.min_size)
        .filter(|&min_size| min_size > 0)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// True when `size` is exactly one below, at, or one above any bucket
/// threshold.
pub(crate) fn is_bucket_boundary(size: u64) -> bool {
    bucket_thresholds().into_iter().any(|threshold| {
        size == threshold
            || threshold.checked_sub(1) == Some(size)
            || threshold.checked_add(1) == Some(size)
    })
}

/// A ranked eligible path. Ordered by digest, then raw path bytes as the
/// collision tie-break.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RankedPath {
    pub rank: [u8; 32],
    pub path: Vec<u8>,
}

/// Bounded max-heap that retains the `capacity` lowest-ranked candidates
/// seen so far. Memory is `O(capacity)`, independent of tree size. The
/// heap tracks every eligible path, mandatory or not: the `capacity` lowest
/// overall always contain the lowest `capacity - mandatory` non-mandatory
/// paths, so the seeded remainder can be filled from it exactly.
pub(crate) struct SeededHeap {
    capacity: usize,
    heap: BinaryHeap<RankedPath>,
}

impl SeededHeap {
    pub(crate) fn new(capacity: u64) -> Self {
        let capacity = usize::try_from(capacity).unwrap_or(usize::MAX);
        Self {
            capacity,
            heap: BinaryHeap::with_capacity(capacity.min(1 << 16)),
        }
    }

    pub(crate) fn push(&mut self, rank: [u8; 32], path: &[u8]) {
        if self.capacity == 0 {
            return;
        }
        if self.heap.len() < self.capacity {
            self.heap.push(RankedPath {
                rank,
                path: path.to_vec(),
            });
            return;
        }
        let largest = self.heap.peek().expect("non-empty heap at capacity");
        if (rank, path) < (largest.rank, largest.path.as_slice()) {
            self.heap.pop();
            self.heap.push(RankedPath {
                rank,
                path: path.to_vec(),
            });
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.heap.len()
    }

    /// Retained candidates in ascending rank order.
    pub(crate) fn into_ascending(self) -> Vec<RankedPath> {
        self.heap.into_sorted_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank_is_deterministic_and_seed_sensitive() {
        let a = Ranker::new("run", 0);
        assert_eq!(a.rank(b"a"), Ranker::new("run", 0).rank(b"a"));
        assert_ne!(a.rank(b"a"), a.rank(b"b"));
        assert_ne!(a.rank(b"a"), Ranker::new("run", 1).rank(b"a"));
        assert_ne!(a.rank(b"a"), Ranker::new("run2", 0).rank(b"a"));
    }

    #[test]
    fn rank_matches_the_documented_layout() {
        let mut expected = Sha256::new();
        expected.update(b"vamoose-sample-v1\0");
        expected.update(3u64.to_be_bytes());
        expected.update(b"run");
        expected.update(7u64.to_be_bytes());
        expected.update(2u64.to_be_bytes());
        expected.update(b"\xff/");
        let expected: [u8; 32] = expected.finalize().into();
        assert_eq!(Ranker::new("run", 7).rank(b"\xff/"), expected);
    }

    #[test]
    fn reason_sets_union_and_iterate_in_declaration_order() {
        let mut set = ReasonSet::only(RiskReason::Seeded);
        set.insert(RiskReason::MetadataMismatch);
        let set = set.union(ReasonSet::only(RiskReason::BucketBoundary));
        assert_eq!(
            set.iter().collect::<Vec<_>>(),
            vec![
                RiskReason::MetadataMismatch,
                RiskReason::BucketBoundary,
                RiskReason::Seeded
            ]
        );
        assert!(set.is_risk());
        assert!(!ReasonSet::only(RiskReason::Seeded).is_risk());
        assert!(ReasonSet::EMPTY.is_empty());
        assert_eq!(ReasonSet::from_bits(set.bits()), set);
    }

    #[test]
    fn thresholds_come_from_the_mover_buckets() {
        assert_eq!(bucket_thresholds(), vec![1 << 20, 1 << 30]);
        for threshold in bucket_thresholds() {
            assert!(is_bucket_boundary(threshold - 1));
            assert!(is_bucket_boundary(threshold));
            assert!(is_bucket_boundary(threshold + 1));
            assert!(!is_bucket_boundary(threshold - 2));
            assert!(!is_bucket_boundary(threshold + 2));
        }
        assert!(!is_bucket_boundary(0));
        assert!(!is_bucket_boundary(u64::MAX));
    }

    #[test]
    fn heap_keeps_the_lowest_candidates_with_path_tie_break() {
        let mut heap = SeededHeap::new(2);
        heap.push([9; 32], b"z");
        heap.push([1; 32], b"b");
        heap.push([1; 32], b"a");
        heap.push([5; 32], b"m");
        let kept = heap.into_ascending();
        assert_eq!(
            kept.iter().map(|c| c.path.as_slice()).collect::<Vec<_>>(),
            vec![&b"a"[..], &b"b"[..]]
        );

        let mut empty = SeededHeap::new(0);
        empty.push([0; 32], b"x");
        assert_eq!(empty.len(), 0);
    }
}
