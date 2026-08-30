//! Per-RPC latency histograms for the three things that can slow a
//! worker down: the source NFS server, the destination NFS server,
//! and the S3 control plane.
//!
//! The question an operator asks when the rate is low is "who is
//! slow?". Throughput alone cannot answer it — a saturated
//! destination and a starved client look the same from files/s.
//! This module records the wall time of every raw NFS RPC (tagged
//! with the side it went to) and every S3 call into lock-free
//! log-linear histograms, and turns a window of those into a
//! [`Summary`]: per-op count / mean / p50 / p95 / p99 / max, plus the
//! derived **busy share** per side — the fraction of `pairs ×
//! window` that connection pairs spent waiting on that server. A side
//! near 100 % busy is the bottleneck; both low means the client (CPU,
//! scheduling, S3 gaps between shards) is.
//!
//! Recording is a handful of relaxed atomic adds per RPC; at the
//! ~100 K RPC/s a worker issues that is noise. The registry is
//! process-global because one worker process drives one mover and
//! one S3 client; the heartbeat task owns the window (it snapshots on
//! every tick and hands the [`Summary`] to whoever wants it), so the
//! per-op `max` — which resets on read — is single-reader.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

/// Which NFS server a raw RPC went to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Src,
    Dst,
}

impl Side {
    pub fn name(self) -> &'static str {
        match self {
            Side::Src => "src",
            Side::Dst => "dst",
        }
    }
    fn index(self) -> usize {
        match self {
            Side::Src => 0,
            Side::Dst => 1,
        }
    }
}

/// Raw NFSv3 procedures the mover issues. `from_tag` maps the
/// procedure name the raw layer already carries for error reporting
/// (`"LOOKUP"`, `"WRITE"`, …); anything unknown lands in `Other` so
/// a new procedure is counted rather than dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NfsOp {
    RootFh,
    Lookup,
    Readdirplus,
    Getattr,
    Read,
    Create,
    Mkdir,
    Write,
    Commit,
    Setattr,
    Rename,
    Other,
}

impl NfsOp {
    pub const ALL: [NfsOp; 12] = [
        NfsOp::RootFh,
        NfsOp::Lookup,
        NfsOp::Readdirplus,
        NfsOp::Getattr,
        NfsOp::Read,
        NfsOp::Create,
        NfsOp::Mkdir,
        NfsOp::Write,
        NfsOp::Commit,
        NfsOp::Setattr,
        NfsOp::Rename,
        NfsOp::Other,
    ];

    pub fn from_tag(tag: &str) -> Self {
        match tag {
            "ROOT" | "ROOTFH" | "MOUNT" => NfsOp::RootFh,
            "LOOKUP" => NfsOp::Lookup,
            "READDIRPLUS" => NfsOp::Readdirplus,
            "GETATTR" => NfsOp::Getattr,
            "READ" => NfsOp::Read,
            "CREATE" => NfsOp::Create,
            "MKDIR" => NfsOp::Mkdir,
            "WRITE" => NfsOp::Write,
            "COMMIT" => NfsOp::Commit,
            "SETATTR" => NfsOp::Setattr,
            "RENAME" => NfsOp::Rename,
            _ => NfsOp::Other,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            NfsOp::RootFh => "ROOTFH",
            NfsOp::Lookup => "LOOKUP",
            NfsOp::Readdirplus => "READDIRPLUS",
            NfsOp::Getattr => "GETATTR",
            NfsOp::Read => "READ",
            NfsOp::Create => "CREATE",
            NfsOp::Mkdir => "MKDIR",
            NfsOp::Write => "WRITE",
            NfsOp::Commit => "COMMIT",
            NfsOp::Setattr => "SETATTR",
            NfsOp::Rename => "RENAME",
            NfsOp::Other => "OTHER",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// S3 calls the control plane makes. `Download`/`Upload` are the
/// whole-object transfers (shard parquet in, index shards out); the
/// rest are the small-object atoms of the claim protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3Op {
    Get,
    Put,
    PutIfAbsent,
    Head,
    Delete,
    DeleteIfMatch,
    List,
    Download,
    Upload,
    Versioning,
}

impl S3Op {
    pub const ALL: [S3Op; 10] = [
        S3Op::Get,
        S3Op::Put,
        S3Op::PutIfAbsent,
        S3Op::Head,
        S3Op::Delete,
        S3Op::DeleteIfMatch,
        S3Op::List,
        S3Op::Download,
        S3Op::Upload,
        S3Op::Versioning,
    ];

    pub fn name(self) -> &'static str {
        match self {
            S3Op::Get => "GET",
            S3Op::Put => "PUT",
            S3Op::PutIfAbsent => "PUT-IF-ABSENT",
            S3Op::Head => "HEAD",
            S3Op::Delete => "DELETE",
            S3Op::DeleteIfMatch => "DELETE-IF-MATCH",
            S3Op::List => "LIST",
            S3Op::Download => "DOWNLOAD",
            S3Op::Upload => "UPLOAD",
            S3Op::Versioning => "VERSIONING",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

// =============================================================================
// Histogram
// =============================================================================

/// Log-linear buckets over microseconds: four sub-buckets per power
/// of two from 1 µs to 2^24 µs (~16.8 s), then one overflow bucket.
/// Resolution is ≤ 25 % at every scale, which is plenty for "p95 went
/// from 1 ms to 14 ms".
pub const BUCKETS: usize = 100;
const OVERFLOW_EXP: usize = 24;

fn bucket_of(us: u64) -> usize {
    if us < 1 {
        return 0;
    }
    let exp = (63 - us.leading_zeros()) as usize;
    if exp >= OVERFLOW_EXP {
        return BUCKETS - 1;
    }
    let sub = if exp >= 2 {
        ((us >> (exp - 2)) & 3) as usize
    } else {
        ((us << (2 - exp)) & 3) as usize
    };
    (exp * 4 + sub).min(BUCKETS - 1)
}

/// Inclusive upper bound (µs) of a bucket — what a percentile reports.
fn bucket_upper(idx: usize) -> u64 {
    if idx >= BUCKETS - 1 {
        return 1u64 << OVERFLOW_EXP;
    }
    let exp = idx / 4;
    let sub = (idx % 4) as u64;
    if exp >= 2 {
        (1u64 << exp) + (sub + 1) * (1u64 << (exp - 2)) - 1
    } else {
        // exp 0: [1]; exp 1: [2,3] — sub-buckets collapse.
        (1u64 << (exp + 1)) - 1
    }
}

/// One op's histogram. All relaxed atomics; a reader sees a
/// consistent-enough view for 30 s windows.
pub struct Histogram {
    buckets: [AtomicU64; BUCKETS],
    count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
}

impl Histogram {
    #[allow(clippy::declare_interior_mutable_const)]
    const ZERO: AtomicU64 = AtomicU64::new(0);

    pub const fn new() -> Self {
        Self {
            buckets: [Self::ZERO; BUCKETS],
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        }
    }

    pub fn record(&self, d: Duration) {
        let us = d.as_micros().min(u64::MAX as u128) as u64;
        self.buckets[bucket_of(us)].fetch_add(1, Relaxed);
        self.count.fetch_add(1, Relaxed);
        self.sum_us.fetch_add(us, Relaxed);
        self.max_us.fetch_max(us, Relaxed);
    }

    /// Read the cumulative counters. `max` is reset to zero on read so
    /// it describes the window since the previous snapshot; counts
    /// and sums are cumulative and diffed by [`HistSnap::since`].
    fn snap(&self) -> HistSnap {
        let mut buckets = [0u64; BUCKETS];
        for (i, b) in self.buckets.iter().enumerate() {
            buckets[i] = b.load(Relaxed);
        }
        HistSnap {
            buckets,
            count: self.count.load(Relaxed),
            sum_us: self.sum_us.load(Relaxed),
            max_us: self.max_us.swap(0, Relaxed),
        }
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Plain-data copy of one histogram.
#[derive(Debug, Clone)]
pub struct HistSnap {
    pub buckets: [u64; BUCKETS],
    pub count: u64,
    pub sum_us: u64,
    pub max_us: u64,
}

impl HistSnap {
    fn zero() -> Self {
        Self {
            buckets: [0; BUCKETS],
            count: 0,
            sum_us: 0,
            max_us: 0,
        }
    }

    /// This snapshot minus `prev` (cumulative fields); `max` is this
    /// snapshot's window max as-is.
    fn since(&self, prev: &HistSnap) -> HistSnap {
        let mut buckets = [0u64; BUCKETS];
        for (out, (cur, old)) in buckets
            .iter_mut()
            .zip(self.buckets.iter().zip(prev.buckets.iter()))
        {
            *out = cur.saturating_sub(*old);
        }
        HistSnap {
            buckets,
            count: self.count.saturating_sub(prev.count),
            sum_us: self.sum_us.saturating_sub(prev.sum_us),
            max_us: self.max_us,
        }
    }

    /// Upper bound of the bucket holding the `q` quantile (0..=1).
    pub fn quantile_us(&self, q: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = ((self.count as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0u64;
        for (i, b) in self.buckets.iter().enumerate() {
            seen += b;
            if seen >= target {
                return if i == BUCKETS - 1 {
                    self.max_us.max(bucket_upper(i))
                } else {
                    bucket_upper(i).min(self.max_us.max(1))
                };
            }
        }
        self.max_us
    }

    pub fn mean_us(&self) -> u64 {
        self.sum_us.checked_div(self.count).unwrap_or(0)
    }
}

// =============================================================================
// Registry
// =============================================================================

const NFS_OPS: usize = NfsOp::ALL.len();
const S3_OPS: usize = S3Op::ALL.len();

pub struct Registry {
    nfs: [[Histogram; NFS_OPS]; 2],
    s3: [Histogram; S3_OPS],
}

impl Registry {
    #[allow(clippy::declare_interior_mutable_const)]
    const H: Histogram = Histogram::new();

    pub const fn new() -> Self {
        Self {
            nfs: [[Self::H; NFS_OPS], [Self::H; NFS_OPS]],
            s3: [Self::H; S3_OPS],
        }
    }

    pub fn record_nfs(&self, side: Side, op: NfsOp, d: Duration) {
        self.nfs[side.index()][op.index()].record(d);
    }

    pub fn record_s3(&self, op: S3Op, d: Duration) {
        self.s3[op.index()].record(d);
    }

    /// Read every histogram. Resets each op's window max (see
    /// [`Histogram::snap`]) — one reader owns the window.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            nfs: [
                std::array::from_fn(|i| self.nfs[0][i].snap()),
                std::array::from_fn(|i| self.nfs[1][i].snap()),
            ],
            s3: std::array::from_fn(|i| self.s3[i].snap()),
        }
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

static GLOBAL: Registry = Registry::new();
static PAIRS: AtomicU64 = AtomicU64::new(0);
static LATEST: std::sync::RwLock<Option<Summary>> = std::sync::RwLock::new(None);

/// Tell the registry how many NFS connection pairs this process
/// runs (the busy-share divisor). The orchestrator sets it when the
/// pool is built.
pub fn set_pairs(pairs: u32) {
    PAIRS.store(pairs as u64, Relaxed);
}

pub fn pairs() -> u32 {
    PAIRS.load(Relaxed) as u32
}

/// Publish the most recent window summary for other tasks (the coord
/// heartbeat, the progress writer) to pick up. The heartbeat task is
/// the single producer.
pub fn publish(summary: Summary) {
    if let Ok(mut g) = LATEST.write() {
        *g = Some(summary);
    }
}

pub fn latest() -> Option<Summary> {
    LATEST.read().ok().and_then(|g| g.clone())
}

/// The process-wide registry every RPC and S3 call records into.
pub fn global() -> &'static Registry {
    &GLOBAL
}

/// Drop guard that records the elapsed time of an S3 call when the
/// call's future completes or is cancelled. One line at the top of
/// each `S3Client` method.
pub struct S3Timer {
    op: S3Op,
    start: std::time::Instant,
}

impl S3Timer {
    pub fn start(op: S3Op) -> Self {
        Self {
            op,
            start: std::time::Instant::now(),
        }
    }
}

impl Drop for S3Timer {
    fn drop(&mut self) {
        GLOBAL.record_s3(self.op, self.start.elapsed());
    }
}

// =============================================================================
// Snapshot → Summary
// =============================================================================

/// Every histogram at one instant. Diff two with [`Snapshot::since`]
/// to get a window, then [`Snapshot::summarize`].
#[derive(Debug, Clone)]
pub struct Snapshot {
    nfs: [[HistSnap; NFS_OPS]; 2],
    s3: [HistSnap; S3_OPS],
}

impl Snapshot {
    /// The all-zero snapshot — the `prev` for a first window.
    pub fn zero() -> Self {
        Self {
            nfs: [
                std::array::from_fn(|_| HistSnap::zero()),
                std::array::from_fn(|_| HistSnap::zero()),
            ],
            s3: std::array::from_fn(|_| HistSnap::zero()),
        }
    }

    pub fn since(&self, prev: &Snapshot) -> Snapshot {
        Snapshot {
            nfs: [
                std::array::from_fn(|i| self.nfs[0][i].since(&prev.nfs[0][i])),
                std::array::from_fn(|i| self.nfs[1][i].since(&prev.nfs[1][i])),
            ],
            s3: std::array::from_fn(|i| self.s3[i].since(&prev.s3[i])),
        }
    }

    /// Roll a window up into the operator-facing summary. `pairs` is
    /// the number of NFS connection pairs the worker runs — each one
    /// executes its RPCs serially, so `pairs × window` is the total
    /// time they could have spent waiting on a server.
    pub fn summarize(&self, window: Duration, pairs: u32) -> Summary {
        let window_us = window.as_micros().max(1) as f64;
        let mut ops = Vec::new();
        let mut side_total = [0u64; 2];
        for side in [Side::Src, Side::Dst] {
            for op in NfsOp::ALL {
                let h = &self.nfs[side.index()][op.index()];
                if h.count == 0 {
                    continue;
                }
                side_total[side.index()] += h.sum_us;
                ops.push(OpLatency::from_snap(side.name(), op.name(), h));
            }
        }
        let mut s3_total = 0u64;
        for op in S3Op::ALL {
            let h = &self.s3[op.index()];
            if h.count == 0 {
                continue;
            }
            s3_total += h.sum_us;
            ops.push(OpLatency::from_snap("s3", op.name(), h));
        }
        let capacity_us = window_us * pairs.max(1) as f64;
        Summary {
            window_secs: window.as_secs_f64(),
            pairs,
            src_busy_pct: side_total[0] as f64 * 100.0 / capacity_us,
            dst_busy_pct: side_total[1] as f64 * 100.0 / capacity_us,
            s3_wait_pct: s3_total as f64 * 100.0 / window_us,
            ops,
        }
    }
}

/// One op's window statistics. `side` is `"src"`, `"dst"`, or `"s3"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpLatency {
    pub side: String,
    pub op: String,
    pub count: u64,
    pub mean_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
    /// Sum of the op's wall time in the window — what the busy
    /// shares are built from.
    pub total_us: u64,
}

impl OpLatency {
    fn from_snap(side: &str, op: &str, h: &HistSnap) -> Self {
        Self {
            side: side.to_string(),
            op: op.to_string(),
            count: h.count,
            mean_us: h.mean_us(),
            p50_us: h.quantile_us(0.50),
            p95_us: h.quantile_us(0.95),
            p99_us: h.quantile_us(0.99),
            max_us: h.max_us,
            total_us: h.sum_us,
        }
    }
}

/// A window's latency picture. Serialized into the worker's progress
/// record and its coord heartbeat; rendered by the TUI.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    pub window_secs: f64,
    /// NFS connection pairs the worker runs (the busy-share divisor).
    pub pairs: u32,
    /// Percent of `pairs × window` spent waiting on the source server.
    pub src_busy_pct: f64,
    /// Percent of `pairs × window` spent waiting on the destination.
    pub dst_busy_pct: f64,
    /// Percent of the window spent inside S3 calls (they are mostly
    /// serialized on the orchestrator, so this is against `window`,
    /// not `pairs × window`).
    pub s3_wait_pct: f64,
    pub ops: Vec<OpLatency>,
}

impl Summary {
    /// One line for the journal: `src 61% (LOOKUP p50 0.4ms p95 2.1ms
    /// n=12345, …) · dst 97% (…) · s3 2% (…)`.
    pub fn one_line(&self) -> String {
        let fmt_side = |side: &str| -> String {
            self.ops
                .iter()
                .filter(|o| o.side == side)
                .map(|o| {
                    format!(
                        "{} p50 {} p95 {} n={}",
                        o.op,
                        fmt_us(o.p50_us),
                        fmt_us(o.p95_us),
                        o.count
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!(
            "src {:.0}% [{}] · dst {:.0}% [{}] · s3 {:.0}% [{}]",
            self.src_busy_pct,
            fmt_side("src"),
            self.dst_busy_pct,
            fmt_side("dst"),
            self.s3_wait_pct,
            fmt_side("s3"),
        )
    }
}

/// `0.4ms`, `14ms`, `1.2s` — the way a latency reads in a log line.
pub fn fmt_us(us: u64) -> String {
    if us >= 1_000_000 {
        format!("{:.1}s", us as f64 / 1e6)
    } else if us >= 10_000 {
        format!("{}ms", us / 1000)
    } else if us >= 1000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{us}µs")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_monotone_and_bounds_contain_their_values() {
        let mut last = 0;
        for us in [
            1u64,
            2,
            3,
            4,
            5,
            7,
            8,
            100,
            999,
            1000,
            4095,
            65_536,
            1 << 23,
        ] {
            let idx = bucket_of(us);
            assert!(idx >= last, "bucket index must not decrease ({us})");
            last = idx;
            assert!(
                bucket_upper(idx) >= us,
                "upper bound {} of bucket {} must contain {us}",
                bucket_upper(idx),
                idx
            );
        }
        assert_eq!(bucket_of(1 << 24), BUCKETS - 1);
        assert_eq!(bucket_of(u64::MAX), BUCKETS - 1);
    }

    #[test]
    fn quantiles_reflect_recorded_values() {
        let h = Histogram::new();
        for _ in 0..95 {
            h.record(Duration::from_micros(1000));
        }
        for _ in 0..5 {
            h.record(Duration::from_millis(14));
        }
        let s = h.snap();
        assert_eq!(s.count, 100);
        assert!(
            (1000..=1249).contains(&s.quantile_us(0.50)),
            "{}",
            s.quantile_us(0.50)
        );
        assert!(
            (1000..=1249).contains(&s.quantile_us(0.95)),
            "{}",
            s.quantile_us(0.95)
        );
        assert!(s.quantile_us(0.99) >= 14_000 && s.quantile_us(0.99) <= 16_400);
        assert_eq!(s.max_us, 14_000);
        assert_eq!(s.mean_us(), (95 * 1000 + 5 * 14_000) / 100);
    }

    #[test]
    fn window_diff_and_busy_share() {
        let r = Registry::new();
        let before = r.snapshot();
        // 4 pairs, 1 s window: dst spent 3.6 s waiting, src 0.4 s.
        for _ in 0..36 {
            r.record_nfs(Side::Dst, NfsOp::Create, Duration::from_millis(100));
        }
        for _ in 0..40 {
            r.record_nfs(Side::Src, NfsOp::Read, Duration::from_millis(10));
        }
        r.record_s3(S3Op::Head, Duration::from_millis(50));
        let win = r.snapshot().since(&before);
        let s = win.summarize(Duration::from_secs(1), 4);
        assert!((s.dst_busy_pct - 90.0).abs() < 0.01, "{}", s.dst_busy_pct);
        assert!((s.src_busy_pct - 10.0).abs() < 0.01, "{}", s.src_busy_pct);
        assert!((s.s3_wait_pct - 5.0).abs() < 0.01, "{}", s.s3_wait_pct);
        assert_eq!(s.ops.len(), 3);
        let create = s.ops.iter().find(|o| o.op == "CREATE").unwrap();
        assert_eq!(create.side, "dst");
        assert_eq!(create.count, 36);
        assert_eq!(create.max_us, 100_000);
        // A second window with nothing recorded is empty, and the
        // max reset on read.
        let again = r.snapshot();
        let empty = again.since(&r.snapshot());
        assert!(empty.summarize(Duration::from_secs(1), 4).ops.is_empty());
        assert_eq!(again.nfs[1][NfsOp::Create.index()].max_us, 0);
    }

    #[test]
    fn op_tags_round_trip_and_unknown_is_other() {
        for op in NfsOp::ALL {
            if op != NfsOp::Other {
                assert_eq!(NfsOp::from_tag(op.name()), op);
            }
        }
        assert_eq!(NfsOp::from_tag("FSSTAT"), NfsOp::Other);
    }

    #[test]
    fn one_line_and_fmt_us_read_well() {
        assert_eq!(fmt_us(400), "400µs");
        assert_eq!(fmt_us(1400), "1.4ms");
        assert_eq!(fmt_us(14_250), "14ms");
        assert_eq!(fmt_us(1_200_000), "1.2s");
        let r = Registry::new();
        r.record_nfs(Side::Dst, NfsOp::Rename, Duration::from_millis(14));
        let s = r.snapshot().summarize(Duration::from_secs(30), 110);
        let line = s.one_line();
        assert!(
            line.contains("dst 0% [RENAME p50 14ms p95 14ms n=1]"),
            "{line}"
        );
    }
}
