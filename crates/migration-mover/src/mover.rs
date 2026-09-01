//! The `Mover` orchestrates one file copy from start to finish:
//! strategy selection → data movement → attribute application → atomic
//! rename. The shard processor calls `move_one` per row (and
//! `move_hardlink` for rows that have already been copied earlier in
//! the shard).
//!
//! ## Threading model (M3)
//!
//! libnfs is a userspace transport whose ops *block* the calling
//! thread on the network socket. The mover's public `move_*` entry
//! points are therefore split:
//!
//! 1. **Async prologue** — pick the strategy (cheap, sync) and
//!    `pool.acquire().await` (channel recv, properly async).
//! 2. **Blocking body** — `tokio::task::spawn_blocking` runs the
//!    libnfs work on the runtime's blocking pool so the worker
//!    threads stay free for other tasks.
//!
//! This is what makes the M3 concurrent shard dispatch viable. With
//! M2's "everything async" shape, N concurrent libnfs copies would
//! pin N tokio workers; routing through the blocking pool removes
//! that ceiling.
//!
//! ## Order on commit (R4)
//!
//! 1. data WRITE
//! 2. close write fh
//! 3. chown (uid/gid) — skipped or downgraded if `require_chown`
//!    not set and EPERM is observed; runs *before* chmod because
//!    NFSv3 SETATTR of uid/gid clears S_ISUID/S_ISGID on regular
//!    files (kill-priv semantics) — see `attr_plan` (F08)
//! 4. chmod (mode)
//! 5. utimes (atime/mtime) — last, because some servers update mtime
//!    as a side effect of mode/owner changes
//! 6. rename `.partial` → final — **commit point**

use crate::attr_plan::{self, AttrExec, ChownOutcome};
use crate::attrs::{self, AttrPolicy};
use crate::batch::InflightProfile;
use crate::downgrade::DowngradeSink;
use crate::error::MoveError;
use crate::libnfs::raw::{self, RawSattr};
use crate::libnfs::{ops, ContextPair, LibnfsContextPool, NfsContext};
use crate::paths::{join_root, partial_path};
use crate::strategy::{self, Strategy, StrategyContext};
use migration_core::fence::Fence;
use migration_core::records::{DowngradeKind, FailurePhase, MigrationOptions};
use migration_core::shard::RowView;
use std::sync::Arc;

/// Conventional symlink mode on POSIX (`rwxrwxrwx`). When source
/// reports this, no mode-on-symlink work is needed — that's already
/// what `nfs_symlink` produces.
const SYMLINK_DEFAULT_MODE: u32 = 0o0777;

/// Streaming buffer size for the sync libnfs READ→WRITE path. Per-task
/// allocation is negligible beside the libnfs network round trips, so the
/// active path deliberately uses a plain buffer.
const STREAM_BUF_SIZE: usize = 1 << 20; // 1 MiB

/// Bound READDIRPLUS prefetch memory for pathological flat directories.
/// Typical source directories in the 600M-file run held ~120 children.
const DIR_CHILDREN_ENTRY_CAP: usize = 100_000;
/// Bound the aggregate number of cached child filehandles instead of assuming
/// a small directory working set. The worker interleaves parents within each
/// dispatch batch; a 91-directory hardware smoke test thrashed a 64-directory
/// LRU even though those directories held only ~13K children in total.
const DIR_CHILDREN_CACHE_FH_CAPACITY: usize = 1_000_000;
/// Disabled sentinels have no child filehandles, so retain a separate directory
/// bound to keep failed/oversized prefetches from accumulating indefinitely.
const DIR_CHILDREN_CACHE_DIR_CAPACITY: usize = 4_096;
/// Bound on the evicted-dirs set (see `DirChildrenState::evicted`).
/// ~60 bytes per remembered path: full at ~60 MB.
const DIR_CHILDREN_EVICTED_CAP: usize = 1_000_000;

/// Outcome of attempting to move one file.
#[derive(Debug, Clone)]
pub struct MoveOutcome {
    pub row_id: u64,
    pub strategy: Strategy,
    /// Bytes actually written for this row (F41) — the streaming
    /// copy's byte count for regular files (less than `row.size` on
    /// an EarlyEof short copy), and 0 for failures and for rows that
    /// move no file data (skip / symlink / hardlink / dir-attrs /
    /// empty). Never `row.size` taken on faith: this value feeds the
    /// throughput sample that gates backpressure, progress records,
    /// and coord aggregation.
    pub bytes_moved: u64,
    /// True iff the copy committed but the source changed under it
    /// (`FileCopyResult::torn` → `file_mover::classify_copy`). The
    /// row still counts as copied; a `DowngradeKind::TornCopy` record
    /// was emitted, and the shard processor bumps `files_torn`.
    /// Detection is async-path-only: the sync path
    /// (`do_libnfs_copy`) has no pre/post stat bracket and always
    /// reports `false`.
    pub torn: bool,
    pub result: Result<(), MoveError>,
}

/// Internal configuration consumed by the two implemented libnfs movers.
/// Historical operator fields remain accepted by `migration-worker` for TOML
/// compatibility but are not projected into this executable configuration.
#[derive(Clone)]
pub struct MoverConfig {
    pub source_url: String,
    pub dest_url: String,
    /// `endpoint.root` from the manifest's `source` block. Joined with
    /// `row.path` via [`join_root`] to produce the absolute path used
    /// by every source-side libnfs op. See SCHEMA_CONTRACT.md "Path
    /// encoding" and BUGFIX_PLAN.md.
    pub source_root: String,
    /// `endpoint.root` from the manifest's `dest` block. Joined with
    /// `row.path` via [`join_root`] for every dest-side libnfs op.
    pub dest_root: String,
    pub policy: AttrPolicy,
    pub inflight: InflightProfile,
    /// True if the worker has CAP_CHOWN (or `require_chown_capability`
    /// is set). Controls whether `chown` EPERM is fatal or degraded
    /// to a recorded warning.
    pub require_chown: bool,
    /// When true, verify the bytes written equal `row.size` and fail
    /// the row with `SIZE_CHANGED` on mismatch. See
    /// `SCHEMA_CONTRACT.md` "Size semantics". Default false: source
    /// truth wins over walker's stale `size`.
    pub require_unchanged_size: bool,
    /// Route regular-file copies through the raw NFSv3 filehandle
    /// path ([`crate::libnfs::raw`]): cached parent-dir filehandles,
    /// attrs stamped at CREATE, FILE_SYNC single-chunk writes, one
    /// SETATTR for times, and READDIRPLUS child-FH prefetch. ~5 RPCs
    /// per small file (plus an amortized per-directory READDIRPLUS)
    /// vs ~60-80 on the path-based API. Regular files only; other row
    /// types keep the path-based ops.
    pub use_raw_fh: bool,
    /// Raw-FH path only: CREATE the destination under its *final*
    /// name and skip the `.partial` + RENAME publish — 4 RPCs per
    /// typical small file instead of 5. Trades the atomic-publish property
    /// for throughput: a crash mid-copy can leave a torn file
    /// visible at the final path. Safe when nothing consumes the
    /// destination namespace until the migration completes — CREATE
    /// is UNCHECKED with size=0, so a re-run truncates and heals any
    /// torn file. Off by default.
    pub direct_commit: bool,
    /// F12: per-RPC timeout in milliseconds, applied to every libnfs
    /// context (sync pools and the bucketed async pool) at creation.
    /// `0` = leave the libnfs built-in default untouched. Seeded to
    /// [`crate::libnfs::DEFAULT_RPC_TIMEOUT_MS`] by `from_options`;
    /// the orchestrator overrides it from `[mover] rpc_timeout_ms`.
    pub rpc_timeout_ms: u32,
}

impl MoverConfig {
    pub fn from_options(
        source_url: String,
        dest_url: String,
        source_root: String,
        dest_root: String,
        opts: &MigrationOptions,
    ) -> Self {
        Self {
            source_url,
            dest_url,
            source_root,
            dest_root,
            policy: AttrPolicy::from_options(opts),
            inflight: InflightProfile::default(),
            require_chown: true,
            require_unchanged_size: false,
            use_raw_fh: false,
            direct_commit: false,
            rpc_timeout_ms: crate::libnfs::DEFAULT_RPC_TIMEOUT_MS,
        }
    }
}

/// The mover. Holds long-lived resources: libnfs context pool, the
/// host id and pid (used to construct `.partial` names), the downgrade
/// sink, the fence (consulted immediately before each commit-point op
/// per R8), and policy. Cloning is cheap (Arc inside) and required
/// because concurrent shard dispatch hands a clone to each spawned
/// task. The fence is Arc-backed; all clones share the same atomic flag.
/// Per-directory single-flight guards keyed by destination path.
type DirLocks = std::sync::Mutex<std::collections::HashMap<Vec<u8>, Arc<std::sync::Mutex<()>>>>;

#[derive(Clone)]
pub struct Mover {
    cfg: Arc<MoverConfig>,
    pool: Arc<dyn LibnfsContextPool>,
    host_id: Arc<str>,
    pid: u32,
    downgrades: DowngradeSink,
    fence: Fence,
    /// Destination directories confirmed present, shared across all
    /// blocking copies. Entries are only added after a successful
    /// `mkdir_p`, and nothing removes destination directories during a
    /// run, so a hit can never mask a missing directory. Guards the
    /// per-file ancestor mkdir probes, which otherwise dominate the RPC
    /// budget on small-file trees (measured ~7 EEXIST round-trips per
    /// file on a depth-8 tree).
    dirs_known: Arc<std::sync::Mutex<std::collections::HashSet<Vec<u8>>>>,
    /// Single-flight guards per directory currently being created.
    /// Without this the cache misses under fan-out: every file of a
    /// directory is already in flight before the first `mkdir_p`
    /// finishes and populates `dirs_known`, so all of them re-probe the
    /// ancestor chain (measured: ~7 EEXIST round-trips per file even
    /// with the plain cache). Losers of the race block on the winner's
    /// per-dir mutex (blocking-pool threads, so parking is fine) and
    /// then hit the cache.
    dir_locks: Arc<DirLocks>,
    /// Raw-FH path (see [`crate::libnfs::raw`]): directory filehandles
    /// resolved once and shared across every context — NFSv3 fhs are
    /// server-scoped, not connection-scoped. Separate caches per side
    /// because source and destination are different exports.
    src_dir_fhs: Arc<FhCache>,
    dst_dir_fhs: Arc<FhCache>,
    /// Source directory → child-name/filehandle maps filled by paged
    /// READDIRPLUS. This removes the per-file source LOOKUP from the
    /// raw-FH path. Misses, omitted handles, oversize directories,
    /// and prefetch errors all fall back to ordinary LOOKUP.
    src_dir_children: Arc<DirChildren>,
}

/// Path → directory-filehandle cache with per-path single-flight, so a
/// burst of files landing in one new directory costs one LOOKUP/MKDIR
/// chain, not one per in-flight file.
#[derive(Default)]
struct FhCache {
    map: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Arc<Vec<u8>>>>,
    locks: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Arc<std::sync::Mutex<()>>>>,
}

type ChildFhs = std::collections::HashMap<Vec<u8>, Arc<Vec<u8>>>;

#[derive(Clone)]
enum DirChildrenEntry {
    Ready(Arc<ChildFhs>),
    /// Prefetch was abandoned (entry cap or RPC error). Keep the
    /// sentinel in the bounded cache so every file in the same
    /// grouped directory does not retry the failed optimization.
    Disabled,
}

impl DirChildrenEntry {
    fn child_fh_count(&self) -> usize {
        match self {
            Self::Ready(children) => children.len(),
            Self::Disabled => 0,
        }
    }
}

struct CachedDirChildren {
    entry: DirChildrenEntry,
    /// Second-chance bit: child hits set it in O(1); eviction clears it
    /// and rotates the directory once before considering it again.
    referenced: bool,
}

enum DirChildLookup {
    Uncached,
    Hit(Arc<Vec<u8>>),
    Miss,
}

#[derive(Default)]
struct DirChildrenState {
    entries: std::collections::HashMap<Vec<u8>, CachedDirChildren>,
    lru: std::collections::VecDeque<Vec<u8>>,
    cached_child_fhs: usize,
    /// Dirs that were prefetched and then evicted. A dir in this set is
    /// never prefetched again — its children resolve via per-name
    /// LOOKUP. Without this, a working set larger than the cache
    /// thrashes: every row re-prefetches its whole directory (canonical
    /// shards interleave rows across dirs, so eviction happens between
    /// two rows of the same dir), turning the one-RPC LOOKUP the
    /// prefetch was meant to save into a full multi-page READDIRPLUS
    /// per file. Bounded by [`DIR_CHILDREN_EVICTED_CAP`]; past the cap
    /// new evictions go unrecorded and those dirs fall back to the old
    /// re-prefetch behavior.
    evicted: std::collections::HashSet<Vec<u8>>,
}

/// Bounded source-directory child-FH cache with per-directory single-flight.
/// Its second-chance queue keeps hits O(1), holds exactly one queue key per
/// cached directory, and still keeps recently used parents through eviction.
struct DirChildren {
    state: std::sync::Mutex<DirChildrenState>,
    locks: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Arc<std::sync::Mutex<()>>>>,
    dir_capacity: usize,
    child_fh_capacity: usize,
}

impl Default for DirChildren {
    fn default() -> Self {
        Self {
            state: std::sync::Mutex::new(DirChildrenState::default()),
            locks: std::sync::Mutex::new(std::collections::HashMap::new()),
            dir_capacity: DIR_CHILDREN_CACHE_DIR_CAPACITY,
            child_fh_capacity: DIR_CHILDREN_CACHE_FH_CAPACITY,
        }
    }
}

impl DirChildren {
    #[cfg(test)]
    fn with_limits(dir_capacity: usize, child_fh_capacity: usize) -> Self {
        assert!(dir_capacity > 0);
        assert!(child_fh_capacity > 0);
        Self {
            state: std::sync::Mutex::new(DirChildrenState::default()),
            locks: std::sync::Mutex::new(std::collections::HashMap::new()),
            dir_capacity,
            child_fh_capacity,
        }
    }

    fn lookup(&self, dir: &[u8], name: &[u8]) -> DirChildLookup {
        let mut state = self.state.lock().unwrap();
        let entry = match state.entries.get_mut(dir) {
            Some(cached) => {
                cached.referenced = true;
                cached.entry.clone()
            }
            None => return DirChildLookup::Uncached,
        };
        match entry {
            DirChildrenEntry::Ready(children) => {
                children.get(name).map_or(DirChildLookup::Miss, |fh| {
                    DirChildLookup::Hit(Arc::clone(fh))
                })
            }
            DirChildrenEntry::Disabled => DirChildLookup::Miss,
        }
    }

    fn insert(&self, dir: Vec<u8>, entry: DirChildrenEntry) {
        let mut state = self.state.lock().unwrap();
        if let Some(pos) = state.lru.iter().position(|key| key == &dir) {
            state.lru.remove(pos);
        }
        if let Some(replaced) = state.entries.remove(dir.as_slice()) {
            state.cached_child_fhs -= replaced.entry.child_fh_count();
        }

        let child_fh_count = entry.child_fh_count();
        while state.entries.len() >= self.dir_capacity
            || state.cached_child_fhs.saturating_add(child_fh_count) > self.child_fh_capacity
        {
            let Some(oldest) = state.lru.pop_front() else {
                break;
            };
            if let Some(cached) = state.entries.get_mut(oldest.as_slice()) {
                if cached.referenced {
                    cached.referenced = false;
                    state.lru.push_back(oldest);
                    continue;
                }
            }
            if let Some(evicted) = state.entries.remove(oldest.as_slice()) {
                state.cached_child_fhs -= evicted.entry.child_fh_count();
                if state.evicted.len() < DIR_CHILDREN_EVICTED_CAP {
                    state.evicted.insert(oldest);
                }
            }
        }
        state.cached_child_fhs += child_fh_count;
        state.lru.push_back(dir.clone());
        state.entries.insert(
            dir,
            CachedDirChildren {
                entry,
                referenced: false,
            },
        );
        debug_assert!(state.entries.len() <= self.dir_capacity);
        debug_assert_eq!(state.entries.len(), state.lru.len());
        debug_assert!(state.cached_child_fhs <= self.child_fh_capacity);
    }

    fn disable(&self, dir: &[u8]) {
        self.insert(dir.to_vec(), DirChildrenEntry::Disabled);
    }

    /// True when `dir` was prefetched once and then evicted — the
    /// caller must resolve via LOOKUP instead of re-prefetching.
    fn was_evicted(&self, dir: &[u8]) -> bool {
        self.state.lock().unwrap().evicted.contains(dir)
    }

    fn flight(&self, dir: &[u8]) -> Arc<std::sync::Mutex<()>> {
        let mut locks = self.locks.lock().unwrap();
        Arc::clone(
            locks
                .entry(dir.to_vec())
                .or_insert_with(|| Arc::new(std::sync::Mutex::new(()))),
        )
    }

    fn finish_flight(&self, dir: &[u8], flight: &Arc<std::sync::Mutex<()>>) {
        let mut locks = self.locks.lock().unwrap();
        if locks
            .get(dir)
            .is_some_and(|current| Arc::ptr_eq(current, flight))
            && Arc::strong_count(flight) == 2
        {
            locks.remove(dir);
        }
    }
}

impl Mover {
    pub fn new(
        cfg: MoverConfig,
        pool: Arc<dyn LibnfsContextPool>,
        host_id: impl Into<Arc<str>>,
        downgrades: DowngradeSink,
        fence: Fence,
    ) -> Self {
        Self {
            cfg: Arc::new(cfg),
            pool,
            host_id: host_id.into(),
            pid: std::process::id(),
            downgrades,
            fence,
            dirs_known: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
            dir_locks: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            src_dir_fhs: Arc::new(FhCache::default()),
            dst_dir_fhs: Arc::new(FhCache::default()),
            src_dir_children: Arc::new(DirChildren::default()),
        }
    }

    /// Resolve a source child filehandle through the READDIRPLUS
    /// cache. The optimization is fail-open: a missing/omitted handle,
    /// an oversize directory, or any prefetch RPC error uses the same
    /// per-name LOOKUP as the original raw path.
    fn resolve_source_child_fh(
        &self,
        ctx: &mut NfsContext,
        dir_path: &[u8],
        dir_fh: &[u8],
        name: &[u8],
    ) -> Result<(Arc<Vec<u8>>, bool), raw::RawError> {
        match self.src_dir_children.lookup(dir_path, name) {
            DirChildLookup::Hit(fh) => return Ok((fh, true)),
            DirChildLookup::Miss => {
                return raw::lookup(ctx, dir_fh, name).map(|fh| (Arc::new(fh), false));
            }
            DirChildLookup::Uncached => {}
        }
        if self.src_dir_children.was_evicted(dir_path) {
            return raw::lookup(ctx, dir_fh, name).map(|fh| (Arc::new(fh), false));
        }

        let flight = self.src_dir_children.flight(dir_path);
        let _guard = flight.lock().unwrap();
        match self.src_dir_children.lookup(dir_path, name) {
            DirChildLookup::Hit(fh) => {
                self.src_dir_children.finish_flight(dir_path, &flight);
                return Ok((fh, true));
            }
            DirChildLookup::Miss => {
                self.src_dir_children.finish_flight(dir_path, &flight);
                return raw::lookup(ctx, dir_fh, name).map(|fh| (Arc::new(fh), false));
            }
            DirChildLookup::Uncached => {}
        }

        let entry = match raw::readdirplus(ctx, dir_fh, DIR_CHILDREN_ENTRY_CAP) {
            Ok(raw::ReaddirplusResult::Complete(entries)) => {
                let entry_count = entries.len();
                let children: ChildFhs = entries
                    .into_iter()
                    .filter_map(|entry| entry.fh.map(|fh| (entry.name, Arc::new(fh))))
                    .collect();
                tracing::debug!(
                    dir = %String::from_utf8_lossy(dir_path),
                    entries = entry_count,
                    handles = children.len(),
                    "source child filehandles prefetched",
                );
                DirChildrenEntry::Ready(Arc::new(children))
            }
            Ok(raw::ReaddirplusResult::TooMany) => {
                tracing::debug!(
                    dir = %String::from_utf8_lossy(dir_path),
                    cap = DIR_CHILDREN_ENTRY_CAP,
                    "source directory exceeds prefetch cap; using LOOKUP",
                );
                DirChildrenEntry::Disabled
            }
            Err(error) => {
                tracing::warn!(
                    dir = %String::from_utf8_lossy(dir_path),
                    error = %error.detail,
                    "source READDIRPLUS prefetch failed; using LOOKUP",
                );
                DirChildrenEntry::Disabled
            }
        };
        self.src_dir_children
            .insert(dir_path.to_vec(), entry.clone());
        self.src_dir_children.finish_flight(dir_path, &flight);

        match entry {
            DirChildrenEntry::Ready(children) => match children.get(name) {
                Some(fh) => Ok((Arc::clone(fh), true)),
                None => raw::lookup(ctx, dir_fh, name).map(|fh| (Arc::new(fh), false)),
            },
            DirChildrenEntry::Disabled => {
                raw::lookup(ctx, dir_fh, name).map(|fh| (Arc::new(fh), false))
            }
        }
    }

    /// Resolve `dir_path`'s filehandle, walking component-by-component
    /// from the export root with every prefix cached. With
    /// `create_missing`, absent components are MKDIRed (0755 stand-in
    /// mode; the dir-attrs pass overwrites, same contract as
    /// `ops::mkdir_p`). Concurrent resolvers of the same prefix
    /// single-flight on a per-prefix mutex.
    fn resolve_dir_fh(
        &self,
        ctx: &mut NfsContext,
        cache: &FhCache,
        dir_path: &[u8],
        create_missing: bool,
    ) -> Result<Arc<Vec<u8>>, raw::RawError> {
        if dir_path.is_empty() || dir_path == b"/" {
            return Ok(Arc::new(raw::root_fh(ctx)?));
        }
        if let Some(fh) = cache.map.lock().unwrap().get(dir_path) {
            return Ok(Arc::clone(fh));
        }
        let mut cur: Arc<Vec<u8>> = Arc::new(raw::root_fh(ctx)?);
        let mut acc: Vec<u8> = Vec::with_capacity(dir_path.len());
        for comp in dir_path.split(|&b| b == b'/') {
            if comp.is_empty() {
                continue;
            }
            acc.push(b'/');
            acc.extend_from_slice(comp);
            if let Some(fh) = cache.map.lock().unwrap().get(acc.as_slice()) {
                cur = Arc::clone(fh);
                continue;
            }
            let flight = {
                let mut locks = cache.locks.lock().unwrap();
                Arc::clone(
                    locks
                        .entry(acc.clone())
                        .or_insert_with(|| Arc::new(std::sync::Mutex::new(()))),
                )
            };
            let _g = flight.lock().unwrap();
            if let Some(fh) = cache.map.lock().unwrap().get(acc.as_slice()) {
                cur = Arc::clone(fh);
                continue;
            }
            let fh = match raw::lookup(ctx, &cur, comp) {
                Ok(fh) => fh,
                Err(e) if e.tag == "ENOENT" && create_missing => {
                    match raw::mkdir(ctx, &cur, comp, 0o755) {
                        Ok(fh) => fh,
                        // Lost a cross-host race; the dir exists now.
                        Err(e2) if e2.tag == "EEXIST" => raw::lookup(ctx, &cur, comp)?,
                        Err(e2) => return Err(e2),
                    }
                }
                Err(e) => return Err(e),
            };
            let fh = Arc::new(fh);
            cache
                .map
                .lock()
                .unwrap()
                .insert(acc.clone(), Arc::clone(&fh));
            cache.locks.lock().unwrap().remove(acc.as_slice());
            cur = fh;
        }
        Ok(cur)
    }

    /// Cached `mkdir -p` of `file_path`'s parent (see `dirs_known`).
    fn ensure_parent_dir(&self, ctx: &mut NfsContext, file_path: &[u8]) -> Result<(), MoveError> {
        let last_slash = match file_path.iter().rposition(|&b| b == b'/') {
            Some(0) | None => return Ok(()), // root or no parent; root always exists
            Some(i) => i,
        };
        let parent = &file_path[..last_slash];
        if parent.is_empty() {
            return Ok(());
        }
        self.ensure_dir(ctx, parent)
    }

    /// Cached `mkdir -p <path>`. On a miss, performs the real chain and
    /// then records `path` plus every ancestor, so sibling subtrees skip
    /// the shared prefix entirely.
    fn ensure_dir(&self, ctx: &mut NfsContext, path: &[u8]) -> Result<(), MoveError> {
        if self.dirs_known.lock().unwrap().contains(path) {
            return Ok(());
        }
        // Single-flight: exactly one task per directory runs the real
        // mkdir chain; concurrent requesters park on the per-dir mutex
        // and re-check the cache once the winner finishes.
        let dir_lock = {
            let mut locks = self.dir_locks.lock().unwrap();
            Arc::clone(
                locks
                    .entry(path.to_vec())
                    .or_insert_with(|| Arc::new(std::sync::Mutex::new(()))),
            )
        };
        let _flight = dir_lock.lock().unwrap();
        if self.dirs_known.lock().unwrap().contains(path) {
            return Ok(());
        }
        ops::mkdir_p(ctx, path)?;
        {
            let mut known = self.dirs_known.lock().unwrap();
            let mut end = path.len();
            loop {
                known.insert(path[..end].to_vec());
                match path[..end].iter().rposition(|&b| b == b'/') {
                    Some(i) if i > 0 => end = i,
                    _ => break,
                }
            }
        }
        self.dir_locks.lock().unwrap().remove(path);
        Ok(())
    }

    /// Borrow the downgrade sink. Used by the orchestrator to drain
    /// JSONL between shards and to update the current shard name, and
    /// by the processor to record FsidUngrouped fallbacks.
    pub fn downgrade_sink(&self) -> &DowngradeSink {
        &self.downgrades
    }

    // =========================================================================
    // Public entry points used by the shard processor.
    // =========================================================================

    /// Move a single file. Picks a strategy from `(row, ctx)`, executes
    /// it, and returns the outcome. Hardlinks-to-already-copied-inodes
    /// are *not* dispatched here — call [`Self::move_hardlink`] for
    /// those (the shard processor's per-group logic is the authority).
    pub async fn move_one(&self, row: &RowView) -> MoveOutcome {
        let strat_ctx = StrategyContext {
            already_copied_inode: false,
        };
        let strategy = strategy::pick(row, &strat_ctx);
        let row_owned = row.clone();
        self.run_with_pair(row, strategy, move |me, pair| {
            me.execute(pair, &row_owned, strategy)
        })
        .await
    }

    /// Hardlink an already-copied dest path to a new linkpath. Caller
    /// (the shard processor) holds the per-group "first path" state
    /// and is responsible for passing the *final* (post-rename) path
    /// — see R5.
    pub async fn move_hardlink(&self, row: &RowView, link_target: &[u8]) -> MoveOutcome {
        let target = link_target.to_vec();
        let path = row.path.clone();
        self.run_with_pair(row, Strategy::HardlinkExisting, move |me, pair| {
            // F41: a hardlink writes no file data — report 0 bytes.
            me.do_hardlink(pair, &target, &path).map(|()| 0)
        })
        .await
    }

    /// Common framing: pick-strategy → acquire-pair → spawn_blocking →
    /// build MoveOutcome. The closure receives the cloned mover and a
    /// mutable borrow of the pair so it can drive any of the
    /// strategy-specific sync paths; on success it returns the bytes
    /// it actually wrote (F41), which becomes `MoveOutcome::bytes_moved`.
    async fn run_with_pair<F>(&self, row: &RowView, strategy: Strategy, work: F) -> MoveOutcome
    where
        F: FnOnce(&Mover, &mut ContextPair) -> Result<u64, MoveError> + Send + 'static,
    {
        let row_id = row.row_id;

        let pair = match self.pool.acquire().await {
            Ok(p) => p,
            Err(e) => {
                return MoveOutcome {
                    row_id,
                    strategy,
                    bytes_moved: 0,
                    torn: false,
                    result: Err(MoveError::new(FailurePhase::Open, format!("pool: {e}"))),
                };
            }
        };

        // Move-into-blocking. The pair must live for the whole sync
        // body; on completion (or panic) it Drops, which sends the
        // contexts back to the pool.
        let me = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut pair = pair;
            work(&me, &mut pair)
        })
        .await
        .unwrap_or_else(|join_err| {
            Err(MoveError::new(
                FailurePhase::Open,
                format!("spawn_blocking join: {join_err}"),
            ))
        });

        MoveOutcome {
            row_id,
            strategy,
            // F41: the bytes the body actually wrote — never row.size
            // taken on faith. 0 on failure (pre-existing contract).
            bytes_moved: *result.as_ref().unwrap_or(&0),
            // Sync paths have no torn detection (see do_libnfs_copy).
            torn: false,
            result: result.map(|_| ()),
        }
    }

    // =========================================================================
    // Sync strategy dispatch (called from inside spawn_blocking).
    // =========================================================================

    /// Dispatch one strategy body and report the bytes it actually
    /// wrote (F41). Only the streaming copy moves file data; symlink,
    /// hardlink, dir-attrs, empty, and skip rows write no file bytes
    /// and report 0 — `row.size` is never reported on faith.
    fn execute(
        &self,
        pair: &mut ContextPair,
        row: &RowView,
        strategy: Strategy,
    ) -> Result<u64, MoveError> {
        match strategy {
            Strategy::LibnfsIoUring => self.do_libnfs_copy(pair, row),
            Strategy::Symlink => self.do_symlink(pair, row).map(|()| 0),
            Strategy::HardlinkExisting => Err(MoveError::new(FailurePhase::Hardlink, "EINVAL")),
            Strategy::Empty => self.do_empty(pair, row).map(|()| 0),
            Strategy::DirAttrs => self.do_dir_attrs(pair, row).map(|()| 0),
            Strategy::Skip => Ok(0),
        }
    }

    /// Apply mode/owner/mtime to a directory whose row appeared in
    /// the index. Ensures the dir exists first (mkdir-on-demand may
    /// not have created it if no child file landed in it). Caller
    /// (the shard processor) MUST schedule this strategy after all
    /// non-dir rows in the same micro-batch are committed, otherwise file
    /// commits inside the dir will restamp its mtime.
    ///
    /// Cross-batch caveat: if a child file's row lands in a later batch or
    /// shard than its parent dir's row, that child's commit will still restamp
    /// the parent's mtime. Documented in M3_NOTES.md.
    fn do_dir_attrs(&self, pair: &mut ContextPair, row: &RowView) -> Result<(), MoveError> {
        let dst = self.dst_path(row);
        self.ensure_dir(pair.dst(), &dst)?;
        self.apply_attrs(pair.dst(), &dst, row, None)?;
        Ok(())
    }

    // =========================================================================
    // Strategy implementations (all sync, all called from inside
    // spawn_blocking with an owned ContextPair).
    // =========================================================================

    /// Compose the source-side absolute path for `row.path`. Source
    /// libnfs ops MUST go through this — never `&row.path` directly.
    /// See BUGFIX_PLAN.md and SCHEMA_CONTRACT.md "Path encoding".
    fn src_path(&self, row: &RowView) -> Vec<u8> {
        join_root(self.cfg.source_root.as_bytes(), &row.path)
    }

    /// Compose the destination-side absolute path for `row.path`.
    /// All dest libnfs ops MUST go through this.
    fn dst_path(&self, row: &RowView) -> Vec<u8> {
        join_root(self.cfg.dest_root.as_bytes(), &row.path)
    }

    /// Per-file self-target check (Fix 2). Refuses to write when the
    /// computed dest path would collide with the source — either
    /// directly (same path) or through the `.partial` sibling living
    /// next to the source file. Both forms can zero a source file
    /// when `nfs_create` opens with `O_TRUNC`.
    ///
    /// Returns Err with tag `SELF_TARGET` on collision. Only meaningful
    /// when source and dest URLs match — different servers can never
    /// collide regardless of path. Belt-and-suspenders against the
    /// startup overlap guard in the worker; either alone is
    /// insufficient.
    fn check_self_target(
        &self,
        src: &[u8],
        dst: &[u8],
        dst_partial: &[u8],
    ) -> Result<(), MoveError> {
        check_self_target(
            &self.cfg.source_url,
            &self.cfg.dest_url,
            src,
            dst,
            dst_partial,
        )
    }

    /// R8: consult the fence immediately before issuing a commit-point
    /// op (`rename` / `link` / `symlink`). If the fence has tripped
    /// since the shard processor's last between-row check, bail out
    /// with `FailurePhase::Fenced` rather than commit. The shard
    /// processor recognizes that phase and routes the row back to
    /// claimable (via the shard's claim terminating) instead of
    /// recording a per-file failure.
    ///
    /// Note: there is no fence check inside the per-byte READ→WRITE
    /// loop. Once a row's commit op is in flight (mid-syscall) we
    /// accept it — that's the residual at-least-once tolerance the
    /// `.partial`-stamped + atomic-rename safety argument relies on
    /// (see CLAIM_PROTOCOL.md "What's NOT enforced" / R8).
    fn check_fence(&self) -> Result<(), MoveError> {
        if self.fence.is_valid() {
            Ok(())
        } else {
            Err(MoveError::new(FailurePhase::Fenced, "FENCE_TRIPPED"))
        }
    }

    /// Symlink — preserve `target` byte-for-byte from the index column
    /// if present (R8), otherwise readlink from the source.
    ///
    /// Per SCHEMA_CONTRACT.md "Symlink mode preservation": NFSv3 has
    /// no lchmod-equivalent (`nfs_chmod` follows symlinks), so when
    /// `preserve_mode = true` and the source mode bits differ from
    /// the conventional `0o0777`, the mover writes a
    /// `SYMLINK_MODE_NFSV3` downgrade record and counts the row as
    /// success. The destination symlink ends up with whatever default
    /// mode the server assigns. See BUGFIX_PLAN.md "Fix 5".
    ///
    /// Symlink is the commit point (R8), and replay is idempotent
    /// (F10): under at-least-once delivery a worker can die
    /// post-symlink-pre-ack and the row comes back. When `nfs_symlink`
    /// reports `EEXIST`, the destination is readlink'd on the dst
    /// context and [`resolve_symlink_eexist`] treats a byte-equal
    /// target as our own committed work (`Ok`, continuing with the
    /// idempotent post-commit steps), a mismatch as a real conflict
    /// (fail, naming both targets), and a readlink failure as the
    /// original `EEXIST` failure. Unlink-then-create was rejected as
    /// the recovery strategy: it destroys pre-existing data at the
    /// destination and opens a crash window with the link missing.
    fn do_symlink(&self, pair: &mut ContextPair, row: &RowView) -> Result<(), MoveError> {
        let src = self.src_path(row);
        let dst = self.dst_path(row);

        let target = match &row.symlink_target {
            Some(t) => t.clone(),
            None => ops::readlink(pair.src(), &src)?,
        };

        if let Err(e) = self.ensure_parent_dir(pair.dst(), &dst) {
            return Err(MoveError::new(FailurePhase::Symlink, e.error));
        }
        // R8: symlink IS the commit point for symlink rows — there is
        // no .partial + rename pattern (NFSv3 has no atomic
        // symlink-replace primitive). Fence-check immediately before
        // issuing it.
        self.check_fence()?;
        if let Err(e) = ops::symlink(pair.dst(), &target, &dst) {
            if !is_eexist(&e) {
                return Err(e);
            }
            // EEXIST recovery — readlink the destination on the dst
            // context and let `resolve_symlink_eexist` decide replay
            // vs conflict. Runs strictly after `ops::symlink`
            // returned, so the R8 fence check above still guards the
            // commit point. On Ok, fall THROUGH to the post-commit
            // steps below (mode-downgrade record, best-effort
            // lutimes): they are identical to the first run and
            // idempotent under at-least-once replay.
            let dst_readlink = ops::readlink(pair.dst(), &dst);
            resolve_symlink_eexist(e, &target, dst_readlink)?;
        }

        if self.cfg.policy.preserve_mode {
            let link_mode = row.mode & 0o7777;
            if link_mode != SYMLINK_DEFAULT_MODE {
                self.downgrades
                    .record(row.row_id, &row.path, DowngradeKind::SymlinkModeNfsV3);
            }
        }

        // Symlink mtime — best-effort post-commit. libnfs 1.16 does
        // export `nfs_lutimes` (µs precision, the symlink-itself
        // counterpart to `nfs_utimes`). If the row has no mtime,
        // record `NullMtime` consistent with the regular-file path.
        // If `lutimes` itself errors, log + downgrade rather than fail
        // the row — the symlink is already committed.
        if self.cfg.policy.preserve_times {
            match (row.mtime_sec, row.mtime_nsec) {
                (Some(mt_s), mt_n_opt) => {
                    let mt_n = mt_n_opt.unwrap_or(0);
                    let (at_s, at_n) = match (row.atime_sec, row.atime_nsec) {
                        (Some(a_s), a_n_opt) => (a_s, a_n_opt.unwrap_or(0)),
                        _ => {
                            self.downgrades
                                .record(row.row_id, &row.path, DowngradeKind::NullAtime);
                            (mt_s, mt_n)
                        }
                    };
                    if let Err(e) = ops::lutimes(pair.dst(), &dst, at_s, at_n, mt_s, mt_n) {
                        tracing::warn!(
                            row_id = row.row_id,
                            error = %e.error,
                            "lutimes on symlink failed; recording SymlinkTimeNfsV3 downgrade",
                        );
                        self.downgrades.record(
                            row.row_id,
                            &row.path,
                            DowngradeKind::SymlinkTimeNfsV3,
                        );
                    }
                }
                (None, _) => {
                    self.downgrades
                        .record(row.row_id, &row.path, DowngradeKind::NullMtime);
                }
            }
        }

        Ok(())
    }

    /// Hardlink — link an already-copied final path to a new path
    /// within the same shard. Both `target` and `linkpath` are
    /// `row.path`-style (relative to the export root); the mover
    /// composes the absolute dest paths via [`Self::dst_path`].
    ///
    /// Link is the commit point (R8), and replay is idempotent (F10):
    /// under at-least-once delivery a worker can die post-link-pre-ack
    /// and the row comes back. When `nfs_link` reports `EEXIST`, both
    /// sides are stat'd on the dest context and
    /// [`resolve_hardlink_eexist`] treats a fileid match as our own
    /// committed work (`Ok`), a mismatch as a real conflict (fail,
    /// naming both fileids), and a stat failure as the original
    /// `EEXIST` failure. Unlink-then-create was rejected as the
    /// recovery strategy: it destroys pre-existing data at the
    /// linkpath and opens a crash window with the link missing.
    fn do_hardlink(
        &self,
        pair: &mut ContextPair,
        target: &[u8],
        linkpath: &[u8],
    ) -> Result<(), MoveError> {
        let target_abs = join_root(self.cfg.dest_root.as_bytes(), target);
        let linkpath_abs = join_root(self.cfg.dest_root.as_bytes(), linkpath);

        if let Err(e) = self.ensure_parent_dir(pair.dst(), &linkpath_abs) {
            return Err(MoveError::new(FailurePhase::Hardlink, e.error));
        }
        // R8: link IS the commit point for hardlink rows — there is no
        // .partial + rename pattern, the linkpath is the final dest.
        // Fence-check immediately before issuing it.
        self.check_fence()?;
        if let Err(e) = ops::link(pair.dst(), &target_abs, &linkpath_abs) {
            if !is_eexist(&e) {
                return Err(e);
            }
            // EEXIST recovery — stat both sides on the dest context
            // and let `resolve_hardlink_eexist` decide replay vs
            // conflict. Runs strictly after `ops::link` returned, so
            // the R8 fence check above still guards the commit point.
            let target_stat = ops::stat_fileid(pair.dst(), &target_abs);
            let linkpath_stat = ops::stat_fileid(pair.dst(), &linkpath_abs);
            return resolve_hardlink_eexist(e, target_stat, linkpath_stat);
        }
        Ok(())
    }

    /// Empty file (`size == 0`) — no read/write loop, just CREATE +
    /// attrs + rename. R7: keeps the data-loop entirely off the
    /// zero-byte path.
    fn do_empty(&self, pair: &mut ContextPair, row: &RowView) -> Result<(), MoveError> {
        let src = self.src_path(row);
        let dst = self.dst_path(row);
        let dst_partial = partial_path(&dst, &self.host_id, self.pid)?;
        self.check_self_target(&src, &dst, &dst_partial)?;

        self.ensure_parent_dir(pair.dst(), &dst)?;

        let fh = ops::create_write(pair.dst(), &dst_partial, 0o600)?;
        ops::close_fh(pair.dst(), fh, FailurePhase::Write)?;

        self.apply_attrs(pair.dst(), &dst_partial, row, None)?;
        // R8: see do_libnfs_copy — fence check immediately before rename.
        self.check_fence()?;
        tracing::debug!(
            dest = %String::from_utf8_lossy(&dst),
            host = %self.host_id,
            pid = self.pid,
            row_id = row.row_id,
            "commit: rename .partial → final",
        );
        ops::rename(pair.dst(), &dst_partial, &dst)?;
        Ok(())
    }

    /// Default path: libnfs READ → libnfs WRITE through a 1 MiB
    /// streaming buffer, single-fiber within the call. Concurrency
    /// across files comes from the shard processor's JoinSet.
    /// Returns the bytes actually written (F41) — on an EarlyEof
    /// short copy this is less than `row.size` and the row still
    /// commits `Ok` with a `DowngradeKind::EarlyEof` record.
    ///
    /// Torn-copy detection is async-path-only for now: this sync path
    /// has no pre/post source-stat bracket, so a file modified during
    /// the copy commits here with no `DowngradeKind::TornCopy` record
    /// and `MoveOutcome::torn` stays `false`. The bucketed async path
    /// (`pipelined_copy` + `file_mover::classify_copy`) is the one
    /// that detects and records tears; see
    /// docs/work-items/MOVER_TORN_COPY_SURFACE.md (F05).
    /// Raw-FH copy path. Same commit contract as `do_libnfs_copy`
    /// (write `.partial`, durable before publish, attrs before rename,
    /// R8 fence check immediately before RENAME) with the RPC budget
    /// collapsed: one amortized READDIRPLUS per source directory, then
    /// READs + CREATE(attrs) + WRITEs + SETATTR(times) + RENAME for
    /// each cache hit. Single-chunk files write FILE_SYNC and skip
    /// COMMIT; multi-chunk files write UNSTABLE then COMMIT.
    ///
    /// With `direct_commit` the `.partial` + RENAME publish is
    /// skipped: CREATE targets the final name and the R8 fence check
    /// moves to just before CREATE (the new publish point). See the
    /// `MoverConfig::direct_commit` doc for the safety argument.
    fn do_raw_copy(&self, pair: &mut ContextPair, row: &RowView) -> Result<u64, MoveError> {
        const CHUNK: u64 = 1 << 20; // 1 MiB per READ/WRITE

        let src = self.src_path(row);
        let dst = self.dst_path(row);
        let dst_partial = partial_path(&dst, &self.host_id, self.pid)?;
        self.check_self_target(&src, &dst, &dst_partial)?;

        let (src_parent, src_name) =
            split_parent_name(&src).ok_or_else(|| MoveError::new(FailurePhase::Open, "EINVAL"))?;
        let (dst_parent, dst_name) =
            split_parent_name(&dst).ok_or_else(|| MoveError::new(FailurePhase::Open, "EINVAL"))?;
        let (_, partial_name) = split_parent_name(&dst_partial)
            .ok_or_else(|| MoveError::new(FailurePhase::Open, "EINVAL"))?;

        let src_dir = self
            .resolve_dir_fh(pair.src(), &self.src_dir_fhs, src_parent, false)
            .map_err(|e| raw_move_err(e, FailurePhase::Open))?;
        let dst_dir = self
            .resolve_dir_fh(pair.dst(), &self.dst_dir_fhs, dst_parent, true)
            .map_err(|e| raw_move_err(e, FailurePhase::Write))?;

        let (mut src_fh, mut src_fh_prefetched) = self
            .resolve_source_child_fh(pair.src(), src_parent, &src_dir, src_name)
            .map_err(|e| raw_move_err(e, FailurePhase::Open))?;

        // Same null-attribute downgrades the path-based flow records.
        let policy = self.cfg.policy;
        if policy.preserve_owner && (row.uid.is_none() || row.gid.is_none()) {
            self.downgrades
                .record(row.row_id, &row.path, DowngradeKind::NullOwner);
        }
        if policy.preserve_times && row.mtime_sec.is_none() {
            self.downgrades
                .record(row.row_id, &row.path, DowngradeKind::NullMtime);
        }
        if policy.preserve_times && row.mtime_sec.is_some() && row.atime_sec.is_none() {
            self.downgrades
                .record(row.row_id, &row.path, DowngradeKind::NullAtime);
        }

        // mode/uid/gid stamped atomically at CREATE — no chown/chmod
        // ops, and no kill-priv ordering concern (nothing is changed
        // after the mode is set). size=0 truncates a stale `.partial`.
        let a = attrs::build(row, policy);
        let create_attrs = RawSattr {
            mode: a.mode.map(|m| m & 0o7777),
            uid: a.uid,
            gid: a.gid,
            size: Some(0),
            atime: None,
            mtime: None,
        };
        // Direct-commit mode publishes at CREATE (there is no
        // `.partial` + RENAME step), so the R8 fence check moves to
        // this publish point instead of the pre-rename check below.
        let direct = self.cfg.direct_commit;
        let commit_name = if direct { dst_name } else { partial_name };
        if direct {
            self.check_fence()?;
        }
        let dst_fh = raw::create(pair.dst(), &dst_dir, commit_name, &create_attrs)
            .map_err(|e| raw_move_err(e, FailurePhase::Open))?;

        let single_chunk = row.size <= CHUNK;
        let mut off: u64 = 0;
        loop {
            // Size the request to the row hint (min 4 KiB, capped at
            // CHUNK). A flat 1 MiB request forced a zeroed 1 MiB
            // buffer allocation per file — above glibc's mmap
            // threshold, so every tiny file paid mmap/munmap and the
            // process-wide mmap_sem serialized 500 blocking threads
            // (measured: 87% of wall time in futex). If the file is
            // larger than the hint, the loop simply issues more reads.
            let want = (row.size.saturating_sub(off)).clamp(4096, CHUNK) as u32;
            let (data, eof) = match raw::read(pair.src(), &src_fh, off, want) {
                Ok(read) => read,
                Err(error) if src_fh_prefetched && error.tag == "ESTALE" => {
                    // A child may have been replaced between READDIRPLUS
                    // and READ. Disable the whole directory map, resolve
                    // the current name with LOOKUP, and restart if any
                    // bytes from the stale object were already written.
                    self.src_dir_children.disable(src_parent);
                    src_fh = Arc::new(
                        raw::lookup(pair.src(), &src_dir, src_name)
                            .map_err(|e| raw_move_err(e, FailurePhase::Open))?,
                    );
                    src_fh_prefetched = false;
                    if off > 0 {
                        raw::setattr(
                            pair.dst(),
                            &dst_fh,
                            &RawSattr {
                                size: Some(0),
                                ..Default::default()
                            },
                        )
                        .map_err(|e| raw_move_err(e, FailurePhase::Write))?;
                        off = 0;
                    }
                    continue;
                }
                Err(error) => return Err(raw_move_err(error, FailurePhase::Read)),
            };
            if !data.is_empty() {
                let mut sent = 0usize;
                while sent < data.len() {
                    let n = raw::write(
                        pair.dst(),
                        &dst_fh,
                        off + sent as u64,
                        &data[sent..],
                        single_chunk,
                    )
                    .map_err(|e| raw_move_err(e, FailurePhase::Write))?;
                    if n == 0 {
                        return Err(MoveError::new(FailurePhase::Write, "EIO"));
                    }
                    sent += n as usize;
                }
                off += data.len() as u64;
            }
            if eof || data.is_empty() {
                break;
            }
        }
        let written = off;

        if !single_chunk {
            // F09: durable before publish. FILE_SYNC writes already
            // are; UNSTABLE streams need the whole-file COMMIT.
            raw::commit(pair.dst(), &dst_fh).map_err(|e| {
                let mut me = raw_move_err(e, FailurePhase::Write);
                me.error = format!("COMMIT:{}", me.error);
                me
            })?;
        }

        if self.cfg.require_unchanged_size && written != row.size {
            return Err(MoveError::new(FailurePhase::Open, "SIZE_CHANGED"));
        }
        if written < row.size {
            self.downgrades
                .record(row.row_id, &row.path, DowngradeKind::EarlyEof);
        }

        // Times last (WRITE bumped mtime), one SETATTR for both.
        if let Some((msec, mnsec)) = a.mtime {
            let (asec, ansec) = a.atime.unwrap_or((msec, mnsec));
            raw::setattr(
                pair.dst(),
                &dst_fh,
                &RawSattr {
                    atime: Some((asec, ansec.max(0) as u32)),
                    mtime: Some((msec, mnsec.max(0) as u32)),
                    ..Default::default()
                },
            )
            .map_err(|e| raw_move_err(e, FailurePhase::Setattr))?;
        }

        if !direct {
            // R8: fence check immediately before the commit-point rename.
            self.check_fence()?;
            tracing::debug!(
                dest = %String::from_utf8_lossy(&dst),
                host = %self.host_id,
                pid = self.pid,
                row_id = row.row_id,
                "commit: rename .partial → final (raw-fh)",
            );
            raw::rename(pair.dst(), &dst_dir, partial_name, dst_name)
                .map_err(|e| raw_move_err(e, FailurePhase::Rename))?;
        }
        Ok(written)
    }

    fn do_libnfs_copy(&self, pair: &mut ContextPair, row: &RowView) -> Result<u64, MoveError> {
        if self.cfg.use_raw_fh {
            return self.do_raw_copy(pair, row);
        }
        let src = self.src_path(row);
        let dst = self.dst_path(row);
        let dst_partial = partial_path(&dst, &self.host_id, self.pid)?;
        self.check_self_target(&src, &dst, &dst_partial)?;

        self.ensure_parent_dir(pair.dst(), &dst)?;

        let src_fh = ops::open_read(pair.src(), &src)?;
        let dst_fh = match ops::create_write(pair.dst(), &dst_partial, 0o600) {
            Ok(fh) => fh,
            Err(e) => {
                ops::close_quietly(pair.src(), src_fh);
                return Err(e);
            }
        };

        let result = stream_copy(pair, &src_fh, &dst_fh, row.row_id, row.size);

        // F09: whole-file NFS COMMIT before the write fh closes and
        // before the rename below — the streaming loop's WRITEs are
        // UNSTABLE (see DESIGN.md "Mover behavior"), and the rename
        // must never publish bytes the server hasn't acknowledged as
        // stable. Mirrors the async path's `dst.fsync` in
        // `pipelined_copy`. Skipped when the copy already failed —
        // the row fails anyway and nothing gets renamed. A COMMIT
        // failure fails the row through the normal MoveError path
        // (phase Write, error tag `COMMIT:<errno>`); the closes below
        // still run unconditionally for fh hygiene.
        let commit = match &result {
            Ok(_) => ops::fsync(pair.dst(), &dst_fh),
            Err(_) => Ok(()),
        };

        // Apply chown/chmod through the still-open write fh (saves two
        // full-path LOOKUP walks per file); skipped if the copy or
        // COMMIT already failed — the row fails anyway below. utimes
        // runs path-based inside the same plan.
        let attrs = match (&result, &commit) {
            (Ok(_), Ok(())) => self.apply_attrs(pair.dst(), &dst_partial, row, Some(&dst_fh)),
            _ => Ok(()),
        };

        let close_src = ops::close_fh(pair.src(), src_fh, FailurePhase::Read);
        let close_dst = ops::close_fh(pair.dst(), dst_fh, FailurePhase::Write);

        let written = result?;
        commit?;
        attrs?;
        close_src?;
        close_dst?;

        if self.cfg.require_unchanged_size && written != row.size {
            return Err(MoveError::new(FailurePhase::Open, "SIZE_CHANGED"));
        }

        // Per SCHEMA_CONTRACT.md "Size semantics" / decision #11, a
        // short read is *not* a failure on the default path — the
        // file is committed. But surface the discrepancy so the
        // operator sees that actual bytes copied differ from the
        // indexed size. (If we'd had this in place during the M2 FFI
        // verification incident, every non-empty regular file would
        // have produced an EARLY_EOF record; see M2_NOTES.md
        // "M2/M3 verification incidents".)
        if written < row.size {
            self.downgrades
                .record(row.row_id, &row.path, DowngradeKind::EarlyEof);
        }

        // Attributes were applied above through the open fh, before close.
        // R8: last-ditch fence check immediately before the commit-point
        // rename. The shard processor only checks between rows; without
        // this guard, every row already inside spawn_blocking at fence
        // trip time still commits.
        self.check_fence()?;
        tracing::debug!(
            dest = %String::from_utf8_lossy(&dst),
            host = %self.host_id,
            pid = self.pid,
            row_id = row.row_id,
            "commit: rename .partial → final",
        );
        ops::rename(pair.dst(), &dst_partial, &dst)?;
        Ok(written)
    }

    // =========================================================================
    // Helpers.
    // =========================================================================

    /// Apply uid/gid + mode + atime/mtime on the still-`.partial`
    /// destination, in the order planned by
    /// [`attr_plan::plan_attr_ops`]: chown → chmod → utimes (F08 —
    /// owner before mode so NFSv3 kill-priv semantics can't strip
    /// S_ISUID/S_ISGID the chmod just applied; utimes strictly last).
    /// Honors `cfg.policy` and `cfg.require_chown` (chown EPERM in
    /// degraded mode records `NullOwner` and continues to chmod).
    /// Records downgrades for null source attrs the user asked to
    /// preserve, per SCHEMA_CONTRACT.md "Null attribute semantics".
    fn apply_attrs(
        &self,
        ctx: &mut NfsContext,
        dst_partial: &[u8],
        row: &RowView,
        fh: Option<&ops::NfsFh>,
    ) -> Result<(), MoveError> {
        let policy = self.cfg.policy;

        if policy.preserve_owner && (row.uid.is_none() || row.gid.is_none()) {
            self.downgrades
                .record(row.row_id, &row.path, DowngradeKind::NullOwner);
        }
        if policy.preserve_times && row.mtime_sec.is_none() {
            self.downgrades
                .record(row.row_id, &row.path, DowngradeKind::NullMtime);
        }
        if policy.preserve_times && row.mtime_sec.is_some() && row.atime_sec.is_none() {
            self.downgrades
                .record(row.row_id, &row.path, DowngradeKind::NullAtime);
        }

        let plan = attr_plan::plan_attr_ops(row, policy);
        let mut exec = SyncAttrExec {
            ctx,
            dst_partial,
            row,
            fh,
            downgrades: &self.downgrades,
            require_chown: self.cfg.require_chown,
        };
        attr_plan::execute_plan(&plan, &mut exec)
    }
}

/// [`AttrExec`] over the sync libnfs context — each op maps to the
/// pre-existing `ops::` call. The chown-EPERM degraded-mode policy
/// lives here unchanged (record `NullOwner`, report `SkippedDegraded`
/// so the plan continues); only its position in the sequence moved.
struct SyncAttrExec<'a> {
    ctx: &'a mut NfsContext,
    dst_partial: &'a [u8],
    row: &'a RowView,
    /// When the caller still holds the write fh, chown/chmod go
    /// through it (`nfs_fchown`/`nfs_fchmod`) instead of the path
    /// variants — every path-based libnfs op re-walks the full path
    /// with one LOOKUP per component (measured: LOOKUPs were 88% of
    /// all RPCs on a depth-8 tree). utimes has no fh variant in this
    /// libnfs FFI surface and stays path-based.
    fh: Option<&'a ops::NfsFh>,
    downgrades: &'a DowngradeSink,
    require_chown: bool,
}

impl AttrExec for SyncAttrExec<'_> {
    type Err = MoveError;

    fn chown(&mut self, uid: u32, gid: u32) -> Result<ChownOutcome, MoveError> {
        let result = match self.fh {
            Some(fh) => ops::fchown(self.ctx, fh, uid, gid),
            None => ops::chown(self.ctx, self.dst_partial, uid, gid),
        };
        match result {
            Ok(()) => Ok(ChownOutcome::Applied),
            Err(e) if e.error == "EPERM" && !self.require_chown => {
                tracing::debug!(uid, gid, "chown EPERM in degraded mode; skipping");
                self.downgrades
                    .record(self.row.row_id, &self.row.path, DowngradeKind::NullOwner);
                Ok(ChownOutcome::SkippedDegraded)
            }
            Err(e) => Err(e),
        }
    }

    fn chmod(&mut self, mode: u32) -> Result<(), MoveError> {
        match self.fh {
            Some(fh) => ops::fchmod(self.ctx, fh, mode),
            None => ops::chmod(self.ctx, self.dst_partial, mode),
        }
    }

    fn utimes(&mut self, atime: (i64, i32), mtime: (i64, i32)) -> Result<(), MoveError> {
        ops::utimes(
            self.ctx,
            self.dst_partial,
            atime.0,
            atime.1,
            mtime.0,
            mtime.1,
        )
    }
}

/// Convert a raw-op error into a `MoveError`, logging the transport
/// detail at debug (the tag alone feeds failure records).
fn raw_move_err(e: raw::RawError, phase: FailurePhase) -> MoveError {
    tracing::debug!(detail = %e.detail, "raw nfs op failed");
    MoveError::new(phase, e.tag)
}

/// Split an absolute byte path into (parent, basename). Returns None
/// for the root or a path without a slash.
fn split_parent_name(p: &[u8]) -> Option<(&[u8], &[u8])> {
    let i = p.iter().rposition(|&b| b == b'/')?;
    let name = &p[i + 1..];
    if name.is_empty() {
        return None;
    }
    Some((if i == 0 { b"/" } else { &p[..i] }, name))
}

/// Return the parent directory portion of an absolute byte path,
/// keeping the trailing slash so two parents compare equal even when
/// only one originally had a slash. Empty input maps to empty.
fn parent_dir(p: &[u8]) -> &[u8] {
    match p.iter().rposition(|&b| b == b'/') {
        Some(0) => b"/",
        Some(i) => &p[..i],
        None => &[],
    }
}

/// Free-function form of the per-file self-target check, factored out
/// of `Mover` so it's unit-testable without spinning up a libnfs pool.
/// See `Mover::check_self_target` for behavior; this is the body.
pub(crate) fn check_self_target(
    source_url: &str,
    dest_url: &str,
    src: &[u8],
    dst: &[u8],
    dst_partial: &[u8],
) -> Result<(), MoveError> {
    if source_url != dest_url {
        return Ok(());
    }
    if src == dst {
        return Err(MoveError::new(FailurePhase::Open, "SELF_TARGET"));
    }
    if parent_dir(src) == parent_dir(dst_partial) {
        return Err(MoveError::new(FailurePhase::Open, "SELF_TARGET"));
    }
    Ok(())
}

/// The READ→WRITE loop. Sync; runs inside `spawn_blocking`. Returns
/// the total bytes written. EOF before `size` is *not* an error in
/// the default mode — `size` is advisory per SCHEMA_CONTRACT.md —
/// but is surfaced as a tracing warning so the operator can spot a
/// short copy without grep'ing for downgrade records. The caller
/// (`do_libnfs_copy`) writes the corresponding `EARLY_EOF` downgrade
/// record after this returns.
///
/// `row_id` is threaded through purely for the warning's structured
/// fields.
fn stream_copy(
    pair: &mut ContextPair,
    src_fh: &ops::NfsFh,
    dst_fh: &ops::NfsFh,
    row_id: u64,
    size: u64,
) -> Result<u64, MoveError> {
    let (src_ctx, dst_ctx) = pair.split();
    stream_copy_inner(
        size,
        |off, buf| ops::pread(src_ctx, src_fh, off, buf),
        |off, buf| ops::pwrite(dst_ctx, dst_fh, off, buf),
        |off, remaining| {
            tracing::warn!(
                row_id,
                indexed_size = size,
                actual_size = off,
                short = remaining,
                "pread returned 0 with remaining bytes; treating as EOF \
                 (contract: size is advisory)",
            );
        },
    )
}

/// Loop body of `stream_copy`, factored out so it can be exercised
/// against in-memory closures (no libnfs context, no real fhs).
/// Production calls it from `stream_copy` with closures that hit the
/// libnfs FFI; the unit tests below call it with closures that drive
/// pre-canned read returns to reproduce the FFI-bug failure mode
/// (silent zero-byte reads).
fn stream_copy_inner<R, W, S>(
    size: u64,
    mut read_at: R,
    mut write_at: W,
    mut on_short_eof: S,
) -> Result<u64, MoveError>
where
    R: FnMut(u64, &mut [u8]) -> Result<usize, MoveError>,
    W: FnMut(u64, &[u8]) -> Result<usize, MoveError>,
    S: FnMut(u64, u64),
{
    if size == 0 {
        return Ok(0);
    }

    let mut buf = vec![0u8; STREAM_BUF_SIZE];
    let mut off = 0u64;
    let mut remaining = size;

    while remaining > 0 {
        let want = remaining.min(STREAM_BUF_SIZE as u64) as usize;
        let n = read_at(off, &mut buf[..want])?;
        if n == 0 {
            on_short_eof(off, remaining);
            break;
        }
        let mut written_in_chunk = 0;
        while written_in_chunk < n {
            let w = write_at(off + written_in_chunk as u64, &buf[written_in_chunk..n])?;
            if w == 0 {
                return Err(MoveError::new(FailurePhase::Write, "EIO"));
            }
            written_in_chunk += w;
        }
        off += n as u64;
        remaining -= n as u64;
    }
    Ok(off)
}

/// True iff a link-layer error is `EEXIST`. Same errno-name
/// convention as the rest of the safe-wrapper layer (`MoveError.error`
/// carries the errno name from `libnfs::errno_name` — see
/// `ops::mkdir`'s EEXIST handling). `do_hardlink` and `do_symlink`
/// enter EEXIST recovery only behind this guard; every other
/// link/symlink error passes through unchanged.
fn is_eexist(err: &MoveError) -> bool {
    err.error == "EEXIST"
}

/// Decide the outcome of a hardlink EEXIST: `Ok(())` iff the existing
/// linkpath already IS the target (same fileid). `target_stat` /
/// `linkpath_stat` are the results of statting the target and the
/// linkpath on the destination context during recovery.
fn resolve_hardlink_eexist(
    link_err: MoveError,
    target_stat: Result<u64, MoveError>,
    linkpath_stat: Result<u64, MoveError>,
) -> Result<(), MoveError> {
    match (target_stat, linkpath_stat) {
        // Same fileid: the linkpath already IS the target — our own
        // committed link replayed after a died-post-link-pre-ack
        // worker. Idempotent success.
        (Ok(target_id), Ok(linkpath_id)) if target_id == linkpath_id => Ok(()),
        // Different fileid: a real conflict — some other file
        // occupies the linkpath. Name both fileids so the operator
        // can tell conflict from replay in the failure log.
        (Ok(target_id), Ok(linkpath_id)) => Err(MoveError::new(
            FailurePhase::Hardlink,
            format!(
                "EEXIST: linkpath fileid {linkpath_id} != target fileid \
                 {target_id} (conflict, not a replay)"
            ),
        )),
        // Either stat failed: recovery must never mask the primary
        // failure — surface the ORIGINAL EEXIST link error.
        _ => Err(link_err),
    }
}

/// Decide the outcome of a symlink EEXIST: `Ok(())` iff the existing
/// dst entry is a symlink whose target byte-equals `intended`.
/// `dst_readlink` is the result of readlink-ing the destination path
/// on the destination context during recovery.
fn resolve_symlink_eexist(
    link_err: MoveError,
    intended: &[u8],
    dst_readlink: Result<Vec<u8>, MoveError>,
) -> Result<(), MoveError> {
    match dst_readlink {
        // Matching target bytes: the dst symlink already points at
        // the intended target — our own committed symlink replayed
        // after a died-post-symlink-pre-ack worker. Idempotent
        // success.
        Ok(existing) if existing == intended => Ok(()),
        // Different target: a real conflict — some other symlink
        // occupies the destination. Name both targets
        // (lossy-rendered) so the operator can tell conflict from
        // replay in the failure log.
        Ok(existing) => Err(MoveError::new(
            FailurePhase::Symlink,
            format!(
                "EEXIST: dst symlink target \"{}\" != intended target \
                 \"{}\" (conflict, not a replay)",
                String::from_utf8_lossy(&existing),
                String::from_utf8_lossy(intended),
            ),
        )),
        // readlink failed (including EINVAL: the existing entry is
        // not a symlink at all): recovery must never mask the primary
        // failure — surface the ORIGINAL EEXIST symlink error.
        Err(_) => Err(link_err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- parent_dir ----------------------------------------------

    #[test]
    fn parent_dir_root_level_file() {
        assert_eq!(parent_dir(b"/foo.txt"), b"/");
    }

    #[test]
    fn parent_dir_nested() {
        assert_eq!(parent_dir(b"/a/b/c"), b"/a/b");
    }

    #[test]
    fn parent_dir_no_slash() {
        assert_eq!(parent_dir(b"foo"), b"");
    }

    // ---- self-target check ---------------------------------------
    //
    // Belt-and-suspenders against the startup overlap guard. These
    // tests are the regression test for the data-loss bug
    // recovered in M2 verification.

    #[test]
    fn self_target_check_blocks_same_path() {
        let url = "nfs://host/exp";
        let r = check_self_target(url, url, b"/foo/bar", b"/foo/bar", b"/foo/.bar.h.1.partial");
        let e = r.expect_err("identical src and dst must fail SELF_TARGET");
        assert_eq!(e.error, "SELF_TARGET");
        assert_eq!(e.phase, FailurePhase::Open);
    }

    #[test]
    fn self_target_check_blocks_same_parent_dir() {
        // dst path differs but its .partial parent equals src parent —
        // create-with-O_TRUNC would still trash the source file.
        let url = "nfs://host/exp";
        let r = check_self_target(url, url, b"/foo/bar", b"/foo/baz", b"/foo/.bar.h.1.partial");
        assert_eq!(r.unwrap_err().error, "SELF_TARGET");
    }

    #[test]
    fn self_target_check_allows_different_url() {
        // Different servers — paths can collide all they want.
        let r = check_self_target(
            "nfs://srcA/exp",
            "nfs://srcB/exp",
            b"/foo/bar",
            b"/foo/bar",
            b"/foo/.bar.h.1.partial",
        );
        assert!(r.is_ok());
    }

    #[test]
    fn self_target_check_allows_disjoint_dirs() {
        let url = "nfs://host/exp";
        let r = check_self_target(
            url,
            url,
            b"/src/file",
            b"/dst/file",
            b"/dst/.file.h.1.partial",
        );
        assert!(r.is_ok());
    }

    // ---- stream_copy_inner: short-read behavior ------------------
    //
    // Regression coverage for the M2 libnfs FFI bug. The buggy FFI
    // signature caused pread to return 0 on every call against a
    // real export, but the stream_copy loop used to swallow that
    // silently and report success. The contract still says "size is
    // advisory" so the loop must NOT fail — it must return Ok(off)
    // with off == bytes-actually-read, leaving the caller to record
    // the EARLY_EOF downgrade. These tests pin that behavior so the
    // surface can't regress.

    #[test]
    fn stream_copy_short_read_returns_ok_with_partial_off() {
        // Mock pread that always returns 0 — the exact failure mode
        // of the M2 libnfs FFI bug. write should never be called.
        let size: u64 = 4096;
        let mut on_short_called = false;
        let result = stream_copy_inner(
            size,
            |_off, _buf| Ok(0usize),
            |_off, _buf| -> Result<usize, MoveError> {
                panic!("write_at must not be called when read returns 0");
            },
            |off, remaining| {
                on_short_called = true;
                assert_eq!(off, 0);
                assert_eq!(remaining, size);
            },
        );

        let off = result.expect("short read is not an error in default mode");
        assert_eq!(off, 0, "off must equal bytes-actually-read");
        assert!(off < size, "off ({off}) must be < indexed size ({size})");
        assert!(
            on_short_called,
            "on_short_eof must fire so caller can record EARLY_EOF"
        );
    }

    #[test]
    fn stream_copy_short_read_after_partial_progress() {
        // Variant: pread returns one full chunk then 0. off should
        // equal the chunk that did land; the loop still returns Ok.
        let size: u64 = (STREAM_BUF_SIZE as u64) * 4;
        let mut reads = 0;
        let mut writes = 0;
        let result = stream_copy_inner(
            size,
            |_off, buf| {
                reads += 1;
                if reads == 1 {
                    Ok(buf.len()) // first chunk: full read
                } else {
                    Ok(0) // then EOF
                }
            },
            |_off, buf| {
                writes += 1;
                Ok(buf.len())
            },
            |_off, _remaining| {},
        );

        let off = result.expect("short read after progress is not an error");
        assert_eq!(off as usize, STREAM_BUF_SIZE);
        assert!(off < size);
        assert_eq!(reads, 2, "expected one full read then one short read");
        assert_eq!(writes, 1);
    }

    #[test]
    fn stream_copy_size_zero_is_no_op() {
        let result = stream_copy_inner(
            0,
            |_, _| -> Result<usize, MoveError> {
                panic!("read_at must not be called for size 0");
            },
            |_, _| -> Result<usize, MoveError> {
                panic!("write_at must not be called for size 0");
            },
            |_, _| panic!("on_short_eof must not fire for size 0"),
        );
        assert_eq!(result.unwrap(), 0);
    }

    // ---- R8 fence check ------------------------------------------
    //
    // Building a Mover requires a `LibnfsContextPool` to plug into the
    // pre-commit acquire path. The fence check itself doesn't touch
    // the pool — it just reads the atomic flag — so the tests below
    // use a stub pool that never hands out a pair. The strategy
    // bodies all funnel through `check_fence()` immediately before
    // their commit-point op; confirming `check_fence()` honors the
    // flag is sufficient to prove the fence guard fires for each
    // strategy (verified by code inspection at the patch sites).

    use crate::libnfs::{ContextPair, LibnfsContextPool};
    use async_trait::async_trait;

    struct StubPool;
    #[async_trait]
    impl LibnfsContextPool for StubPool {
        async fn acquire(&self) -> anyhow::Result<ContextPair> {
            // The fence-check tests construct the Mover but never
            // actually call into a strategy body — driving libnfs from
            // a unit test would require a real NFS context. We only
            // need the Mover type to exist; acquire is never invoked.
            anyhow::bail!("StubPool::acquire is not implemented for fence tests")
        }
    }

    fn build_mover_with_fence(fence: Fence) -> Mover {
        build_mover(Arc::new(StubPool) as Arc<dyn LibnfsContextPool>, fence)
    }

    fn build_mover(pool: Arc<dyn LibnfsContextPool>, fence: Fence) -> Mover {
        let cfg = MoverConfig {
            source_url: "nfs://srcA/exp".to_string(),
            dest_url: "nfs://srcB/exp".to_string(),
            source_root: "/".to_string(),
            dest_root: "/".to_string(),
            policy: AttrPolicy {
                preserve_mode: true,
                preserve_owner: true,
                preserve_times: true,
                preserve_xattr: false,
            },
            inflight: InflightProfile::default(),
            require_chown: false,
            require_unchanged_size: false,
            use_raw_fh: false,
            direct_commit: false,
            rpc_timeout_ms: crate::libnfs::DEFAULT_RPC_TIMEOUT_MS,
        };
        Mover::new(
            cfg,
            pool,
            "test-host",
            crate::downgrade::DowngradeSink::new(),
            fence,
        )
    }

    /// F12: `MoverConfig::from_options` seeds the explicit per-RPC
    /// timeout default (60_000 ms); the orchestrator overrides it
    /// from `[mover] rpc_timeout_ms` before mounting any pool.
    #[test]
    fn from_options_defaults_rpc_timeout_to_60000() {
        let cfg = MoverConfig::from_options(
            "nfs://src/exp".into(),
            "nfs://dst/exp".into(),
            "/".into(),
            "/".into(),
            &MigrationOptions::default(),
        );
        assert_eq!(cfg.rpc_timeout_ms, crate::libnfs::DEFAULT_RPC_TIMEOUT_MS);
    }

    /// Both raw-path opt-ins are off unless the orchestrator flips
    /// them from `[mover]` config: `use_raw_fh` gates the raw path,
    /// `direct_commit` additionally drops the `.partial` + RENAME
    /// publish (and with it atomic publish — must never be implicit).
    #[test]
    fn from_options_defaults_raw_path_opt_ins_off() {
        let cfg = MoverConfig::from_options(
            "nfs://src/exp".into(),
            "nfs://dst/exp".into(),
            "/".into(),
            "/".into(),
            &MigrationOptions::default(),
        );
        assert!(!cfg.use_raw_fh);
        assert!(!cfg.direct_commit);
    }

    fn child_entry(name: &[u8], fh: &[u8]) -> DirChildrenEntry {
        DirChildrenEntry::Ready(Arc::new(std::collections::HashMap::from([(
            name.to_vec(),
            Arc::new(fh.to_vec()),
        )])))
    }

    fn child_entries(count: usize) -> DirChildrenEntry {
        DirChildrenEntry::Ready(Arc::new(
            (0..count)
                .map(|i| (format!("file-{i}").into_bytes(), Arc::new(vec![i as u8])))
                .collect(),
        ))
    }

    #[test]
    fn dir_children_distinguishes_uncached_hit_and_fallback_miss() {
        let cache = DirChildren::default();
        assert!(matches!(
            cache.lookup(b"/src/d", b"file"),
            DirChildLookup::Uncached
        ));

        cache.insert(b"/src/d".to_vec(), child_entry(b"file", b"prefetched-fh"));
        match cache.lookup(b"/src/d", b"file") {
            DirChildLookup::Hit(fh) => assert_eq!(fh.as_slice(), b"prefetched-fh"),
            _ => panic!("prefetched child must be a cache hit"),
        }
        assert!(matches!(
            cache.lookup(b"/src/d", b"server-omitted-handle"),
            DirChildLookup::Miss
        ));

        cache.disable(b"/src/d");
        assert!(matches!(
            cache.lookup(b"/src/d", b"file"),
            DirChildLookup::Miss
        ));
    }

    #[test]
    fn dir_children_second_chance_is_bounded_and_retains_hits() {
        let cache = DirChildren::with_limits(3, usize::MAX);
        for i in 0..3 {
            cache.insert(
                format!("/dir-{i}").into_bytes(),
                child_entry(b"file", &[i as u8]),
            );
        }

        // Reference the oldest entry, then insert one more. It gets a
        // second chance and the next unreferenced entry is evicted.
        assert!(matches!(
            cache.lookup(b"/dir-0", b"file"),
            DirChildLookup::Hit(_)
        ));
        cache.insert(b"/dir-3".to_vec(), child_entry(b"file", b"new"));

        assert!(matches!(
            cache.lookup(b"/dir-1", b"file"),
            DirChildLookup::Uncached
        ));
        assert!(matches!(
            cache.lookup(b"/dir-0", b"file"),
            DirChildLookup::Hit(_)
        ));
        assert_eq!(cache.state.lock().unwrap().entries.len(), 3);
    }

    #[test]
    fn dir_children_cache_is_bounded_by_total_child_filehandles() {
        let cache = DirChildren::with_limits(10, 3);
        cache.insert(b"/dir-a".to_vec(), child_entries(2));
        cache.insert(b"/dir-b".to_vec(), child_entries(2));

        assert!(matches!(
            cache.lookup(b"/dir-a", b"file-0"),
            DirChildLookup::Uncached
        ));
        assert!(matches!(
            cache.lookup(b"/dir-b", b"file-0"),
            DirChildLookup::Hit(_)
        ));
        let state = cache.state.lock().unwrap();
        assert_eq!(state.entries.len(), 1);
        assert_eq!(state.cached_child_fhs, 2);
    }

    #[test]
    fn dir_children_eviction_is_sticky_so_thrashing_dirs_stop_prefetching() {
        let cache = DirChildren::with_limits(10, 3);
        cache.insert(b"/dir-a".to_vec(), child_entries(2));
        assert!(!cache.was_evicted(b"/dir-a"));

        // /dir-b's insert pushes /dir-a out; a working set larger than
        // the cache must not re-prefetch /dir-a for every row.
        cache.insert(b"/dir-b".to_vec(), child_entries(2));
        assert!(matches!(
            cache.lookup(b"/dir-a", b"file-0"),
            DirChildLookup::Uncached
        ));
        assert!(cache.was_evicted(b"/dir-a"));
        assert!(!cache.was_evicted(b"/dir-b"));
    }

    #[test]
    fn dir_children_default_retains_parent_interleaved_working_set() {
        let cache = DirChildren::default();
        for i in 0..128 {
            cache.insert(
                format!("/dir-{i}").into_bytes(),
                child_entry(b"file", &[i as u8]),
            );
        }

        for i in 0..128 {
            assert!(matches!(
                cache.lookup(format!("/dir-{i}").as_bytes(), b"file"),
                DirChildLookup::Hit(_)
            ));
        }
        let state = cache.state.lock().unwrap();
        assert_eq!(state.entries.len(), 128);
        assert_eq!(state.lru.len(), 128);
        assert_eq!(state.cached_child_fhs, 128);
    }

    #[test]
    fn dir_children_single_flight_guard_is_shared_then_released() {
        let cache = DirChildren::default();
        let first = cache.flight(b"/src/d");
        let second = cache.flight(b"/src/d");
        assert!(Arc::ptr_eq(&first, &second));

        // The first caller to finish must retain the shared flight while
        // another waiter still owns it.
        cache.finish_flight(b"/src/d", &first);
        let still_shared = cache.flight(b"/src/d");
        assert!(Arc::ptr_eq(&first, &still_shared));
        drop(second);
        drop(still_shared);

        cache.finish_flight(b"/src/d", &first);
        let next = cache.flight(b"/src/d");
        assert!(!Arc::ptr_eq(&first, &next));

        // A waiter that still owns the old Arc must not remove the new
        // flight from the map after the directory is evicted/restarted.
        cache.finish_flight(b"/src/d", &first);
        let same_next = cache.flight(b"/src/d");
        assert!(Arc::ptr_eq(&next, &same_next));
    }

    // ---- F41: honest byte counts ----------------------------------
    //
    // `MoveOutcome::bytes_moved` must report the bytes actually
    // written, never `row.size` taken on faith. Two sync-path `Ok`
    // outcomes used to inflate it: `Strategy::Skip` (copies nothing)
    // and an EarlyEof short copy (commits `written < row.size`).
    // See docs/work-items/WORKER_RESILIENCE.md item 2.

    use migration_core::schema::FileTypeTag;
    use migration_core::shard::RowView;

    /// Pool that hands out unmounted pairs — valid for strategy arms
    /// and stubbed bodies that never touch the contexts.
    struct DummyPairPool;
    #[async_trait]
    impl LibnfsContextPool for DummyPairPool {
        async fn acquire(&self) -> anyhow::Result<ContextPair> {
            Ok(ContextPair::unmounted_for_tests())
        }
    }

    fn test_row(size: u64, file_type: FileTypeTag) -> RowView {
        RowView {
            row_id: 7,
            path: b"/data/file".to_vec(),
            size,
            mtime_sec: None,
            mtime_nsec: None,
            atime_sec: None,
            atime_nsec: None,
            mode: 0o644,
            uid: None,
            gid: None,
            nlink: None,
            inode: None,
            fsid: None,
            xattr_blob: None,
            symlink_target: None,
            file_type,
        }
    }

    /// F41 acceptance test 5 (red before fix): a Skip row (fifo /
    /// socket / dev) copies nothing and must report 0 bytes while
    /// still counting as a success. Before the fix it reported
    /// `row.size` — inflating throughput, backpressure inputs, and
    /// coord aggregation.
    #[tokio::test]
    async fn skip_reports_zero_bytes() {
        let mover = build_mover(
            Arc::new(DummyPairPool) as Arc<dyn LibnfsContextPool>,
            Fence::new(),
        );
        let row = test_row(4096, FileTypeTag::Fifo);
        let outcome = mover.move_one(&row).await;
        assert_eq!(outcome.strategy, Strategy::Skip);
        assert!(
            outcome.result.is_ok(),
            "Skip must stay a success: {:?}",
            outcome.result,
        );
        assert_eq!(
            outcome.bytes_moved, 0,
            "Skip copies nothing and must report 0 bytes, not row.size",
        );
    }

    /// F41 acceptance test 6 (red before fix — a type-level red: the
    /// sync copy bodies returned `()`, so a stubbed body could not
    /// even express a written count). `do_libnfs_copy` is FFI-coupled,
    /// so this drives `run_with_pair`'s outcome assembly with a
    /// stubbed body that commits fewer bytes than `row.size` — the
    /// EarlyEof shape (`stream_copy` hit EOF early; the row still
    /// commits `Ok`, with the downgrade recorded by the real body).
    /// The outcome must report the actual written count.
    #[tokio::test]
    async fn early_eof_reports_written_bytes() {
        let mover = build_mover(
            Arc::new(DummyPairPool) as Arc<dyn LibnfsContextPool>,
            Fence::new(),
        );
        let row = test_row(4096, FileTypeTag::Regular);
        let outcome = mover
            .run_with_pair(&row, Strategy::LibnfsIoUring, |_, _| Ok(500))
            .await;
        assert!(outcome.result.is_ok(), "EarlyEof stays a committed success");
        assert_eq!(
            outcome.bytes_moved, 500,
            "outcome must report the bytes actually written, not row.size",
        );
    }

    /// Regression guard (hardware-free analog of file_mover_smoke's
    /// `bytes_moved == size` assertion): a full clean copy still
    /// reports the full size.
    #[tokio::test]
    async fn full_copy_reports_full_size() {
        let mover = build_mover(
            Arc::new(DummyPairPool) as Arc<dyn LibnfsContextPool>,
            Fence::new(),
        );
        let row = test_row(4096, FileTypeTag::Regular);
        let outcome = mover
            .run_with_pair(&row, Strategy::LibnfsIoUring, |_, _| Ok(4096))
            .await;
        assert!(outcome.result.is_ok());
        assert_eq!(outcome.bytes_moved, 4096);
    }

    /// Failed rows keep reporting 0 bytes (pre-F41 behavior pin).
    #[tokio::test]
    async fn failed_copy_reports_zero_bytes() {
        let mover = build_mover(
            Arc::new(DummyPairPool) as Arc<dyn LibnfsContextPool>,
            Fence::new(),
        );
        let row = test_row(4096, FileTypeTag::Regular);
        let outcome = mover
            .run_with_pair(&row, Strategy::LibnfsIoUring, |_, _| {
                Err(MoveError::new(FailurePhase::Write, "EIO"))
            })
            .await;
        assert!(outcome.result.is_err());
        assert_eq!(outcome.bytes_moved, 0);
    }

    /// Pins the F41 plumbing by type: the sync copy body returns the
    /// actual written count (`u64`), not `()`. Never called — the
    /// body is FFI-coupled; the count itself comes from `stream_copy`,
    /// whose short-read behavior is pinned by the tests above.
    #[allow(dead_code)]
    fn _pin_do_libnfs_copy_returns_written(
        m: &Mover,
        p: &mut ContextPair,
        r: &RowView,
    ) -> Result<u64, MoveError> {
        m.do_libnfs_copy(p, r)
    }

    #[test]
    fn check_fence_passes_when_fence_valid() {
        let fence = Fence::new();
        let mover = build_mover_with_fence(fence);
        assert!(mover.check_fence().is_ok());
    }

    #[test]
    fn check_fence_returns_fenced_when_fence_tripped() {
        let fence = Fence::new();
        fence.trip("test trip");
        let mover = build_mover_with_fence(fence);
        let err = mover
            .check_fence()
            .expect_err("tripped fence must short-circuit commit");
        assert_eq!(err.phase, FailurePhase::Fenced);
        assert_eq!(err.error, "FENCE_TRIPPED");
    }

    #[test]
    fn fence_clones_share_state_with_mover() {
        // The Mover's fence is held by-value (Fence is Clone, Arc
        // internally). Tripping the original handle after building
        // the Mover must still cause check_fence() to return Fenced.
        // This pins the "Arc-backed atomic flag" contract the mover
        // relies on per docs/CLAIM_PROTOCOL.md "Self-fencing".
        let fence = Fence::new();
        let mover = build_mover_with_fence(fence.clone());
        assert!(mover.check_fence().is_ok());
        fence.trip("late trip after Mover constructed");
        let err = mover.check_fence().expect_err("late trip must propagate");
        assert_eq!(err.phase, FailurePhase::Fenced);
    }

    // ---- hardlink EEXIST recovery (F10) ---------------------------
    //
    // At-least-once replay of a committed hardlink row: the worker
    // died post-link-pre-ack, the row is redelivered, and nfs_link
    // reports EEXIST. Same fileid on both sides means the linkpath
    // already IS the target — our own committed work — and the row
    // must resolve Ok instead of landing in the failure sink. See
    // docs/work-items/HARDLINK_REPLAY_IDEMPOTENCY.md.

    fn eexist_link_err() -> MoveError {
        MoveError::new(FailurePhase::Hardlink, "EEXIST")
    }

    #[test]
    fn eexist_same_fileid_is_success() {
        let r = resolve_hardlink_eexist(eexist_link_err(), Ok(42), Ok(42));
        assert!(
            r.is_ok(),
            "same fileid = replay of committed work, must be Ok: {r:?}"
        );
    }

    #[test]
    fn eexist_different_fileid_stays_failure() {
        let e = resolve_hardlink_eexist(eexist_link_err(), Ok(111), Ok(222))
            .expect_err("different fileids are a real conflict, not a replay");
        assert_eq!(e.phase, FailurePhase::Hardlink);
        assert!(
            e.error.contains("111") && e.error.contains("222"),
            "conflict message must name both fileids so the operator \
             can tell conflict from replay, got: {}",
            e.error
        );
    }

    #[test]
    fn eexist_stat_failure_preserves_original_error() {
        // Recovery must never mask the primary failure: whichever
        // stat fails, the returned error is the ORIGINAL EEXIST link
        // error, not the stat error.
        let stat_err = || MoveError::new(FailurePhase::Hardlink, "EACCES");
        let cases: [(Result<u64, MoveError>, Result<u64, MoveError>); 3] = [
            (Err(stat_err()), Ok(42)),
            (Ok(42), Err(stat_err())),
            (Err(stat_err()), Err(stat_err())),
        ];
        for (target_stat, linkpath_stat) in cases {
            let e = resolve_hardlink_eexist(eexist_link_err(), target_stat, linkpath_stat)
                .expect_err("stat failure during recovery must stay a failure");
            assert_eq!(e.phase, FailurePhase::Hardlink);
            assert_eq!(
                e.error, "EEXIST",
                "must return the ORIGINAL link error, not the stat error"
            );
        }
    }

    #[test]
    fn non_eexist_errors_pass_through() {
        // Wiring-level guarantee: `do_hardlink` enters the recovery
        // arm only behind `is_eexist` (an early `return Err(e)`
        // otherwise), so `resolve_hardlink_eexist` is unreachable for
        // any other link error. Pin the guard's classification here.
        assert!(is_eexist(&MoveError::new(FailurePhase::Hardlink, "EEXIST")));
        for name in ["ENOSPC", "EACCES", "EIO", "ENOENT", "errno=999"] {
            assert!(
                !is_eexist(&MoveError::new(FailurePhase::Hardlink, name)),
                "{name} must pass through, not enter EEXIST recovery"
            );
        }
    }

    // ---- symlink EEXIST recovery (F10, symlink half) ---------------
    //
    // At-least-once replay of a committed symlink row: the worker
    // died post-symlink-pre-ack, the row is redelivered, and
    // nfs_symlink reports EEXIST. A dst symlink whose target
    // byte-equals the intended target IS our own committed work — the
    // row must resolve Ok instead of landing in the failure sink. See
    // docs/work-items/SYMLINK_REPLAY_IDEMPOTENCY.md.

    fn eexist_symlink_err() -> MoveError {
        MoveError::new(FailurePhase::Symlink, "EEXIST")
    }

    #[test]
    fn symlink_eexist_matching_target_is_success() {
        // Byte-compare, not string-compare: the second target is not
        // valid UTF-8 and must still match.
        let targets: [&[u8]; 2] = [b"/t/plain", b"/t/\xff\xfe"];
        for intended in targets {
            let r = resolve_symlink_eexist(eexist_symlink_err(), intended, Ok(intended.to_vec()));
            assert!(
                r.is_ok(),
                "matching target = replay of committed work, must be Ok: {r:?}"
            );
        }
    }

    #[test]
    fn symlink_eexist_different_target_stays_failure() {
        let e = resolve_symlink_eexist(
            eexist_symlink_err(),
            b"/t/intended",
            Ok(b"/t/existing".to_vec()),
        )
        .expect_err("different targets are a real conflict, not a replay");
        assert_eq!(e.phase, FailurePhase::Symlink);
        assert!(
            e.error.contains("/t/intended") && e.error.contains("/t/existing"),
            "conflict message must name both targets (lossy-rendered) so \
             the operator can tell conflict from replay, got: {}",
            e.error
        );
    }

    #[test]
    fn symlink_eexist_readlink_failure_preserves_original_error() {
        // Recovery must never mask the primary failure. EINVAL is the
        // entry-is-not-a-symlink shape (readlink on a non-symlink
        // entry); the returned error must be the ORIGINAL EEXIST
        // symlink error, not the readlink error.
        let readlink_err = MoveError::new(FailurePhase::Symlink, "EINVAL");
        let e = resolve_symlink_eexist(eexist_symlink_err(), b"/t/intended", Err(readlink_err))
            .expect_err("readlink failure during recovery must stay a failure");
        assert_eq!(e.phase, FailurePhase::Symlink);
        assert_eq!(
            e.error, "EEXIST",
            "must return the ORIGINAL symlink error, not the readlink error"
        );
    }

    #[test]
    fn symlink_non_eexist_errors_pass_through() {
        // Wiring-level guarantee: `do_symlink` enters the recovery
        // arm only behind the shared `is_eexist` guard (an early
        // `return Err(e)` otherwise), so `resolve_symlink_eexist` is
        // unreachable for any other symlink error. The guard
        // classifies by errno name alone; pin that it holds for
        // Symlink-phase errors exactly as for Hardlink-phase ones.
        assert!(is_eexist(&MoveError::new(FailurePhase::Symlink, "EEXIST")));
        for name in ["ENOSPC", "EACCES", "EIO", "ENOENT", "EINVAL", "errno=999"] {
            assert!(
                !is_eexist(&MoveError::new(FailurePhase::Symlink, name)),
                "{name} must pass through, not enter EEXIST recovery"
            );
        }
    }
}
