# Mongoose resync — single-host converging delta passes

Status: implemented (2026-08-31) — `mongoose sync` with `--cutover`,
`crates/migration-resync` classifier, delta emitter, baseline
advance, pass pruning. Deviations from this design are noted inline
as **[implemented]** markers; rig validation is outstanding.
Companion to
`MULTI_PASS_MOVER.md` (the distributed pass driver, phases 3–8 of
which were never started); this work item is the single-host
version, scoped to `mongoose`, and deliberately built so the
classifier and delta-emitter can be lifted into the fleet pass
driver later.

---

## Goal

`mongoose sync`: rescan the source, detect what changed since the
last pass, copy only that, repeat until the delta is small, then run
one final pass with source writes stopped (cutover). Each pass costs
roughly **one metadata walk plus the churn**, not a full recopy.

```
pass 0   mongoose run     bulk copy (exists today)
pass 1   mongoose sync    rescan + delta copy     (source live)
pass N   mongoose sync    …deltas shrink…
final    stop writers; mongoose sync              (cutover: zero new dirt)
```

---

## Decisions inherited from MULTI_PASS_MOVER.md (not relitigated)

- **Dirty = `(size, mtime, ctime)` tuple mismatch.** Whole-file
  recopy on dirty; no block-level diff, no rsync-style rolling
  hashes. ctime is the backstop: content writes, chmod/chown,
  link-count changes, and even `utimes()` games all bump it, and
  users cannot set it.
- **No deletion from the destination in v1.** DELETED rows are
  recorded; `--prune-deleted` is a follow-up flag, never a default.
- **Torn copies converge, not fail.** A row copied while being
  written commits anyway (source intact, at-least-once) and is
  re-copied next pass; the cutover pass (writes stopped) is the
  final settle.
- **NFSv3 only; walker rescan is the change detector.** NFSv3 has
  no change notification. Directory-mtime pruning is unsound (a
  file rewrite does not touch its parent dir), so every pass is a
  full metadata walk — acceptable because the walker is the fastest
  component in the system (~690K entries/s bench; the 600M-file
  tree rescans in roughly an hour against real hardware).

## Where this design diverges (single-host simplifications)

`MULTI_PASS_MOVER.md` builds a per-pass *destination-state manifest*
merged from per-host partial manifests, because a fleet has no other
single place to record what happened. Mongoose is one process, and —
decisively — **the canonical shards already carry the full change
tuple**: `mtime_sec/nsec` as canonical columns and
`ctime_sec/ctime_nsec` via the rewrite's legacy passthrough.

So the baseline for pass N is simply **pass N−1's scan** (in
canonical form), overlaid with a small **pending set** (rows that
failed or tore in earlier passes). No new per-file result sink, no
partial-manifest merging, no new manifest schema. The copy loop is
untouched.

Convergence argument for scan-vs-scan: a file modified at any point
after its pass-N−1 scan row was captured — before, during, or after
its pass-N−1 copy — has a different `(size, mtime, ctime)` at the
pass-N scan and classifies DIRTY. A redundant recopy is possible
(modified before the copy started, copy picked up the new bytes);
a missed change is not. Failures don't reappear in the diff (the
source didn't change), which is exactly what the pending overlay is
for: `pending(N) = (pending(N−1) ∪ failures(N−1) ∪ torn(N−1))
− succeeded(N−1)`, forced DIRTY every pass until they succeed.

---

## Pass lifecycle (`mongoose sync`)

1. **Rescan** — embedded walker into
   `passes/pass-NNNN/scan/attempt-MMMM/`, same invocation as
   prepare.
2. **Classify** — diff new scan against the baseline canonical
   index:
   | New scan | Baseline | Tuple | Classification |
   |---|---|---|---|
   | yes | no  | —        | NEW |
   | yes | yes | match    | UNCHANGED (unless in pending → DIRTY) |
   | yes | yes | mismatch | DIRTY |
   | no  | yes | —        | DELETED (recorded, not propagated) |
   Checkpointed to `classify.json` (counts + partition digests).
3. **Emit delta scan** — write NEW+DIRTY rows as *walker-schema*
   parquet parts into `passes/pass-NNNN/delta-scan/`. This is a
   filtered copy of the new scan's parts (same schema, row mask),
   which means step 4 is zero new code.
4. **Rewrite** — run the existing `mig_walker_rewrite` library over
   the delta scan → canonical delta shards + report, exactly as
   prepare does. Fresh `row_id`s per pass are fine: row ids are
   scoped to the shard set they index.
5. **Copy** — the existing copy loop (ShardProcessor, sinks,
   progress, resume-at-shard) pointed at the pass's delta manifest.
   Nothing in `copy.rs` knows it is a delta.
6. **Advance the baseline** — atomically record pass NNNN's *full*
   scan (canonical form) as the new baseline, write the new pending
   set from this pass's failures + torn downgrades, and record
   DELETED rows to `passes/pass-NNNN/deleted.jsonl`. Baseline
   advance is the pass commit point; an interrupted pass re-runs
   against the old baseline (idempotent — worst case redundant
   recopies).

Baseline in canonical form: the full new scan must be rewritten, not
just the delta. Since the rewrite is 1:1 per part and resumable,
rewrite the full scan once (it is the same work prepare does) and
filter for the delta from the canonical parts; i.e. step 3/4 can be
ordered rewrite-then-filter instead of filter-then-rewrite —
implementer's choice, but rewrite-then-filter yields the baseline
for free and only ever runs the translator on data it needs anyway.

**[implemented]** rewrite-then-filter, as above. The delta filter
preserves each source shard's schema, KV footer (`shard_index`
intact, `row_count` updated) and original `row_id`s, so
`ShardReader`'s validation holds on delta shards. The full pass
index is the pass dir's `manifest.json`; the delta is
`delta-manifest.json`, consumed by the unchanged copy loop via a
manifest-name parameter. Cutover is implemented as the
zero-drift-or-fail gate (drift = keep_rows + deleted) with
`--cutover-allow-drift` to absorb instead; FILE_SYNC opens and hash
re-verify remain future work as designed.

### Classifier mechanics

Both inputs are parquet without a global path sort (walker emission
is DFS-ish and sharded by path-hash). Rather than an external global
sort, use a **hash-partitioned merge-join**: stream both sides once,
routing rows to B buckets by path-hash (B ≈ 256); then per bucket,
load the baseline side into a `HashMap<path-bytes, tuple>` and stream
the new-scan side against it (leftovers in the map = DELETED).
Memory is bounded at ~|baseline|/B per bucket (~250 MB at 600M rows,
B=256); I/O is ~2× both scans, local and sequential. Projection:
only `(path, size, mtime, ctime, file_type)` leaves are read.

The classifier lives in a new lib crate (`crates/migration-resync`)
with no mongoose dependency, so the fleet pass driver
(`MULTI_PASS_MOVER.md` phase 3+) can consume it unchanged — same
extraction pattern as `prepare_tools` and `mover_factory`.

### Cutover

`mongoose sync --cutover` after stopping source writers:

- Classifier must produce **zero** NEW/DIRTY/DELETED beyond the
  pending set; any drift fails the pass loudly
  (`--cutover-allow-drift` to override), same invariant as the
  fleet design.
- v1 cutover is that invariant plus a normal (possibly empty) delta
  copy. FILE_SYNC-stability opens and hash re-verify
  (`--cutover-verify`) remain future work tied to the bucketed
  async path, which is the only mover that computes `file_hash`.

---

## Work-dir layout

```
<work-dir>/
  manifest.json                  pass-0 index (exists today)
  canonical/                     pass-0 shards (exists today)
  baseline.json                  which pass's canonical set is baseline,
                                 + counts; atomic advance = pass commit
  pending.jsonl                  unresolved rows (failures ∪ torn), carried
  passes/pass-0002/
    scan/attempt-0001/           full rescan (walker output + progress log)
    canonical/part-*.parquet     full rescan, canonical (next baseline)
    delta-manifest.json          NEW+DIRTY shard subset for the copy loop
    classify.json                NEW/DIRTY/UNCHANGED/DELETED counts
    deleted.jsonl                recorded deletions (not propagated)
    progress.json  failures/  downgrades/
```

Space: each retained pass costs one index (~65 B/entry scan +
~same canonical). Prune pass N−2 after baseline advances to N;
steady-state overhead ≈ 2 indexes (~80 GB at 600M files).

---

## Correctness caveats (documented, not silently absorbed)

- **Tuple granularity.** A change invisible to `(size, mtime,
  ctime)` — requires the server to report identical ctime across a
  modification, e.g. sub-granularity double-write — is missed by
  scan-diff and converged by nothing except `--cutover-verify`
  (future). Same posture as the fleet design.
- **Renames copy, not move.** A rename classifies as DELETED (old
  path, recorded only) + NEW (new path, recopied). Inode-based
  rename detection is a possible later optimization, not v1.
- **chmod/chown storms recopy.** ctime-only changes (touchless
  chmod -R) classify DIRTY and recopy whole files, per the settled
  whole-file rule. If real workloads hit this, a
  `DirtyReason::AttrsOnly` fast path (setattr, no data) is the
  first optimization to add — the mover's attr machinery already
  exists.
- **Hardlink fidelity stays pass-scoped.** A new link to a file
  copied in an earlier pass arrives alone in its delta shard and is
  copied as an independent file (nlink>1 grouping only sees rows in
  the same pass). Consistent with the existing shard-scoped
  limitation.
- **Dir mtimes.** Delta passes restamp only dirs that appear in the
  delta; a dir whose mtime changed classifies DIRTY via ctime/mtime
  and gets restamped. Root mtime restore runs per pass as today.
- **Non-UTF-8 paths** remain bounded by the walker's Utf8 path
  column (pre-existing; fix belongs in the walker).

---

## CLI

```
mongoose sync --work-dir /var/lib/mongoose/run-001 [copy tuning flags]
              [--cutover] [--cutover-allow-drift]
              [--keep-passes N]       # prune old pass dirs (default 2)
```

Requires a completed pass 0 (`manifest.json`). Copy tuning flags are
the existing `CopyTuning` set. `sync` is itself resumable at every
stage boundary (scan checkpoint, rewrite report, classify
checkpoint, copy progress), same discipline as prepare/copy.

---

## Implementation plan

1. `crates/migration-resync`: partitioned classifier over canonical
   parquet + pending overlay. Unit tests: new/dirty/unchanged/
   deleted matrix, empty baseline (= all NEW), identical inputs
   (= all UNCHANGED), pending forces DIRTY on tuple match, bounded
   memory on a synthetic 1M-row corpus, path-prefix boundary cases.
2. Delta emitter: canonical-shard row filter + delta manifest
   builder (reuses `LocalManifest`).
3. `mongoose sync` driver: scan → rewrite → classify → delta copy →
   baseline advance; pass pruning; `--cutover` drift invariant.
4. Tests: pass lifecycle over fixture parquet (no NFS), baseline
   advance atomicity, pending set carry/clear, deleted recording.
5. Rig validation: bulk + touch-some-files + sync; sync-under-load;
   freeze + cutover showing zero-dirty.

Non-goals for v1: block-level diff, deletion propagation, rename
detection, attrs-only fast path, hash verify, fleet pass driver
(all listed with owners above or in `MULTI_PASS_MOVER.md`).
