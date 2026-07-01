# Wire archive-on-completion; make event reads seq-aware

Status: open — not started.
Ledger: F23 in `docs/REVIEW_LEDGER.md`.
Priority: medium-high — unbounded event log; O(history) reads on
every coord start, TUI start, and SSE reconnect.
Scope: `migration-coord` (`archive.rs` call sites, `events.rs` read
paths, `runtime.rs`).

## Problem

`archive.rs::archive_job` is implemented and unit-tested but has
**zero production callers** — the COORD_PLAN Phase 1 acceptance
("`JobCompleted` rolls the event log to `archivelogs/`") is unmet.
Consequently `events/` grows forever, and every replay
(`state::replay`), SSE catch-up, and `/events` request lists and
reads the entire history. Additionally, `read_all_events_since` /
`read_job_events` ignore the zero-padded start-seq embedded in chunk
keys — they read chunks that cannot contain `since`.

## Required reading

- `crates/migration-coord/src/archive.rs` (+ its tests — the
  semantics are already pinned there)
- `crates/migration-coord/src/events.rs` read paths; the chunk-key
  format in `layout.rs` (zero-padded start seq — lexical == numeric)
- `crates/migration-coord/src/runtime.rs::ingest` (where
  `JobCompleted`/`JobCancelled` pass through)
- `docs/COORD_PLAN.md` Phase 1 acceptance + §3.4 (what archived jobs
  mean for the hot path)

## Acceptance tests — write these FIRST

1. `job_completed_triggers_archive` (red before fix) — ingest a
   job's lifecycle through `JobCompleted` + flush; assert the job's
   chunks moved under `archivelogs/` and are gone from `events/`
   (MemStore).
2. `job_cancelled_triggers_archive` (red before fix).
3. `archive_failure_does_not_lose_events` — make the mock store fail
   the archive copy step; assert chunks remain under `events/`
   (retry-able, logged) and ingest continues — archive must be
   best-effort async (a tick), never inline-blocking command acks.
4. `replay_after_archive_reconstructs_terminal_job` — coord restart
   after archive: the job appears in state (from snapshot) with
   terminal phase; live-path reads don't touch `archivelogs/`.
5. `read_all_events_since_skips_low_chunks` (red before fix) — with
   chunks starting at seqs 0/1000/2000, `since = 1500` must not GET
   the 0-chunk (assert via MemStore op log), and results are
   unchanged vs. the naive read.
6. `sse_reconnect_cost_bounded` — reconnect with a high
   `Last-Event-ID` against a long history lists chunk keys but GETs
   only the tail chunks (op-log assertion).

## Fix shape

- Archive trigger: on the snapshot/flush tick (not inline in
  `ingest`), scan state for jobs newly terminal since the last tick
  and `archive_job` them after their chunks are flushed. Tick-based
  keeps ingest latency flat and gives natural retry.
- Seq-aware reads: parse the start-seq from chunk keys during LIST
  and skip chunks whose *next* chunk's start ≤ `since`; keep one
  chunk of slack rather than clever boundary math.
- The paged REST endpoints (`server/read.rs`) currently read
  everything then truncate — bound them with the same skip.
- TUI note: the TUI's bootstrap-from-seq-0 (F26) becomes actively
  wrong for archived jobs once this lands — do not fix the TUI here,
  but update the F26 ledger row's note to record the dependency.

## Out of scope / do NOT

- No compaction/retention policy for `archivelogs/` (operator
  lifecycle rules can handle that; note it in layout docs).
- No TUI changes (F26).
- Do not archive inline in the command/ingest path.

## Definition of done

- [ ] Tests 1–2, 5 written first and observed red.
- [ ] All acceptance tests green; full gate green.
- [ ] COORD_PLAN Phase 1 acceptance note updated (delivered-by ref).
- [ ] Ledger F23 updated (and F26's note); this doc's Status flipped.
