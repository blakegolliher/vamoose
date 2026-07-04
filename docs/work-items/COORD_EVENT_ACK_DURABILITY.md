# Make event acks mean durable; close the SSE catch-up gap

Status: landed — merged in PR #17.
Ledger: F03, F18 in `docs/REVIEW_LEDGER.md`.
Priority: critical (F03) + major (F18) — same root cause.
Scope: `migration-coord` (`runtime.rs`, `events.rs`,
`server/worker.rs`, `server/command.rs`, `server/stream.rs`).

## Problem

**F03 — ack-before-durable.** `POST /workers/{id}/events`
(`server/worker.rs` ~241–257) returns assigned `seqs` as soon as
events are appended to the in-memory `EventLogWriter` buffer; flush
to S3 happens at 1000 events per chunk or 5 minutes
(`events.rs`). The worker treats returned seqs as delivered and drops
them from its bounded resend buffer. A coord crash therefore loses
up to ~5 minutes of **acked** events. Operator commands are worse:
`pause`/`cancel` return 200 with an `Accepted` audit row while the
`JobPaused` event lives only in RAM (`server/command.rs`) — after a
crash the job is silently un-paused. And replay restarts `next_seq`
below values already handed out, so both the coord-side stream filter
and the TUI (`seq <= last_seen_seq` drop rule) ignore all events
until the counter passes the stale watermark — frozen dashboards.

**F18 — catch-up gap.** SSE catch-up (`server/stream.rs` ~117–134)
reads only flushed chunks, then switches to the live broadcast. A
client connecting with `Last-Event-ID = N` while events sit unflushed
in the writer buffer misses everything between the last flushed chunk
and subscribe time — no `Resync` is emitted. The module doc's "gaps
are impossible by construction" claim is false, and the crate's own
tests mask this by calling `rt.flush_log()` before connecting
(`stream.rs` tests ~369, ~398).

## Required reading

- `crates/migration-coord/src/events.rs` (writer buffer, flush
  triggers, `read_chunk`/`list_chunks`)
- `crates/migration-coord/src/runtime.rs::ingest`
- `crates/migration-coord/src/server/{worker,command,stream}.rs`
- `docs/COORD_PLAN.md` §3.4 (resync contract), Phase 2/3 acceptance
- The existing sse_e2e and worker-endpoint integration tests.

## Acceptance tests — write these FIRST

1. `worker_events_ack_implies_durable` (red before fix) — POST an
   events batch via the worker endpoint against a `MemStore`-backed
   app; when the response returns, assert the events are readable
   from store chunks (not just in RAM). Then simulate crash-restart:
   build a fresh runtime from the same store and assert replay
   contains every acked seq.
2. `command_ack_implies_durable` (red before fix) — `POST
   /jobs/{id}/pause` returns 200 → fresh runtime from the same store
   shows the job paused.
3. `seq_never_regresses_across_restart` (red before fix) — ingest,
   ack, crash, restart: the first seq assigned by the new runtime is
   strictly greater than every acked seq.
4. `sse_catchup_includes_unflushed_tail` (red before fix) — ingest
   events WITHOUT calling `flush_log()`, connect a stream client with
   `Last-Event-ID = 0`, assert it receives all ingested events with
   no gap and no duplicate seqs. Keep one of the existing pre-flush
   tests as-is to prove flushed + unflushed compose.
5. `sse_reconnect_mid_buffer_no_gap_no_dup` — ingest 10, flush,
   ingest 5 more (unflushed), reconnect with `Last-Event-ID = 12`;
   assert exactly 13..15 arrive.
6. Throughput sanity (non-blocking assertion, `#[ignore]`d bench is
   fine): batch of 1000 events acks in one flush write, not 1000 —
   whatever mechanism lands must amortize.

## Fix shape

Two coupled decisions; the doc deliberately fixes the contract and
leaves mechanism latitude:

- **Contract:** an HTTP 200 with seqs (worker events) or a 200 on a
  command means those events survive a coord crash. Mechanism
  options: (a) flush-before-ack — force `flush_log()` for the
  affected chunks before responding (simplest; batches already
  amortize; measure with test 6); (b) ack-at-flush-boundary — hold
  responses until the writer's next flush tick (harder, changes
  latency). Pick (a) unless it measurably can't meet the 10k
  events/s target; record the choice here.

  **Mechanism chosen: (a) flush-before-ack.** Every event-emitting
  request path (`/workers/{id}/events`, `/workers/register`,
  `/workers/{id}/fence`, and the command handlers' shared
  `record_and_ingest`) calls `CoordRuntime::flush_log()` after the
  ingests and before responding. Amortization measured (test 6): a
  1000-event batch costs 1 chunk PUT (threshold flush at 1000 +
  no-op final flush; asserted ≤ 2 writes), and the `#[ignore]`d
  bench sustains ~171k events/s end-to-end through the router on a
  MemStore (release build) — well past the 10k events/s target;
  against S3 the cost is one PUT per request, amortized across the
  batch. `flush_log` is lease-fenced (F02), so a deposed coord
  fails the request instead of acking —
  `fenced_coord_never_acks{,_commands}` pin the composition.
- **Catch-up:** serve the unflushed tail during catch-up — subscribe
  to the broadcast first, then read chunks, then drain the writer's
  in-memory tail (it needs a snapshot/read accessor), dedup by seq
  (dedup logic already exists for the flushed/live seam). With
  flush-before-ack, the unflushed tail shrinks to
  not-yet-acked events only — but it still exists; fix the read
  path, don't argue it away.
- Fix the `stream.rs` module-doc claim either way.
- `next_seq` recovery: with ack==durable, replay-derived
  `next_seq` is ≥ every acked seq by construction — add test 3 to
  pin it.

## Out of scope / do NOT

- F20 (endpoint trust/idempotency) — separate design item.
- Do not remove the existing broadcast/Lagged/Resync machinery.
- Do not change chunk key shape.

## Definition of done

- [x] Tests 1–5 written first; 1–4 observed red (1–3 failed on
      missing durability/seq regression, 4–5 timed out waiting for
      the unflushed tail).
- [x] All acceptance tests green; full gate green.
- [x] `stream.rs` docs match reality; mechanism choice recorded here.
- [ ] Ledger F03/F18 updated (coordinator); this doc's Status flipped.
