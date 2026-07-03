# Fix the EOF-clamp livelock and bound the reorder buffer

Status: in review — fix + tests on branch `mover-eof-reorder-bounds`; hardware `pipelined_copy_smoke` pending before verified.
Ledger: F06, F07 in `docs/REVIEW_LEDGER.md`.
Priority: high — a live (shrinking) source file can wedge a worker.
Scope: `migration-mover/src/pipelined_copy.rs` (+ a small pure module).

## Problem

`pipelined_copy` issues reads at depth, delivers them in order via
`reorder_buf: BTreeMap<u64, Vec<u8>>`, and clamps `effective_size`
when a read returns 0 (EOF) — NFSv3 permits out-of-order completion,
and sources can shrink mid-copy.

**F06 (livelock):** if EOF is observed at offset X while a completed
read for an offset > X already sits in `reorder_buf`, the stale entry
is never drained (delivery can't reach its key) but the exit
condition requires `reorder_buf.is_empty()`. `FuturesUnordered::next`
on an empty set returns `Ready(None)` immediately, so the loop
busy-spins forever — never yielding, never completing, permanently
holding its inflight-limiter permit and a core. Enough occurrences
drain a size class's permits and stall the worker while its heartbeat
keeps it looking healthy.

**F07 (unbounded memory):** the read pump gates only on in-flight
count (`reads_inflight.len() < read_depth`). Completed reads move
into `reorder_buf`; if the read at `next_deliver_off` stalls (one
slow RPC), the pump keeps issuing and buffering — worst case the
whole remaining file in RAM (multi-100-GiB large-bucket files) →
OOM-kill.

## Required reading

- `crates/migration-mover/src/pipelined_copy.rs` — the whole read
  pump / reorder / deliver loop, and the EOF-clamp comments
- `docs/work-items/MULTI_PASS_MOVER.md` (the pipelined-copy design
  sketch it shipped from)
- `docs/CORRECTNESS_RULES.md` (dropping RPC futures does not cancel
  the RPCs — relevant to how the fix drains)

## Acceptance tests — write these FIRST

The loop is FFI-coupled, so step 1 of the fix is extracting the
reorder/EOF/bounds state machine into a pure struct (no behavior
change), then testing it exhaustively. Suggested shape:
`ReorderState { next_deliver, effective_size, buffered_bytes, buf }`
with `on_completion(offset, wanted, bytes) -> Vec<Chunk>`,
`on_eof_clamp(offset)`, `may_issue(read_depth, max_buffered) -> bool`,
`drained() -> bool`.

1. `in_order_delivery_roundtrip` — completions arriving in order
   deliver immediately, `drained()` when done. (Green before fix —
   pins the extraction.)
2. `out_of_order_completions_reorder` — completions 2,0,1 deliver
   0,1,2. (Green before fix.)
3. `eof_clamp_purges_stale_entries` (red before fix) — completion
   for offset 8MiB buffered; EOF clamp at 4MiB; assert the stale
   entry is dropped, `drained()` becomes true, and no delivery beyond
   4MiB happens.
4. `eof_clamp_with_inflight_reads_past_eof` (red before fix) — reads
   in flight beyond the clamp complete after the clamp; their
   completions must be discarded, not buffered forever.
5. `buffered_bytes_bounded` (red before fix) — stall delivery at
   offset 0 (its read never completes) and feed completions for
   later offsets; assert `may_issue` goes false once
   `buffered_bytes` reaches the cap, and flips back after the stall
   clears and chunks deliver.
6. `livelock_regression_loop_exits` — drive the extracted state
   machine through the exact F06 interleaving inside a
   `tokio::time::timeout`-wrapped async test of the real loop if the
   extraction allows injecting a completion source; if the real loop
   can't be driven hermetically, this test lives at the state-machine
   level and the loop's exit condition must be
   `state.drained()` — assert by code inspection note in the PR.

## Fix shape

- Extract first, commit the extraction as its own no-behavior-change
  step (tests 1–2 pin it).
- `on_eof_clamp(offset)`: set `effective_size = min(current,
  offset)`, purge `buf` keys `>= effective_size`, subtract their
  bytes from `buffered_bytes`.
- Discard (don't buffer) any completion with `offset >=
  effective_size`.
- Bound: `max_buffered_bytes` — default `read_depth ×
  max_chunk_size × 2` (config-derived, not a new user knob unless
  one already exists nearby); the pump consults `may_issue`.
- The real loop's exit condition switches to the state machine's
  `drained()`; ensure the empty-`FuturesUnordered` hot-spin is
  structurally impossible (when nothing is in flight and not
  drained, that's now a bug → return an error rather than spin).
- Torn detection interplay: a shrink mid-copy will also flip `torn`
  (pre/post stat) — fine; this item is about not wedging.

## Out of scope / do NOT

- No I/O deadlines (F12, separate FFI design).
- No write-path changes.
- Do not reduce `read_depth` semantics or reorder guarantees —
  delivery must remain strictly in-order for the write cursor.

## Definition of done

- [ ] Extraction landed with tests 1–2 green before any behavior
      change; tests 3–5 observed red then green.
- [ ] Full gate green; hardware smoke (`pipelined_copy_smoke`,
      `#[ignore]`d) re-run on the lab rig before `verified`.
- [ ] Ledger F06/F07 updated; this doc's Status flipped.
