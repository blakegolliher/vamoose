# Worker resilience: etag verify, honest byte counts, transient-S3 retry

Status: done — F40/F41/F42 implemented on branch worker-resilience
(one commit per item), acceptance tests first with observed reds;
full gate green.
Ledger: F40, F41, F42 in `docs/REVIEW_LEDGER.md`.
Priority: high (F40 integrity bypass, F42 fleet-wide outage vector),
medium (F41 counter inflation).
Scope: `migration-worker/src/orchestrator.rs`,
`migration-mover/src/mover.rs`, `migration-core/src/s3.rs` (error
typing only), `migration-core/src/claim.rs` test-util (injection
hooks).

Work the items in order F40 → F41 → F42; each is independently
committable. Write each item's tests before its fix.

## Item 1 — F40: empty etag must not bypass shard verification

### Problem

`verify_shard_etag` (`orchestrator.rs:975-997`, sole caller at :623
right after `download_to`) contains:

```rust
if expected.etag.is_empty() || actual_etag.is_empty() { return Ok(()); }
```

Comment says "Best-effort: some test fixtures don't fill etags" — but
this is production code: a manifest whose indexer emitted empty etags
(or a store returning an empty ETag header) silently skips the only
integrity check between the index and the bytes the worker processes.

### Acceptance tests — write these FIRST

1. `empty_manifest_etag_is_an_error` (red before fix) — manifest entry
   with `etag: ""` + non-empty download etag → `Err`, error text names
   the shard and says the manifest etag is missing (operator can tell
   this apart from a mismatch).
2. `empty_download_etag_is_an_error` (red before fix) — symmetric.
3. `matching_etags_pass` / `mismatched_etags_fail_with_manifest_changed`
   — pin the existing good paths (mismatch stays
   `Error::ManifestChanged`, which F13 classifies `WorkerLocal` —
   release-and-skip, correct for a manifest swap mid-run).
4. Fixture sweep: `two_live_workers.rs:56` (`manifest_with_shards`)
   sets `etag: String::new()` — fill fixture etags with real values
   (FakeStore returns deterministic etags; thread them through) so the
   harness keeps passing with the bypass gone. No production fixture
   may rely on the skip.

### Fix shape

Empty-on-either-side becomes a distinct error (reuse
`Error::ManifestChanged` with a sentinel is NOT acceptable — make the
message distinguishable; a plain `anyhow!` matching the existing
missing-shard style at :982 is fine). Classification: absent/empty
etag is a property of the manifest, not this worker — it will re-occur
for any worker, so let it flow to `Fatal` via the default
(`classify_shard_error` maps non-typed anyhow errors `Fatal`), and say
so in a comment at the return site.

## Item 2 — F41: report actual bytes, not `row.size`, on the sync path

### Problem

Sync `run_with_pair` (`mover.rs:223-267`) sets `let bytes = row.size`
(:228) and reports `bytes_moved: if result.is_ok() { bytes } else { 0 }`
(:259-266). Two `Ok` outcomes inflate:

- **EarlyEof**: `do_libnfs_copy` (:529-587) records the
  `DowngradeKind::EarlyEof` downgrade and deliberately returns `Ok`
  with `written < row.size` — the actual `written` count (from
  `stream_copy`, :546/:551) is discarded.
- **Skip**: `Strategy::Skip => Ok(())` (:287) copies nothing, reports
  `row.size`.

The async path is correct (`file_mover.rs:398-401` uses actual
`bytes_copied`) — but async delegates `Skip` back to the sync
`move_one` (:393-395), so async Skip inherits the inflation.

Inflated `bytes_moved` flows into `ProcessOutcome.bytes_moved`
(`shard_processor.rs:274`), `throughput.add` (:275) → the 60s MB/s
sample that gates backpressure (`orchestrator.rs:732-733`), progress
records (`orchestrator.rs:685`), heartbeat progress, and coord
aggregation (`migration-coord/src/runtime.rs:695`). A destination
slow enough to trip the throughput floor can be masked by a run of
skips/early-EOFs.

### Acceptance tests — write these FIRST

5. `skip_reports_zero_bytes` (red before fix) — drive `Mover::execute`
   with `Strategy::Skip`; outcome `bytes_moved == 0`, still counted a
   success.
6. `early_eof_reports_written_bytes` (red before fix) — hardware-free
   seam: `do_libnfs_copy` is FFI-coupled, so plumb the return value:
   change the sync copy body to return the written count and test the
   outcome assembly (`run_with_pair`'s accounting) with a stubbed body
   if the existing test seams allow; otherwise test at the
   `MoveOutcome` assembly level and pin the plumbing by type (the body
   returns `written: u64`, not `()`).
7. Regression: `file_mover_smoke.rs:210-212` asserts
   `bytes_moved == size` for a full clean copy — must keep passing.

### Fix shape

`do_libnfs_copy` (and the other sync copy bodies) return the actual
written count; `run_with_pair` reports it. `Skip` reports 0. Async
path untouched except it now inherits the correct sync `Skip`. Do NOT
change what counts as success/failure — EarlyEof stays a committed
success with a downgrade record (that contract is F05/F10 territory).

## Item 3 — F42: transient S3 errors must not take the worker down

### Problem

Six call sites in `run()`'s loop `?`-propagate a transient S3 error
straight to process exit — a sustained S3 blip kills every worker in
the fleet simultaneously:

| Site | Location |
|---|---|
| manifest read | `orchestrator.rs:110` (`load_manifest`, GET at :943-945) |
| scan LIST | `scan_shards` internal, :1081 |
| scan claim GET | `scan_shards` internal, :1128 |
| acquire | :536 (`claim::try_acquire`) |
| reclaim | :560 (`claim::reclaim`) |
| shard download | :620-622 (`s3.download_to`) |

Meanwhile the codebase already has the right patterns: the heartbeat's
R6 budget (`retry_budget = lease/interval`, consecutive-failure
counter, `heartbeat.rs:159/:318/:331`), `backoff_after_lost_race`
jitter (`orchestrator.rs:1450-1461`), and F13's
release-and-skip. F13's classifier even left a marker:
`CoreError::S3(_) → WorkerLocal` with the comment "Retry policy for
these on scan/acquire is F42, not built here" (:1516-1527).

### Known trap

`S3Client::download_to` maps GET failures to **`Error::Other`**
(`s3.rs:500`, `:512`), not `Error::S3` — and `classify_shard_error`
maps `Other → Fatal`. Fix the typing (`download_to` returns
`Error::S3` for SDK/transport errors) as part of this item, or a
transient download blip terminal-fails the shard. Re-typing is the
right fix; do it first and pin it with a classifier-table addition.

### Acceptance tests — write these FIRST

Test-infra prerequisite (in scope): `FakeStore`'s failure injection
(`rig_next_puts_to_fail`, `claim.rs:521`) is wired ONLY into
`put_if_absent`. Extend injection to `list` and `get` (same rigging
style, `RiggedFailureKind::Transient`). `download_to` is an
`S3Client`-only method unreachable via `FakeStore` — introduce a thin
download seam in the scan loop (closure or small trait) so the retry
wrapper is testable; the S3-backed impl stays untested wiring, like
the F29 classifiers.

8. `scan_transient_list_error_retries_with_backoff` (red before fix)
   — rig N transient `list` failures then success; paused time;
   assert the loop retried (op-ish counts), waited ≥ backoff, and did
   NOT exit.
9. `scan_budget_exhausted_exits_with_error` — sustained failure past
   the budget → `run` (or the extracted scan wrapper) returns `Err`;
   a worker must not spin forever on a dead bucket. Budget: reuse the
   R6 shape — `max(1, lease/heartbeat)` consecutive failures —
   constant next to the F13 machinery, documented.
10. `acquire_transient_error_backs_off_not_exit` (red before fix) —
    transient error from `try_acquire` → backoff + continue scan
    (the shard is NOT skip-set — it's not the shard's fault).
11. `download_transient_error_releases_and_retries_later` — with the
    re-typed `Error::S3`, a download failure flows through F13's
    existing `WorkerLocal` path (release + skip + backoff). Assert
    exactly that — no new machinery. Plus
    `download_sdk_error_is_s3_typed` pinning the re-type.
12. `budget_resets_on_success` — mirror
    `refresh_transient_errors_within_budget_no_trip`
    (`heartbeat.rs:727`).

### Fix shape

One small retry wrapper (budget + `backoff_after_lost_race`-style
jitter, paused-time friendly) applied at the scan/manifest/acquire
sites; download keeps flowing through `handle_process_error` (it
already does the right thing once the error is typed `S3`). Follow the
F13 test template (`worker_error_classification.rs:432-483`: drive the
extracted fn against `FakeStore` under paused time, assert elapsed
time + op counts). Do NOT retry inside the claim atoms themselves —
wrap the call sites; the atoms stay untouched.

## Out of scope / do NOT

- No changes to the four claim atoms in `migration-core/src/claim.rs`
  (test-util injection hooks are fine — they're `#[cfg(test/test-util)]`).
- No PUT If-Match anywhere.
- No FFI changes (`do_libnfs_copy`'s return-type plumbing is Rust-side
  only; the `libnfs/` bindings are untouchable).
- No skip-set TTL / cross-run persistence.
- F41: no change to EarlyEof success semantics (downgrade + commit
  stays — that contract belongs to F05/F10).

## Definition of done

- [ ] Tests 1–2, 5–6, 8, 10 written first and observed red.
- [ ] All acceptance tests green; full gate green (fmt, clippy,
      workspace tests, deny).
- [ ] `download_to` errors typed `Error::S3`; classifier table updated.
- [ ] Ledger F40/F41/F42 updated; this doc's Status flipped.
