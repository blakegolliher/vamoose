# TUI robustness: REST bootstrap, unknown-event tolerance, quiet logging

Status: landed — merged in PR #26 (F38 → F26 → F39). Post-review fix: a torn bootstrap walk is a retryable error (pre-walk cursor double-counted aggregates).
Ledger: F26, F38, F39 in `docs/REVIEW_LEDGER.md`.
Priority: high — F26 is now actively wrong (F23 archives history out
from under the seq-0 bootstrap), F38 bricks every deployed TUI on the
first coord upgrade that adds an event kind, F39 corrupts the operator
display.
Scope: `migration-tui` (`client.rs`, `app.rs`, `state.rs`),
`vamoose-cli` (`cmd/tui.rs`, `logging.rs`, `main.rs`).

Work the items in order F38 → F26 → F39; each is independently
committable. Write each item's tests before its fix.

## Item 1 — F38: unknown `EventKind` must not brick the client

### Problem

`EventKind` is internally tagged (`#[serde(tag = "kind")]`,
`schema.rs:513-620`) with no catch-all — zero `serde(other)` hits in
the repo. The failure chain: unknown `kind` → serde error at
`client.rs:384-386` → stream yields `Err` → `sse_driver`'s
`Some(Err(e))` arm (`app.rs:840-845`) disconnects → backoff → reconnect
resumes from `last_seq()`, which is still *below* the unknown frame's
seq (it never applied) → coord replays the same frame → same error.
A permanent reconnect loop against any newer coord.

### Acceptance tests — write these FIRST

1. `parser_skips_unknown_kind_and_reports_seq` (red before fix) —
   extend the channel-driven parser harness
   (`client.rs:405-511`, `bytes_stream`): feed
   `event:FutureThing\nid:5\ndata:{"kind":"FutureThing"}\n\n` followed
   by a valid known frame; assert the stream yields a
   skipped-unknown item carrying seq 5 (NOT `Err`), then the valid
   frame.
2. `driver_advances_cursor_past_unknown` — app-reducer level
   (`handle_input` pattern, `app.rs:987+`): the skipped-unknown input
   advances `last_seen_seq` to the skipped seq and bumps an
   operator-visible "unknown events" counter; no disconnect.
3. `malformed_data_still_errors` — a frame with a valid `id:` but
   garbage non-JSON `data:` should still surface as an error
   (corruption ≠ future-version skew); pin the distinction: JSON that
   parses but has an unrecognized `kind` → skip; JSON that doesn't
   parse → `ClientError::Serde` as today.
4. Integration (`tests/integration.rs` `spawn_coord` harness): drive a
   raw SSE body with an interleaved unknown-kind frame through the
   parser path and assert the client ends in sync (state matches
   coord) — no reconnect storm. If injecting a synthetic frame through
   the live coord is impractical, the parser + reducer tests carry the
   contract and this test may drive `parse_sse_stream` directly.

### Fix shape

Client-side skip-with-cursor-advance (preferred over a schema-side
`Unknown` variant: old TUIs in the field must tolerate *future* coords,
and the envelope's useful fields can't be recovered from an unknown
tag anyway):

- In `PartialFrame::take` (`client.rs:366-396`): when the `data:` JSON
  parses as a JSON object but `EventEnvelope` deserialization fails on
  the `kind` tag AND the frame carried a numeric `id:`, yield a new
  `SseFrame::UnknownEvent { seq, kind: String }` instead of `Err`.
  Distinguish via a lightweight pre-parse (`serde_json::Value`, read
  `kind` as string) — do not regex.
- `sse_driver`/`handle_sse_frame`: apply the seq (advance
  `last_seen_seq` via the existing drop-rule path so duplicates stay
  deduped), increment a counter surfaced in the status bar (reuse the
  errors-tab/banner conventions in `render.rs`), `tracing::debug!` the
  kind string.
- `schema_version` already has `serde(default)` — leave it; this item
  is about unknown variant names.

## Item 2 — F26: REST bootstrap + real Resync recovery

### Problem

`sse_driver` (`app.rs:808-820`) opens the stream with
`Some(state.last_seq())`, which is 0 on first connect — a full replay
of the entire event log, and since F23 (archive-on-completion) a
**wrong** one: archived terminal jobs' chunks are deleted from
`events/`, so they exist in coord's `GET /jobs` view but never appear
in a seq-0 replay. The REST bootstrap the plan requires
(COORD_PLAN §3.4: "re-fetch `/jobs` snapshot" on Resync; §3.7 P4) was
never built — `client.rs` even has an unused `healthz` whose doc calls
itself the "authoritative source of `last_seq` for the initial SSE
resume cursor" (`client.rs:126-127`).

Resync is worse: `handle_sse_frame`'s `Resync` arm (`app.rs:122-128`)
only flips the banner to Reconnecting — the stream is never dropped,
no REST re-fetch happens, no seq is reset. The skipped events'
`ProgressDelta`/`ErrorEmitted` counts are lost forever
(coord drops them at `stream.rs:187-193`); counters desync
permanently. The existing test
`sse_frame_resync_marks_reconnecting` (`app.rs:1163-1172`) pins the
wrong behavior and will be replaced.

### Acceptance tests — write these FIRST

5. `bootstrap_fetches_jobs_then_streams_from_last_seq` (red before
   fix) — integration harness (`tests/integration.rs`): ingest a
   job's lifecycle, `archive_job` it (the F23 path), ingest a second
   live job; start the TUI's bootstrap+stream path; assert the
   archived job appears in state (from REST) AND the stream request
   used `last-event-id = healthz.last_seq`-or-jobs-derived cursor, not
   0 (the coord-side op log / a request-inspecting shim can pin the
   header; asserting no replay of the live job's early events is an
   acceptable proxy).
6. `resync_refetches_snapshot_and_resumes` (red before fix) — reducer
   level: `Resync` input → action that (a) drops the stream, (b)
   re-fetches `/jobs` (+ per-job detail as needed), (c) applies via
   `replace_snapshot` (`state.rs:752-755` — note it already keeps
   `last_seen_seq.max(snapshot.last_seq)`), (d) resumes the stream
   from the new cursor. Drive with the integration harness: force a
   Resync (tiny `bus_capacity` — the stream unit tests use 4,
   `stream.rs:319`), assert post-recovery counters match
   `rt.state()` exactly (the desync this item kills).
7. `replace_snapshot_newer_wins_older_kept` — extend the existing
   `replace_snapshot_keeps_last_seen_when_snapshot_is_older`
   (`state.rs:919`) for the bootstrap path.
8. Keep `appstate_after_replay_matches_coord_view`
   (`tests/integration.rs:215-295`) green — replay-from-0 must still
   work when explicitly requested (`Some(0)` semantics unchanged in
   `client.rs`).

### Fix shape

- Bootstrap: on startup and on Resync, `GET /healthz` (cursor) +
  `GET /jobs` (paginate via `next_cursor`) — both methods already
  exist (`client.rs:126-192`) — build the snapshot, apply via
  `replace_snapshot`, then `stream(Some(last_seq), ...)`. The
  jobs→snapshot conversion is the only new state code; keep it a pure
  fn with its own unit test.
- Resync: the driver owns the recovery (drop stream → bootstrap →
  re-stream); the reducer emits an action, it does not do I/O —
  follow the existing `Input`/`AppAction` conventions
  (`app.rs:987+` tests).
- Per-job deep state (workers/errors tabs) may lazily re-fetch via the
  existing `/jobs/{id}/workers`+`/errors` endpoints if the snapshot
  conversion can't cover them; note what's deferred in the doc-comment.

## Item 3 — F39: logging must not write to the TUI's terminal

### Problem

`logging::install_subscriber` (`logging.rs:181-194`) always installs a
stderr fmt layer (`:182`), and `main.rs:78-98` calls `logging::init`
for every subcommand including `Tui` — one log line from any dependency
corrupts the ratatui alternate screen. Plan §3.7
(`COORD_PLAN.md:396-400`) requires: "The TUI suppresses tracing-fmt
output entirely (logs to a file via tracing-appender if `--log-file`
is passed)." Additionally `logging::init` spawns the S3 log uploader
whenever `[logging].s3_upload` is set (`logging.rs:98-102`) — config-
driven, not subcommand-gated — so `vamoose tui` silently starts an S3
uploader.

### Acceptance tests — write these FIRST

9. `tui_subscriber_has_no_stderr_layer` (red before fix) — a
   mode/parameter on `install_subscriber` (e.g.
   `LogMode::TuiQuiet`) that installs no stderr writer; assert via a
   captured-writer seam or by construction (the fn signature makes
   stderr impossible in that mode — then a compile-level pin plus a
   smoke test that emitting a tracing event while in TUI mode writes
   nothing to a test-injected stderr buffer).
10. `tui_log_file_flag_routes_to_file` — new `--log-file` on the tui
    subcommand: events land in the file (existing rotating-appender
    machinery), still nothing on stderr.
11. `tui_never_starts_s3_uploader` (red before fix) — with
    `[logging].s3_upload = true` in config, the TUI init path must not
    spawn the uploader; assert via the uploader's spawn seam
    (`spawn_uploader`, `logging.rs:198-269` — give it an injectable
    gate or return-handle the test can inspect).
12. `worker_logging_unchanged` — regression: worker/coord subcommands
    keep stderr + file + (configured) uploader exactly as today.

### Fix shape

- `logging::init` grows a caller-supplied mode (enum, not bool):
  `Standard` (today) vs `TuiQuiet` (no stderr layer, no uploader,
  optional file appender from `--log-file`). `main.rs` picks by
  subcommand. The fallback-subscriber path (`main.rs:88-97`) must
  respect the mode too.
- `cmd/tui.rs` `Args` gains `--log-file <path>` (plan-mandated name).
- Do not remove the uploader machinery — gate it.

## Out of scope / do NOT

- No coord-side changes (the F23 seq-aware reads and stream seams are
  fresh; consume them as-is). If a genuine coord bug blocks a test,
  report it — don't fix it here.
- No new TUI features/tabs; F26 is bootstrap/recovery only.
- No ratatui upgrade.
- Do not change `client.stream`'s `Some(0)`/`None` semantics.

## Definition of done

- [ ] Tests 1, 5, 6, 9, 11 written first and observed red (or
      compile-red where the seam is new).
- [ ] All acceptance tests green; full gate green (fmt, clippy,
      workspace tests, deny).
- [ ] `sse_frame_resync_marks_reconnecting` replaced by the real
      recovery test; COORD_PLAN §3.4/§3.7 contract references added to
      the module docs of the touched files.
- [ ] Ledger F26/F38/F39 updated; this doc's Status flipped.
