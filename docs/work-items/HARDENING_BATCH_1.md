# Hardening batch 1 — small, independent operator-facing fixes

Status: in review — items A–G on branch `hardening-batch-1`, one
commit per item, full gate green. Per-item outcomes:

- A (F17): fixed — watchdog exits 2 on any wedge (decision: 2 for
  wedged-after-fenced too; run outcome goes in the stderr line).
- B (F27): fixed — `install_panic_hook()` + shared
  `restore_terminal()`; `VAMOOSE_TUI_PANIC_AFTER_MS` manual hook.
- C (F21): fixed — default DENY dev mode on non-loopback binds;
  escape hatch `--allow-unauthenticated-nonloopback`.
- D (F37): fixed — `_cluster`, `.`/`..`, control chars, >128 bytes
  rejected; worker side is operator-set config validated at startup,
  no clamping needed.
- E (F35): fixed — decision: typed `Err(UringError::Unimplemented)`
  rather than deletion (pool is the published M3.5 API surface).
- F (F33): fixed — 15 lines → `tracing::debug!(target: "shutdown")`;
  `libc::write` exit-path lines left alone; `RUST_LOG=shutdown=debug`
  documented in the orchestrator module doc.
- G (F31): done — clap `debug_assert`, config fixtures, dual-format
  seam pinned, `parse_size` table. F45 decision: NOT a one-liner
  (ripples through `cmd::doctor`); current behavior pinned by test,
  F45 stays open.
Ledger: F17, F21, F27, F31, F33, F35, F37 in `docs/REVIEW_LEDGER.md`.
Priority: medium — each small; batched for one session.
Scope: worker, coord, tui, cli, mover — disjoint small diffs.

Work the items in order; each is independently committable. Write
each item's test(s) before its fix.

## Item A — watchdog exit code (F17)

`orchestrator.rs` ~829: the shutdown watchdog `libc::_exit(0)`s if
shutdown wedges — supervisors see success for fenced/failed runs.

Tests first: extract `watchdog_exit_code(run_outcome) -> i32` (pure)
— table test: clean → 0 is NOT the watchdog's business (watchdog only
fires on wedge) → wedged-after-clean → 2, wedged-after-error → 2 (or
1; pick and document), and the normal `_exit` path keeps 0/1.
Then wire: the watchdog thread receives the outcome via the existing
channel/atomic it's armed with.

## Item B — TUI terminal restore under panic=abort (F27)

Terminal restore is `Drop`-based (`TerminalGuard`), which never runs
under release `panic = "abort"` — a panic leaves the operator's
terminal raw in the alternate screen.

Tests first: unit test that `install_panic_hook()` (new) composes
with the existing hook (calls the previous hook after restoring), is
idempotent, and that the restore fn it uses is the same one the guard
uses (single source of truth — assert by construction: one pub(crate)
`restore_terminal()` used by both). Manual verification note: `kill
-SEGV` isn't a panic; add a debug-only `:panic` palette command or a
`#[cfg(test)]`-gated trigger? No — keep it simple: a hidden
`VAMOOSE_TUI_PANIC_AFTER_MS` env hook used by one ignored test is
acceptable; document it in the test.

## Item C — coord dev-mode must not bind non-loopback (F21)

No tokens configured = dev mode (no auth) while the default bind is
`0.0.0.0:8443`.

Tests first (in `cmd/coord.rs` or wherever the seam lands):
`dev_mode_nonloopback_refused` (red) — dev auth + `0.0.0.0` →
startup error naming both flags; `dev_mode_loopback_ok` —
`127.0.0.1`/`::1` fine with a warning; `authed_any_bind_ok`. An
explicit `--allow-unauthenticated-nonloopback` escape hatch is
acceptable if a lab flow needs it; default deny.

## Item D — JobId validation (F37)

`schema.rs::JobId` rejects only empty and `/`. It permits `_cluster`
(collides with `events/_cluster/`), `.`/`..`, control chars,
unbounded length.

Tests first (next to existing schema tests): reserved `_cluster`
rejected; `.`/`..` rejected; control chars rejected; > 128 bytes
rejected; existing valid ids still accepted (grep tests/fixtures for
ids in use). Then tighten `JobId::new`. Check the worker side
generates compliant ids (`coord_client` registration) — if it can
produce long ids, clamp there too.

## Item E — uring todo!() landmine (F35)

`mover/src/uring.rs::FixedBufferPool::acquire` is `todo!()` —
panic=abort kills the worker if any future caller reaches it.

Test first: `acquire_returns_unimplemented_error` — convert to
`Err(...)` with a clear "M3.5 not implemented" message; keep the
type. (If the pool is genuinely dead code, deleting the fn + its
constructor is the better fix — check callers; deletion needs no
test, just the build.)

## Item F — shutdown eprintln!s → tracing (F33)

~12 `eprintln!("[shutdown] ...")` lines in the orchestrator shutdown
path. Convert to `tracing::debug!(target: "shutdown", ...)` EXCEPT
any line after the tokio runtime may be gone (the `libc::write`
exit-path lines are deliberate — leave those). No test; reviewer
checks the diff. `RUST_LOG=shutdown=debug` documented in the module
doc.

## Item G — vamoose-cli config + clap tests (F31)

Zero tests over the code every subcommand funnels through.

These ARE tests: (1) `Cli::command().debug_assert()` — catches
invalid clap wiring at test time; (2) `Config::load` fixtures:
minimal unified config parses; legacy worker-only TOML falls back via
the dual-format path in `cmd/worker.rs` (subtly-invalid unified must
NOT silently fall through — pin current behavior or fix it if it's
cheap, note which); missing `[nfs]` section errors for worker but —
per F45 — decide whether coord should accept its absence (if that's
a one-liner, do it and test it; if not, pin current behavior and
leave F45 open); (3) `logging::parse_size` table test ("50 MiB",
"1 GiB", "garbage").

## Definition of done

- [x] Every item's tests written before its fix; reds observed where
      marked.
- [x] Full gate green after the batch (fmt, clippy, workspace tests,
      deny).
- [ ] Ledger rows F17/F21/F27/F31/F33/F35/F37 updated individually
      (an item can land `wontfix` with a reason — say so in the row).
      (Coordinator: ledger is updated separately, not on this branch.)
- [x] This doc's Status flipped.
