# Status

m5-harness-verified. The harness mechanics — workers under sudo +
setsid, dest-tree wipe, mid-shard catch via A.out rename-count,
SIGSTOP/SIGCONT lifecycle, B reclaim observation, post-resume tick
capture — all work end-to-end on var204 hardware.

# What this work item closes

The harness deliverable from M5_SELF_FENCE.md: the test rig itself,
its scripts, its assertions wiring, the fence-trip detection logic,
the observability adds in heartbeat.rs, and the documented quirks of
running mig-worker under sudo (sudo's child-stop self-suspend
behavior plus the setsid workaround that bypasses it). The harness
is now a reliable instrument: it sees what it is supposed to see,
records what it is supposed to record, and fails for reasons that
belong to the system under test rather than to the test rig.

Tag the commit at this point as `m5-harness-verified` so future
bisects have a known-good marker for the harness layer independent
of whatever assertion outcomes the protocol layer produces.

# What this work item does NOT close

The actual seven assertions A–G. They cannot pass on var204 in their
current form because the underlying protocol assumption — that
`If-Match` and `If-None-Match` are symmetric on VAST S3 — turns out
to be wrong. See M5_NOTES.md for the asymmetric If-* finding and the
run logs that surfaced it, and CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md
for the protocol revision proposal that replaces conditional update
with delete-then-create against the same key.

The assertions are not weakened, deferred, or reframed; they are
correct as written. They are simply blocked on the protocol fix.

# When M5 produces its first green run

After the v2 claim protocol lands and is the default, rerun the
existing harness as-is:

    scripts/m5-self-fence-test.sh --files 10000 --file-size 4096

At that point assertions A–G should pass without any harness
changes. That run is the M5 green-run record; append it to
M5_NOTES.md or create M5_PASS.md alongside, whichever fits the
shape of the evidence (a single tick capture vs. a fuller
narrative).

# Operator notes / caveats

- Always run with `VAMOOSE_DST_ROOT` set distinct from
  `VAMOOSE_SRC_ROOT`. The per-file self-target check exists for a
  reason; the harness should never be the thing that exercises it.
- Heartbeat is set to 1s in this script for diagnostic visibility;
  consider raising to 10s for longer-running scenarios where the log
  volume becomes a concern. The 1s cadence is what makes the
  post-resume tick capture legible in the current run logs.
- The bash budget multipliers (`LEASE_TIMEOUT_SEC`, `HEARTBEAT_SEC`)
  in the script are decoupled from the toml values — when a future
  change tightens or loosens leases in production config, the
  harness budgets may need a corresponding adjustment so the
  fence-trip window still falls inside the observation window.

# References

- M5_SELF_FENCE.md — original harness design and the seven assertions.
- M5_NOTES.md — run record, the asymmetric If-* finding, raw logs.
- CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md — the protocol redesign
  that unblocks the green run.
