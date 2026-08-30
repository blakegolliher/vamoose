# Beta notes: known limitations, operational posture, contracts

Audience: beta operators and anyone writing tooling against
vamoose's outputs. Everything here is a deliberate, owner-decided
posture (2026-07-31) — not an unknown. Source of truth for status:
`docs/REVIEW_LEDGER.md`; for pending work: `docs/NEXT.md`.

## Known limitations

- **Hardlink fidelity is micro-batch-scoped (F15).** The shard processor
  groups `(fsid, inode)` members only within the current byte/file-budgeted
  batch. Members split across batches or shards are migrated as independent
  files, so `nlink` fidelity is not preserved across those boundaries. The
  complete fix belongs in the multi-pass mover design.
- **Pre-upgrade workers keep at-least-once event semantics.** Event
  idempotency (exactly-once effective delivery to the coord) engages
  only for workers that stamp `client_seq` (all current builds).
  Older workers are still correct but can double-apply progress/
  error counters on resend after a lost response. No flag day: mixed
  fleets work.
- **The TUI's live view is intentionally rate-limited.** The bus
  coalesces ProgressDelta to 1 Hz per (job, worker) and caps
  ErrorEmitted at 10/class/sec; a quieting burst's final value
  arrives within ~2 s via the trailing-edge flush. The durable event
  log always carries everything — accounting and replay are exact.
- **Two coordinator commands are compatibility placeholders.** `drain`
  currently records a paused control mode, and `retry-failed` records an audit
  entry without queuing worker retries. Both fail safely, but neither provides
  the richer behavior implied by its name yet.
- **Pause is batch-granular; cancel is shard-granular and final.** `:stop`
  (pause) takes effect when the batch in flight finishes — seconds to a minute
  — and holds every claim while the heartbeat keeps the worker alive.
  `:abort` (cancel) lets each worker finish the shard in hand and exit 0; the
  job then hands `Cancel` to any worker that registers again, so a cancelled
  job is not resumable through the coordinator. `systemctl stop` is the hard
  stop; the interrupted shard is reclaimed after ~2× `heartbeat_sec`.
- **Job provisioning follows the manifest.** The coordinator seeds the job
  from `manifest.json` (id = run id); a worker whose `[coord] job_id` names a
  job the coordinator never seeds waits forever, logging
  `job not found on coord` at every retry.

## Security posture (trusted-network beta)

Run vamoose on a trusted network for beta. Concretely:

- One shared `X-Cluster-Secret` for all workers; rotate it like any
  shared credential. Admin bearer tokens are full-authority (labels
  are for audit, not scoping).
- The coord binds `0.0.0.0:8443` by default; enable TLS
  (server-side) in any real deployment. No mTLS. Dev mode (no
  tokens AND no secret) refuses non-loopback binds unless explicitly
  overridden.
- The worker-event route enforces a strict EventKind allow-list and
  caller identity binding — a cluster secret grants worker-level
  power only, never operator power, and workers cannot impersonate
  or fence each other.
- Post-beta hardening track (decided, not scheduled): per-worker
  credentials, scoped tokens, mTLS, loopback-default bind.

## Operational contracts

- **Worker exit codes** (also in `vamoose worker --help`): `0` clean
  completion; `1` error; `2` watchdog wedge (shutdown hung); `3`
  **fenced** — the run ended because a peer took the worker's claim
  or its clock jumped. Supervisors: treat 3 as alert/investigate, not
  restart-into-the-same-condition. `4` **store unreachable** — the
  worker could not confirm its claim for a full lease window (S3
  endpoint, DNS, or network outage), surrendered the shard, and
  should simply be restarted; the shipped unit does so.
- **Worker liveness**: the coord marks a worker `Disconnected` when it
  has not heartbeated for 90 s, when a replacement registers from the
  same machine, or when the worker announces an orderly exit
  (`POST /workers/{id}/leave`). The TUI's headline worker count is the
  connected workers; the Workers tab still lists the disconnected
  ones with their last state.
- **S3 request deadlines**: every control-plane S3 call carries
  connect / first-byte / per-attempt / whole-operation bounds (5 s /
  30 s / 60 s / 180 s), and the heartbeat, lease-refresh, and shard-
  download loops bound their own waits on top. A request the endpoint
  never answers now fails like any other transient error — it counts
  against the worker's retry budget (exit `4` after a lease window)
  and forfeits the coord's lease after one TTL — instead of parking
  the loop while the rest of the process carries on.
- **Latency telemetry — "who is slow?"**: every raw NFS RPC (tagged
  source or destination) and every S3 call is timed into lock-free
  histograms. Each heartbeat the worker rolls the last window up into
  per-op count / mean / p50 / p95 / p99 / max plus the **busy share**
  per server — the fraction of `pairs × window` its connection pairs
  spent waiting on that server — and the share of the window spent
  inside S3 calls. It lands in three places: the journal (`heartbeat:
  latency window …` every tick), the worker's `progress/<host>.json`
  in the bucket (`latency`), and the coord heartbeat, which the TUI
  renders as the Overview's `Latency` section (fleet roll-up: worst
  percentiles across workers, pair-weighted busy shares, and a
  verdict — destination-bound / source-bound / S3-bound / client-
  bound) and per worker in the worker modal. A side ≥ 85 % busy is
  the bottleneck; both under 60 % means the servers have headroom and
  the client is the limit.
- **Finished jobs in the TUI**: the Overview closes the run window at
  the terminal transition and reports "took" and the average over
  the manifest's files; rows re-copied after reclaims are shown on a
  separate `Replayed` line rather than inflating the average.
- **Durability**: file data is COMMITted (whole-file NFS COMMIT)
  before the rename publishes it, on both copy paths; every data-
  plane RPC carries a deadline (`[mover] rpc_timeout_ms`, default
  60 s, `0` = libnfs default). See DESIGN.md "Mover behavior".
- **`mig-aggr clean-partials`** is dry-run by default; deletion
  requires `--delete` and is gated on claim liveness using the
  protocol-default 180 s lease window. If your fleet configures
  longer `worker.lease_timeout_sec`, wait out your window or use
  `--force` deliberately.
- **Failure-sink wire format**: failures JSONL carries errno NAMES
  (timeouts read `EINTR`, commit failures are phase `Write` with a
  `COMMIT:<errno>` tag). Parsers should key on names, not numbers.

## Verification status

Automated checks cover the landed findings. F15 remains open with its
micro-batch limitation accepted for beta. Rows marked `landed` rather than
`verified` still await the applicable VAST-rig pass; the checklist lives in
`docs/NEXT.md` §2 (reclaim drill, pipelined smoke, replay smokes, bounded
timeout, drain, fsync readback, 4755, crash drill). Run it before broad beta
exposure.
