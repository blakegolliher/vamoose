# Sampled content verification (V2 core) — coding-agent handoff

Status: ready to implement.
Date: 2026-09-28.
Parent contract: `docs/VERIFICATION.md`.
Baseline: metadata verifier merged in PR #84 (`9887663`, merge
`4156eb7`).

## Objective

Implement a production-safe `vamoose verify --mode sample` that retains the
complete V1 namespace/metadata comparison and independently SHA-256 hashes a
deterministic set of regular files from both NFS exports.

The terminal result is allowed to be `passed` only when:

- both independent scans completed;
- every selected content job completed;
- no metadata or content mismatch was found;
- no source or destination read was unreadable;
- no selected file changed between the scan and its content read, or while it
  was being read; and
- the run's failure, downgrade, and retry history was successfully inspected
  for mandatory risk selection.

Do not use the migration index, mover hashes, or destination data as source
evidence. Migration artifacts may select additional risky paths, but the bytes
must be read independently through fresh verifier-owned libnfs contexts.

## Required reading before editing

- `docs/VERIFICATION.md`
- `crates/migration-verify/src/{lib,model,scanner,store}.rs`
- `crates/vamoose-cli/src/cmd/verify.rs`
- `crates/migration-mover/src/libnfs/{mod,ops}.rs`
- `crates/migration-mover/src/bucketed_pool.rs` (`BUCKETS`)
- `crates/migration-core/src/{layout,records,shard}.rs`
- `crates/migration-worker/src/orchestrator.rs` (`flush_sinks`)
- `docs/design/PROTECTED_FFI_DATA_PLANE.md`

The implementation must preserve raw path bytes. UTF-8 rendering is for logs
only; selection, SQLite keys, NFS calls, and JSON evidence identity use the raw
path / `path_b64`.

## Scope boundary

This work item includes the V2 content-verification core:

1. deterministic sample and mandatory risk selection;
2. resumable SQLite content jobs;
3. stable, bracketed, streaming SHA-256 reads through libnfs;
4. content/unreadable/unstable mismatch evidence;
5. V2 report fields, CLI flags, exit semantics, and tests; and
6. automatic risk-history discovery for configured S3 runs.

It does **not** include distributed verification, full-content mode,
finalization gating, TUI progress, xattrs, ACLs, sparse extents, or rate
limiting. `--mode full` must continue to fail closed as unimplemented.

Coordinator events are also a separate follow-up. Today
`VerifyStarted`/`VerifyCompleted` are intentionally denied on the worker event
route, while the standalone verifier is neither a registered worker nor an
authenticated admin command. Do not weaken that trust boundary or emit fake
events from this change. The follow-up needs a reviewed authenticated verifier
ingest API and bounded mismatch sampling.

## Fixed product decisions

### CLI

Enable these options on `vamoose verify`:

```text
--mode sample
--sample-files N             default: 10000; must be greater than zero
--sample-seed U64            default: 0
--content-workers N          default: 8; must be greater than zero
--content-read-size BYTES    default: 4194304; must be 1..=i32::MAX
--risk-evidence PATH         local-manifest mode only; canonical JSONL below
--assume-no-risk-history     local-manifest mode only; explicit assertion
```

`--risk-evidence` and `--assume-no-risk-history` conflict. When sample mode is
used with `--manifest`, require exactly one of them. This prevents a local
manifest from silently omitting prior failures/retries. With a configured S3
run, discover risk history automatically and reject both local-risk flags.

Sample-only flags supplied with `--mode metadata` are errors. Operational
tuning (`content-workers`, `content-read-size`) and selection inputs are part of
the request fingerprint, matching V1's conservative resume identity policy.
Keep the sample-only Clap fields optional at parse time and apply the defaults
after validating the selected mode; otherwise an explicit flag cannot be
distinguished from a default that Clap injected for metadata mode.

The requested sample count is a target total, not an addition to mandatory
risk paths:

```text
selected = all eligible mandatory-risk paths
         + lowest-ranked remaining paths until selected >= sample_files
```

Mandatory risk may make `selected` larger than `sample_files`. If the eligible
population is smaller, select the complete eligible population.

### Eligible population

A path is content-eligible only when the independent scans contain the same
raw path on both sides and both observations are regular files. Missing paths,
type collisions, and non-regular entries remain metadata evidence; do not try
to open them as content jobs.

Metadata differences do not make an otherwise eligible regular-file pair
ineligible. They make it mandatory-risk, and both sides are still hashed. This
can yield both metadata and content mismatch records for one path.

### Deterministic seeded rank

Use this exact, versioned ranking algorithm:

```text
SHA256(
  "vamoose-sample-v1\0" ||
  u64_be(len(run_id))    || run_id_bytes ||
  u64_be(sample_seed)    ||
  u64_be(len(raw_path))  || raw_path
)
```

Order by the 32-byte digest lexicographically, then by raw path bytes as the
collision tie-break. Keep the lowest ranks without replacement. Selection must
be invariant to scan/insertion order and process restart. Use a bounded max
heap for the seeded remainder; memory is `O(sample_files)`, not `O(tree size)`.

### Mandatory-risk reasons

Reasons are additive; store all reasons for a selected path in deterministic
order. The closed V2 reason set is:

- `metadata_mismatch`: any V1 comparison difference on an eligible pair;
- `bucket_boundary`: source size is exactly threshold - 1, threshold, or
  threshold + 1 for each transition derived from `migration_mover::BUCKETS`
  (currently 1 MiB and 1 GiB; do not duplicate those constants);
- `hardlink_group`: the lexicographically smallest **eligible** raw path in
  every detected source hardlink equivalence class (count a group with no
  eligible member as risk-ineligible rather than silently satisfying it);
- `migration_failure`: path appeared in a failure JSONL record;
- `migration_downgrade`: path appeared in a downgrade JSONL record, including
  `EARLY_EOF` and `TORN_COPY`;
- `retried_shard`: regular-file path belonged to a shard whose terminal claim
  has `epoch > 1`; and
- `seeded`: selected from the eligible non-mandatory remainder by seeded rank.

Risk inputs are targeting hints only. A risk path not present as an eligible
pair is counted as `risk_ineligible`; its existing namespace/type mismatch is
still authoritative. Duplicates union their reasons.

### S3 risk-history discovery

For a configured S3 run, before entering the blocking verifier:

1. Require one terminal `Completed` claim for every manifest shard. Missing,
   active, or failed claims make the verification operationally failed; sample
   verification must not race an unfinished migration.
2. LIST and GET every object under `failures/` and `downgrades/`, in lexical key
   order. Parse every non-empty JSONL line as `FailureRecord` or
   `DowngradeRecord`. A malformed or unreadable object fails closed.
3. For every terminal claim with `epoch > 1`, download the corresponding
   immutable manifest index shard, verify its returned ETag against the
   manifest, stream it with `migration_core::shard::ShardReader`, and add every
   regular-file path as `retried_shard`.
4. Write a canonical local `risk-evidence.jsonl` beside the SQLite database,
   fsync it, publish without replacement, and record its SHA-256 and byte
   length. A rerun validates the artifact before reuse.

Canonical risk-evidence lines are:

```json
{"path_b64":"...","reason":"migration_failure","source":"failures/...","source_etag":"..."}
```

Sort by `(path bytes, reason, source, source_etag)` and deduplicate exact lines
before publication. For a local manifest, `--risk-evidence` must already use
this format; copy/validate it into the work directory. With
`--assume-no-risk-history`, publish the same canonical artifact with zero
lines and record the operator assertion in the report.

Use a transient SQLite spool in the verification directory for that external
sort/dedup; a reclaimed shard can contain millions of paths. The spool is
scratch, not another resumable checkpoint, and is removed only after the
immutable JSONL has been published and validated.

The request fingerprint includes the canonical risk artifact digest, not its
filesystem path. Do not hold all risk paths in RAM: import the JSONL into a
SQLite `risk_paths(path BLOB, reason TEXT, PRIMARY KEY(path, reason))` table.

### Content read and stability bracket

Hash source and destination independently. Each side must follow this order:

1. path `stat64`;
2. open read-only;
3. `fstat64` the open handle (`before`);
4. compare both observations with the scan baseline;
5. stream exactly `before.size` bytes through SHA-256 using positional reads;
6. perform a one-byte read at offset `before.size` to prove EOF;
7. `fstat64` the same open handle (`after`);
8. path `stat64` again to prove the path still names that file; and
9. close the handle on every branch.

The stable identity tuple is:

```text
(file type, size, mtime sec/nsec, ctime sec/nsec, dev, ino)
```

Compare scan baseline, path-before, handle-before, handle-after, and path-after
as applicable. Any tuple change, early EOF, or data beyond the bracketed size
is `unstable_source` / `unstable_destination` and makes the entire result
`inconclusive`. Do not retry an unstable path until it happens to look stable.

An open/read/stat/close error without observed mutation is
`unreadable_source` / `unreadable_destination`, makes the result `failed`, and
must not be converted to a content mismatch. Always attempt close after a
successful open; preserve the primary error and attach a close error as
additional detail.

Only two stable complete digests are comparable. Emit `content` when their
SHA-256 values or logical byte counts differ. `expected` is the source
`{"sha256", "bytes"}` object and `observed` is the destination object. Do not
place successful per-file hashes in the terminal report or mismatch JSONL;
keep them in SQLite for resume/audit without exploding artifact size.

Zero-length files use SHA-256 of the empty byte string and still execute the
stat/open/fstat/EOF/fstat/stat/close bracket.

### libnfs surface

Add one new sync FFI declaration alongside the separately documented protected
FFI additions in `migration-mover/src/libnfs/mod.rs`:

```c
int nfs_fstat64(struct nfs_context *nfs,
                struct nfsfh *nfsfh,
                struct nfs_stat_64 *st);
```

The handoff author verified on 2026-09-28 that this signature is byte-identical
in `/usr/local/include/nfsc/libnfs.h` and the pinned
`/home/vastdata/projects/libnfs/include/nfsc/libnfs.h`, and that
`/usr/local/lib/libnfs.so.16.0.2` exports `nfs_fstat64`. Re-run and record this
audit in the implementation commit; do not alter existing extern declarations
or C struct layouts.

Expose safe `ops::stat_snapshot` and `ops::fstat_snapshot` wrappers returning a
small public Rust snapshot type. Map failures through existing `MoveError`
machinery. The verifier must not call raw FFI itself.

### Concurrency and bounded memory

Use `content_workers` long-lived blocking workers. Each owns one fresh source
and one fresh destination `NfsContext`; never share a context concurrently.
Feed them through a bounded queue no larger than `2 * content_workers`, and
write results to SQLite on the coordinator thread. It is acceptable for one
worker to hash the two sides sequentially; workers should alternate which side
goes first so aggregate traffic is balanced.

Do not spawn a thread per file and do not allocate a file-sized buffer. Each
worker reuses at most two `content_read_size` buffers. A worker/context mount
failure is an operational failure, not a reason to silently reduce requested
verification depth.

## SQLite/resume design

Extend the existing database; do not introduce a second checkpoint database.
Add schema-version metadata and these logical tables (exact SQL types may be
adapted to existing helpers):

```text
risk_paths(path, reason)
content_jobs(path, reasons, rank, state,
             source_sha256, source_bytes,
             destination_sha256, destination_bytes,
             outcome_kind, outcome_detail)
```

Requirements:

- `path` is a BLOB primary key.
- `state` is `pending` or `complete`; no other state is durable.
- Complete both sides and update one job atomically. A crash mid-file leaves
  the job pending and rereads it from byte zero.
- A completed job is never reread on resume.
- Persist a `selection_complete` marker only after the complete selection is
  committed. On restart without the marker, delete/rebuild selection
  deterministically rather than trusting a partial set.
- Enumerate pending work and terminal content outcomes in raw-path order.
- Artifact generation happens only after all content jobs are terminal, unless
  a global operational failure prevents completion; that case still produces
  a durable `failed` report with `content_complete: false`.
- Existing V1 observation databases must fail with a clear schema/version
  message, never be partially upgraded in place under the same verification
  id.

Keep mismatch JSONL deterministic regardless of worker completion order by
emitting persisted content outcomes in raw-path order after the existing V1
metadata records.

## Public model and report changes

Bump `REPORT_SCHEMA_VERSION` to 2. Replace the internal stringly mode with a
serialized `VerificationMode` enum (`metadata`, `sample`); keep `full` CLI-only
until implemented.

Add a `SamplePolicyReport` (present only for sample mode) containing at least:

```text
algorithm = "sha256-smallest-v1"
seed
requested_files
eligible_files
selected_files
risk_candidates
risk_selected_files
risk_ineligible_files
seeded_selected_files
selection_reasons (count by reason)
risk_evidence_artifact (path, sha256, bytes)
risk_history_asserted_empty
```

All sample-policy path counts are unique-path counts. `selection_reasons`
counts selected paths carrying each reason, so its values need not sum to
`selected_files`. `risk_candidates` is the unique mandatory target set before
eligibility filtering; for a hardlink group with no eligible member, use its
raw group-root path as the ineligible representative. `risk_selected_files`
and `risk_ineligible_files` partition `risk_candidates`.

Add content counts:

```text
content_complete
source_files_hashed
source_logical_bytes_hashed
destination_files_hashed
destination_logical_bytes_hashed
content_matches
content_mismatches
```

For metadata mode, `sample_policy` is absent, content counts are zero,
`content_complete` is true (there was no requested content work), and
`comparison_policy.content` remains false. For sample mode it is true.

Use these final-status priorities, matching V1:

1. `failed` if any operational/unreadable error or incomplete requested work;
2. `inconclusive` if any unstable source/destination observation;
3. `mismatched` if any metadata/content mismatch;
4. `passed` otherwise.

`mismatch_count` counts JSONL records, including content/unreadable/unstable
records. `unreadable_entries` and `unstable_entries` count unique `(side,
path)` pairs across scan and content phases.

Terminal replay must continue validating the mismatch artifact. It must also
validate the recorded canonical risk-evidence artifact for sample reports.

## Suggested implementation layout

Keep orchestration in `migration-verify/src/lib.rs`, but avoid making that file
another monolith:

- `model.rs`: V2 request/report enums and counters;
- `sample.rs`: ranking, reason union, boundary/hardlink/metadata selectors;
- `content.rs`: reader trait, libnfs implementation, stability comparison,
  SHA-256 streaming, bounded workers;
- `risk.rs`: canonical evidence parsing/import helpers (pure pieces live here;
  async S3 staging may stay in the CLI);
- `store.rs`: schema version, selection/result checkpoints, ordered iterators;
- `vamoose-cli/src/cmd/verify.rs`: flags, configured-run risk staging, local
  assertions, request construction, and expanded human summary; and
- `migration-mover/src/libnfs/{mod,ops}.rs`: audited `fstat64` binding and safe
  snapshots only.

Refactor `emit_entry_differences` so the same comparison result can both emit
V1 records and mark `metadata_mismatch`; do not reimplement metadata policy in
the sampler.

## Tests to write before the implementation

Keep new tests in existing crate unit-test modules where practical; do not add
another top-level integration-test binary just for pure logic.

### Selection

- Same run/seed/population yields byte-identical selected paths across input
  permutations and restart.
- Different seeds change a nontrivial seeded selection.
- Raw non-UTF-8 paths rank and round-trip correctly.
- Selection has no duplicates and selects `min(requested, eligible)` when
  mandatory risk does not exceed the target.
- Mandatory risk is never evicted and may exceed the target.
- Multiple reasons union deterministically.
- Threshold - 1 / threshold / threshold + 1 are selected for every `BUCKETS`
  transition; adjacent non-boundary sizes are not.
- Exactly one deterministic member (the group root) is selected per source
  hardlink group.
- Eligible regular pairs with metadata differences are mandatory; missing,
  collided, and non-regular paths are counted ineligible rather than opened.

### Streaming and classification (fake `ContentReader`, no NFS)

- equal multi-chunk and short-read streams match;
- same-size byte corruption emits one `content` mismatch;
- zero-length files hash the empty stream and prove EOF;
- early EOF and bytes beyond the pre-stat size classify unstable;
- scan-to-open mutation, handle mutation, and path replacement each classify
  the correct side unstable;
- source and destination permission/read/stat failures classify the correct
  side unreadable;
- close runs on every post-open branch and a close failure is retained;
- an unreadable or unstable side never emits a content digest mismatch;
- status priority is failed > inconclusive > mismatched > passed.

### Resume/artifacts

- Simulated crash after K completed jobs rereads only pending jobs.
- Crash during selection rebuilds the exact same set.
- Changing mode/count/seed/risk digest/worker or read-size settings rejects DB
  reuse under the same verification id.
- Completion order does not affect mismatch JSONL bytes or digest.
- Terminal replay detects tampered mismatch and risk-evidence artifacts.
- Malformed/truncated risk JSONL and invalid base64 fail closed.
- S3 history staging detects missing/nonterminal claims, malformed sink rows,
  index ETag drift, and an epoch-2 shard's complete regular-file population.
- The bounded queue never exceeds its configured capacity.

### CLI and hardware

- Clap tests pin all new flags, conflicts, defaults, metadata-mode rejection,
  local risk assertion, and still-unimplemented full mode.
- Extend an existing migration-mover libnfs smoke test (do not create a new
  test binary) with an ignored `stat64/open/fstat64/read/fstat64/stat64` case.
- Extend `scripts/manual-verify.sh` with injected equal, same-size-corrupt,
  truncated, zero-length, unreadable, non-UTF-8, and mid-read-mutated files.
  The script must assert exact exit code, status, and mismatch-kind counts.

No CI test may require a live NFS or S3 server.

## Commit sequence

Use reviewable commits; tests first within each slice:

1. `Add deterministic sampled-content selection` — models, request
   fingerprint, SQLite schema/selection, pure tests.
2. `Add stable streaming NFS content hashing` — audited FFI wrapper, reader
   seam, workers, outcomes, resume tests.
3. `Stage migration risk evidence for verification` — S3/local staging and
   fail-closed tests.
4. `Expose sampled verification in the CLI` — flags, report/human output,
   docs, manual/ignored hardware cases.

Do not push or open a PR until the full gate is green. Do not add AI attribution
or `Co-Authored-By` trailers.

## Gate and definition of done

Run from `migration/`:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
cargo deny --all-features check
```

Also run the repository's release-toolchain and third-party-license checks used
by CI. The work is done when:

- all gates pass;
- `--mode sample` can produce each of `passed`, `mismatched`, `failed`, and
  `inconclusive` under deterministic injected cases;
- restart never rereads completed files or changes the selected set;
- no successful report can be produced with incomplete risk discovery or
  incomplete content work;
- mismatch and report artifacts are immutable and replay-validated; and
- `docs/VERIFICATION.md` is updated from “future” to the exact implemented V2
  behavior without claiming coordinator events, full verification, or
  hardware qualification that did not run.
