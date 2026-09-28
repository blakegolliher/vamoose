# Verification product contract

Status: V1 metadata verification and V2 sampled content verification are
implemented. Full content verification, distributed verification, coordinator
verification events, and automatic finalization gates remain future milestones.

Vamoose must prove that a migration produced the requested destination tree.
Worker success, terminal shard claims, and mover-computed hashes are useful
evidence, but they are not independent verification. A run is verified only
after a separate read path has compared the source and destination and written
a durable, machine-readable report.

This is the bar set by products such as NetApp XCP's
[`verify` command](https://docs.netapp.com/us-en/xcp/xcp-nfs-reference-verify.html):
content and metadata verification are part of the migration lifecycle, not an
ad-hoc operator script.

## Customer-visible contract

The current command is:

```text
vamoose verify [--mode metadata|sample|full]
               [--manifest PATH]
               (--writers-stopped |
                 --source-snapshot-id ID --destination-snapshot-id ID)
               [--verification-id ID] [--work-dir PATH]
               [--report PATH] [--mismatches PATH] [--json]
               [--sample-files N] [--sample-seed U64]
               [--content-workers N] [--content-read-size BYTES]
               [--risk-evidence PATH | --assume-no-risk-history]
```

The default mode is `metadata`. `sample` is implemented. `full` is a visible
reservation that fails closed as unimplemented; it does not silently degrade
to a lighter mode and will become the default only when V3 lands.

Sample-only options are rejected with `--mode metadata`. Their defaults are
10,000 files, seed 0, 8 content workers, and 4 MiB reads; the count, workers,
and read size must be greater than zero and the read size at most
`i32::MAX`. With `--manifest`, sample mode requires exactly one of
`--risk-evidence PATH` (a canonical risk-evidence artifact) or
`--assume-no-risk-history` (an explicit operator assertion recorded in the
report), so a local manifest cannot silently omit the run's failure and retry
history. With a configured S3 run, both flags are rejected and the history is
discovered automatically.

- `metadata` compares the complete namespace and supported metadata for every
  entry, but does not read regular-file content.
- `sample` performs the complete metadata comparison, plus independent SHA-256
  content verification for a deterministic sample and every risk-selected
  file. The requested count is a target total: every mandatory-risk path is
  selected first, then the lowest-ranked seeded paths until the target is
  reached, so mandatory risk may exceed the target and a smaller eligible
  population is selected completely.
- `full` performs the complete metadata comparison and content verification for
  every regular file.

The command exits:

- `0` only when the selected verification completed with zero mismatches,
  zero unreadable entries, zero unstable files, and zero unapproved fidelity
  exceptions;
- `1` for an operational failure that prevented a complete result;
- `2` when verification completed and found mismatches; and
- `3` when the source or destination changed during verification, making the
  result inconclusive.

The human summary and JSON report must distinguish `failed`, `mismatched`, and
`inconclusive`. None may be presented as "verified."

## Consistency boundary

A verification certificate is valid only against stable trees. Production
verification therefore requires one of:

1. source and destination snapshots identified in the report; or
2. writers stopped for the entire verification window.

For each content read, the verifier obtains attributes before and after reading.
A change in size, mtime, ctime, file identity, or type marks the entry unstable
and the run inconclusive. It must never be silently retried until it happens to
match.

The verifier refuses to start without paired snapshot identifiers or the
explicit `--writers-stopped` assertion. Snapshot identifiers are evidence
recorded in the report; the verifier does not create snapshots or rewrite
endpoint roots on the operator's behalf. Every mode detects directory mutation
across enumeration and marks the result `inconclusive`.

Sample mode brackets every content read on each side, in this order: path
`stat64`, open read-only, `fstat64` of the handle, comparison of both
observations with the scan baseline, a stream of exactly the bracketed size
through SHA-256, a one-byte read at that offset to prove EOF, `fstat64` of the
same handle, a final path `stat64` to prove the path still names that file,
and close on every branch. The stable identity tuple is `(file type, size,
mtime sec/nsec, ctime sec/nsec, dev, ino)`. Any change, early EOF, or readable
data beyond the bracketed size is `unstable_source` / `unstable_destination`
and makes the whole result `inconclusive`; an unstable path is never retried
until it happens to look stable. An open, read, stat, or close error without
an observed mutation is `unreadable_source` / `unreadable_destination` and
makes the result `failed`; it is never converted into a content mismatch. A
close failure is retained as detail and never turns a digest into a pass.

## Independent evidence

The verifier independently scans both exports through libnfs. It does not treat
the source migration index, worker progress, or the mover's in-flight hash as
proof of destination correctness.

The migration manifest supplies run identity, roots, exclusions, and the
expected source population. Comparing independent source and destination scans
also detects:

- source entries omitted from the migration plan;
- missing destination entries;
- unexpected destination entries;
- path/type collisions; and
- rows reported successful whose destination bytes later changed.

Raw POSIX path bytes are the identity key. JSON records use `path_b64`; lossy
UTF-8 rendering is display-only.

## Comparison policy

Every mode compares the following for every entry:

- relative raw path and file type;
- regular-file logical size;
- mode bits, uid, and gid when their preservation policy is enabled;
- mtime at the protocol's supported precision;
- symlink target bytes;
- directory presence and metadata after child comparison; and
- hardlink equivalence classes by path membership, not destination inode number.

As support lands, the same policy extends to xattrs, POSIX/NFSv4 ACLs, sparse
extent maps, device numbers, and other special-file data. Until then,
encountering an unsupported feature is a fidelity exception. It fails
verification unless the run manifest contains an explicit operator-approved
policy for that feature.

Content verification reads source and destination independently through
fresh verifier-owned libnfs contexts and computes a SHA-256 digest while
streaming positional reads of `--content-read-size` bytes. The two sides of a
file are read one after the other by one worker; workers alternate which side
they read first so aggregate traffic stays balanced. Nothing shares buffers
with, or trusts data produced by, the migration: the migration index, mover
hashes, and destination state only ever select paths, never prove them. Only
two stable, complete digests are compared; a `content` mismatch records the
source `{sha256, bytes}` as expected and the destination's as observed.
Successful per-file digests stay in the SQLite checkpoint for resume and
audit rather than in the report or JSONL. Zero-length files hash the empty
string and still run the full bracket.

`sample` selection is deterministic from `(run_id, seed, raw_path)`. A path is
content-eligible only when both independent scans hold the same raw path as a
regular file; missing paths, type collisions, and non-regular entries remain
metadata evidence and are never opened. Metadata differences do not make a
regular-file pair ineligible: they make it mandatory, and both sides are still
hashed, so one path can carry both metadata and content records. The closed
reason set, stored additively per path in this order, is:

- `metadata_mismatch`: any V1 comparison difference on an eligible pair;
- `bucket_boundary`: source size exactly one below, at, or one above each
  transition derived from the mover's `BUCKETS` (1 MiB and 1 GiB today);
- `hardlink_group`: the lexicographically smallest eligible member of every
  detected source hardlink group (a group with no eligible member is counted as
  risk-ineligible under its group root);
- `migration_failure`, `migration_downgrade`: the path appeared in a failure or
  downgrade record (including `EARLY_EOF` and `TORN_COPY`);
- `retried_shard`: the regular file belonged to a shard whose terminal claim
  had `epoch > 1`; and
- `seeded`: selected from the eligible remainder by rank.

Risk inputs are targeting hints only. A risk path that is not an eligible pair
is counted as risk-ineligible; its existing namespace or type mismatch remains
authoritative. Ranking is the versioned `sha256-smallest-v1` algorithm:
`SHA256("vamoose-sample-v1\0" || u64_be(len(run_id)) || run_id ||
u64_be(seed) || u64_be(len(raw_path)) || raw_path)`, ordered by digest and then
raw path bytes, keeping the lowest ranks without replacement through a bounded
heap so memory is proportional to the requested count rather than the tree.
Selection is invariant to scan order and process restart.

The report's `sample_policy` records the algorithm, seed, requested count,
workers, read size, eligible, selected, risk-candidate, risk-selected,
risk-ineligible, and seeded counts, the per-reason selection counts, the
risk-evidence artifact digest, and whether the operator asserted an empty
history, so the sample can be reproduced exactly.

## Mismatch records

Mismatch JSONL is the high-cardinality evidence format. Today it is one
deterministic, immutable file: V1 metadata records carry shard id
`metadata-v1`, and sample-mode content records (`content`,
`unreadable_*`, `unstable_*`) follow them in raw-path order under shard id
`content-v2`, independent of worker completion order. V3 will partition that
format into independently published verification shards. Each record
contains:

```text
schema_version, verification_id, run_id, path_b64, kind,
expected, observed, source_observation, destination_observation,
worker_id, shard_id, observed_utc
```

`kind` is a closed, versioned enum initially containing:

```text
missing_destination, unexpected_destination, type, size, content,
mode, owner, mtime, symlink_target, hardlink_group,
unsupported_feature, unreadable_source, unreadable_destination,
unstable_source, unstable_destination
```

High-cardinality details stay in JSONL. Coordinator events, when they land,
will carry bounded samples and aggregate counts only; the standalone verifier
does not emit them today.

## V1 implementation

V1 performs fresh source and destination scans over the pinned raw NFSv3
READDIRPLUS interface. Attributes returned with directory entries avoid a
separate GETATTR in the common case. Opaque NFS cookies, cookie verifiers, raw
path bytes, directory filehandles, observations, issues, and scan completion
are transactionally checkpointed in a local SQLite database. Restarting with
the same verification id resumes incomplete directory batches; a request
fingerprint rejects reuse with changed endpoints, policy, exclusions, or
consistency evidence.

The comparison is a merge join over SQLite's raw-path ordering, so memory is
bounded independently of tree size. Hardlink equivalence classes are
materialized across the complete observed namespace, rather than inferred per
migration micro-batch. V1 is intentionally a single-node verifier; distributed
partitioning remains V3 work.

Mismatch JSONL and the report are written beside their final paths, fsynced,
and published without replacement. Reports contain SHA-256 and byte length for
the mismatch artifact and validate that artifact on terminal replay. A local
advisory lock prevents concurrent writers to one verification work directory.

V1 returns `mismatched` for namespace or supported metadata differences,
`failed` for unreadable/operational observations, and `inconclusive` for a
detected mutation. Unsupported special file types are explicit
`unsupported_feature` mismatches. In metadata mode content, xattrs, ACLs, and
sparse extents are recorded as disabled in the comparison policy and are not
claimed as verified.

## V2 implementation

Sample mode extends the same verification database (schema version 2; a V1
database is rejected with a clear message rather than upgraded in place under
the same verification id) and the same immutable artifacts.

Before the blocking verifier starts, the CLI stages the run's risk history
into a canonical `risk-evidence.jsonl` beside the database. For a configured
S3 run it requires one terminal `Completed` claim for every manifest shard
(missing, active, or failed claims are operational failures, so sample
verification never races an unfinished migration), reads every object under
`failures/` and `downgrades/` in lexical key order with every non-empty line
parsed fail-closed, and for every claim with `epoch > 1` downloads the
immutable index shard, verifies its ETag against the manifest, and streams its
regular-file paths. Lines are `{"path_b64","reason","source","source_etag"}`,
sorted by those fields and deduplicated through a transient SQLite spool, then
published without replacement. The artifact's SHA-256 and length, not its
path, join the request fingerprint alongside the mode, sample count, seed,
workers, read size, and the asserted-empty flag; a rerun validates the artifact
before reuse. A local manifest must supply `--risk-evidence` in the same
format (validated and canonicalized into the work directory) or
`--assume-no-risk-history`, which publishes a zero-line artifact and records
the assertion.

Selection runs once, after both scans and hardlink materialization, in a
single transaction that commits the complete job set together with a
`selection_complete` marker; a restart without the marker rebuilds the
identical set. Content jobs are `pending` or `complete`, both sides of a job
complete atomically, a crash mid-file rereads that file from byte zero, and a
completed job is never reread. Long-lived workers read through a bounded queue
of twice the worker count; a context mount failure stops the phase, and the
run still publishes a durable `failed` report with `content_complete: false`
rather than a shallower pass. Artifacts are generated only after every job is
terminal or such a failure occurred.

The terminal status follows the V1 priorities: `failed` for any unreadable or
operational error or incomplete requested work, then `inconclusive` for any
unstable observation, then `mismatched` for any metadata or content
difference, otherwise `passed`. `mismatch_count` counts JSONL records including
content, unreadable, and unstable records; `unreadable_entries` and
`unstable_entries` count unique `(side, path)` pairs across the scan and
content phases. Terminal replay validates both the mismatch and the
risk-evidence artifacts.

## Durable report contract

The terminal report contains at least:

- schema version, verification id, run id, and software version;
- source and destination endpoints, roots, and snapshot identifiers;
- request fingerprint, exclusions, mode, and comparison policy, plus the
  sample policy for sample mode;
- start/end time and whether the stability precondition was satisfied;
- entries scanned and compared by type;
- whether requested content work completed, and the regular files, logical
  bytes hashed, matches, and mismatches on each side;
- mismatches by kind, unreadable entries, unstable entries, and approved
  fidelity exceptions;
- references and SHA-256 digests for every mismatch JSONL object; and
- final `passed`, `mismatched`, `failed`, or `inconclusive` status.

Resume checkpoints cannot overwrite a prior terminal result.

## Scale architecture

Verification must remain bounded at billion-entry scale:

1. Independently scan source and destination into canonical verification
   indexes.
2. Hash-partition both indexes by raw path.
3. Merge one partition at a time to produce comparison shards and
   namespace/metadata mismatches.
4. Claim content-verification shards through the existing fenced claim protocol.
5. Stream source/destination content through independent libnfs contexts.
6. Publish immutable shard results and reduce them into one terminal report.

Memory is proportional to one hash partition, not the whole namespace. Shards
are independently retryable. A reclaimed verification shard may duplicate work,
but conditional terminal publication prevents double-counted results.

Verification traffic has separate connection, inflight, and bandwidth limits so
operators can protect production storage. The same latency and busy-share
telemetry used by migration is reported for both sides.

## Implementation sequence

### V1: complete metadata verifier — implemented

- Add a `migration-verify` library with versioned report and mismatch schemas.
- Add independent source and destination scans and the bounded path join.
- Compare namespace, type, size, configured POSIX metadata, symlink targets, and
  hardlink groupings.
- Add `vamoose verify --mode metadata`, resume checkpoints, JSON output, and the
  documented exit codes.
- Treat source/destination mutation and unsupported features as non-success.

The remaining V1 release qualification is hardware-backed NFS fault injection
and scale/performance evidence, not another in-process verifier design.

### V2: sampled content verification — implemented

- Deterministic sampling and mandatory risk selection, with automatic risk
  history discovery for configured runs.
- Bracketed, streaming SHA-256 over independent libnfs reads through the
  audited `nfs_fstat64` binding.
- Mismatch JSONL and a durable terminal report with the V2 counters.

Coordinator events remain a separate follow-up. `VerifyStarted` /
`VerifyCompleted` are intentionally denied on the worker event route, and the
standalone verifier is neither a registered worker nor an authenticated admin
command; connecting them needs a reviewed authenticated verifier ingest API and
bounded mismatch sampling, not fake events from the verifier.

The remaining V2 release qualification is the hardware pass for the content
bracket: the ignored `stat64/open/fstat64/read/fstat64/stat64` case in
`libnfs_ffi_smoke.rs` and `scripts/manual-verify.sh inject`, which asserts the
exact exit code, status, and mismatch-kind counts for injected equal,
same-size-corrupt, truncated, zero-length, unreadable, non-UTF-8, and
mid-read-mutated files. Neither has run against a VAST export yet.

### V3: distributed full verification

- Add verification shard plans, fenced claims, retries, and aggregation.
- Make `--mode full` the default.
- Surface verification progress, throughput, ETA, and mismatch counts in status
  and the TUI.
- Require a passing configured verification policy before a job becomes
  successfully finalized.

### V4: complete fidelity

- Add sparse extent, xattr, ACL, and special-file comparisons alongside the
  corresponding mover support.
- Add snapshot integration and durable snapshot identifiers where supported.

## Required test gates

Automated V1 tests inject and detect missing/extra paths, type and size
drift, mode/owner/mtime drift, symlink-target drift, hardlink membership
drift, unsupported special types, non-UTF-8 paths, invalid consistency
boundaries, and full-width NFS identifiers in SQLite. They also compile/link
the verifier through the workspace's pinned libnfs surface.

Automated V2 tests run without a live NFS or S3 server. Selection tests cover
insertion-order and restart invariance, seed sensitivity, non-UTF-8 paths,
`min(requested, eligible)`, mandatory risk exceeding the target, deterministic
reason union, every bucket transition, one eligible representative per
hardlink group, and ineligible risk targets. Content tests drive the bracket
through an in-memory fault-injecting reader: multi-chunk and short reads,
same-size corruption, zero-length EOF proof, early EOF and bytes beyond the
size, scan-to-open, handle, and path-replacement mutation, per-operation
failures on the correct side, close on every branch with close errors
retained, no digest mismatch from an unreadable or unstable side, status
priority, resume rereading only pending jobs, completion order not changing
the artifact bytes, tampered artifacts refused on replay, a refused context
producing a failed report, and workers reading sequentially through the
bounded queue. Staging tests cover missing/active/failed claims, malformed
sink rows, index ETag drift, and an epoch-2 shard's complete regular-file
population. Clap tests pin every flag, conflict, default, and rejection.

Production qualification still requires:

- the hardware pass of the content bracket and the injected scenarios above;
- verifier crash/restart under real NFS load and claim-reclaim races once
  distributed verification exists; and
- tampered or partially published checkpoints beyond the artifact digests
  already validated.

Hardware qualification covers empty trees, tiny-file trees, mixed enterprise
trees, multi-terabyte files, deep/hot directories, one-billion-entry indexes,
and source/destination server restarts. For every case, the expected mismatch
count is injected in advance and must equal the report exactly.

Performance results report the complete verification wall time, files/s,
logical GiB/s, CPU, memory, connection count, and source/destination busy share.
Comparisons with XCP use identical trees, hardware, network, cache state, and
verification depth, with at least three repetitions and raw result publication.
