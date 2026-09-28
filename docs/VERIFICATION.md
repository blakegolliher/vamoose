# Verification product contract

Status: implementation contract; no production verifier is shipped yet.

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

The product command will be:

```text
vamoose verify [--mode metadata|sample|full]
               [--sample-files N] [--seed SEED]
               [--report PATH] [--json]
```

`full` is the default. Operators must deliberately choose a weaker mode.

- `metadata` compares the complete namespace and supported metadata for every
  entry, but does not read regular-file content.
- `sample` performs the complete metadata comparison, plus content verification
  for a deterministic sample and every risk-selected file.
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

Verification against a live tree is allowed as a diagnostic mode, but its report
is explicitly `inconclusive` and cannot complete a migration job.

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

Content verification reads source and destination independently and computes a
SHA-256 digest while streaming. Source and destination reads may run
concurrently, but they do not share buffers or trust data produced during
migration. Reports record logical bytes hashed on each side.

`sample` selection is deterministic from `(run_id, seed, raw_path)`. It always
includes:

- every previous failure, retry, torn-copy, or fidelity-downgrade path;
- every file whose size or metadata differs before content selection;
- boundary sizes around mover bucket thresholds;
- at least one member of every detected hardlink group; and
- the remaining seeded sample, without replacement.

The report records the algorithm, seed, requested sample size, selected count,
and risk-selected count so the sample can be reproduced exactly.

## Mismatch records

Mismatch JSONL is append-only and partitioned by verification shard. Each record
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

High-cardinality details stay in JSONL. Coordinator events carry bounded samples
and aggregate counts only.

## Durable report

The terminal report contains at least:

- schema version, verification id, run id, and software/build provenance;
- source and destination endpoints, roots, and snapshot identifiers;
- mode, sample policy, seed, and comparison policy;
- start/end time and whether the stability precondition was satisfied;
- entries scanned and compared by type;
- regular files and logical bytes hashed on each side;
- mismatches by kind, unreadable entries, unstable entries, and approved
  fidelity exceptions;
- references and SHA-256 digests for every mismatch JSONL object; and
- final `passed`, `failed`, or `inconclusive` status.

Report and mismatch publication use temporary objects/files, fsync where local,
and conditional creation of the terminal report. Resume checkpoints cannot
overwrite a prior terminal result.

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

### V1: complete metadata verifier

- Add a `migration-verify` library with versioned report and mismatch schemas.
- Add independent source and destination scans and the bounded path join.
- Compare namespace, type, size, configured POSIX metadata, symlink targets, and
  hardlink groupings.
- Add `vamoose verify --mode metadata`, resume checkpoints, JSON output, and the
  documented exit codes.
- Treat source/destination mutation and unsupported features as non-success.

### V2: sampled content verification

- Add deterministic sampling and mandatory risk selection.
- Add bracketed, streaming SHA-256 over independent libnfs reads.
- Publish mismatch JSONL and a durable terminal report.
- Connect `VerifyStarted`, bounded `VerifyFileMismatch`, and `VerifyCompleted`
  events to real execution; the existing protocol shapes are not proof by
  themselves.

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

The verifier does not ship until automated tests prove that it detects injected:

- missing, extra, truncated, same-size-corrupted, and zero-length files;
- mode, uid, gid, and mtime drift;
- symlink-target and hardlink-group drift;
- non-UTF-8 path mismatches;
- read permission and mid-read mutation failures;
- verifier crash/restart and claim-reclaim races; and
- tampered or partially published checkpoints and reports.

Hardware qualification covers empty trees, tiny-file trees, mixed enterprise
trees, multi-terabyte files, deep/hot directories, one-billion-entry indexes,
and source/destination server restarts. For every case, the expected mismatch
count is injected in advance and must equal the report exactly.

Performance results report the complete verification wall time, files/s,
logical GiB/s, CPU, memory, connection count, and source/destination busy share.
Comparisons with XCP use identical trees, hardware, network, cache state, and
verification depth, with at least three repetitions and raw result publication.
