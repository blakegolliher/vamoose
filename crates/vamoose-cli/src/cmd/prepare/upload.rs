//! Verified upload of the canonical index and conditional creation of
//! `manifest.json`.
//!
//! Semantics (same contract as the harness this replaces):
//!
//! - Every shard in the bucket must be provably the one on disk: HEAD
//!   size plus the `vamoose-run-id` / `canonical-sha256` user metadata
//!   stamped at upload. Anything else is re-uploaded — unless a
//!   manifest already exists, in which case the index is immutable and
//!   a mismatch is an error.
//! - `manifest.json` is created with `If-None-Match: *`. If it already
//!   exists it must be semantically identical to what this run would
//!   write; a differing manifest means the bucket belongs to another run.
//! - The upload checkpoint is rewritten after every shard so a retry
//!   never re-hashes or re-uploads finished work.
//! - Shards are uploaded while the rewrite is still producing them
//!   ([`stream_uploads`]) and the local copy is removed once the bucket
//!   holds it, so scratch never has to fit the whole index: peak is the
//!   scan plus a shard or two. A shard that is gone from disk is
//!   accepted only when the checkpoint records the exact bytes the
//!   rewrite report describes.

use super::checkpoint::{canonical_json, read_json_opt, sha256_file, utc_now, write_json_atomic};
use anyhow::{Context, Result};
use async_trait::async_trait;
use migration_core::claim::ClaimStore;
use migration_core::records::{Endpoint, EndpointKind, Manifest, MigrationOptions, ShardEntry};
use migration_core::s3::{ObjectHead, S3Client};
use migration_core::time::UtcTime;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

pub(crate) const META_RUN_ID: &str = "vamoose-run-id";
pub(crate) const META_SHA256: &str = "canonical-sha256";

/// The bucket operations the upload needs, abstracted so the whole
/// stage runs against an in-memory store in tests.
#[async_trait]
pub(crate) trait IndexStore: Send + Sync {
    async fn head(&self, key: &str) -> Result<Option<ObjectHead>>;
    async fn upload_file(
        &self,
        key: &str,
        path: &Path,
        metadata: HashMap<String, String>,
    ) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    /// `Ok(true)` when created, `Ok(false)` when the key already existed.
    async fn create_if_absent(&self, key: &str, body: Vec<u8>) -> Result<bool>;
}

#[async_trait]
impl IndexStore for S3Client {
    async fn head(&self, key: &str) -> Result<Option<ObjectHead>> {
        Ok(self.head_meta(key).await?)
    }
    async fn upload_file(
        &self,
        key: &str,
        path: &Path,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        self.put_file_with_metadata(key, path, metadata).await?;
        Ok(())
    }
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(ClaimStore::get(self, key).await?.map(|(body, _etag)| body))
    }
    async fn create_if_absent(&self, key: &str, body: Vec<u8>) -> Result<bool> {
        match ClaimStore::put_if_absent(self, key, body).await {
            Ok(_) => Ok(true),
            Err(migration_core::errors::Error::PreconditionFailed) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

// ---------------------------------------------------------------------
// Rewrite report → upload plan
// ---------------------------------------------------------------------

/// The subset of `mig-walker-rewrite`'s report this stage consumes.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct RewriteReport {
    pub(crate) schema_version: u32,
    pub(crate) input_dir: String,
    pub(crate) output_dir: String,
    pub(crate) source_root: String,
    pub(crate) walker_version: String,
    pub(crate) complete: bool,
    pub(crate) shards: Vec<RewriteShard>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct RewriteShard {
    pub(crate) output_name: String,
    pub(crate) output_bytes: u64,
    pub(crate) output_sha256: String,
    pub(crate) rows: u64,
}

/// One canonical shard, verified on disk against its checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShardPlan {
    pub(crate) name: String,
    pub(crate) path: PathBuf,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

/// Re-verify one reported shard against the bytes on disk (size and
/// SHA256) so the upload never trusts a checkpoint over the bytes.
fn verify_shard_on_disk(output_dir: &Path, shard: &RewriteShard) -> Result<ShardPlan> {
    let path = output_dir.join(&shard.output_name);
    let size = std::fs::metadata(&path)
        .map(|m| m.len())
        .with_context(|| format!("canonical shard {} is missing", path.display()))?;
    if size != shard.output_bytes {
        anyhow::bail!(
            "canonical shard {} is {size} bytes; rewrite checkpoint says {}",
            path.display(),
            shard.output_bytes
        );
    }
    let digest = sha256_file(&path)?;
    if digest != shard.output_sha256 {
        anyhow::bail!(
            "canonical shard {} SHA256 does not match the rewrite checkpoint",
            path.display()
        );
    }
    Ok(ShardPlan {
        name: shard.output_name.clone(),
        path,
        rows: shard.rows,
        bytes: size,
        sha256: digest,
    })
}

/// A reported shard that is no longer on disk: fine when `uploaded`
/// records the very bytes the report describes (the streaming upload
/// removed it), an error otherwise.
fn plan_for_removed_shard(
    output_dir: &Path,
    shard: &RewriteShard,
    uploaded: Option<&UploadedShard>,
) -> Result<ShardPlan> {
    let path = output_dir.join(&shard.output_name);
    match uploaded {
        Some(rec) if rec.sha256 == shard.output_sha256 && rec.bytes == shard.output_bytes => {
            Ok(ShardPlan {
                name: shard.output_name.clone(),
                path,
                rows: shard.rows,
                bytes: rec.bytes,
                sha256: rec.sha256.clone(),
            })
        }
        Some(_) => anyhow::bail!(
            "canonical shard {} was uploaded and removed, but the upload checkpoint describes \
             different bytes than the rewrite report; start a fresh run (--fresh)",
            path.display()
        ),
        None => anyhow::bail!(
            "canonical shard {} is missing and was never uploaded; start a fresh run (--fresh)",
            path.display()
        ),
    }
}

/// Load the completed rewrite report and turn every shard into an
/// upload plan: verified on disk, or accepted as already uploaded and
/// removed when `uploaded` records its exact bytes.
pub(crate) fn load_rewrite_plan(
    report_path: &Path,
    uploaded: &[UploadedShard],
) -> Result<(RewriteReport, Vec<ShardPlan>)> {
    let report: RewriteReport = read_json_opt(report_path)?
        .ok_or_else(|| anyhow::anyhow!("rewrite report {} is missing", report_path.display()))?;
    if report.schema_version != 1 || !report.complete {
        anyhow::bail!(
            "rewrite report {} is not a completed schema-version-1 checkpoint",
            report_path.display()
        );
    }
    let output_dir = PathBuf::from(&report.output_dir);
    let mut plans = Vec::with_capacity(report.shards.len());
    for shard in &report.shards {
        let plan = if output_dir.join(&shard.output_name).exists() {
            verify_shard_on_disk(&output_dir, shard)?
        } else {
            let rec = uploaded.iter().find(|u| u.name == shard.output_name);
            plan_for_removed_shard(&output_dir, shard, rec)?
        };
        plans.push(plan);
    }
    plans.sort_by(|a, b| a.name.cmp(&b.name));
    if plans.is_empty() {
        anyhow::bail!(
            "rewrite report {} contains no shards",
            report_path.display()
        );
    }
    Ok((report, plans))
}

/// Stable identity of a rewrite so an upload checkpoint can refuse to
/// continue against a different rewrite of the same run. Built from the
/// rewrite's inputs — the scan it reads, the root it strips, the
/// walker that produced the scan — because the upload starts before the
/// shard list exists; the shards themselves are held to the checkpoint
/// per shard by size and SHA256.
pub(crate) fn rewrite_identity(input_dir: &str, source_root: &str, walker_version: &str) -> String {
    let stable = serde_json::json!({
        "schema_version": 1,
        "input_dir": input_dir,
        "source_root": source_root,
        "walker_version": walker_version,
    });
    super::checkpoint::sha256_bytes(&canonical_json(&stable))
}

// ---------------------------------------------------------------------
// Upload checkpoint + manifest
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct EndpointSpec {
    pub(crate) url: String,
    pub(crate) root: String,
}

/// Everything the upload's outcome depends on; a checkpoint whose
/// context differs is refused rather than reused.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct UploadContext {
    pub(crate) run_id: String,
    pub(crate) bucket: String,
    pub(crate) endpoint: String,
    pub(crate) rewrite_identity: String,
    pub(crate) source: EndpointSpec,
    pub(crate) dest: EndpointSpec,
    #[serde(default)]
    pub(crate) exclusions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct UploadedShard {
    pub(crate) name: String,
    pub(crate) key: String,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
    pub(crate) etag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct UploadCheckpoint {
    pub(crate) complete: bool,
    pub(crate) started_utc: String,
    pub(crate) updated_utc: String,
    pub(crate) context: UploadContext,
    /// Fixed at first attempt so retries reproduce an identical manifest.
    pub(crate) manifest_created_utc: String,
    pub(crate) shards: Vec<UploadedShard>,
    #[serde(default)]
    pub(crate) manifest_sha256: Option<String>,
    #[serde(default)]
    pub(crate) total_rows: u64,
}

/// Copy options recorded in the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CopyOptions {
    pub(crate) preserve_owner: bool,
    pub(crate) preserve_mode: bool,
    pub(crate) preserve_times: bool,
    pub(crate) preserve_xattr: bool,
}

#[cfg(test)]
pub(crate) struct UploadInputs<'a> {
    pub(crate) context: UploadContext,
    pub(crate) plans: &'a [ShardPlan],
    pub(crate) options: CopyOptions,
    pub(crate) checkpoint_path: &'a Path,
    pub(crate) manifest_path: &'a Path,
}

fn remote_matches(head: Option<&ObjectHead>, plan: &ShardPlan, run_id: &str) -> bool {
    let Some(head) = head else {
        return false;
    };
    head.size == plan.bytes
        && head.metadata.get(META_RUN_ID).map(String::as_str) == Some(run_id)
        && head.metadata.get(META_SHA256).map(String::as_str) == Some(plan.sha256.as_str())
}

fn build_manifest(
    ctx: &UploadContext,
    created_utc: &str,
    shards: &BTreeMap<String, UploadedShard>,
    options: CopyOptions,
) -> Result<Manifest> {
    let created = chrono::DateTime::parse_from_rfc3339(created_utc)
        .with_context(|| format!("manifest_created_utc {created_utc:?}"))?
        .with_timezone(&chrono::Utc);
    let mut entries: Vec<ShardEntry> = shards
        .values()
        .map(|s| ShardEntry {
            key: s.key.clone(),
            rows: s.rows,
            bytes: s.bytes,
            etag: s.etag.clone(),
        })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    let total_rows = entries.iter().map(|e| e.rows).sum();
    Ok(Manifest {
        format_version: migration_core::records::RUN_FORMAT_VERSION,
        run_id: ctx.run_id.clone(),
        created_utc: UtcTime(created),
        shards: entries,
        total_rows,
        source: Endpoint {
            kind: EndpointKind::Nfs,
            url: ctx.source.url.clone(),
            root: ctx.source.root.clone(),
        },
        dest: Endpoint {
            kind: EndpointKind::Nfs,
            url: ctx.dest.url.clone(),
            root: ctx.dest.root.clone(),
        },
        exclusions: ctx.exclusions.clone(),
        options: MigrationOptions {
            preserve_owner: options.preserve_owner,
            preserve_mode: options.preserve_mode,
            preserve_times: options.preserve_times,
            preserve_xattr: options.preserve_xattr,
            // Compatibility-reserved; the NFSv3 mover never selects a
            // server-side strategy. Recorded as `off` for parity with
            // manifests the previous harness produced.
            server_side_copy: migration_core::records::ServerSideCopy::Off,
        },
    })
}

/// Two manifests describe the same run if they agree on everything
/// except `created_utc`: run id, shards (keys, etags, rows, bytes),
/// endpoints, and options. Byte layout may differ between producers,
/// and the creation stamp is bookkeeping that a lost checkpoint would
/// otherwise regenerate.
fn same_manifest(existing: &[u8], ours: &Manifest) -> Result<bool> {
    let mut theirs: serde_json::Value = serde_json::from_slice(existing)
        .context("existing manifest.json in the bucket is not valid JSON")?;
    let mut mine = serde_json::to_value(ours)?;
    for v in [&mut theirs, &mut mine] {
        if let Some(obj) = v.as_object_mut() {
            obj.remove("created_utc");
        }
    }
    Ok(canonical_json(&theirs) == canonical_json(&mine))
}

/// `created_utc` of a published manifest for `run_id`, if the bucket
/// holds one, so a resumed upload reproduces the published stamp.
fn published_created_utc(existing: Option<&[u8]>, run_id: &str) -> Option<String> {
    let manifest: Manifest = serde_json::from_slice(existing?).ok()?;
    (manifest.run_id == run_id).then(|| {
        manifest
            .created_utc
            .0
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string()
    })
}

/// One run's upload, resumable: the checkpoint on disk, what the
/// bucket held when it opened, and the shards known to be in place.
pub(crate) struct UploadSession<'a> {
    store: &'a dyn IndexStore,
    context: UploadContext,
    checkpoint_path: PathBuf,
    checkpoint: UploadCheckpoint,
    manifest_before: Option<Vec<u8>>,
    done: BTreeMap<String, UploadedShard>,
}

impl<'a> UploadSession<'a> {
    /// Load or create the checkpoint (refusing one from another run,
    /// bucket, or rewrite) and read the manifest the bucket holds now.
    pub(crate) async fn open(
        store: &'a dyn IndexStore,
        context: UploadContext,
        checkpoint_path: &Path,
    ) -> Result<Self> {
        let mut checkpoint = match read_json_opt::<UploadCheckpoint>(checkpoint_path)? {
            Some(cp) => {
                if cp.context != context {
                    anyhow::bail!(
                        "upload checkpoint {} belongs to a different run, bucket, or rewrite; \
                         start a fresh run (--fresh) or remove the checkpoint deliberately",
                        checkpoint_path.display()
                    );
                }
                cp
            }
            None => {
                let now = utc_now();
                let cp = UploadCheckpoint {
                    complete: false,
                    started_utc: now.clone(),
                    updated_utc: now.clone(),
                    context: context.clone(),
                    manifest_created_utc: now,
                    shards: Vec::new(),
                    manifest_sha256: None,
                    total_rows: 0,
                };
                write_json_atomic(checkpoint_path, &cp)?;
                cp
            }
        };

        let manifest_before = store
            .get(migration_core::layout::MANIFEST_KEY)
            .await
            .context("reading manifest.json")?;
        if checkpoint.shards.is_empty() {
            if let Some(stamp) = published_created_utc(manifest_before.as_deref(), &context.run_id)
            {
                checkpoint.manifest_created_utc = stamp;
                write_json_atomic(checkpoint_path, &checkpoint)?;
            }
        }
        let done = checkpoint
            .shards
            .iter()
            .cloned()
            .map(|s| (s.name.clone(), s))
            .collect();
        Ok(Self {
            store,
            context,
            checkpoint_path: checkpoint_path.to_path_buf(),
            checkpoint,
            manifest_before,
            done,
        })
    }

    /// Shards the checkpoint records as in the bucket.
    pub(crate) fn uploaded(&self) -> Vec<UploadedShard> {
        self.done.values().cloned().collect()
    }

    /// Whether the checkpoint already records exactly this shard.
    pub(crate) fn is_recorded(&self, plan: &ShardPlan) -> bool {
        self.done
            .get(&plan.name)
            .is_some_and(|rec| rec.sha256 == plan.sha256 && rec.bytes == plan.bytes)
    }

    /// Make the bucket hold `plan`: verify what is there, upload when
    /// it is absent or different (before a manifest exists), and record
    /// it. Returns `true` when bytes were sent.
    pub(crate) async fn ensure_shard(&mut self, plan: &ShardPlan) -> Result<bool> {
        let key = format!("{}{}", migration_core::layout::INDEX_PREFIX, plan.name);
        let mut head = self
            .store
            .head(&key)
            .await
            .with_context(|| format!("HEAD {key}"))?;
        let uploaded = if !remote_matches(head.as_ref(), plan, &self.context.run_id) {
            if self.manifest_before.is_some() {
                anyhow::bail!(
                    "manifest.json already exists in the bucket but immutable shard {key} does \
                     not match this run's index; this bucket belongs to another run"
                );
            }
            if !plan.path.is_file() {
                anyhow::bail!(
                    "shard {key} is not in the bucket (or not this run's bytes) and its local \
                     copy {} is gone; start a fresh run (--fresh)",
                    plan.path.display()
                );
            }
            println!("  upload {key} ({} bytes, {} rows)", plan.bytes, plan.rows);
            let metadata = HashMap::from([
                (META_RUN_ID.to_string(), self.context.run_id.clone()),
                (META_SHA256.to_string(), plan.sha256.clone()),
            ]);
            self.store
                .upload_file(&key, &plan.path, metadata)
                .await
                .with_context(|| format!("uploading {key}"))?;
            head = self
                .store
                .head(&key)
                .await
                .with_context(|| format!("HEAD {key}"))?;
            if !remote_matches(head.as_ref(), plan, &self.context.run_id) {
                anyhow::bail!("uploaded shard failed authoritative HEAD validation: {key}");
            }
            true
        } else {
            println!("  verified {key} (already in bucket)");
            false
        };
        let etag = head.map(|h| h.etag).unwrap_or_default();
        if etag.is_empty() {
            anyhow::bail!("S3 returned an empty ETag for {key}");
        }
        self.done.insert(
            plan.name.clone(),
            UploadedShard {
                name: plan.name.clone(),
                key,
                rows: plan.rows,
                bytes: plan.bytes,
                sha256: plan.sha256.clone(),
                etag,
            },
        );
        self.checkpoint.shards = self.done.values().cloned().collect();
        self.checkpoint.updated_utc = utc_now();
        self.checkpoint.complete = false;
        write_json_atomic(&self.checkpoint_path, &self.checkpoint)?;
        Ok(uploaded)
    }

    /// Build the manifest from every recorded shard, write it locally,
    /// and create it in the bucket (or accept an identical one already
    /// there). Returns the manifest now in the bucket.
    pub(crate) async fn publish(
        mut self,
        options: CopyOptions,
        manifest_path: &Path,
    ) -> Result<Manifest> {
        let manifest = build_manifest(
            &self.context,
            &self.checkpoint.manifest_created_utc,
            &self.done,
            options,
        )?;
        let manifest_bytes = canonical_json(&serde_json::to_value(&manifest)?);
        if let Some(parent) = manifest_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(manifest_path, &manifest_bytes)
            .with_context(|| format!("writing {}", manifest_path.display()))?;

        let published: Vec<u8> = match self.manifest_before.take() {
            Some(existing) => existing,
            None => {
                if self
                    .store
                    .create_if_absent(migration_core::layout::MANIFEST_KEY, manifest_bytes.clone())
                    .await
                    .context("conditional PUT of manifest.json")?
                {
                    manifest_bytes.clone()
                } else {
                    // A concurrent retry won the create; read what it wrote.
                    self.store
                        .get(migration_core::layout::MANIFEST_KEY)
                        .await?
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "manifest.json conditional create returned 'exists' but a GET \
                                 found nothing"
                            )
                        })?
                }
            }
        };
        if !same_manifest(&published, &manifest)? {
            anyhow::bail!(
                "existing manifest.json in bucket {} differs from this run's index; the bucket \
                 belongs to another run",
                self.context.bucket
            );
        }

        self.checkpoint.complete = true;
        self.checkpoint.updated_utc = utc_now();
        self.checkpoint.manifest_sha256 = Some(super::checkpoint::sha256_bytes(&manifest_bytes));
        self.checkpoint.total_rows = manifest.total_rows;
        write_json_atomic(&self.checkpoint_path, &self.checkpoint)?;
        Ok(manifest)
    }
}

/// How many shards the rewrite report lists right now (0 before the
/// report exists) — the rewrite's own progress, for the operator.
pub(crate) fn reported_shard_count(report_path: &Path) -> Result<usize> {
    Ok(read_json_opt::<RewriteReport>(report_path)?.map_or(0, |r| r.shards.len()))
}

/// Upload the shards the rewrite report lists that this invocation has
/// not handled yet, removing each local copy once the bucket holds it
/// (unless `keep_local`). Safe to call while the rewrite is still
/// running: the report is rewritten atomically after every shard and
/// only lists finalized files. Returns how many shards were handled.
///
/// A listed shard that is already gone from disk must be one the
/// checkpoint records with the report's bytes — an earlier invocation
/// streamed it — otherwise the run cannot be completed from here.
pub(crate) async fn stream_uploads(
    session: &mut UploadSession<'_>,
    report_path: &Path,
    seen: &mut BTreeSet<String>,
    keep_local: bool,
) -> Result<usize> {
    let Some(report) = read_json_opt::<RewriteReport>(report_path)? else {
        return Ok(0);
    };
    let output_dir = PathBuf::from(&report.output_dir);
    let mut pending: Vec<&RewriteShard> = report
        .shards
        .iter()
        .filter(|s| !seen.contains(&s.output_name))
        .collect();
    pending.sort_by(|a, b| a.output_name.cmp(&b.output_name));
    let mut handled = 0;
    for shard in pending {
        let plan = if output_dir.join(&shard.output_name).exists() {
            verify_shard_on_disk(&output_dir, shard)?
        } else {
            let rec = session.done.get(&shard.output_name);
            plan_for_removed_shard(&output_dir, shard, rec)?
        };
        if plan.path.is_file() || !session.is_recorded(&plan) {
            session.ensure_shard(&plan).await?;
        }
        if !keep_local && plan.path.is_file() {
            std::fs::remove_file(&plan.path)
                .with_context(|| format!("removing uploaded shard {}", plan.path.display()))?;
        }
        seen.insert(shard.output_name.clone());
        handled += 1;
    }
    Ok(handled)
}

/// Upload every planned shard and publish: the non-streaming shape,
/// kept for the tests that pin the bucket contract.
#[cfg(test)]
pub(crate) async fn upload_index(
    store: &dyn IndexStore,
    inputs: UploadInputs<'_>,
) -> Result<Manifest> {
    let UploadInputs {
        context,
        plans,
        options,
        checkpoint_path,
        manifest_path,
    } = inputs;
    let mut session = UploadSession::open(store, context, checkpoint_path).await?;
    for plan in plans {
        session.ensure_shard(plan).await?;
    }
    session.publish(options, manifest_path).await
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Mutex;

    type Stored = (Vec<u8>, HashMap<String, String>);

    /// In-memory bucket: objects with metadata, plus a log of uploads.
    #[derive(Default)]
    pub(crate) struct MemStore {
        objects: Mutex<HashMap<String, Stored>>,
        pub(crate) uploads: Mutex<Vec<String>>,
    }

    impl MemStore {
        fn etag_of(body: &[u8]) -> String {
            super::super::checkpoint::sha256_bytes(body)[..16].to_string()
        }
        pub(crate) fn keys(&self) -> Vec<String> {
            let mut k: Vec<String> = self.objects.lock().unwrap().keys().cloned().collect();
            k.sort();
            k
        }
        pub(crate) fn put_raw(&self, key: &str, body: &[u8], metadata: HashMap<String, String>) {
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_string(), (body.to_vec(), metadata));
        }
    }

    #[async_trait]
    impl IndexStore for MemStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectHead>> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(key)
                .map(|(body, meta)| ObjectHead {
                    etag: Self::etag_of(body),
                    size: body.len() as u64,
                    metadata: meta.clone(),
                }))
        }
        async fn upload_file(
            &self,
            key: &str,
            path: &Path,
            metadata: HashMap<String, String>,
        ) -> Result<()> {
            let body = std::fs::read(path)?;
            self.uploads.lock().unwrap().push(key.to_string());
            self.put_raw(key, &body, metadata);
            Ok(())
        }
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(key)
                .map(|(b, _)| b.clone()))
        }
        async fn create_if_absent(&self, key: &str, body: Vec<u8>) -> Result<bool> {
            let mut objects = self.objects.lock().unwrap();
            if objects.contains_key(key) {
                return Ok(false);
            }
            objects.insert(key.to_string(), (body, HashMap::new()));
            Ok(true)
        }
    }

    pub(crate) struct Fixture {
        pub(crate) dir: tempfile::TempDir,
        pub(crate) report_path: PathBuf,
    }

    /// A completed rewrite: two shards on disk plus a matching report.
    pub(crate) fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("canonical");
        std::fs::create_dir_all(&out).unwrap();
        let mut shards = Vec::new();
        for (i, body) in [b"shard-zero".as_slice(), b"shard-one-longer".as_slice()]
            .iter()
            .enumerate()
        {
            let name = format!("part-{i:04}.parquet");
            std::fs::write(out.join(&name), body).unwrap();
            shards.push(RewriteShard {
                output_name: name,
                output_bytes: body.len() as u64,
                output_sha256: super::super::checkpoint::sha256_bytes(body),
                rows: 10 * (i as u64 + 1),
            });
        }
        let report = RewriteReport {
            schema_version: 1,
            input_dir: "/scan".into(),
            output_dir: out.to_string_lossy().into_owned(),
            source_root: "/data".into(),
            walker_version: "nfs-walker 0.1.0".into(),
            complete: true,
            shards,
        };
        let report_path = dir.path().join("rewrite.json");
        write_json_atomic(&report_path, &report).unwrap();
        Fixture { dir, report_path }
    }

    pub(crate) fn context(rewrite: &RewriteReport) -> UploadContext {
        UploadContext {
            run_id: "run-1".into(),
            bucket: "b".into(),
            endpoint: "https://s3.example.test".into(),
            rewrite_identity: rewrite_identity(
                &rewrite.input_dir,
                &rewrite.source_root,
                &rewrite.walker_version,
            ),
            source: EndpointSpec {
                url: "nfs://src/export".into(),
                root: "/data".into(),
            },
            dest: EndpointSpec {
                url: "nfs://dst/export".into(),
                root: "/".into(),
            },
            exclusions: vec![],
        }
    }

    const OPTS: CopyOptions = CopyOptions {
        preserve_owner: true,
        preserve_mode: true,
        preserve_times: true,
        preserve_xattr: false,
    };

    async fn run_upload(store: &MemStore, fx: &Fixture) -> Result<Manifest> {
        let (report, plans) = load_rewrite_plan(&fx.report_path, &[])?;
        upload_index(
            store,
            UploadInputs {
                context: context(&report),
                plans: &plans,
                options: OPTS,
                checkpoint_path: &fx.dir.path().join("upload.json"),
                manifest_path: &fx.dir.path().join("manifest.json"),
            },
        )
        .await
    }

    #[test]
    fn load_rewrite_plan_verifies_bytes_on_disk() {
        let fx = fixture();
        let (_, plans) = load_rewrite_plan(&fx.report_path, &[]).unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].name, "part-0000.parquet");
        // Tamper with a shard: the checkpoint no longer describes it.
        std::fs::write(
            fx.dir.path().join("canonical/part-0000.parquet"),
            b"shard-ZERO",
        )
        .unwrap();
        let err = load_rewrite_plan(&fx.report_path, &[]).unwrap_err();
        assert!(format!("{err:#}").contains("SHA256"), "{err:#}");
    }

    #[tokio::test]
    async fn fresh_upload_publishes_every_shard_and_a_manifest() {
        let fx = fixture();
        let store = MemStore::default();
        let manifest = run_upload(&store, &fx).await.unwrap();
        assert_eq!(
            store.keys(),
            vec![
                "index/part-0000.parquet",
                "index/part-0001.parquet",
                "manifest.json"
            ]
        );
        assert_eq!(manifest.run_id, "run-1");
        assert_eq!(manifest.total_rows, 30);
        assert_eq!(manifest.shards.len(), 2);
        assert_eq!(manifest.shards[0].key, "index/part-0000.parquet");
        assert_eq!(manifest.source.root, "/data");
        assert!(!manifest.options.preserve_xattr);
        assert_eq!(
            serde_json::to_value(&manifest).unwrap()["options"]["server_side_copy"],
            "off"
        );
        assert_eq!(
            manifest.format_version,
            migration_core::records::RUN_FORMAT_VERSION
        );
        // The worker's own manifest parser accepts what we published.
        let published = store.get("manifest.json").await.unwrap().unwrap();
        let parsed: Manifest = serde_json::from_slice(&published).unwrap();
        assert_eq!(parsed.shards[1].etag, manifest.shards[1].etag);
        let cp: UploadCheckpoint = read_json_opt(&fx.dir.path().join("upload.json"))
            .unwrap()
            .unwrap();
        assert!(cp.complete);
        assert_eq!(cp.total_rows, 30);
    }

    #[tokio::test]
    async fn rerun_uploads_nothing_and_accepts_the_identical_manifest() {
        let fx = fixture();
        let store = MemStore::default();
        run_upload(&store, &fx).await.unwrap();
        store.uploads.lock().unwrap().clear();
        run_upload(&store, &fx).await.unwrap();
        assert!(
            store.uploads.lock().unwrap().is_empty(),
            "verified shards are not re-sent"
        );
    }

    /// A lost upload checkpoint (or a rerun from another host) must
    /// re-verify against the published manifest, not refuse it over
    /// the regenerated creation stamp.
    #[tokio::test]
    async fn lost_checkpoint_resumes_against_the_published_manifest() {
        let fx = fixture();
        let store = MemStore::default();
        let first = run_upload(&store, &fx).await.unwrap();
        std::fs::remove_file(fx.dir.path().join("upload.json")).unwrap();
        store.uploads.lock().unwrap().clear();
        let again = run_upload(&store, &fx).await.unwrap();
        assert!(store.uploads.lock().unwrap().is_empty());
        assert_eq!(
            again.created_utc.0, first.created_utc.0,
            "published stamp adopted"
        );
        let cp: UploadCheckpoint = read_json_opt(&fx.dir.path().join("upload.json"))
            .unwrap()
            .unwrap();
        assert!(cp.complete);
    }

    #[tokio::test]
    async fn interrupted_upload_resumes_only_the_missing_shard() {
        let fx = fixture();
        let store = MemStore::default();
        let (report, plans) = load_rewrite_plan(&fx.report_path, &[]).unwrap();
        // Pretend the first shard landed with the right stamps earlier.
        store.put_raw(
            "index/part-0000.parquet",
            b"shard-zero",
            HashMap::from([
                (META_RUN_ID.to_string(), "run-1".to_string()),
                (META_SHA256.to_string(), plans[0].sha256.clone()),
            ]),
        );
        let _ = report;
        run_upload(&store, &fx).await.unwrap();
        assert_eq!(
            *store.uploads.lock().unwrap(),
            vec!["index/part-0001.parquet".to_string()]
        );
    }

    #[tokio::test]
    async fn stale_shard_is_replaced_before_the_manifest_exists() {
        let fx = fixture();
        let store = MemStore::default();
        store.put_raw("index/part-0000.parquet", b"old bytes", HashMap::new());
        run_upload(&store, &fx).await.unwrap();
        let body = store.get("index/part-0000.parquet").await.unwrap().unwrap();
        assert_eq!(body, b"shard-zero");
    }

    #[tokio::test]
    async fn mismatched_shard_under_an_existing_manifest_is_refused() {
        let fx = fixture();
        let store = MemStore::default();
        store.put_raw("manifest.json", b"{}", HashMap::new());
        store.put_raw("index/part-0000.parquet", b"old bytes", HashMap::new());
        let err = run_upload(&store, &fx).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("belongs to another run"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn differing_existing_manifest_is_refused_after_shards_verify() {
        let fx = fixture();
        let store = MemStore::default();
        run_upload(&store, &fx).await.unwrap();
        // Same shards, but somebody else's manifest.
        let other = serde_json::json!({"run_id": "someone-else"});
        store.put_raw(
            "manifest.json",
            &serde_json::to_vec(&other).unwrap(),
            HashMap::new(),
        );
        std::fs::remove_file(fx.dir.path().join("upload.json")).unwrap();
        let err = run_upload(&store, &fx).await.unwrap_err();
        assert!(format!("{err:#}").contains("differs"), "{err:#}");
    }

    #[tokio::test]
    async fn checkpoint_from_another_context_is_refused() {
        let fx = fixture();
        let store = MemStore::default();
        let (report, plans) = load_rewrite_plan(&fx.report_path, &[]).unwrap();
        let mut ctx = context(&report);
        ctx.run_id = "run-other".into();
        let cp_path = fx.dir.path().join("upload.json");
        upload_index(
            &store,
            UploadInputs {
                context: ctx,
                plans: &plans,
                options: OPTS,
                checkpoint_path: &cp_path,
                manifest_path: &fx.dir.path().join("manifest.json"),
            },
        )
        .await
        .unwrap();
        let err = run_upload(&store, &fx).await.unwrap_err();
        assert!(format!("{err:#}").contains("different run"), "{err:#}");
    }

    #[test]
    fn rewrite_identity_follows_the_inputs_not_the_shards() {
        let a = rewrite_identity("/scan/attempt-0001", "/", "nfs-walker 0.1.0");
        assert_eq!(
            a,
            rewrite_identity("/scan/attempt-0001", "/", "nfs-walker 0.1.0")
        );
        assert_ne!(
            a,
            rewrite_identity("/scan/attempt-0002", "/", "nfs-walker 0.1.0")
        );
        assert_ne!(
            a,
            rewrite_identity("/scan/attempt-0001", "/data", "nfs-walker 0.1.0")
        );
        assert_ne!(
            a,
            rewrite_identity("/scan/attempt-0001", "/", "nfs-walker 0.2.0")
        );
    }

    /// Write the fixture's report with only the first `n` shards, as
    /// the rewrite does after each shard (`complete` on the last).
    fn write_partial_report(fx: &Fixture, full: &RewriteReport, n: usize, complete: bool) {
        let mut report = full.clone();
        report.shards.truncate(n);
        report.complete = complete;
        write_json_atomic(&fx.report_path, &report).unwrap();
    }

    /// Streaming: each shard the report lists is uploaded and its local
    /// copy removed; the completed report then plans every shard from
    /// the checkpoint, and the manifest covers them all.
    #[tokio::test]
    async fn streaming_uploads_as_the_report_grows_and_removes_local_copies() {
        let fx = fixture();
        let full: RewriteReport = read_json_opt(&fx.report_path).unwrap().unwrap();
        let store = MemStore::default();
        let cp_path = fx.dir.path().join("upload.json");
        let mut session = UploadSession::open(&store, context(&full), &cp_path)
            .await
            .unwrap();
        let mut seen = BTreeSet::new();
        let canonical = fx.dir.path().join("canonical");

        // Nothing reported yet.
        std::fs::remove_file(&fx.report_path).unwrap();
        assert_eq!(
            stream_uploads(&mut session, &fx.report_path, &mut seen, false)
                .await
                .unwrap(),
            0
        );

        write_partial_report(&fx, &full, 1, false);
        assert_eq!(
            stream_uploads(&mut session, &fx.report_path, &mut seen, false)
                .await
                .unwrap(),
            1
        );
        assert_eq!(store.keys(), vec!["index/part-0000.parquet"]);
        assert!(!canonical.join("part-0000.parquet").exists(), "removed");
        assert!(
            canonical.join("part-0001.parquet").exists(),
            "not yet reported"
        );
        // Same report again: nothing new.
        assert_eq!(
            stream_uploads(&mut session, &fx.report_path, &mut seen, false)
                .await
                .unwrap(),
            0
        );

        write_partial_report(&fx, &full, 2, true);
        assert_eq!(
            stream_uploads(&mut session, &fx.report_path, &mut seen, false)
                .await
                .unwrap(),
            1
        );
        assert!(!canonical.join("part-0001.parquet").exists());
        assert_eq!(*store.uploads.lock().unwrap(), store.keys());

        // The final plan accepts both shards from the checkpoint alone…
        let (_, plans) = load_rewrite_plan(&fx.report_path, &session.uploaded()).unwrap();
        assert_eq!(plans.len(), 2);
        assert!(plans.iter().all(|p| session.is_recorded(p)));
        // …and without the checkpoint they are simply missing.
        let err = load_rewrite_plan(&fx.report_path, &[]).unwrap_err();
        assert!(format!("{err:#}").contains("never uploaded"), "{err:#}");

        let manifest = session
            .publish(OPTS, &fx.dir.path().join("manifest.json"))
            .await
            .unwrap();
        assert_eq!(manifest.shards.len(), 2);
        assert_eq!(manifest.total_rows, 30);
        let cp: UploadCheckpoint = read_json_opt(&cp_path).unwrap().unwrap();
        assert!(cp.complete);
    }

    /// A resumed invocation sees shards that an earlier one streamed
    /// and removed: accepted from the checkpoint, never re-sent; a
    /// checkpoint that describes other bytes is refused.
    #[tokio::test]
    async fn resumed_streaming_accepts_removed_shards_only_from_a_matching_checkpoint() {
        let fx = fixture();
        let full: RewriteReport = read_json_opt(&fx.report_path).unwrap().unwrap();
        let store = MemStore::default();
        let cp_path = fx.dir.path().join("upload.json");
        {
            let mut session = UploadSession::open(&store, context(&full), &cp_path)
                .await
                .unwrap();
            let mut seen = BTreeSet::new();
            stream_uploads(&mut session, &fx.report_path, &mut seen, false)
                .await
                .unwrap();
        }
        store.uploads.lock().unwrap().clear();

        // New invocation, fresh `seen`: both shards gone from disk.
        let mut session = UploadSession::open(&store, context(&full), &cp_path)
            .await
            .unwrap();
        let mut seen = BTreeSet::new();
        assert_eq!(
            stream_uploads(&mut session, &fx.report_path, &mut seen, false)
                .await
                .unwrap(),
            2
        );
        assert!(store.uploads.lock().unwrap().is_empty());

        // Checkpoint disagrees with the report about a removed shard.
        let mut report = full.clone();
        report.shards[1].output_sha256 = "0".repeat(64);
        write_json_atomic(&fx.report_path, &report).unwrap();
        let err = load_rewrite_plan(&fx.report_path, &session.uploaded()).unwrap_err();
        assert!(format!("{err:#}").contains("different bytes"), "{err:#}");
        let mut seen = BTreeSet::new();
        let err = stream_uploads(&mut session, &fx.report_path, &mut seen, false)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("different bytes"), "{err:#}");
    }

    #[tokio::test]
    async fn keep_local_leaves_uploaded_shards_on_disk() {
        let fx = fixture();
        let full: RewriteReport = read_json_opt(&fx.report_path).unwrap().unwrap();
        let store = MemStore::default();
        let mut session =
            UploadSession::open(&store, context(&full), &fx.dir.path().join("upload.json"))
                .await
                .unwrap();
        let mut seen = BTreeSet::new();
        stream_uploads(&mut session, &fx.report_path, &mut seen, true)
            .await
            .unwrap();
        assert_eq!(store.keys().len(), 2);
        assert!(fx.dir.path().join("canonical/part-0000.parquet").exists());
        assert!(fx.dir.path().join("canonical/part-0001.parquet").exists());
    }
}
