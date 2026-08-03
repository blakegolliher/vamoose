//! Storage abstraction for the coord.
//!
//! `CoordStore` is the small set of object-store primitives the lease,
//! snapshot, event-log, and archive layers need. It exists so unit
//! tests can swap in an in-memory implementation without touching
//! S3 — the real-S3 path is covered by integration tests against
//! VAST (see `tests/`).
//!
//! ## Primitives
//!
//! - [`CoordStore::get`] / [`CoordStore::head`] — read with etag.
//! - [`CoordStore::put`] — unconditional last-write-wins.
//! - [`CoordStore::put_if_absent`] — atomic create. Returns
//!   [`PutOutcome::AlreadyExists`] if the key is present (412 on S3,
//!   the same primitive the v2 claim protocol uses).
//! - [`CoordStore::delete`] — unconditional delete. Idempotent.
//! - [`CoordStore::delete_if_match`] — atomic delete-if-etag-current.
//!   Same primitive the v2 claim protocol uses for safe takeover.
//! - [`CoordStore::list`] — prefix list, returns keys in lexical
//!   order. The event-log layer relies on this matching numeric `seq`
//!   order via the zero-padded chunk-key format in `crate::layout`.
//!
//! ## Implementations
//!
//! - [`S3Store`] — wraps [`migration_core::s3::S3Client`]. Production
//!   path.
//! - [`MemStore`] — `Mutex<BTreeMap>`. Used by every unit test in
//!   this crate. Behavior mirrors S3 in the ways the coord depends on
//!   (lexical list order, etag stability across reads, 412 semantics
//!   for `put_if_absent` and `delete_if_match`).

use crate::errors::{Error, Result};
use async_trait::async_trait;
use migration_core::claim::{ClaimStore, DeleteOutcome};
use migration_core::s3::S3Client;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// Result of a conditional create. `PUT If-None-Match: *` on S3
/// returns `412 PreconditionFailed` when the key exists; we surface
/// that as a typed variant rather than a generic error so the lease
/// logic can branch on it cleanly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutOutcome {
    /// Object was created. Returned etag is the new etag.
    Created(String),
    /// Object already existed; no write happened.
    AlreadyExists,
}

#[derive(Debug, Clone)]
pub struct ListEntry {
    pub key: String,
    pub etag: String,
    pub size: u64,
}

/// Object-store primitives the coord state-machine layers depend on.
///
/// All methods are `async`; implementations should be `Send + Sync`
/// so the coord's HTTP handlers can share a single store across
/// tasks.
#[async_trait]
pub trait CoordStore: Send + Sync + std::fmt::Debug {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>>;
    async fn head(&self, key: &str) -> Result<Option<String>>;
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<String>;
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<PutOutcome>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn delete_if_match(&self, key: &str, etag: &str) -> Result<DeleteOutcome>;
    async fn list(&self, prefix: &str) -> Result<Vec<ListEntry>>;
}

// =============================================================================
// S3 implementation
// =============================================================================

/// `CoordStore` over a real S3 endpoint. Wraps
/// [`migration_core::s3::S3Client`] so the VAST-specific TLS, profile,
/// and path-style addressing knobs are shared with the worker.
#[derive(Debug, Clone)]
pub struct S3Store {
    inner: S3Client,
}

impl S3Store {
    pub fn new(inner: S3Client) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &S3Client {
        &self.inner
    }
}

#[async_trait]
impl CoordStore for S3Store {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        Ok(<S3Client as ClaimStore>::get(&self.inner, key).await?)
    }

    async fn head(&self, key: &str) -> Result<Option<String>> {
        // ClaimStore::head_object returns (etag, body). We only want
        // the etag here; the body cost is one extra round-trip's worth
        // of bytes the SDK already paid for. Tighten this only if it
        // shows up in production profiles.
        match <S3Client as ClaimStore>::head_object(&self.inner, key).await? {
            Some((etag, _body)) => Ok(Some(etag)),
            None => Ok(None),
        }
    }

    async fn put(&self, key: &str, body: Vec<u8>) -> Result<String> {
        Ok(self.inner.put(key, body).await?)
    }

    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<PutOutcome> {
        match <S3Client as ClaimStore>::put_if_absent(&self.inner, key, body).await {
            Ok(etag) => Ok(PutOutcome::Created(etag)),
            Err(migration_core::Error::PreconditionFailed) => Ok(PutOutcome::AlreadyExists),
            Err(e) => Err(Error::Storage(e)),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await?;
        Ok(())
    }

    async fn delete_if_match(&self, key: &str, etag: &str) -> Result<DeleteOutcome> {
        Ok(<S3Client as ClaimStore>::delete_if_match(&self.inner, key, etag).await?)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ListEntry>> {
        let entries = <S3Client as ClaimStore>::list(&self.inner, prefix).await?;
        Ok(entries
            .into_iter()
            .map(|e| ListEntry {
                key: e.key,
                etag: e.etag,
                size: e.size,
            })
            .collect())
    }
}

// =============================================================================
// In-memory implementation (tests)
// =============================================================================

#[derive(Debug, Default)]
struct MemInner {
    /// Stable etag counter. Each successful write increments it and
    /// stamps the entry; reads return the stamped etag.
    next_etag: u64,
    /// Number of write calls (`put` + `put_if_absent`) observed,
    /// regardless of outcome. Test-only knob — lets tests assert
    /// "no writes happened" after a lease-lost fence.
    write_calls: u64,
    /// Chronological log of every store call (`"GET key"`,
    /// `"LIST prefix"`, ...). Test-only knob — lets tests assert
    /// which objects a read path actually touched (seq-aware chunk
    /// skipping, archive/live-path isolation).
    ops: Vec<String>,
    objects: BTreeMap<String, MemObject>,
}

#[derive(Debug, Clone)]
struct MemObject {
    body: Vec<u8>,
    etag: String,
}

/// In-memory `CoordStore`. Mirrors S3 in the behaviors the coord
/// relies on:
///
/// - Lexical list order (the `BTreeMap` gives us this for free).
/// - Stable etags within a single object lifetime — every write
///   produces a fresh etag.
/// - `put_if_absent` returns `AlreadyExists` if the key is present.
/// - `delete_if_match` distinguishes `Deleted` / `EtagMismatch` /
///   `NotFound` exactly as the S3 path does.
///
/// Backed by a `Mutex<MemInner>` — no `async` work happens inside the
/// critical section, so contention on the mutex is bounded.
#[derive(Debug, Default)]
pub struct MemStore {
    inner: Mutex<MemInner>,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn next_etag(inner: &mut MemInner) -> String {
        inner.next_etag += 1;
        format!("e{}", inner.next_etag)
    }

    /// Test-only knob — total number of objects currently stored.
    /// Useful for asserting that snapshot history pruning or archive
    /// cleanup left the right footprint.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Test-only knob — number of write calls (`put` +
    /// `put_if_absent`) seen so far, regardless of outcome. Lets a
    /// test snapshot the count, poke the code under test, and assert
    /// zero new writes (the lease-lost fence contract).
    pub fn write_count(&self) -> u64 {
        self.inner.lock().unwrap().write_calls
    }

    /// Test-only knob — chronological log of every store call seen so
    /// far, formatted `"<VERB> <key-or-prefix>"` (`GET`, `HEAD`,
    /// `PUT`, `PUT_IF_ABSENT`, `DELETE`, `DELETE_IF_MATCH`, `LIST`).
    /// Lets tests assert which objects a code path actually touched —
    /// e.g. that seq-aware reads skip low chunks, or that replay never
    /// reads `archivelogs/`.
    pub fn ops(&self) -> Vec<String> {
        self.inner.lock().unwrap().ops.clone()
    }

    /// Test-only knob — reset the op log (typically after setup so
    /// assertions only see the code under test).
    pub fn clear_ops(&self) {
        self.inner.lock().unwrap().ops.clear();
    }
}

#[async_trait]
impl CoordStore for MemStore {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let mut inner = self.inner.lock().unwrap();
        inner.ops.push(format!("GET {key}"));
        Ok(inner
            .objects
            .get(key)
            .map(|o| (o.body.clone(), o.etag.clone())))
    }

    async fn head(&self, key: &str) -> Result<Option<String>> {
        let mut inner = self.inner.lock().unwrap();
        inner.ops.push(format!("HEAD {key}"));
        Ok(inner.objects.get(key).map(|o| o.etag.clone()))
    }

    async fn put(&self, key: &str, body: Vec<u8>) -> Result<String> {
        let mut inner = self.inner.lock().unwrap();
        inner.ops.push(format!("PUT {key}"));
        inner.write_calls += 1;
        let etag = Self::next_etag(&mut inner);
        inner.objects.insert(
            key.to_string(),
            MemObject {
                body,
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }

    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<PutOutcome> {
        let mut inner = self.inner.lock().unwrap();
        inner.ops.push(format!("PUT_IF_ABSENT {key}"));
        inner.write_calls += 1;
        if inner.objects.contains_key(key) {
            return Ok(PutOutcome::AlreadyExists);
        }
        let etag = Self::next_etag(&mut inner);
        inner.objects.insert(
            key.to_string(),
            MemObject {
                body,
                etag: etag.clone(),
            },
        );
        Ok(PutOutcome::Created(etag))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.ops.push(format!("DELETE {key}"));
        inner.objects.remove(key);
        Ok(())
    }

    async fn delete_if_match(&self, key: &str, etag: &str) -> Result<DeleteOutcome> {
        let mut inner = self.inner.lock().unwrap();
        inner.ops.push(format!("DELETE_IF_MATCH {key}"));
        match inner.objects.get(key) {
            None => Ok(DeleteOutcome::NotFound),
            Some(o) if o.etag == etag => {
                inner.objects.remove(key);
                Ok(DeleteOutcome::Deleted)
            }
            Some(_) => Ok(DeleteOutcome::EtagMismatch),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ListEntry>> {
        let mut inner = self.inner.lock().unwrap();
        inner.ops.push(format!("LIST {prefix}"));
        Ok(inner
            .objects
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, o)| ListEntry {
                key: k.clone(),
                etag: o.etag.clone(),
                size: o.body.len() as u64,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> MemStore {
        MemStore::new()
    }

    #[tokio::test]
    async fn put_then_get() {
        let s = store();
        let etag = s.put("k", b"v".to_vec()).await.unwrap();
        let (body, got_etag) = s.get("k").await.unwrap().unwrap();
        assert_eq!(body, b"v");
        assert_eq!(got_etag, etag);
        assert_eq!(s.head("k").await.unwrap(), Some(etag));
    }

    #[tokio::test]
    async fn put_changes_etag() {
        let s = store();
        let e1 = s.put("k", b"a".to_vec()).await.unwrap();
        let e2 = s.put("k", b"b".to_vec()).await.unwrap();
        assert_ne!(e1, e2);
    }

    #[tokio::test]
    async fn put_if_absent_only_on_first_write() {
        let s = store();
        match s.put_if_absent("k", b"a".to_vec()).await.unwrap() {
            PutOutcome::Created(_) => {}
            o => panic!("expected Created, got {o:?}"),
        }
        match s.put_if_absent("k", b"b".to_vec()).await.unwrap() {
            PutOutcome::AlreadyExists => {}
            o => panic!("expected AlreadyExists, got {o:?}"),
        }
        let (body, _) = s.get("k").await.unwrap().unwrap();
        assert_eq!(body, b"a", "second put_if_absent must not overwrite");
    }

    #[tokio::test]
    async fn delete_idempotent() {
        let s = store();
        s.put("k", b"v".to_vec()).await.unwrap();
        s.delete("k").await.unwrap();
        s.delete("k").await.unwrap();
        assert!(s.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_if_match_three_outcomes() {
        let s = store();
        let etag = s.put("k", b"v".to_vec()).await.unwrap();

        assert_eq!(
            s.delete_if_match("k", "bogus").await.unwrap(),
            DeleteOutcome::EtagMismatch,
        );
        assert_eq!(
            s.delete_if_match("k", &etag).await.unwrap(),
            DeleteOutcome::Deleted,
        );
        assert_eq!(
            s.delete_if_match("k", &etag).await.unwrap(),
            DeleteOutcome::NotFound,
        );
    }

    #[tokio::test]
    async fn list_returns_lexical_order_within_prefix() {
        let s = store();
        // Insert out of lexical order to confirm the impl sorts.
        s.put("events/_cluster/00000000000000000010.jsonl", vec![1])
            .await
            .unwrap();
        s.put("events/_cluster/00000000000000000001.jsonl", vec![1])
            .await
            .unwrap();
        s.put("events/_cluster/00000000000000000005.jsonl", vec![1])
            .await
            .unwrap();
        s.put("state/snapshot.json", vec![9]).await.unwrap();

        let list = s.list("events/_cluster/").await.unwrap();
        let keys: Vec<_> = list.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "events/_cluster/00000000000000000001.jsonl",
                "events/_cluster/00000000000000000005.jsonl",
                "events/_cluster/00000000000000000010.jsonl",
            ],
        );
    }

    #[tokio::test]
    async fn list_excludes_other_prefixes() {
        let s = store();
        s.put("events/bobby/00000000000000000001.jsonl", vec![1])
            .await
            .unwrap();
        s.put("events/mary/00000000000000000001.jsonl", vec![1])
            .await
            .unwrap();
        let list = s.list("events/bobby/").await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].key, "events/bobby/00000000000000000001.jsonl");
    }
}
