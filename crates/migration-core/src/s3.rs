//! S3 client wrapper.
//!
//! Implements [`crate::claim::ClaimStore`] over `aws-sdk-s3`. Configured
//! to talk to a custom endpoint (VAST S3) with path-style addressing.
//!
//! The conditional primitives this code relies on (claim protocol v2):
//!
//! - `PUT If-None-Match: *`  — atomic create-if-absent. Returns 412
//!   if the object exists. Empirically enforced on VAST S3.
//! - `DELETE If-Match: <etag>` — atomic delete-if-current. Returns
//!   412 if the etag doesn't match, 404 if the object is absent.
//!   Documented and empirically enforced on VAST S3.
//! - HEAD / GET — used for etag-compare in `claim::refresh`. The
//!   implementation here uses GET (claim objects are a few hundred
//!   bytes; saves the HEAD round-trip when the body is needed).
//!
//! v2 explicitly does **not** use `PUT If-Match` for ownership
//! transitions: that primitive is not enforced on VAST endpoints
//! (silently overwrites). See
//! `docs/work-items/CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md`.

use crate::claim::{ClaimStore, DeleteOutcome, ListEntry};
use crate::errors::{Error, Result};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::Client;
use tokio::io::AsyncWriteExt;

/// What an HTTP HEAD reports about an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectHead {
    /// Unquoted etag.
    pub etag: String,
    pub size: u64,
    /// User metadata (`x-amz-meta-*`), keys lower-cased.
    pub metadata: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct S3Client {
    inner: Client,
    bucket: String,
    /// Key prefix every logical key is placed under — `""` or
    /// `"<path>/"` (see [`normalize_prefix`]). Lets several runs share
    /// one bucket: `[run] prefix = "v4"` puts this run's
    /// `manifest.json`, `shards/`, `index/`, `state/`… under `v4/`
    /// and nothing above the client ever sees the prefix.
    prefix: String,
}

/// Canonical form of a configured key prefix: no leading slash, one
/// trailing slash, empty stays empty. `"v4"`, `"/v4/"`, `"v4//"` all
/// become `"v4/"`; `"a/b"` becomes `"a/b/"`.
pub fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim().trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        let parts: Vec<&str> = trimmed.split('/').filter(|p| !p.is_empty()).collect();
        format!("{}/", parts.join("/"))
    }
}

impl S3Client {
    pub fn new(inner: Client, bucket: impl Into<String>) -> Self {
        Self {
            inner,
            bucket: bucket.into(),
            prefix: String::new(),
        }
    }

    /// Place every key under `prefix` (normalized). An empty prefix
    /// is the bucket root, as before.
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = normalize_prefix(prefix);
        self
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The configured prefix in canonical form (`""` or `"x/"`).
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// `s3://bucket/prefix` — what to print where the bucket alone
    /// used to be shown.
    pub fn location(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }

    /// The wire key for a logical key.
    fn object_key(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }

    /// Build an `S3Client` from environment + endpoint URL. The
    /// endpoint is the VAST S3 cluster URL.
    ///
    /// Implementation note: use path-style addressing
    /// (`endpoint/bucket/key`) rather than virtual-host-style — VAST
    /// S3 deployments are typically reached by IP or short hostname
    /// where `bucket.endpoint` doesn't resolve.
    pub async fn from_env(endpoint_url: &str, region: &str, bucket: &str) -> Result<Self> {
        Self::from_config(endpoint_url, region, bucket, None, true).await
    }

    /// Build an `S3Client` with optional credentials profile and TLS
    /// verification toggle. Both knobs exist for VAST lab workflows
    /// (custom profile names, self-signed certs); the defaults match
    /// `from_env`.
    ///
    /// `verify_tls = false` plumbs a custom rustls `ClientConfig` whose
    /// `ServerCertVerifier` accepts every certificate. Equivalent to
    /// `aws-cli --no-verify-ssl`. Do not use against production
    /// endpoints — silently accepting any certificate defeats TLS.
    pub async fn from_config(
        endpoint_url: &str,
        region: &str,
        bucket: &str,
        profile: Option<&str>,
        verify_tls: bool,
    ) -> Result<Self> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .endpoint_url(endpoint_url);

        if let Some(p) = profile {
            // Pin credentials to a named profile. Without this the SDK
            // walks the default chain (env, config files, IMDS), which
            // fails when the operator is using a non-default profile —
            // and especially when running under `sudo`, where root's
            // HOME points elsewhere from the user's `~/.aws/credentials`.
            let creds = aws_config::profile::ProfileFileCredentialsProvider::builder()
                .profile_name(p)
                .build();
            loader = loader.credentials_provider(creds);
        }

        let aws_cfg = loader.load().await;

        let mut s3_cfg = aws_sdk_s3::config::Builder::from(&aws_cfg)
            .force_path_style(true)
            .timeout_config(client_timeouts());

        if !verify_tls {
            tracing::warn!(
                endpoint = endpoint_url,
                "TLS certificate verification disabled — accepting any cert from the S3 endpoint",
            );
            s3_cfg = s3_cfg.http_client(insecure_http_client());
        }

        let client = Client::from_conf(s3_cfg.build());
        Ok(Self::new(client, bucket.to_string()))
    }
}

/// Connect timeout: a socket that cannot be opened in this long is
/// not going to open.
pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
/// Read timeout: request sent → first byte of the response. Covers
/// the black-holed-connection case (request acknowledged, server
/// side gone, no RST, no keepalive) that no retransmit timer ever
/// resolves.
pub const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Bound on one attempt of one operation, body transfer included.
/// Sized for the largest single PUT in the system (a ~150 MB index
/// shard from `prepare`) at a few MB/s; every control-plane call
/// (claims, heartbeats, lease, progress, events) is bytes and
/// finishes in milliseconds.
pub const ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
/// Bound on the whole operation across the SDK's retries.
pub const OPERATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);
/// Bound on waiting for one body chunk in [`S3Client::download_to`].
/// The SDK's operation timeouts end when the response headers are
/// deserialized; the streamed body is read afterwards and needs its
/// own bound.
pub const BODY_CHUNK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The SDK ships with a connect timeout only — no read, attempt, or
/// operation bound — so one request on a black-holed connection hangs
/// its caller forever. The 600M run of 2026-08-28 lost the coord's
/// lease-refresh loop for 30 h and a worker's heartbeat loop for 19 h
/// to exactly that: one PUT that never returned, while every other
/// connection in the pool kept working. Every S3 call in the system
/// now carries these bounds; loops that must keep ticking add their
/// own `tokio::time::timeout` on top (heartbeat, lease refresh).
pub fn client_timeouts() -> aws_sdk_s3::config::timeout::TimeoutConfig {
    aws_sdk_s3::config::timeout::TimeoutConfig::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_TIMEOUT)
        .operation_attempt_timeout(ATTEMPT_TIMEOUT)
        .operation_timeout(OPERATION_TIMEOUT)
        .build()
}

/// Build a `SharedHttpClient` whose TLS layer accepts any server
/// certificate. Used when `verify_tls = false` in worker config.
///
/// The smithy 1.x `TlsContext` has no public "danger" knob, so we
/// route through the deprecated `hyper_014::HyperClientBuilder` —
/// public, but flagged for removal upstream. When smithy exposes a
/// supported knob we collapse this back to a one-liner.
fn insecure_http_client() -> aws_sdk_s3::config::SharedHttpClient {
    use aws_smithy_http_client::hyper_014::HyperClientBuilder;
    use std::sync::Arc;

    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(NoCertVerifier))
        .with_no_client_auth();

    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(crypto)
        // VAST endpoints are HTTPS; allow plain HTTP too for the rare
        // bench-mode override. https_only would be stricter; not worth
        // the surprise factor on a debugging knob.
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();

    HyperClientBuilder::new().build(connector)
}

/// rustls 0.21 `ServerCertVerifier` that performs no validation. The
/// behavior is intentional: this struct is only constructed on the
/// `verify_tls = false` branch of `S3Client::from_config`.
#[derive(Debug)]
struct NoCertVerifier;

impl rustls::client::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> std::result::Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

// =============================================================================
// Pure wire-semantics helpers
// =============================================================================
//
// The 200/404/412(+code-string) → outcome mapping and the etag
// (un)quoting are what make v2 at-most-once. They are extracted as
// pure functions so the semantics get table tests while the SDK
// plumbing stays thin wiring.

/// Strip the RFC-7232 quoting S3 puts around etags on the wire.
///
/// All etags stored and compared inside vamoose are unquoted; every
/// read path (PUT response, GET, LIST, HEAD-via-GET, download)
/// funnels through here so a quoted `"abc"` from the wire compares
/// equal to a stored `abc` — the apples-to-apples invariant.
/// Idempotent on already-unquoted input.
fn unquote_etag(raw: &str) -> String {
    raw.trim_matches('"').to_string()
}

/// Re-quote a stored (unquoted) etag for an outbound `If-Match`
/// header. The SDK's `if_match` builder passes the value verbatim and
/// the server compares against the quoted wire form, so the header
/// must carry the quotes. Idempotent on already-quoted input.
fn quote_etag(etag: &str) -> String {
    format!("\"{}\"", etag.trim_matches('"'))
}

/// Classification of a conditional-PUT (`If-None-Match: *`) service
/// error. Anything that is not an unambiguous precondition failure is
/// `Other` — ambiguous errors must surface as transient errors, never
/// as success or as a 412.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PutErrorClass {
    /// The object already exists — the caller lost the create race.
    PreconditionFailed,
    /// Anything else (5xx, throttle, transport oddity): transient.
    Other,
}

/// Pure mapping of a PUT service-error's `(HTTP status, error code)`
/// pair. The HTTP 412 status is the canonical signal; the
/// `PreconditionFailed` code string is the fallback for transports
/// (VAST-style bodies) that surface the code without the status.
fn classify_put_response(status: u16, code: &str) -> PutErrorClass {
    if status == 412 || code == "PreconditionFailed" {
        PutErrorClass::PreconditionFailed
    } else {
        PutErrorClass::Other
    }
}

/// Classification of a conditional-DELETE (`If-Match: <etag>`)
/// service error. 412 and 404 are protocol control-flow signals
/// (mapped to [`DeleteOutcome`] by the caller); everything else is
/// `Other` and must surface as a transient error, never as success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteErrorClass {
    /// 412 — object exists but its etag is not what we provided.
    EtagMismatch,
    /// 404 — object does not exist.
    NotFound,
    /// Anything else: transient, surface as `Err`.
    Other,
}

/// Pure mapping of a DELETE service-error's `(HTTP status, error
/// code)` pair. Same canonical-status-with-code-string-fallback shape
/// as [`classify_put_response`].
fn classify_delete_response(status: u16, code: &str) -> DeleteErrorClass {
    if status == 412 || code == "PreconditionFailed" {
        DeleteErrorClass::EtagMismatch
    } else if status == 404 || code == "NoSuchKey" {
        DeleteErrorClass::NotFound
    } else {
        DeleteErrorClass::Other
    }
}

#[async_trait]
impl ClaimStore for S3Client {
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<String> {
        let _timer = crate::latency::S3Timer::start(crate::latency::S3Op::PutIfAbsent);
        let resp = self
            .inner
            .put_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .if_none_match("*")
            .body(body.into())
            .send()
            .await
            .map_err(map_put_err)?;
        Ok(unquote_etag(resp.e_tag().ok_or_else(|| {
            Error::Other(anyhow::anyhow!("PUT returned no etag"))
        })?))
    }

    async fn put_unconditional(&self, key: &str, body: Vec<u8>) -> Result<String> {
        let _timer = crate::latency::S3Timer::start(crate::latency::S3Op::Put);
        self.put(key, body).await
    }

    async fn head_object(&self, key: &str) -> Result<Option<(String, Vec<u8>)>> {
        let _timer = crate::latency::S3Timer::start(crate::latency::S3Op::Head);
        // Claim objects are a few hundred bytes; GET-once is cheaper
        // than HEAD-then-GET when the body is needed (it always is in
        // v2 reclaim — caller parses claimed_utc).
        match self
            .inner
            .get_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .send()
            .await
        {
            Ok(resp) => {
                let etag = unquote_etag(resp.e_tag().ok_or_else(|| {
                    Error::Other(anyhow::anyhow!(
                        "head_object: GET returned no etag for {key}"
                    ))
                })?);
                let body = resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| Error::Other(anyhow::anyhow!("body collect: {e}")))?
                    .into_bytes()
                    .to_vec();
                Ok(Some((etag, body)))
            }
            Err(SdkError::ServiceError(svc)) if svc.err().is_no_such_key() => Ok(None),
            Err(e) => Err(Error::Other(anyhow::anyhow!("S3 head_object {key}: {e}"))),
        }
    }

    async fn delete_if_match(&self, key: &str, etag: &str) -> Result<DeleteOutcome> {
        let _timer = crate::latency::S3Timer::start(crate::latency::S3Op::DeleteIfMatch);
        // S3 etags are quoted on the wire. The SDK's `if_match` builder
        // takes the value verbatim; pass it pre-quoted to match
        // exactly what the server sees in HEAD/GET responses.
        let etag_quoted = quote_etag(etag);
        let resp = self
            .inner
            .delete_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .if_match(&etag_quoted)
            .send()
            .await;
        match resp {
            Ok(_) => Ok(DeleteOutcome::Deleted),
            Err(SdkError::ServiceError(svc)) => {
                let status = svc.raw().status().as_u16();
                let code = svc.err().meta().code().unwrap_or_default().to_string();
                match classify_delete_response(status, &code) {
                    DeleteErrorClass::EtagMismatch => Ok(DeleteOutcome::EtagMismatch),
                    DeleteErrorClass::NotFound => Ok(DeleteOutcome::NotFound),
                    DeleteErrorClass::Other => Err(Error::Other(anyhow::anyhow!(
                        "S3 DELETE {key}: status={status} code={code}"
                    ))),
                }
            }
            Err(e) => Err(Error::Other(anyhow::anyhow!("S3 DELETE {key}: {e:?}"))),
        }
    }

    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let _timer = crate::latency::S3Timer::start(crate::latency::S3Op::Get);
        match self
            .inner
            .get_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .send()
            .await
        {
            Ok(resp) => {
                let etag = resp.e_tag().map(unquote_etag).unwrap_or_default();
                let bytes = resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| Error::Other(anyhow::anyhow!("body collect: {e}")))?
                    .into_bytes()
                    .to_vec();
                Ok(Some((bytes, etag)))
            }
            Err(SdkError::ServiceError(svc)) if svc.err().is_no_such_key() => Ok(None),
            Err(e) => Err(Error::Other(anyhow::anyhow!("S3 GET {key}: {e}"))),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ListEntry>> {
        let _timer = crate::latency::S3Timer::start(crate::latency::S3Op::List);
        let mut out = Vec::new();
        let mut cont: Option<String> = None;
        loop {
            let mut req = self
                .inner
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(self.object_key(prefix));
            if let Some(t) = cont.take() {
                req = req.continuation_token(t);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| Error::Other(anyhow::anyhow!("S3 LIST {prefix}: {e}")))?;
            for o in resp.contents() {
                // Callers reason in logical keys (`shards/x.claim`);
                // strip the run prefix the wire key carries.
                let wire = o.key().unwrap_or_default();
                let key = wire
                    .strip_prefix(self.prefix.as_str())
                    .unwrap_or(wire)
                    .to_string();
                let etag = unquote_etag(o.e_tag().unwrap_or_default());
                let size = o.size().unwrap_or(0) as u64;
                out.push(ListEntry { key, etag, size });
            }
            if resp.is_truncated().unwrap_or(false) {
                cont = resp.next_continuation_token().map(|s| s.to_string());
                if cont.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(out)
    }
}

/// Translate AWS SDK PUT errors into our error type, with special
/// handling for the conditional-PUT 412 case. Pinned to the default
/// HttpResponse so we can access the raw status; do not generalize.
fn map_put_err(err: SdkError<aws_sdk_s3::operation::put_object::PutObjectError>) -> Error {
    if let SdkError::ServiceError(svc) = &err {
        // The HTTP 412 status is the canonical signal; the modeled
        // error code is the fallback in case the transport layer
        // surfaced it differently. See `classify_put_response`.
        let status = svc.raw().status().as_u16();
        let code = svc.err().meta().code().unwrap_or_default();
        if classify_put_response(status, code) == PutErrorClass::PreconditionFailed {
            return Error::PreconditionFailed;
        }
    }
    Error::Other(anyhow::anyhow!("S3 PUT: {err:?}"))
}

// =============================================================================
// Convenience helpers for non-claim objects
// =============================================================================

/// Bucket versioning status, normalized for the v2 claim protocol's
/// expectations. Versioning **must be off** on a vamoose bucket: the
/// claim protocol depends on `DELETE If-Match` actually removing the
/// object, not creating a delete marker that future `PUT If-None-Match`
/// calls then race against. We accept only `NotEnabled`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketVersioning {
    /// Versioning has never been enabled on the bucket. Safe for v2.
    NotEnabled,
    /// Versioning is currently enabled. Unsafe — every DELETE leaves a
    /// delete marker; `PUT If-None-Match: *` may see the marker as
    /// "object exists" and 412 on what should be a free claim.
    Enabled,
    /// Versioning was enabled at some point and has been suspended.
    /// Still unsafe — prior delete markers and non-current versions
    /// can persist and produce the same race symptoms as `Enabled`.
    Suspended,
    /// Any value the SDK doesn't recognize, surfaced rather than
    /// silently treated as `NotEnabled`. Treat as unsafe.
    Unknown(String),
}

impl S3Client {
    /// Return the bucket's versioning state. Used as a startup guard
    /// by the worker: a vamoose bucket must have versioning off.
    /// See `BucketVersioning` for why.
    pub async fn get_bucket_versioning(&self) -> Result<BucketVersioning> {
        let _timer = crate::latency::S3Timer::start(crate::latency::S3Op::Versioning);
        use aws_sdk_s3::types::BucketVersioningStatus;
        let out = self
            .inner
            .get_bucket_versioning()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|e| Error::Other(anyhow::anyhow!("get_bucket_versioning: {e:?}")))?;
        Ok(match out.status() {
            None => BucketVersioning::NotEnabled,
            Some(BucketVersioningStatus::Enabled) => BucketVersioning::Enabled,
            Some(BucketVersioningStatus::Suspended) => BucketVersioning::Suspended,
            Some(other) => BucketVersioning::Unknown(other.as_str().to_string()),
        })
    }

    /// Delete an object unconditionally. The coord uses this on the
    /// archive path (after copying an `events/<job>/` chunk into
    /// `archivelogs/<job>/`, the source chunk is removed). Workers do
    /// not use this — claim deletes go through `delete_if_match` for
    /// the v2 protocol's safety.
    ///
    /// Treats `404 NoSuchKey` as success: idempotent retry of an
    /// already-deleted key is harmless and the caller (archive)
    /// should not have to special-case it.
    pub async fn delete(&self, key: &str) -> Result<()> {
        let _timer = crate::latency::S3Timer::start(crate::latency::S3Op::Delete);
        match self
            .inner
            .delete_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(SdkError::ServiceError(svc))
                if svc.raw().status().as_u16() == 404
                    || svc.err().meta().code().unwrap_or_default() == "NoSuchKey" =>
            {
                Ok(())
            }
            Err(e) => Err(Error::Other(anyhow::anyhow!("S3 DELETE {key}: {e:?}"))),
        }
    }

    /// Upload an object unconditionally. Used for progress, batches,
    /// failures — anything that isn't a claim.
    pub async fn put(&self, key: &str, body: Vec<u8>) -> Result<String> {
        let _timer = crate::latency::S3Timer::start(crate::latency::S3Op::Put);
        let resp = self
            .inner
            .put_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .body(body.into())
            .send()
            .await
            .map_err(|e| Error::Other(anyhow::anyhow!("S3 PUT {key}: {e:?}")))?;
        Ok(resp.e_tag().map(unquote_etag).unwrap_or_default())
    }

    /// HTTP HEAD with the object's size and user metadata. `None` when
    /// the key is absent. Used by `vamoose prepare` to decide whether an
    /// index shard already in the bucket is byte-for-byte the one it
    /// would upload.
    pub async fn head_meta(&self, key: &str) -> Result<Option<ObjectHead>> {
        let _timer = crate::latency::S3Timer::start(crate::latency::S3Op::Head);
        match self
            .inner
            .head_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .send()
            .await
        {
            Ok(resp) => {
                let etag = resp.e_tag().map(unquote_etag).unwrap_or_default();
                let size = u64::try_from(resp.content_length().unwrap_or(0)).unwrap_or(0);
                let metadata = resp
                    .metadata()
                    .map(|m| {
                        m.iter()
                            .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(Some(ObjectHead {
                    etag,
                    size,
                    metadata,
                }))
            }
            Err(SdkError::ServiceError(svc)) if svc.err().is_not_found() => Ok(None),
            Err(e) => Err(Error::Other(anyhow::anyhow!("S3 HEAD {key}: {e}"))),
        }
    }

    /// Upload a local file with user metadata, streaming from disk.
    /// Returns the object's etag. Single-part PUT, so the etag is the
    /// body MD5 that workers compare on download.
    pub async fn put_file_with_metadata(
        &self,
        key: &str,
        path: &std::path::Path,
        metadata: std::collections::HashMap<String, String>,
    ) -> Result<String> {
        let _timer = crate::latency::S3Timer::start(crate::latency::S3Op::Upload);
        let body = aws_sdk_s3::primitives::ByteStream::from_path(path)
            .await
            .map_err(|e| Error::Io(std::io::Error::other(format!("{}: {e}", path.display()))))?;
        let resp = self
            .inner
            .put_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .set_metadata(Some(metadata))
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Other(anyhow::anyhow!("S3 PUT {key}: {e:?}")))?;
        Ok(resp.e_tag().map(unquote_etag).unwrap_or_default())
    }

    /// Download an object to a local path. Used for shard parquet
    /// downloads to tmpfs. Streams the body — does not buffer the whole
    /// object in memory. Returns the object's etag.
    ///
    /// Error typing (F42): SDK/service failures on the GET surface as
    /// [`Error::S3`] and mid-stream body failures as [`Error::Io`] —
    /// both classify WorkerLocal in the worker's F13 taxonomy. Never
    /// `Error::Other`, which classifies Fatal and would terminal-fail
    /// a shard on a transient transport blip.
    pub async fn download_to(&self, key: &str, dest: &std::path::Path) -> Result<String> {
        let _timer = crate::latency::S3Timer::start(crate::latency::S3Op::Download);
        let mut resp = self
            .inner
            .get_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .send()
            .await
            .map_err(|e| Error::S3(e.into()))?;

        let etag = resp.e_tag().map(unquote_etag).unwrap_or_default();

        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut out = tokio::fs::File::create(dest).await?;
        loop {
            // The SDK's timeouts stop at the response headers; a body
            // chunk that never arrives would otherwise hang the shard
            // download forever. Io classifies WorkerLocal like a
            // mid-stream failure.
            let next = tokio::time::timeout(BODY_CHUNK_TIMEOUT, resp.body.try_next())
                .await
                .map_err(|_| {
                    Error::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!(
                            "S3 GET {key} body: no data for {}s",
                            BODY_CHUNK_TIMEOUT.as_secs()
                        ),
                    ))
                })?;
            let Some(chunk) = next.map_err(|e| {
                // ByteStream errors have no aws_sdk_s3::Error conversion;
                // a mid-stream failure is read-side I/O (host/network) —
                // Io also classifies WorkerLocal.
                Error::Io(std::io::Error::other(format!("S3 GET {key} body: {e}")))
            })?
            else {
                break;
            };
            out.write_all(&chunk).await?;
        }
        out.flush().await?;
        Ok(etag)
    }
}

// =============================================================================
// Tests — F29. Table tests over the pure wire-semantics helpers; the
// SDK plumbing above stays thin, untested wiring by design (see
// docs/work-items/PROTOCOL_TEST_PACK.md). No live endpoints.
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // Client timeouts — every bound set, none left at the SDK default.
    // -------------------------------------------------------------------------

    /// Regression pin for the 2026-08-28 hang: the SDK default config
    /// has a connect timeout only. A client built without a read,
    /// attempt, and operation bound can park a caller forever on one
    /// black-holed request.
    #[test]
    fn client_timeouts_bound_every_phase_of_a_request() {
        let t = client_timeouts();
        assert_eq!(t.connect_timeout(), Some(CONNECT_TIMEOUT));
        assert_eq!(t.read_timeout(), Some(READ_TIMEOUT));
        assert_eq!(t.operation_attempt_timeout(), Some(ATTEMPT_TIMEOUT));
        assert_eq!(t.operation_timeout(), Some(OPERATION_TIMEOUT));
        assert!(
            ATTEMPT_TIMEOUT < OPERATION_TIMEOUT,
            "the operation bound must leave room for at least one retry",
        );
    }

    // -------------------------------------------------------------------------
    // Conditional PUT (If-None-Match: *) — the acquire/complete atom.
    // -------------------------------------------------------------------------

    #[test]
    fn put_if_absent_412_maps_to_precondition_failed() {
        // Canonical: HTTP 412 with or without the modeled code.
        assert_eq!(
            classify_put_response(412, ""),
            PutErrorClass::PreconditionFailed
        );
        assert_eq!(
            classify_put_response(412, "PreconditionFailed"),
            PutErrorClass::PreconditionFailed
        );
        // VAST-style: the body carries the `PreconditionFailed` code
        // string while the transport surfaces a different status.
        assert_eq!(
            classify_put_response(400, "PreconditionFailed"),
            PutErrorClass::PreconditionFailed
        );
        // Unrelated 4xx must NOT be treated as a lost create race.
        assert_eq!(
            classify_put_response(403, "AccessDenied"),
            PutErrorClass::Other
        );
        assert_eq!(classify_put_response(409, "Conflict"), PutErrorClass::Other);
    }

    // -------------------------------------------------------------------------
    // Conditional DELETE (If-Match: <etag>) — the old-state half of
    // every reclaim/complete.
    // -------------------------------------------------------------------------

    #[test]
    fn delete_if_match_412_maps_to_lost_race_outcome() {
        // 412 → EtagMismatch, which reclaim/complete map to
        // LostRace / Lost. Canonical status and code-string fallback.
        assert_eq!(
            classify_delete_response(412, ""),
            DeleteErrorClass::EtagMismatch
        );
        assert_eq!(
            classify_delete_response(412, "PreconditionFailed"),
            DeleteErrorClass::EtagMismatch
        );
        assert_eq!(
            classify_delete_response(400, "PreconditionFailed"),
            DeleteErrorClass::EtagMismatch
        );
        // Precedence: a PreconditionFailed code wins over a 404
        // status — same order as the original inline mapping.
        assert_eq!(
            classify_delete_response(404, "PreconditionFailed"),
            DeleteErrorClass::EtagMismatch
        );
    }

    #[test]
    fn delete_if_match_404_maps_to_not_found() {
        assert_eq!(
            classify_delete_response(404, ""),
            DeleteErrorClass::NotFound
        );
        assert_eq!(
            classify_delete_response(404, "NoSuchKey"),
            DeleteErrorClass::NotFound
        );
        // Code-string fallback without the canonical status.
        assert_eq!(
            classify_delete_response(400, "NoSuchKey"),
            DeleteErrorClass::NotFound
        );
    }

    // -------------------------------------------------------------------------
    // Etag quoting — the apples-to-apples invariant. Every read path
    // (PUT response, GET, LIST, HEAD-via-GET, download) funnels
    // through `unquote_etag`; the outbound If-Match header goes
    // through `quote_etag`.
    // -------------------------------------------------------------------------

    #[test]
    fn etag_unquoted_on_put_get_list() {
        // A quoted etag from the wire compares equal to the stored
        // unquoted form, whichever read path produced it.
        let wire = "\"3858f62230ac3c915f300c664312c63f\"";
        let stored = "3858f62230ac3c915f300c664312c63f";
        assert_eq!(unquote_etag(wire), stored);
        // Idempotent — an already-unquoted etag passes through.
        assert_eq!(unquote_etag(stored), stored);
        // Both forms normalize to the same comparison key.
        assert_eq!(unquote_etag(wire), unquote_etag(stored));
        // Multipart-style etags (with the part-count suffix) survive.
        assert_eq!(unquote_etag("\"abc-2\""), "abc-2");
    }

    #[test]
    fn if_match_header_requotes_stored_etag() {
        // Stored (unquoted) etag goes out quoted.
        assert_eq!(quote_etag("abc123"), "\"abc123\"");
        // Idempotent — never double-quote a wire-form etag.
        assert_eq!(quote_etag("\"abc123\""), "\"abc123\"");
        // Round trip: unquote then requote recovers the wire form.
        assert_eq!(quote_etag(&unquote_etag("\"abc123\"")), "\"abc123\"");
    }

    // -------------------------------------------------------------------------
    // Run prefix: one bucket, many runs.
    // -------------------------------------------------------------------------

    #[test]
    fn prefix_normalizes_to_empty_or_path_with_one_trailing_slash() {
        assert_eq!(normalize_prefix(""), "");
        assert_eq!(normalize_prefix("  "), "");
        assert_eq!(normalize_prefix("/"), "");
        assert_eq!(normalize_prefix("v4"), "v4/");
        assert_eq!(normalize_prefix("/v4/"), "v4/");
        assert_eq!(normalize_prefix("v4//"), "v4/");
        assert_eq!(normalize_prefix("runs//aug/v4"), "runs/aug/v4/");
    }

    #[test]
    fn prefixed_client_places_every_key_under_the_prefix() {
        use aws_sdk_s3::config::{Credentials, Region};
        let conf = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url("http://127.0.0.1:1")
            .credentials_provider(Credentials::new("test", "test", None, None, "test"))
            .force_path_style(true)
            .build();
        let plain = S3Client::new(Client::from_conf(conf.clone()), "b");
        assert_eq!(plain.object_key("manifest.json"), "manifest.json");
        assert_eq!(plain.location(), "s3://b/");
        let run = S3Client::new(Client::from_conf(conf), "b").with_prefix("/v4/");
        assert_eq!(run.prefix(), "v4/");
        assert_eq!(run.object_key("manifest.json"), "v4/manifest.json");
        assert_eq!(run.object_key("shards/"), "v4/shards/");
        assert_eq!(run.location(), "s3://b/v4/");
    }

    // -------------------------------------------------------------------------
    // F42: download_to error typing.
    // -------------------------------------------------------------------------

    /// F42 (red before fix): `download_to` must surface SDK/transport
    /// failures as `Error::S3`, not `Error::Other`. The worker's F13
    /// classifier maps `S3 → WorkerLocal` (release-and-skip) and
    /// `Other → Fatal` — with the old typing, a transient download
    /// blip terminal-failed the shard. Uses a closed local port so
    /// the GET fails at the transport layer with no network
    /// dependency; retries are disabled so the failure is immediate.
    #[tokio::test]
    async fn download_sdk_error_is_s3_typed() {
        use aws_sdk_s3::config::{Credentials, Region};

        let conf = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url("http://127.0.0.1:1") // closed port → connection refused
            .credentials_provider(Credentials::new("test", "test", None, None, "test"))
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
            .force_path_style(true)
            .build();
        let client = S3Client::new(Client::from_conf(conf), "test-bucket");

        let dest = std::env::temp_dir().join("vamoose-f42-download-typing-test.parquet");
        let err = client
            .download_to("index/part-0001.parquet", &dest)
            .await
            .expect_err("GET against a closed port must fail");
        assert!(
            matches!(err, Error::S3(_)),
            "download_to must type SDK errors as Error::S3, got: {err:?}",
        );
        let _ = tokio::fs::remove_file(&dest).await;
    }

    // -------------------------------------------------------------------------
    // Taxonomy pin: ambiguous errors are transient, never success and
    // never a protocol control-flow signal.
    // -------------------------------------------------------------------------

    #[test]
    fn unexpected_5xx_maps_to_transient_not_success() {
        for status in [500u16, 502, 503] {
            assert_eq!(
                classify_put_response(status, "InternalError"),
                PutErrorClass::Other,
                "PUT {status} must classify as transient"
            );
            assert_eq!(
                classify_delete_response(status, "InternalError"),
                DeleteErrorClass::Other,
                "DELETE {status} must classify as transient"
            );
        }
        // Empty code strings don't accidentally match anything.
        assert_eq!(classify_put_response(500, ""), PutErrorClass::Other);
        assert_eq!(classify_delete_response(503, ""), DeleteErrorClass::Other);
        // SlowDown throttling is transient, not a lost race.
        assert_eq!(
            classify_delete_response(503, "SlowDown"),
            DeleteErrorClass::Other
        );
    }
}
