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

#[derive(Debug, Clone)]
pub struct S3Client {
    inner: Client,
    bucket: String,
}

impl S3Client {
    pub fn new(inner: Client, bucket: impl Into<String>) -> Self {
        Self {
            inner,
            bucket: bucket.into(),
        }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
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

        let mut s3_cfg = aws_sdk_s3::config::Builder::from(&aws_cfg).force_path_style(true);

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

#[async_trait]
impl ClaimStore for S3Client {
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<String> {
        let resp = self
            .inner
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .if_none_match("*")
            .body(body.into())
            .send()
            .await
            .map_err(map_put_err)?;
        Ok(resp
            .e_tag()
            .ok_or_else(|| Error::Other(anyhow::anyhow!("PUT returned no etag")))?
            .trim_matches('"')
            .to_string())
    }

    async fn put_unconditional(&self, key: &str, body: Vec<u8>) -> Result<String> {
        self.put(key, body).await
    }

    async fn head_object(&self, key: &str) -> Result<Option<(String, Vec<u8>)>> {
        // Claim objects are a few hundred bytes; GET-once is cheaper
        // than HEAD-then-GET when the body is needed (it always is in
        // v2 reclaim — caller parses claimed_utc).
        match self
            .inner
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(resp) => {
                let etag = resp
                    .e_tag()
                    .ok_or_else(|| {
                        Error::Other(anyhow::anyhow!(
                            "head_object: GET returned no etag for {key}"
                        ))
                    })?
                    .trim_matches('"')
                    .to_string();
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
        // S3 etags are quoted on the wire. The SDK's `if_match` builder
        // takes the value verbatim; pass it pre-quoted to match
        // exactly what the server sees in HEAD/GET responses.
        let etag_quoted = format!("\"{}\"", etag.trim_matches('"'));
        let resp = self
            .inner
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .if_match(&etag_quoted)
            .send()
            .await;
        match resp {
            Ok(_) => Ok(DeleteOutcome::Deleted),
            Err(SdkError::ServiceError(svc)) => {
                let status = svc.raw().status().as_u16();
                let code = svc.err().meta().code().unwrap_or_default().to_string();
                if status == 412 || code == "PreconditionFailed" {
                    Ok(DeleteOutcome::EtagMismatch)
                } else if status == 404 || code == "NoSuchKey" {
                    Ok(DeleteOutcome::NotFound)
                } else {
                    Err(Error::Other(anyhow::anyhow!(
                        "S3 DELETE {key}: status={status} code={code}"
                    )))
                }
            }
            Err(e) => Err(Error::Other(anyhow::anyhow!("S3 DELETE {key}: {e:?}"))),
        }
    }

    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        match self
            .inner
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(resp) => {
                let etag = resp
                    .e_tag()
                    .map(|s| s.trim_matches('"').to_string())
                    .unwrap_or_default();
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
        let mut out = Vec::new();
        let mut cont: Option<String> = None;
        loop {
            let mut req = self
                .inner
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(t) = cont.take() {
                req = req.continuation_token(t);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| Error::Other(anyhow::anyhow!("S3 LIST {prefix}: {e}")))?;
            for o in resp.contents() {
                let key = o.key().unwrap_or_default().to_string();
                let etag = o.e_tag().unwrap_or_default().trim_matches('"').to_string();
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
        // PreconditionFailed has HTTP status 412 — the canonical signal.
        if svc.raw().status().as_u16() == 412 {
            return Error::PreconditionFailed;
        }
        // Fall back to the modeled error code in case the transport
        // layer surfaced it differently.
        let code = svc.err().meta().code().unwrap_or_default();
        if code == "PreconditionFailed" {
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

    /// Upload an object unconditionally. Used for progress, batches,
    /// failures — anything that isn't a claim.
    pub async fn put(&self, key: &str, body: Vec<u8>) -> Result<String> {
        let resp = self
            .inner
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body.into())
            .send()
            .await
            .map_err(|e| Error::Other(anyhow::anyhow!("S3 PUT {key}: {e:?}")))?;
        Ok(resp
            .e_tag()
            .map(|s| s.trim_matches('"').to_string())
            .unwrap_or_default())
    }

    /// Download an object to a local path. Used for shard parquet
    /// downloads to tmpfs. Streams the body — does not buffer the whole
    /// object in memory. Returns the object's etag.
    pub async fn download_to(&self, key: &str, dest: &std::path::Path) -> Result<String> {
        let mut resp = self
            .inner
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Error::Other(anyhow::anyhow!("S3 GET {key}: {e:?}")))?;

        let etag = resp
            .e_tag()
            .map(|s| s.trim_matches('"').to_string())
            .unwrap_or_default();

        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut out = tokio::fs::File::create(dest).await?;
        while let Some(chunk) = resp
            .body
            .try_next()
            .await
            .map_err(|e| Error::Other(anyhow::anyhow!("S3 GET {key} body: {e}")))?
        {
            out.write_all(&chunk).await?;
        }
        out.flush().await?;
        Ok(etag)
    }
}
