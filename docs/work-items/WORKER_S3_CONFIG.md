# Worker S3 client config: profile + verify_tls

Small follow-up to enable manual verification against VAST S3 with a
self-signed cert and explicit credential profile.

## Background

During M2/M3 manual verification, two operational issues blocked the
worker from talking to VAST S3:

1. **Credentials profile is not configurable.** `S3Client::from_env`
   uses the default profile chain, which doesn't pick up the
   `var204` profile the operator configured. Running under `sudo`
   compounds the issue because root has a different `HOME`.

2. **TLS verification can't be disabled.** VAST clusters in lab
   environments commonly use self-signed certs. `aws-cli` has
   `--no-verify-ssl`; the worker has no equivalent.

Both are real config gaps, not test-environment workarounds. Add them
as first-class fields in `[run]`.

## Changes

### `migration-worker/src/config.rs`

Add to `RunCfg`:

```rust
#[derive(Debug, Deserialize)]
pub struct RunCfg {
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    /// Optional AWS credentials profile name. If omitted, uses
    /// the default credential chain.
    #[serde(default)]
    pub profile: Option<String>,
    /// Whether to verify the TLS certificate of the S3 endpoint.
    /// Default: true. Set to false for lab/dev environments with
    /// self-signed certs.
    #[serde(default = "default_verify_tls")]
    pub verify_tls: bool,
}

fn default_verify_tls() -> bool { true }
```

### `migration-core::s3::S3Client`

Update `from_env` (or add a new `from_config` constructor) to accept
the new fields:

```rust
pub async fn from_config(
    endpoint_url: &str,
    region: &str,
    bucket: &str,
    profile: Option<&str>,
    verify_tls: bool,
) -> Result<Self> {
    use aws_config::{BehaviorVersion, profile::ProfileFileCredentialsProvider};

    let mut loader = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_string()))
        .endpoint_url(endpoint_url);

    if let Some(p) = profile {
        let creds = ProfileFileCredentialsProvider::builder()
            .profile_name(p)
            .build();
        loader = loader.credentials_provider(creds);
    }

    let aws_cfg = loader.load().await;

    let mut s3_cfg = aws_sdk_s3::config::Builder::from(&aws_cfg)
        .force_path_style(true);

    if !verify_tls {
        // Build a custom HTTP connector with TLS verification
        // disabled. Use rustls or hyper-tls in dangerous mode.
        s3_cfg = s3_cfg.http_client(insecure_http_client()?);
    }

    let client = aws_sdk_s3::Client::from_conf(s3_cfg.build());
    Ok(Self::new(client, bucket))
}
```

For `insecure_http_client()`, the SDK's `aws-smithy-http-client` (or
the older `aws_smithy_runtime::client::http::hyper_014`) supports
configuring TLS via a custom `HttpsConnector`. Rough shape with
rustls:

```rust
fn insecure_http_client() -> Result<aws_smithy_runtime_api::client::http::SharedHttpClient> {
    use aws_smithy_http_client::{tls, Builder};

    let tls_ctx = tls::TlsContext::builder()
        .with_native_roots()
        .danger_accept_invalid_certs(true)
        .build()?;

    Ok(Builder::new()
        .tls_provider(tls::Provider::Rustls(tls::rustls_provider::CryptoMode::AwsLc))
        .with_tls_context(tls_ctx)
        .build_https())
}
```

The exact method names depend on the version of `aws-smithy-http-client`
in the workspace. If the API surface differs, plumb it however the
crate exposes "skip cert verification" — the goal is one method call,
not a re-implementation of TLS.

When `verify_tls = true` (default), use the SDK's standard HTTP client
with normal verification.

### `migration-worker/src/orchestrator.rs`

Update the call site that constructs `S3Client`:

```rust
let s3 = S3Client::from_config(
    &cfg.run.endpoint,
    &cfg.run.region,
    &cfg.run.bucket,
    cfg.run.profile.as_deref(),
    cfg.run.verify_tls,
).await?;
```

### `examples/worker.toml`

Add the new fields to the example config, defaulting to safe values:

```toml
[run]
bucket   = "vamoose"
endpoint = "https://vast-s3.example.com"
region   = "us-east-1"
# profile  = "default"     # uncomment to override the default credential chain
# verify_tls = false       # uncomment for self-signed certs in lab environments
```

The commented-out lines make the safe default obvious and document
the unsafe option.

## Tests

- Unit test for `RunCfg` deserialization with and without the new
  fields. Defaults must be correct (`profile = None`, `verify_tls = true`).
- No integration test for `verify_tls = false` — that's a behavioral
  change in the underlying TLS stack; trust the SDK to do what we asked.

## Done criteria

- `cargo build --workspace` clean.
- Existing tests still pass.
- New unit tests for config defaults pass.
- Worker started against VAST S3 with `profile = "var204"` and
  `verify_tls = false` reads `manifest.json` successfully (operator
  validates manually; not part of automated tests).

## Out of scope

- Installing the VAST CA into the system trust store (separate ops
  task; orthogonal to this change).
- A "trusted CAs file" config field. If someone needs a custom CA
  bundle later, that's a separate option; for now `verify_tls = false`
  is the only knob.
- Migrating other binaries (`mig-aggr`) to use the new config — they
  load their own S3 client. Apply the same pattern there in a
  follow-up if needed for verification.
