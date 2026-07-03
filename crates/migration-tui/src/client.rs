//! HTTP client for the TUI.
//!
//! Two surfaces, both over the same `reqwest::Client`:
//!
//! - REST snapshot fetch: [`get_jobs`], [`get_job`], [`get_workers`],
//!   [`get_errors`], [`healthz`]. Each is a single round trip; the
//!   types come from `migration_coord::server::read` so coord and
//!   client share the wire shape and cargo catches a drift at
//!   compile time.
//!
//! - SSE consumer: [`Client::stream`] opens `GET /stream` with
//!   `Last-Event-ID` and yields an `async_stream::try_stream` of
//!   server-sent events. The SSE wire format is the same minimal
//!   subset the coord emits — `event:`, `id:`, `data:` lines
//!   terminated by a blank line; comments (`: ping`) surfaced as
//!   `SseFrame { kind: Keepalive, .. }` so the caller can refresh a
//!   "last seen" timer without parsing JSON.

use bytes::Bytes;
use futures::{Stream, StreamExt};
use migration_coord::schema::EventEnvelope;
use migration_coord::schema::Job;
use migration_coord::server::command::{CommandAccepted, ReasonBody};
use migration_coord::server::read::{
    HealthzResponse, ListErrorsResponse, ListEventsResponse, ListJobsResponse, ListWorkersResponse,
};
use reqwest::header::{HeaderMap, HeaderValue};
use std::pin::Pin;
use std::time::Duration;
use thiserror::Error;

// =============================================================================
// Errors
// =============================================================================

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("http {status}: {body}")]
    Http {
        status: reqwest::StatusCode,
        body: String,
    },

    #[error("malformed SSE frame: {0}")]
    MalformedFrame(String),

    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, ClientError>;

// =============================================================================
// Client
// =============================================================================

/// Top-level TUI HTTP client. Holds the base URL, a configured
/// `reqwest::Client` (with optional bearer token, verify-tls toggle,
/// request timeout), and exposes the four REST methods plus
/// [`Client::stream`] for the SSE consumer.
#[derive(Debug, Clone)]
pub struct Client {
    /// Client for REST round-trips. Has a per-request timeout so a
    /// dead coord doesn't wedge the snapshot fetch.
    http: reqwest::Client,
    /// Separate client for SSE streaming. NO request timeout — the
    /// stream is meant to stay open for the life of the TUI; reusing
    /// the REST client would kill the connection every
    /// `request_timeout` seconds, making the banner flicker between
    /// connected and reconnecting on a perfectly healthy coord.
    /// Connect timeout still bounds the initial handshake.
    stream_http: reqwest::Client,
    base_url: String,
}

impl Client {
    /// Build a client against `base_url` (e.g.
    /// `https://coord.example:8443`).
    ///
    /// `admin_token` adds `Authorization: Bearer <token>` to every
    /// REST + SSE call. `verify_tls = false` accepts any TLS cert
    /// (lab escape hatch — same one the worker's S3 client uses).
    pub fn new(
        base_url: impl Into<String>,
        admin_token: Option<&str>,
        verify_tls: bool,
        request_timeout: Duration,
    ) -> Result<Self> {
        let headers = build_default_headers(admin_token)?;

        // REST client: full per-request timeout so a dead coord
        // doesn't hang the snapshot fetch on the render thread.
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(!verify_tls)
            .default_headers(headers.clone())
            .timeout(request_timeout)
            .build()?;

        // Stream client: NO request timeout. SSE connections are
        // meant to stay open for the life of the TUI; the coord
        // sends periodic `: ping` keepalives so the link stays
        // live even when no events are flowing. Connect timeout is
        // kept (defaults to reqwest's internal value) so a bad
        // host still fails fast on the initial handshake.
        let stream_http = reqwest::Client::builder()
            .danger_accept_invalid_certs(!verify_tls)
            .default_headers(headers)
            .connect_timeout(request_timeout)
            .build()?;

        Ok(Self {
            http,
            stream_http,
            base_url: base_url.into().trim_end_matches('/').to_string(),
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// `GET /healthz`. Cheap probe used by the TUI's connection
    /// status banner. Authoritative source of `last_seq` for the
    /// initial SSE resume cursor.
    pub async fn healthz(&self) -> Result<HealthzResponse> {
        let resp = self.http.get(self.url("/healthz")).send().await?;
        check_status(resp).await?.json().await.map_err(Into::into)
    }

    /// `GET /jobs?cursor=&limit=`. The TUI bootstraps state by
    /// walking pages until `next_cursor` is None.
    pub async fn get_jobs(
        &self,
        cursor: Option<&str>,
        limit: Option<usize>,
    ) -> Result<ListJobsResponse> {
        let mut req = self.http.get(self.url("/jobs"));
        if let Some(c) = cursor {
            req = req.query(&[("cursor", c)]);
        }
        if let Some(l) = limit {
            req = req.query(&[("limit", l)]);
        }
        let resp = req.send().await?;
        check_status(resp).await?.json().await.map_err(Into::into)
    }

    /// `GET /jobs/{id}` — single-job view.
    pub async fn get_job(&self, id: &str) -> Result<Job> {
        let resp = self
            .http
            .get(self.url(&format!("/jobs/{id}")))
            .send()
            .await?;
        check_status(resp).await?.json().await.map_err(Into::into)
    }

    /// `GET /jobs/{id}/workers` — workers assigned to the job.
    pub async fn get_workers(&self, id: &str) -> Result<ListWorkersResponse> {
        let resp = self
            .http
            .get(self.url(&format!("/jobs/{id}/workers")))
            .send()
            .await?;
        check_status(resp).await?.json().await.map_err(Into::into)
    }

    /// `GET /jobs/{id}/errors` — aggregated error buckets for the job.
    pub async fn get_errors(&self, id: &str) -> Result<ListErrorsResponse> {
        let resp = self
            .http
            .get(self.url(&format!("/jobs/{id}/errors")))
            .send()
            .await?;
        check_status(resp).await?.json().await.map_err(Into::into)
    }

    /// `GET /jobs/{id}/events?since=` — raw event tail for a job.
    /// The TUI does not use this on the hot path (SSE is the primary
    /// feed) but it's useful for diagnostics.
    pub async fn get_events(&self, id: &str, since: u64) -> Result<ListEventsResponse> {
        let resp = self
            .http
            .get(self.url(&format!("/jobs/{id}/events")))
            .query(&[("since", since)])
            .send()
            .await?;
        check_status(resp).await?.json().await.map_err(Into::into)
    }

    // ----- Admin commands (POST /jobs/{id}/<action>) -----
    //
    // Each calls the matching coord handler with an optional
    // human-readable `reason` (defaults coord-side to "operator").
    // Bearer auth is set on the default headers; without an admin
    // token these will 401 (or 200 on a coord running in dev
    // mode). Returns the audit `command_id` on success.

    pub async fn pause(&self, id: &str, reason: Option<&str>) -> Result<CommandAccepted> {
        self.post_command(id, "pause", reason).await
    }

    pub async fn resume(&self, id: &str, reason: Option<&str>) -> Result<CommandAccepted> {
        self.post_command(id, "resume", reason).await
    }

    pub async fn cancel(&self, id: &str, reason: Option<&str>) -> Result<CommandAccepted> {
        self.post_command(id, "cancel", reason).await
    }

    pub async fn drain(&self, id: &str, reason: Option<&str>) -> Result<CommandAccepted> {
        self.post_command(id, "drain", reason).await
    }

    pub async fn retry_failed(&self, id: &str, reason: Option<&str>) -> Result<CommandAccepted> {
        self.post_command(id, "retry-failed", reason).await
    }

    async fn post_command(
        &self,
        id: &str,
        action: &str,
        reason: Option<&str>,
    ) -> Result<CommandAccepted> {
        let body = ReasonBody {
            reason: reason.map(|s| s.to_string()),
        };
        let resp = self
            .http
            .post(self.url(&format!("/jobs/{id}/{action}")))
            .json(&body)
            .send()
            .await?;
        check_status(resp).await?.json().await.map_err(Into::into)
    }

    /// `GET /stream` with `Last-Event-ID: <last_event_id>` (omitted
    /// when None — coord interprets as "live only"; pass `Some(0)`
    /// to catch up from the beginning of the event log).
    ///
    /// Returns a stream of [`SseFrame`]s. The caller is responsible
    /// for treating connection-close as a signal to back off and
    /// resume with the highest seq it has seen so far.
    pub async fn stream(
        &self,
        last_event_id: Option<u64>,
        job_filter: Option<&str>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<SseFrame>> + Send>>> {
        let mut url = self.url("/stream");
        if let Some(job) = job_filter {
            url = format!("{url}?job_id={job}");
        }
        // Uses the dedicated stream client — NO per-request
        // timeout. If we used `self.http` here the connection would
        // be killed every `request_timeout` seconds and the banner
        // would flicker connected → reconnecting on a healthy coord.
        let mut req = self.stream_http.get(&url);
        if let Some(seq) = last_event_id {
            req = req.header("last-event-id", seq.to_string());
        }
        let resp = req.send().await?;
        let resp = check_status(resp).await?;
        let byte_stream = resp.bytes_stream();
        Ok(Box::pin(parse_sse_stream(byte_stream)))
    }
}

fn build_default_headers(admin_token: Option<&str>) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    if let Some(t) = admin_token {
        let value = format!("Bearer {t}");
        let mut v = HeaderValue::from_str(&value).map_err(|_| ClientError::Http {
            status: reqwest::StatusCode::BAD_REQUEST,
            body: "admin token contains non-ASCII".into(),
        })?;
        v.set_sensitive(true);
        headers.insert("authorization", v);
    }
    Ok(headers)
}

async fn check_status(resp: reqwest::Response) -> Result<reqwest::Response> {
    let status = resp.status();
    if status.is_success() {
        Ok(resp)
    } else {
        let body = resp.text().await.unwrap_or_default();
        Err(ClientError::Http { status, body })
    }
}

// =============================================================================
// SSE parser
// =============================================================================

/// One SSE frame.
///
/// `Event` corresponds to a complete `event:/id:/data:` triple
/// terminated by a blank line. `Keepalive` is the coord's periodic
/// `: ping` comment — the TUI uses it to refresh a "last seen"
/// timer for the connection status banner.
///
/// `UnknownEvent` is the forward-compatibility escape hatch (F38): a
/// frame whose `data:` is a valid JSON object carrying a string
/// `kind` this build has no [`EventKind`] variant for — i.e. a
/// *newer* coord talking to an older TUI. Surfacing it as an `Err`
/// instead would tear the stream down, and because the resume cursor
/// never advances past the frame the reconnect would replay it
/// forever. The caller applies `seq` to its cursor and counts the
/// skip; the payload is unrecoverable by definition.
#[derive(Debug, Clone)]
pub enum SseFrame {
    Event { seq: u64, envelope: EventEnvelope },
    UnknownEvent { seq: u64, kind: String },
    Resync,
    Keepalive,
}

/// Turn a `Stream<Item = Result<Bytes>>` into a `Stream<Item =
/// Result<SseFrame>>`. Hand-rolled minimal parser — handles only the
/// subset of SSE the coord emits, documented in
/// `migration_coord::server::stream`.
///
/// Public so the integration suite can drive the exact parser path
/// with synthetic future-coord frames the live coord cannot emit
/// (its `EventKind` schema is closed).
pub fn parse_sse_stream<S>(byte_stream: S) -> impl Stream<Item = Result<SseFrame>> + Send
where
    S: Stream<Item = std::result::Result<Bytes, reqwest::Error>> + Send + 'static,
{
    async_stream::try_stream! {
        let mut leftover: Vec<u8> = Vec::new();
        let mut pending = PartialFrame::default();
        let mut stream = Box::pin(byte_stream);
        while let Some(chunk_res) = stream.next().await {
            let chunk = chunk_res?;
            leftover.extend_from_slice(&chunk);
            while let Some(pos) = leftover.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = leftover.drain(..=pos).take(pos).collect();
                let line = std::str::from_utf8(&line).unwrap_or("").trim_end_matches('\r');
                if line.is_empty() {
                    // Frame terminator. Emit if non-empty.
                    if let Some(frame) = pending.take() {
                        yield frame?;
                    }
                } else if let Some(_rest) = line.strip_prefix(':') {
                    // Comment line. Coord emits this as a periodic
                    // keepalive — surface it so the TUI can refresh
                    // its "last seen" timer.
                    yield SseFrame::Keepalive;
                } else if let Some(rest) = line.strip_prefix("event:") {
                    pending.event = Some(rest.trim_start().to_string());
                } else if let Some(rest) = line.strip_prefix("id:") {
                    pending.id = Some(rest.trim_start().to_string());
                } else if let Some(rest) = line.strip_prefix("data:") {
                    pending.data = Some(rest.trim_start().to_string());
                }
                // Any other line is silently ignored (matches the
                // SSE spec's tolerance for unknown fields).
            }
        }
    }
}

#[derive(Debug, Default)]
struct PartialFrame {
    event: Option<String>,
    id: Option<String>,
    data: Option<String>,
}

impl PartialFrame {
    /// Materialize a complete frame at the blank-line terminator and
    /// reset internal state. Returns Ok(Some(frame)) on a real
    /// event, Ok(None) on a no-op blank line (no event accumulated
    /// since the last terminator), Err on a malformed frame.
    fn take(&mut self) -> Option<Result<SseFrame>> {
        let mine = std::mem::take(self);
        match (
            mine.event.as_deref(),
            mine.id.as_deref(),
            mine.data.as_deref(),
        ) {
            (None, None, None) => None,
            (Some("Resync"), _, _) => Some(Ok(SseFrame::Resync)),
            (Some(_kind), Some(id_str), Some(data_str)) => {
                let seq = match id_str.parse::<u64>() {
                    Ok(s) => s,
                    Err(e) => {
                        return Some(Err(ClientError::MalformedFrame(format!(
                            "non-numeric id {id_str:?}: {e}"
                        ))));
                    }
                };
                match serde_json::from_str::<EventEnvelope>(data_str) {
                    Ok(envelope) => Some(Ok(SseFrame::Event { seq, envelope })),
                    // F38: distinguish future-version skew from
                    // corruption via a lightweight pre-parse. A JSON
                    // object with a string `kind` tag is a versioned
                    // event from a coord newer than this build —
                    // skippable, with the seq preserved so the
                    // cursor can advance past it. Anything else
                    // (non-JSON, non-object, missing tag) stays an
                    // error exactly as before.
                    Err(e) => match serde_json::from_str::<serde_json::Value>(data_str) {
                        Ok(serde_json::Value::Object(map))
                            if map.get("kind").is_some_and(|v| v.is_string()) =>
                        {
                            let kind = map
                                .get("kind")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_string();
                            Some(Ok(SseFrame::UnknownEvent { seq, kind }))
                        }
                        _ => Some(Err(ClientError::Serde(e))),
                    },
                }
            }
            _ => Some(Err(ClientError::MalformedFrame(format!(
                "incomplete frame: event={:?} id={:?} data_some={}",
                mine.event,
                mine.id,
                mine.data.is_some()
            )))),
        }
    }
}

// =============================================================================
// Tests — only the SSE parser is unit-testable in isolation. The REST
// methods + the streaming round trip are exercised in
// `tests/integration.rs` against an in-process coord.
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;

    /// Build a fixed byte-stream from a list of byte chunks. Used to
    /// feed the parser with both well-aligned and split-across-chunk
    /// inputs.
    fn bytes_stream(
        chunks: Vec<Vec<u8>>,
    ) -> impl Stream<Item = std::result::Result<Bytes, reqwest::Error>> {
        stream::iter(
            chunks
                .into_iter()
                .map(|c| std::result::Result::<_, reqwest::Error>::Ok(Bytes::from(c))),
        )
    }

    #[tokio::test]
    async fn parser_handles_single_well_formed_frame() {
        let data = "{\"seq\":1,\"at\":\"2026-05-29T14:32:00Z\",\"schema_version\":1,\
                    \"kind\":\"JobCreated\",\"job_id\":\"bobby\",\"name\":\"bobby-mig\",\
                    \"source\":\"nfs://src\",\"dest\":\"nfs://dst\",\"owner\":\"test\",\
                    \"config_hash\":\"ab\"}";
        let raw = format!("event:JobCreated\nid:1\ndata:{data}\n\n");
        let stream = parse_sse_stream(bytes_stream(vec![raw.into_bytes()]));
        tokio::pin!(stream);
        let first = stream.next().await.expect("frame yielded").expect("ok");
        match first {
            SseFrame::Event { seq, envelope } => {
                assert_eq!(seq, 1);
                assert_eq!(envelope.seq, 1);
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert!(stream.next().await.is_none(), "stream ends after one frame");
    }

    #[tokio::test]
    async fn parser_emits_keepalive_for_comment_line() {
        let raw = ": ping\n\n".to_string();
        let stream = parse_sse_stream(bytes_stream(vec![raw.into_bytes()]));
        tokio::pin!(stream);
        match stream.next().await.expect("frame").expect("ok") {
            SseFrame::Keepalive => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn parser_handles_resync_frame() {
        let raw = "event:Resync\nid:0\ndata:{}\n\n".to_string();
        let stream = parse_sse_stream(bytes_stream(vec![raw.into_bytes()]));
        tokio::pin!(stream);
        match stream.next().await.expect("frame").expect("ok") {
            SseFrame::Resync => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn parser_reassembles_frame_split_across_chunks() {
        let data = "{\"seq\":7,\"at\":\"2026-05-29T14:32:00Z\",\"schema_version\":1,\
                    \"kind\":\"WorkerLeft\",\"worker_id\":\"00000000-0000-0000-0000-000000000000\",\
                    \"reason\":\"test\"}";
        let raw = format!("event:WorkerLeft\nid:7\ndata:{data}\n\n");
        // Split into three arbitrary chunks.
        let bytes = raw.as_bytes();
        let third = bytes.len() / 3;
        let chunks = vec![
            bytes[..third].to_vec(),
            bytes[third..2 * third].to_vec(),
            bytes[2 * third..].to_vec(),
        ];
        let stream = parse_sse_stream(bytes_stream(chunks));
        tokio::pin!(stream);
        match stream.next().await.expect("frame").expect("ok") {
            SseFrame::Event { seq, .. } => assert_eq!(seq, 7),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn parser_skips_unknown_kind_and_reports_seq() {
        // F38: a frame whose JSON parses but whose `kind` is a
        // variant this build doesn't know (a *newer* coord) must be
        // surfaced as a skippable item carrying its seq — NOT as an
        // Err, which would tear the stream down and put the client
        // in a permanent reconnect loop (the resume cursor never
        // passes the unknown frame).
        let unknown = "event:FutureThing\nid:5\ndata:{\"kind\":\"FutureThing\"}\n\n";
        let data = "{\"seq\":6,\"at\":\"2026-05-29T14:32:00Z\",\"schema_version\":1,\
                    \"kind\":\"JobCreated\",\"job_id\":\"bobby\",\"name\":\"bobby-mig\",\
                    \"source\":\"nfs://src\",\"dest\":\"nfs://dst\",\"owner\":\"test\",\
                    \"config_hash\":\"ab\"}";
        let raw = format!("{unknown}event:JobCreated\nid:6\ndata:{data}\n\n");
        let stream = parse_sse_stream(bytes_stream(vec![raw.into_bytes()]));
        tokio::pin!(stream);
        match stream.next().await.expect("frame").expect("must not Err") {
            SseFrame::UnknownEvent { seq, kind } => {
                assert_eq!(seq, 5);
                assert_eq!(kind, "FutureThing");
            }
            other => panic!("expected UnknownEvent, got {other:?}"),
        }
        // The stream keeps going: the following valid frame arrives.
        match stream.next().await.expect("frame").expect("ok") {
            SseFrame::Event { seq, .. } => assert_eq!(seq, 6),
            other => panic!("expected Event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn malformed_data_still_errors() {
        // Corruption ≠ future-version skew. Pin the distinction:
        // data that is not JSON at all must still surface as
        // ClientError::Serde exactly as before F38 ...
        let raw = "event:JobCreated\nid:7\ndata:this is not json\n\n".to_string();
        let stream = parse_sse_stream(bytes_stream(vec![raw.into_bytes()]));
        tokio::pin!(stream);
        match stream.next().await.expect("frame").expect_err("must err") {
            ClientError::Serde(_) => {}
            other => panic!("expected Serde, got {other:?}"),
        }
        // ... and JSON that parses but has no string `kind` field is
        // also corruption (every versioned event carries the tag),
        // not skew — it must error, not be skipped.
        let raw = "event:JobCreated\nid:8\ndata:{\"seq\":8}\n\n".to_string();
        let stream = parse_sse_stream(bytes_stream(vec![raw.into_bytes()]));
        tokio::pin!(stream);
        match stream.next().await.expect("frame").expect_err("must err") {
            ClientError::Serde(_) => {}
            other => panic!("expected Serde, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn parser_errors_on_non_numeric_id() {
        let raw = "event:JobCreated\nid:abc\ndata:{}\n\n".to_string();
        let stream = parse_sse_stream(bytes_stream(vec![raw.into_bytes()]));
        tokio::pin!(stream);
        let err = stream.next().await.expect("frame").expect_err("must err");
        match err {
            ClientError::MalformedFrame(msg) => assert!(msg.contains("non-numeric")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn parser_errors_on_incomplete_frame() {
        // event + id but NO data — incomplete.
        let raw = "event:JobCreated\nid:1\n\n".to_string();
        let stream = parse_sse_stream(bytes_stream(vec![raw.into_bytes()]));
        tokio::pin!(stream);
        match stream.next().await.expect("frame").expect_err("err") {
            ClientError::MalformedFrame(msg) => assert!(msg.contains("incomplete")),
            other => panic!("unexpected: {other:?}"),
        }
    }
}
