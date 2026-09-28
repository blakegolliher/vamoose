//! Stable, bracketed, streaming SHA-256 content reads.
//!
//! Each side of a selected regular file is read through a fresh,
//! verifier-owned libnfs context in the order
//! `stat64 / open / fstat64 / read / EOF probe / fstat64 / stat64 / close`.
//! The identity tuple `(file type, size, mtime, ctime, dev, ino)` must agree
//! between the scan baseline and every observation, or the side is
//! `unstable`. Any NFS error without an observed mutation is `unreadable`.
//! Only two stable, complete digests are ever compared.
//!
//! The reader is a trait so the whole classification is testable without a
//! server; the libnfs implementation never lets the verifier touch raw FFI.

use crate::model::FileType;
use crate::scanner::{file_type_from_mode, full_path};
use crate::store::{ContentJob, Entry, Side, Store};
use anyhow::{Context, Result};
use migration_core::records::FailurePhase;
use migration_mover::libnfs::{ops, NfsContext};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

pub(crate) use migration_mover::libnfs::ops::StatSnapshot;

/// Jobs fetched from SQLite per coordinator round trip.
const DISPATCH_BATCH: u32 = 1_000;
/// Results grouped into one SQLite transaction when they are available
/// together; never a durability boundary a single result waits for.
const COMMIT_BATCH: usize = 256;

/// The bounded job queue holds at most this many jobs beyond the ones
/// workers are executing.
pub(crate) fn queue_capacity(content_workers: u32) -> usize {
    usize::try_from(content_workers)
        .unwrap_or(usize::MAX / 2)
        .saturating_mul(2)
}

/// One failed NFS operation: which call and the errno-style tag the mover's
/// error machinery produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadFailure {
    pub operation: &'static str,
    pub error: String,
}

/// Opaque token for an open file inside one reader.
#[derive(Debug)]
pub(crate) struct OpenHandle(u64);

/// Read-only NFS surface the bracket needs. Implementations own their
/// contexts; a reader is used by exactly one worker thread.
pub(crate) trait ContentReader: Send {
    fn stat(&mut self, path: &[u8]) -> Result<StatSnapshot, ReadFailure>;
    fn open(&mut self, path: &[u8]) -> Result<OpenHandle, ReadFailure>;
    fn fstat(&mut self, handle: &OpenHandle) -> Result<StatSnapshot, ReadFailure>;
    fn pread(
        &mut self,
        handle: &OpenHandle,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, ReadFailure>;
    fn close(&mut self, handle: OpenHandle) -> Result<(), ReadFailure>;
}

/// Creates fresh readers for worker threads. A failure here is an
/// operational failure of the whole content phase, never a reason to hash
/// fewer files.
pub(crate) trait ReaderFactory: Sync {
    fn open_side(&self, side: Side) -> Result<Box<dyn ContentReader>>;
}

pub(crate) struct LibnfsReaderFactory {
    pub source_url: String,
    pub destination_url: String,
    pub rpc_timeout_ms: u32,
}

impl ReaderFactory for LibnfsReaderFactory {
    fn open_side(&self, side: Side) -> Result<Box<dyn ContentReader>> {
        let (url, latency_side) = match side {
            Side::Source => (&self.source_url, migration_core::latency::Side::Src),
            Side::Destination => (&self.destination_url, migration_core::latency::Side::Dst),
        };
        let mut ctx = NfsContext::mount_url(url, self.rpc_timeout_ms)
            .with_context(|| format!("mounting {} for content verification", side.label()))?;
        ctx.set_side(latency_side);
        Ok(Box::new(LibnfsReader {
            ctx,
            handles: HashMap::new(),
            next_handle: 0,
        }))
    }
}

struct LibnfsReader {
    ctx: NfsContext,
    handles: HashMap<u64, ops::NfsFh>,
    next_handle: u64,
}

fn failure(operation: &'static str, error: migration_mover::MoveError) -> ReadFailure {
    ReadFailure {
        operation,
        error: error.error,
    }
}

impl ContentReader for LibnfsReader {
    fn stat(&mut self, path: &[u8]) -> Result<StatSnapshot, ReadFailure> {
        ops::stat_snapshot(&mut self.ctx, path).map_err(|e| failure("stat", e))
    }

    fn open(&mut self, path: &[u8]) -> Result<OpenHandle, ReadFailure> {
        let fh = ops::open_read(&mut self.ctx, path).map_err(|e| failure("open", e))?;
        let token = self.next_handle;
        self.next_handle += 1;
        self.handles.insert(token, fh);
        Ok(OpenHandle(token))
    }

    fn fstat(&mut self, handle: &OpenHandle) -> Result<StatSnapshot, ReadFailure> {
        let fh = self.handles.get(&handle.0).ok_or(ReadFailure {
            operation: "fstat",
            error: "EBADF".to_string(),
        })?;
        ops::fstat_snapshot(&mut self.ctx, fh).map_err(|e| failure("fstat", e))
    }

    fn pread(
        &mut self,
        handle: &OpenHandle,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, ReadFailure> {
        let fh = self.handles.get(&handle.0).ok_or(ReadFailure {
            operation: "pread",
            error: "EBADF".to_string(),
        })?;
        ops::pread(&mut self.ctx, fh, offset, buf).map_err(|e| failure("pread", e))
    }

    fn close(&mut self, handle: OpenHandle) -> Result<(), ReadFailure> {
        let fh = self.handles.remove(&handle.0).ok_or(ReadFailure {
            operation: "close",
            error: "EBADF".to_string(),
        })?;
        ops::close_fh(&mut self.ctx, fh, FailurePhase::Read).map_err(|e| failure("close", e))
    }
}

/// The stable identity tuple compared across every observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Identity {
    pub file_type: FileType,
    pub size: u64,
    pub mtime_sec: i64,
    pub mtime_nsec: u32,
    pub ctime_sec: i64,
    pub ctime_nsec: u32,
    pub dev: u64,
    pub ino: u64,
}

impl Identity {
    pub(crate) fn of_entry(entry: &Entry) -> Self {
        Self {
            file_type: entry.file_type,
            size: entry.size,
            mtime_sec: entry.mtime_sec,
            mtime_nsec: entry.mtime_nsec,
            ctime_sec: entry.ctime_sec,
            ctime_nsec: entry.ctime_nsec,
            dev: entry.dev,
            ino: entry.ino,
        }
    }

    fn of_snapshot(snapshot: &StatSnapshot) -> Result<Self, String> {
        let mode = u32::try_from(snapshot.mode).map_err(|_| format!("mode {}", snapshot.mode))?;
        let nanoseconds = |label: &str, value: u64| {
            if value >= 1_000_000_000 {
                return Err(format!("{label} nanoseconds {value}"));
            }
            u32::try_from(value).map_err(|_| format!("{label} nanoseconds {value}"))
        };
        Ok(Self {
            file_type: file_type_from_mode(mode),
            size: snapshot.size,
            mtime_sec: i64::try_from(snapshot.mtime_sec)
                .map_err(|_| format!("mtime seconds {}", snapshot.mtime_sec))?,
            mtime_nsec: nanoseconds("mtime", snapshot.mtime_nsec)?,
            ctime_sec: i64::try_from(snapshot.ctime_sec)
                .map_err(|_| format!("ctime seconds {}", snapshot.ctime_sec))?,
            ctime_nsec: nanoseconds("ctime", snapshot.ctime_nsec)?,
            dev: snapshot.dev,
            ino: snapshot.ino,
        })
    }

    fn describe_change(&self, observed: &Self) -> String {
        let mut changes = Vec::new();
        if self.file_type != observed.file_type {
            changes.push(format!(
                "type {:?} -> {:?}",
                self.file_type, observed.file_type
            ));
        }
        if self.size != observed.size {
            changes.push(format!("size {} -> {}", self.size, observed.size));
        }
        if (self.mtime_sec, self.mtime_nsec) != (observed.mtime_sec, observed.mtime_nsec) {
            changes.push(format!(
                "mtime {}.{:09} -> {}.{:09}",
                self.mtime_sec, self.mtime_nsec, observed.mtime_sec, observed.mtime_nsec
            ));
        }
        if (self.ctime_sec, self.ctime_nsec) != (observed.ctime_sec, observed.ctime_nsec) {
            changes.push(format!(
                "ctime {}.{:09} -> {}.{:09}",
                self.ctime_sec, self.ctime_nsec, observed.ctime_sec, observed.ctime_nsec
            ));
        }
        if (self.dev, self.ino) != (observed.dev, observed.ino) {
            changes.push(format!(
                "identity {}:{} -> {}:{}",
                self.dev, self.ino, observed.dev, observed.ino
            ));
        }
        changes.join(", ")
    }
}

/// Terminal result for one side of one job. Persisted as JSON in SQLite and
/// replayed in raw-path order when the mismatch artifact is written.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum SideResult {
    Hashed { sha256: String, bytes: u64 },
    Unreadable { operation: String, error: String },
    Unstable { detail: String },
}

impl SideResult {
    pub(crate) fn status(&self) -> &'static str {
        match self {
            Self::Hashed { .. } => "hashed",
            Self::Unreadable { .. } => "unreadable",
            Self::Unstable { .. } => "unstable",
        }
    }

    fn unreadable(failure: ReadFailure) -> Self {
        Self::Unreadable {
            operation: failure.operation.to_string(),
            error: failure.error,
        }
    }

    fn unstable(detail: String) -> Self {
        Self::Unstable { detail }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContentOutcome {
    pub source: SideResult,
    pub destination: SideResult,
}

/// Summary of a job outcome, in status-priority order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutcomeKind {
    Match,
    Content,
    Unstable,
    Unreadable,
}

impl OutcomeKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Match => "match",
            Self::Content => "content",
            Self::Unstable => "unstable",
            Self::Unreadable => "unreadable",
        }
    }
}

impl ContentOutcome {
    pub(crate) fn kind(&self) -> OutcomeKind {
        let sides = [&self.source, &self.destination];
        if sides
            .iter()
            .any(|side| matches!(side, SideResult::Unreadable { .. }))
        {
            OutcomeKind::Unreadable
        } else if sides
            .iter()
            .any(|side| matches!(side, SideResult::Unstable { .. }))
        {
            OutcomeKind::Unstable
        } else if self.content_mismatch().is_some() {
            OutcomeKind::Content
        } else {
            OutcomeKind::Match
        }
    }

    /// `(source, destination)` digests when both sides hashed completely
    /// and differ in SHA-256 or logical byte count.
    pub(crate) fn content_mismatch(&self) -> Option<(ContentDigest<'_>, ContentDigest<'_>)> {
        match (&self.source, &self.destination) {
            (
                SideResult::Hashed {
                    sha256: source,
                    bytes: source_bytes,
                },
                SideResult::Hashed {
                    sha256: destination,
                    bytes: destination_bytes,
                },
            ) if source != destination || source_bytes != destination_bytes => Some((
                ContentDigest {
                    sha256: source,
                    bytes: *source_bytes,
                },
                ContentDigest {
                    sha256: destination,
                    bytes: *destination_bytes,
                },
            )),
            _ => None,
        }
    }
}

/// A complete, stable digest of one side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentDigest<'a> {
    pub sha256: &'a str,
    pub bytes: u64,
}

/// Reads one side with the full stability bracket. `buffer` is the worker's
/// reusable read buffer; nothing file-sized is ever allocated.
pub(crate) fn hash_side(
    reader: &mut dyn ContentReader,
    path: &[u8],
    baseline: &Identity,
    buffer: &mut [u8],
) -> SideResult {
    let path_before = match reader.stat(path) {
        Ok(snapshot) => snapshot,
        Err(failure) => return SideResult::unreadable(failure),
    };
    let path_before = match Identity::of_snapshot(&path_before) {
        Ok(identity) => identity,
        Err(detail) => return decode_failure(detail),
    };
    if path_before != *baseline {
        return SideResult::unstable(format!(
            "path attributes differ from the scan baseline before open: {}",
            baseline.describe_change(&path_before)
        ));
    }
    let handle = match reader.open(path) {
        Ok(handle) => handle,
        Err(failure) => return SideResult::unreadable(failure),
    };
    let result = hash_open_file(reader, &handle, path, baseline, buffer);
    match reader.close(handle) {
        Ok(()) => result,
        Err(close) => attach_close_failure(result, close),
    }
}

fn decode_failure(detail: String) -> SideResult {
    SideResult::Unreadable {
        operation: "decode_attributes".to_string(),
        error: format!("invalid {detail}"),
    }
}

/// Steps 3–8 of the bracket on an open handle. The caller always closes.
fn hash_open_file(
    reader: &mut dyn ContentReader,
    handle: &OpenHandle,
    path: &[u8],
    baseline: &Identity,
    buffer: &mut [u8],
) -> SideResult {
    let before = match reader.fstat(handle) {
        Ok(snapshot) => snapshot,
        Err(failure) => return SideResult::unreadable(failure),
    };
    let before = match Identity::of_snapshot(&before) {
        Ok(identity) => identity,
        Err(detail) => return decode_failure(detail),
    };
    if before != *baseline {
        return SideResult::unstable(format!(
            "open handle attributes differ from the scan baseline: {}",
            baseline.describe_change(&before)
        ));
    }

    let size = before.size;
    let mut hasher = Sha256::new();
    let mut offset = 0u64;
    while offset < size {
        let remaining = size - offset;
        let want = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
        let read = match reader.pread(handle, offset, &mut buffer[..want]) {
            Ok(read) => read,
            Err(failure) => return SideResult::unreadable(failure),
        };
        if read == 0 {
            return SideResult::unstable(format!(
                "early EOF at byte {offset} of the bracketed size {size}"
            ));
        }
        if read > want {
            return SideResult::unstable(format!(
                "read returned {read} bytes for a {want}-byte request at offset {offset}"
            ));
        }
        hasher.update(&buffer[..read]);
        offset += read as u64;
    }
    match reader.pread(handle, size, &mut buffer[..1]) {
        Ok(0) => {}
        Ok(extra) => {
            return SideResult::unstable(format!(
                "{extra} byte(s) readable beyond the bracketed size {size}"
            ))
        }
        Err(failure) => return SideResult::unreadable(failure),
    }

    let after = match reader.fstat(handle) {
        Ok(snapshot) => snapshot,
        Err(failure) => return SideResult::unreadable(failure),
    };
    let after = match Identity::of_snapshot(&after) {
        Ok(identity) => identity,
        Err(detail) => return decode_failure(detail),
    };
    if after != before {
        return SideResult::unstable(format!(
            "open handle attributes changed during the read: {}",
            before.describe_change(&after)
        ));
    }
    let path_after = match reader.stat(path) {
        Ok(snapshot) => snapshot,
        Err(failure) => return SideResult::unreadable(failure),
    };
    let path_after = match Identity::of_snapshot(&path_after) {
        Ok(identity) => identity,
        Err(detail) => return decode_failure(detail),
    };
    if path_after != before {
        return SideResult::unstable(format!(
            "path no longer names the file that was read: {}",
            before.describe_change(&path_after)
        ));
    }
    SideResult::Hashed {
        sha256: hex::encode(hasher.finalize()),
        bytes: size,
    }
}

/// A close failure never converts a stable digest into a pass; the primary
/// classification is kept and the close error attached as detail.
fn attach_close_failure(result: SideResult, close: ReadFailure) -> SideResult {
    match result {
        SideResult::Hashed { .. } => SideResult::Unreadable {
            operation: "close".to_string(),
            error: close.error,
        },
        SideResult::Unreadable { operation, error } => SideResult::Unreadable {
            operation,
            error: format!("{error}; close failed: {}", close.error),
        },
        SideResult::Unstable { detail } => SideResult::Unstable {
            detail: format!("{detail}; close failed: {}", close.error),
        },
    }
}

pub(crate) struct ContentPhaseConfig {
    pub content_workers: u32,
    pub content_read_size: u32,
    pub source_root: String,
    pub destination_root: String,
}

enum WorkerMessage {
    Done {
        path: Vec<u8>,
        outcome: ContentOutcome,
    },
    Fatal {
        worker: usize,
        detail: String,
    },
}

/// Runs every pending content job through `content_workers` long-lived
/// worker threads. Jobs flow through a bounded queue of
/// `2 * content_workers`; results are written to SQLite on this thread.
/// Returns the operational failure that stopped the phase early, if any.
/// Completed jobs are durable regardless.
pub(crate) fn run_content_phase(
    store: &mut Store,
    config: &ContentPhaseConfig,
    factory: &dyn ReaderFactory,
) -> Result<Option<String>> {
    let workers = usize::try_from(config.content_workers).context("worker count")?;
    let read_size = usize::try_from(config.content_read_size).context("read size")?;
    let (job_tx, job_rx) = mpsc::sync_channel::<ContentJob>(queue_capacity(config.content_workers));
    let job_rx = Arc::new(Mutex::new(job_rx));
    let (message_tx, message_rx) = mpsc::channel::<WorkerMessage>();

    std::thread::scope(|scope| -> Result<Option<String>> {
        for index in 0..workers {
            let job_rx = Arc::clone(&job_rx);
            let message_tx = message_tx.clone();
            scope.spawn(move || worker_loop(index, config, factory, job_rx, message_tx, read_size));
        }
        // Only workers may hold the receiver: once they are all gone, sends
        // fail instead of blocking forever on a full queue.
        drop(job_rx);
        drop(message_tx);

        let mut fatal: Option<String> = None;
        let mut results: Vec<(Vec<u8>, ContentOutcome)> = Vec::new();
        let mut last_path: Option<Vec<u8>> = None;
        'dispatch: loop {
            let batch = store.pending_jobs_after(last_path.as_deref(), DISPATCH_BATCH)?;
            if batch.is_empty() {
                break;
            }
            for job in batch {
                last_path = Some(job.path.clone());
                if job_tx.send(job).is_err() {
                    break 'dispatch;
                }
                while let Ok(message) = message_rx.try_recv() {
                    handle_message(message, &mut results, &mut fatal);
                }
                if !results.is_empty() {
                    store.complete_jobs(&results)?;
                    results.clear();
                }
                if fatal.is_some() {
                    break 'dispatch;
                }
            }
        }
        drop(job_tx);
        while let Ok(message) = message_rx.recv() {
            handle_message(message, &mut results, &mut fatal);
            while results.len() < COMMIT_BATCH {
                match message_rx.try_recv() {
                    Ok(message) => handle_message(message, &mut results, &mut fatal),
                    Err(_) => break,
                }
            }
            if !results.is_empty() {
                store.complete_jobs(&results)?;
                results.clear();
            }
        }
        Ok(fatal)
    })
}

fn handle_message(
    message: WorkerMessage,
    results: &mut Vec<(Vec<u8>, ContentOutcome)>,
    fatal: &mut Option<String>,
) {
    match message {
        WorkerMessage::Done { path, outcome } => results.push((path, outcome)),
        WorkerMessage::Fatal { worker, detail } => {
            if fatal.is_none() {
                *fatal = Some(format!("content worker {worker}: {detail}"));
            }
        }
    }
}

fn worker_loop(
    index: usize,
    config: &ContentPhaseConfig,
    factory: &dyn ReaderFactory,
    job_rx: Arc<Mutex<mpsc::Receiver<ContentJob>>>,
    message_tx: mpsc::Sender<WorkerMessage>,
    read_size: usize,
) {
    let mut readers = match (
        factory.open_side(Side::Source),
        factory.open_side(Side::Destination),
    ) {
        (Ok(source), Ok(destination)) => (source, destination),
        (Err(error), _) | (_, Err(error)) => {
            let _ = message_tx.send(WorkerMessage::Fatal {
                worker: index,
                detail: format!("{error:#}"),
            });
            return;
        }
    };
    let mut buffer = vec![0u8; read_size.max(1)];
    // Workers alternate which side they read first, starting on different
    // sides, so aggregate traffic stays balanced across both servers.
    let mut source_first = index.is_multiple_of(2);
    loop {
        let job = {
            let receiver = match job_rx.lock() {
                Ok(receiver) => receiver,
                Err(_) => return,
            };
            receiver.recv()
        };
        let Ok(job) = job else {
            return;
        };
        let outcome = process_job(&job, config, &mut readers, &mut buffer, source_first);
        source_first = !source_first;
        if message_tx
            .send(WorkerMessage::Done {
                path: job.path,
                outcome,
            })
            .is_err()
        {
            return;
        }
    }
}

fn process_job(
    job: &ContentJob,
    config: &ContentPhaseConfig,
    readers: &mut (Box<dyn ContentReader>, Box<dyn ContentReader>),
    buffer: &mut [u8],
    source_first: bool,
) -> ContentOutcome {
    let mut side_result = |side: Side, buffer: &mut [u8]| -> SideResult {
        let (root, reader, baseline) = match side {
            Side::Source => (&config.source_root, &mut readers.0, &job.source),
            Side::Destination => (&config.destination_root, &mut readers.1, &job.destination),
        };
        let full = match full_path(root, &job.path) {
            Ok(full) => full,
            Err(error) => {
                return SideResult::Unreadable {
                    operation: "path".to_string(),
                    error: error.to_string(),
                }
            }
        };
        hash_side(
            reader.as_mut(),
            &full,
            &Identity::of_entry(baseline),
            buffer,
        )
    };
    if source_first {
        let source = side_result(Side::Source, buffer);
        let destination = side_result(Side::Destination, buffer);
        ContentOutcome {
            source,
            destination,
        }
    } else {
        let destination = side_result(Side::Destination, buffer);
        let source = side_result(Side::Source, buffer);
        ContentOutcome {
            source,
            destination,
        }
    }
}

#[cfg(test)]
pub(crate) mod fake {
    //! In-memory `ContentReader` with per-file fault injection, shared by
    //! the classification tests here and the end-to-end tests in `lib.rs`.

    use super::*;

    #[derive(Debug, Clone, Default)]
    pub(crate) struct Faults {
        pub stat_error: Option<&'static str>,
        pub open_error: Option<&'static str>,
        pub fstat_error: Option<&'static str>,
        /// Fail the read that starts at this offset.
        pub read_error_at: Option<(u64, &'static str)>,
        pub close_error: Option<&'static str>,
        /// Handle observations (`fstat`) report this snapshot instead.
        pub handle_snapshot: Option<StatSnapshot>,
        /// The post-read handle observation reports this snapshot.
        pub after_read_snapshot: Option<StatSnapshot>,
        /// Largest byte count one read returns (models short reads).
        pub max_read: Option<usize>,
        /// Sleep this long per read (models slow servers).
        pub read_delay: Option<std::time::Duration>,
    }

    #[derive(Debug, Clone)]
    pub(crate) struct FakeFile {
        pub data: Vec<u8>,
        pub snapshot: StatSnapshot,
        pub faults: Faults,
    }

    #[derive(Debug, Default)]
    pub(crate) struct FakeState {
        pub files: HashMap<Vec<u8>, FakeFile>,
        pub open_now: usize,
        pub max_open: usize,
        pub opens: HashMap<Vec<u8>, usize>,
        pub closes: usize,
        pub reads: Vec<(Vec<u8>, u64, usize)>,
        /// Sides the factory refuses to open.
        pub refuse_side: Option<Side>,
    }

    pub(crate) type SharedState = Arc<Mutex<FakeState>>;

    pub(crate) fn snapshot(size: u64) -> StatSnapshot {
        StatSnapshot {
            dev: 1,
            ino: 1,
            mode: 0o100644,
            nlink: 1,
            uid: 10,
            gid: 20,
            size,
            mtime_sec: 100,
            mtime_nsec: 123_456_789,
            ctime_sec: 100,
            ctime_nsec: 0,
        }
    }

    pub(crate) fn file(data: &[u8]) -> FakeFile {
        FakeFile {
            data: data.to_vec(),
            snapshot: snapshot(data.len() as u64),
            faults: Faults::default(),
        }
    }

    pub(crate) struct FakeFactory {
        pub source: SharedState,
        pub destination: SharedState,
    }

    impl ReaderFactory for FakeFactory {
        fn open_side(&self, side: Side) -> Result<Box<dyn ContentReader>> {
            let state = match side {
                Side::Source => &self.source,
                Side::Destination => &self.destination,
            };
            if state.lock().unwrap().refuse_side == Some(side) {
                anyhow::bail!("mount {} refused by the fake", side.label());
            }
            Ok(Box::new(FakeReader {
                state: Arc::clone(state),
                handles: HashMap::new(),
                next: 0,
            }))
        }
    }

    pub(crate) struct FakeReader {
        pub state: SharedState,
        handles: HashMap<u64, (Vec<u8>, u64)>,
        next: u64,
    }

    impl FakeReader {
        pub(crate) fn new(state: SharedState) -> Self {
            Self {
                state,
                handles: HashMap::new(),
                next: 0,
            }
        }

        fn err(operation: &'static str, error: &'static str) -> ReadFailure {
            ReadFailure {
                operation,
                error: error.to_string(),
            }
        }
    }

    impl ContentReader for FakeReader {
        fn stat(&mut self, path: &[u8]) -> Result<StatSnapshot, ReadFailure> {
            let state = self.state.lock().unwrap();
            let file = state
                .files
                .get(path)
                .ok_or_else(|| Self::err("stat", "ENOENT"))?;
            if let Some(error) = file.faults.stat_error {
                return Err(Self::err("stat", error));
            }
            Ok(file.snapshot)
        }

        fn open(&mut self, path: &[u8]) -> Result<OpenHandle, ReadFailure> {
            let mut state = self.state.lock().unwrap();
            let file = state
                .files
                .get(path)
                .ok_or_else(|| Self::err("open", "ENOENT"))?;
            if let Some(error) = file.faults.open_error {
                return Err(Self::err("open", error));
            }
            let token = self.next;
            self.next += 1;
            self.handles.insert(token, (path.to_vec(), 0));
            *state.opens.entry(path.to_vec()).or_insert(0) += 1;
            state.open_now += 1;
            state.max_open = state.max_open.max(state.open_now);
            Ok(OpenHandle(token))
        }

        fn fstat(&mut self, handle: &OpenHandle) -> Result<StatSnapshot, ReadFailure> {
            let (path, reads) = self
                .handles
                .get(&handle.0)
                .cloned()
                .ok_or_else(|| Self::err("fstat", "EBADF"))?;
            let state = self.state.lock().unwrap();
            let file = &state.files[&path];
            if let Some(error) = file.faults.fstat_error {
                return Err(Self::err("fstat", error));
            }
            if reads > 0 {
                if let Some(after) = file.faults.after_read_snapshot {
                    return Ok(after);
                }
            }
            Ok(file.faults.handle_snapshot.unwrap_or(file.snapshot))
        }

        fn pread(
            &mut self,
            handle: &OpenHandle,
            offset: u64,
            buf: &mut [u8],
        ) -> Result<usize, ReadFailure> {
            let entry = self
                .handles
                .get_mut(&handle.0)
                .ok_or_else(|| Self::err("pread", "EBADF"))?;
            entry.1 += 1;
            let path = entry.0.clone();
            let (data, faults) = {
                let state = self.state.lock().unwrap();
                let file = &state.files[&path];
                (file.data.clone(), file.faults.clone())
            };
            if let Some((at, error)) = faults.read_error_at {
                if at == offset {
                    return Err(Self::err("pread", error));
                }
            }
            if let Some(delay) = faults.read_delay {
                std::thread::sleep(delay);
            }
            let start = usize::try_from(offset).unwrap_or(usize::MAX);
            let available = data.get(start..).unwrap_or(&[]);
            let mut n = available.len().min(buf.len());
            if let Some(max) = faults.max_read {
                n = n.min(max);
            }
            buf[..n].copy_from_slice(&available[..n]);
            let mut state = self.state.lock().unwrap();
            state.reads.push((path, offset, n));
            Ok(n)
        }

        fn close(&mut self, handle: OpenHandle) -> Result<(), ReadFailure> {
            let (path, _) = self
                .handles
                .remove(&handle.0)
                .ok_or_else(|| Self::err("close", "EBADF"))?;
            let mut state = self.state.lock().unwrap();
            state.open_now -= 1;
            state.closes += 1;
            if let Some(error) = state.files[&path].faults.close_error {
                return Err(Self::err("close", error));
            }
            Ok(())
        }
    }

    impl FakeState {
        pub(crate) fn insert(&mut self, path: &[u8], file: FakeFile) {
            self.files.insert(path.to_vec(), file);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::*;
    use super::*;

    fn baseline(file: &FakeFile) -> Identity {
        Identity::of_snapshot(&file.snapshot).unwrap()
    }

    fn run(file: FakeFile, read_size: usize) -> (SideResult, SharedState) {
        let state: SharedState = Arc::new(Mutex::new(FakeState::default()));
        let baseline = baseline(&file);
        state.lock().unwrap().insert(b"/f", file);
        let mut reader = FakeReader::new(Arc::clone(&state));
        let mut buffer = vec![0u8; read_size];
        let result = hash_side(&mut reader, b"/f", &baseline, &mut buffer);
        (result, state)
    }

    fn sha(data: &[u8]) -> String {
        hex::encode(Sha256::digest(data))
    }

    fn assert_closed(state: &SharedState) {
        let state = state.lock().unwrap();
        assert_eq!(state.open_now, 0, "handle leaked");
        let opens: usize = state.opens.values().sum();
        assert_eq!(
            state.closes, opens,
            "close must run on every post-open branch"
        );
    }

    #[test]
    fn equal_multi_chunk_and_short_read_streams_hash_identically() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let (whole, state) = run(file(&data), 8);
        assert_eq!(
            whole,
            SideResult::Hashed {
                sha256: sha(&data),
                bytes: 1000
            }
        );
        assert_closed(&state);
        let reads = state.lock().unwrap().reads.len();
        assert_eq!(reads, 125 + 1, "eight-byte chunks plus the EOF probe");

        let mut short = file(&data);
        short.faults.max_read = Some(3);
        let (result, state) = run(short, 64);
        assert_eq!(result, whole);
        assert_closed(&state);
    }

    #[test]
    fn same_size_corruption_is_a_content_mismatch_only_when_both_sides_are_stable() {
        let (a, _) = run(file(b"hello world"), 4);
        let (b, _) = run(file(b"hello_world"), 4);
        let outcome = ContentOutcome {
            source: a.clone(),
            destination: b,
        };
        assert_eq!(outcome.kind(), OutcomeKind::Content);
        let (source, destination) = outcome.content_mismatch().unwrap();
        assert_eq!(
            (source.sha256, source.bytes),
            (sha(b"hello world").as_str(), 11)
        );
        assert_eq!(
            (destination.sha256, destination.bytes),
            (sha(b"hello_world").as_str(), 11)
        );
        let same = ContentOutcome {
            source: a.clone(),
            destination: a,
        };
        assert_eq!(same.kind(), OutcomeKind::Match);
        assert!(same.content_mismatch().is_none());
    }

    #[test]
    fn zero_length_files_hash_the_empty_stream_and_prove_eof() {
        let (result, state) = run(file(b""), 16);
        assert_eq!(
            result,
            SideResult::Hashed {
                sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
                bytes: 0
            }
        );
        let reads = state.lock().unwrap().reads.clone();
        assert_eq!(reads, vec![(b"/f".to_vec(), 0, 0)], "exactly one EOF probe");
        assert_closed(&state);
    }

    #[test]
    fn early_eof_and_bytes_beyond_size_are_unstable() {
        let mut truncated = file(b"0123456789");
        truncated.data.truncate(6);
        let (result, state) = run(truncated, 4);
        assert!(
            matches!(&result, SideResult::Unstable { detail } if detail.contains("early EOF")),
            "{result:?}"
        );
        assert_closed(&state);

        let mut grown = file(b"0123456789");
        grown.data.extend_from_slice(b"extra");
        let (result, state) = run(grown, 4);
        assert!(
            matches!(&result, SideResult::Unstable { detail } if detail.contains("beyond")),
            "{result:?}"
        );
        assert_closed(&state);
    }

    #[test]
    fn mutation_before_open_on_the_handle_and_on_the_path_is_unstable() {
        // Scan-to-open mutation: the baseline predates a size change.
        let changed = file(b"new contents");
        let mut stale = changed.clone();
        stale.snapshot.size = 3;
        stale.snapshot.mtime_sec = 1;
        let state: SharedState = Arc::new(Mutex::new(FakeState::default()));
        state.lock().unwrap().insert(b"/f", changed);
        let mut reader = FakeReader::new(Arc::clone(&state));
        let result = hash_side(&mut reader, b"/f", &baseline(&stale), &mut [0u8; 8]);
        assert!(
            matches!(&result, SideResult::Unstable { detail } if detail.contains("before open")),
            "{result:?}"
        );
        assert_eq!(state.lock().unwrap().opens.len(), 0, "not opened");

        // Handle observation disagrees with the path observation.
        let mut swapped = file(b"contents");
        let mut other = swapped.snapshot;
        other.ino = 99;
        swapped.faults.handle_snapshot = Some(other);
        let (result, state) = run(swapped, 8);
        assert!(
            matches!(&result, SideResult::Unstable { detail } if detail.contains("identity 1:1 -> 1:99")),
            "{result:?}"
        );
        assert_closed(&state);

        // Handle mutates while it is being read.
        let mut torn = file(b"contents");
        let mut later = torn.snapshot;
        later.mtime_nsec += 1;
        torn.faults.after_read_snapshot = Some(later);
        let (result, state) = run(torn, 8);
        assert!(
            matches!(&result, SideResult::Unstable { detail } if detail.contains("during the read")),
            "{result:?}"
        );
        assert_closed(&state);
    }

    #[test]
    fn path_replacement_after_the_read_is_unstable() {
        // The final path stat must name the file that was read; model a
        // rename-over by making the path observation change after the
        // first read.
        struct Replacing {
            inner: FakeReader,
            reads: usize,
        }
        impl ContentReader for Replacing {
            fn stat(&mut self, path: &[u8]) -> Result<StatSnapshot, ReadFailure> {
                let mut snapshot = self.inner.stat(path)?;
                if self.reads > 0 {
                    snapshot.ino = 7;
                }
                Ok(snapshot)
            }
            fn open(&mut self, path: &[u8]) -> Result<OpenHandle, ReadFailure> {
                self.inner.open(path)
            }
            fn fstat(&mut self, handle: &OpenHandle) -> Result<StatSnapshot, ReadFailure> {
                self.inner.fstat(handle)
            }
            fn pread(
                &mut self,
                handle: &OpenHandle,
                offset: u64,
                buf: &mut [u8],
            ) -> Result<usize, ReadFailure> {
                self.reads += 1;
                self.inner.pread(handle, offset, buf)
            }
            fn close(&mut self, handle: OpenHandle) -> Result<(), ReadFailure> {
                self.inner.close(handle)
            }
        }
        let state: SharedState = Arc::new(Mutex::new(FakeState::default()));
        let target = file(b"contents");
        let baseline = baseline(&target);
        state.lock().unwrap().insert(b"/f", target);
        let mut reader = Replacing {
            inner: FakeReader::new(Arc::clone(&state)),
            reads: 0,
        };
        let result = hash_side(&mut reader, b"/f", &baseline, &mut [0u8; 8]);
        assert!(
            matches!(&result, SideResult::Unstable { detail } if detail.contains("no longer names")),
            "{result:?}"
        );
        assert_closed(&state);
    }

    #[test]
    fn nfs_failures_without_observed_mutation_are_unreadable() {
        for (fault, operation) in [
            (
                Faults {
                    stat_error: Some("EACCES"),
                    ..Faults::default()
                },
                "stat",
            ),
            (
                Faults {
                    open_error: Some("EACCES"),
                    ..Faults::default()
                },
                "open",
            ),
            (
                Faults {
                    fstat_error: Some("EIO"),
                    ..Faults::default()
                },
                "fstat",
            ),
            (
                Faults {
                    read_error_at: Some((4, "EIO")),
                    ..Faults::default()
                },
                "pread",
            ),
            (
                Faults {
                    read_error_at: Some((8, "EINTR")),
                    ..Faults::default()
                },
                "pread",
            ),
        ] {
            let mut file = file(b"01234567");
            file.faults = fault;
            let (result, state) = run(file, 4);
            match &result {
                SideResult::Unreadable {
                    operation: seen, ..
                } => assert_eq!(seen, operation),
                other => panic!("expected unreadable {operation}, got {other:?}"),
            }
            assert_closed(&state);
        }
        // A missing path is unreadable, not unstable: no mutation was seen.
        let state: SharedState = Arc::new(Mutex::new(FakeState::default()));
        let mut reader = FakeReader::new(Arc::clone(&state));
        let result = hash_side(
            &mut reader,
            b"/missing",
            &baseline(&file(b"x")),
            &mut [0u8; 4],
        );
        assert_eq!(
            result,
            SideResult::Unreadable {
                operation: "stat".into(),
                error: "ENOENT".into()
            }
        );
    }

    #[test]
    fn close_failures_are_retained_on_every_branch() {
        let mut clean = file(b"abc");
        clean.faults.close_error = Some("EIO");
        let (result, state) = run(clean, 4);
        assert_eq!(
            result,
            SideResult::Unreadable {
                operation: "close".into(),
                error: "EIO".into()
            }
        );
        assert_closed(&state);

        let mut unreadable = file(b"abc");
        unreadable.faults.read_error_at = Some((0, "EIO"));
        unreadable.faults.close_error = Some("EBADF");
        let (result, _) = run(unreadable, 4);
        assert_eq!(
            result,
            SideResult::Unreadable {
                operation: "pread".into(),
                error: "EIO; close failed: EBADF".into()
            }
        );

        let mut unstable = file(b"abc");
        unstable.data.truncate(1);
        unstable.faults.close_error = Some("EBADF");
        let (result, _) = run(unstable, 4);
        assert!(
            matches!(&result, SideResult::Unstable { detail } if detail.ends_with("close failed: EBADF")),
            "{result:?}"
        );
    }

    #[test]
    fn unreadable_or_unstable_sides_never_emit_a_content_digest_mismatch() {
        let (hashed, _) = run(file(b"abc"), 4);
        let unreadable = SideResult::Unreadable {
            operation: "open".into(),
            error: "EACCES".into(),
        };
        let unstable = SideResult::Unstable {
            detail: "early EOF".into(),
        };
        for (source, destination, kind) in [
            (unreadable.clone(), hashed.clone(), OutcomeKind::Unreadable),
            (hashed.clone(), unreadable.clone(), OutcomeKind::Unreadable),
            (unstable.clone(), hashed.clone(), OutcomeKind::Unstable),
            (
                unreadable.clone(),
                unstable.clone(),
                OutcomeKind::Unreadable,
            ),
        ] {
            let outcome = ContentOutcome {
                source,
                destination,
            };
            assert_eq!(outcome.kind(), kind);
            assert!(outcome.content_mismatch().is_none());
        }
    }

    #[test]
    fn identity_decoding_rejects_out_of_range_attributes() {
        let mut bad = snapshot(1);
        bad.mtime_nsec = 1_000_000_000;
        assert!(Identity::of_snapshot(&bad).is_err());
        let mut bad = snapshot(1);
        bad.ctime_sec = u64::MAX;
        assert!(Identity::of_snapshot(&bad).is_err());
        let mut file = file(b"x");
        file.faults.handle_snapshot = Some(bad);
        let (result, state) = run(file, 4);
        assert!(
            matches!(&result, SideResult::Unreadable { operation, .. } if operation == "decode_attributes"),
            "{result:?}"
        );
        assert_closed(&state);
    }

    #[test]
    fn queue_capacity_is_twice_the_worker_count() {
        assert_eq!(queue_capacity(1), 2);
        assert_eq!(queue_capacity(8), 16);
    }
}
