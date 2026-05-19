//! `LibnfsContextPool` trait and the `SimplePool` implementation used
//! in M2.
//!
//! Per the M2 design (decision #1), the pool surface is built now even
//! though M2 only ever holds one source/dest pair. Bolting concurrency
//! onto a `Mutex<Pair>` later is the kind of refactor that breaks
//! subtle correctness invariants — better to introduce the trait
//! before the data path needs it.
//!
//! The M2 `SimplePool` holds a single pre-mounted pair returned to a
//! tokio `mpsc::UnboundedChannel` on guard drop. Acquire blocks if the
//! pair is held. M3 reshapes this into an N-pair pool driving
//! concurrent copies; the trait surface should not need to change.

use super::NfsContext;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Single in-flight (src, dst) pair handed out by [`LibnfsContextPool`].
/// Returns the pair to the pool on drop.
pub struct ContextPair {
    src: Option<NfsContext>,
    dst: Option<NfsContext>,
    return_to: mpsc::UnboundedSender<(NfsContext, NfsContext)>,
}

impl ContextPair {
    /// Mutable borrow of the source context. Caller is the sole user
    /// of this context until the guard drops, satisfying libnfs's
    /// no-concurrent-use rule.
    pub fn src(&mut self) -> &mut NfsContext {
        self.src.as_mut().expect("ContextPair::src after drop")
    }

    /// Mutable borrow of the destination context.
    pub fn dst(&mut self) -> &mut NfsContext {
        self.dst.as_mut().expect("ContextPair::dst after drop")
    }

    /// Disjoint mutable borrows of (src, dst) so two callers — e.g.
    /// the read closure and the write closure of `stream_copy` — can
    /// each hold an independent `&mut NfsContext` without a second
    /// borrow of the whole pair. Both contexts must be present.
    pub fn split(&mut self) -> (&mut NfsContext, &mut NfsContext) {
        let s = self
            .src
            .as_mut()
            .expect("ContextPair::split src after drop");
        let d = self
            .dst
            .as_mut()
            .expect("ContextPair::split dst after drop");
        (s, d)
    }
}

impl Drop for ContextPair {
    fn drop(&mut self) {
        let src = self.src.take();
        let dst = self.dst.take();
        if let (Some(s), Some(d)) = (src, dst) {
            // UnboundedSender::send only fails if the receiver was
            // dropped, which means the pool itself is gone — no
            // recipient cares about our pair anymore.
            let _ = self.return_to.send((s, d));
        }
    }
}

#[async_trait]
pub trait LibnfsContextPool: Send + Sync {
    /// Acquire a (src, dst) pair. Awaits if every pair is currently
    /// held by another task. Caller drops the returned guard to
    /// release.
    async fn acquire(&self) -> anyhow::Result<ContextPair>;
}

/// M2 single-pair pool. Built once at worker startup with the source
/// and destination URLs from the manifest. Equivalent to
/// `MultiPool::build(.., 1)` and kept as a small specialization for
/// tests and very-low-concurrency configurations.
pub struct SimplePool {
    inner: MultiPool,
}

impl SimplePool {
    pub fn build(src_url: &str, dst_url: &str) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            inner: MultiPool::build_inner(src_url, dst_url, 1)?,
        }))
    }
}

#[async_trait]
impl LibnfsContextPool for SimplePool {
    async fn acquire(&self) -> anyhow::Result<ContextPair> {
        self.inner.acquire().await
    }
}

/// M3 multi-pair pool. Pre-mounts N (src, dst) context pairs and hands
/// them out to concurrent acquirers. Pairs are returned via the
/// guard's Drop, same as `SimplePool`. The Mutex around the receiver
/// serializes acquire moments but each acquire is a non-blocking
/// channel `recv()` once a pair is available, so contention is on the
/// (cheap) lock, not the (expensive) work.
pub struct MultiPool {
    receiver: Mutex<mpsc::UnboundedReceiver<(NfsContext, NfsContext)>>,
    sender: mpsc::UnboundedSender<(NfsContext, NfsContext)>,
    capacity: usize,
}

impl MultiPool {
    /// Mount `n` (src, dst) pairs against the same URLs and return the
    /// pool. Mount failures during seeding return early — already-mounted
    /// pairs drop cleanly via `NfsContext::Drop`.
    pub fn build(src_url: &str, dst_url: &str, n: usize) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self::build_inner(src_url, dst_url, n)?))
    }

    fn build_inner(src_url: &str, dst_url: &str, n: usize) -> anyhow::Result<Self> {
        if n == 0 {
            anyhow::bail!("MultiPool requires n >= 1, got 0");
        }
        let (sender, receiver) = mpsc::unbounded_channel();
        for i in 0..n {
            let src = NfsContext::mount_url(src_url)
                .map_err(|e| anyhow::anyhow!("mount source {src_url} (#{i}): {e}"))?;
            let dst = NfsContext::mount_url(dst_url)
                .map_err(|e| anyhow::anyhow!("mount dest {dst_url} (#{i}): {e}"))?;
            sender
                .send((src, dst))
                .map_err(|_| anyhow::anyhow!("seed send failed (receiver dropped)"))?;
        }
        Ok(Self {
            receiver: Mutex::new(receiver),
            sender,
            capacity: n,
        })
    }

    /// Number of pairs the pool was built with. Constant for the life
    /// of the pool.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[async_trait]
impl LibnfsContextPool for MultiPool {
    async fn acquire(&self) -> anyhow::Result<ContextPair> {
        let mut rx = self.receiver.lock().await;
        let (src, dst) = rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("pool channel closed"))?;
        Ok(ContextPair {
            src: Some(src),
            dst: Some(dst),
            return_to: self.sender.clone(),
        })
    }
}
