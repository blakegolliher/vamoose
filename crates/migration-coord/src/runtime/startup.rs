use super::{Clock, CoordRuntime, RuntimeConfig, RuntimeInner};
use crate::errors::{Error, Result};
use crate::events::EventLogWriter;
use crate::lease::{self, AcquireOutcome, Identity, LeaseHandle};
use crate::state;
use crate::store::CoordStore;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

impl CoordRuntime {
    /// Bring up the runtime: acquire the lease (backing off on
    /// `Held`), replay state from snapshot + log, open a fresh
    /// event-log writer, and seed the broadcast channel.
    pub async fn start(
        store: Arc<dyn CoordStore>,
        clock: Arc<dyn Clock>,
        me: Identity,
        cfg: RuntimeConfig,
    ) -> Result<Self> {
        let lease = acquire_with_backoff(store.as_ref(), &me, &cfg, clock.as_ref()).await?;
        let replay = state::replay(store.as_ref(), clock.now()).await?;
        let writer = EventLogWriter::new(cfg.events);
        let (bus, _) = broadcast::channel(cfg.bus_capacity);

        Ok(Self {
            inner: Arc::new(Mutex::new(RuntimeInner {
                state: replay.state,
                prepare: None,
                next_seq: replay.next_seq,
                writer,
                lease,
                lease_lost: false,
                archive_eligible: Default::default(),
                archived_jobs: Default::default(),
                stream_caps: Default::default(),
            })),
            flush_token: Arc::new(Mutex::new(())),
            bus,
            store,
            clock,
            cfg,
        })
    }
}

async fn acquire_with_backoff(
    store: &dyn CoordStore,
    me: &Identity,
    cfg: &RuntimeConfig,
    clock: &dyn Clock,
) -> Result<LeaseHandle> {
    let mut attempts = 0u32;
    loop {
        match lease::try_acquire(store, me, cfg.lease, clock.now()).await? {
            AcquireOutcome::Acquired(h) => return Ok(h),
            AcquireOutcome::Held {
                holder_id,
                expires_at,
            } => {
                tracing::warn!(
                    holder = %holder_id,
                    %expires_at,
                    attempt = attempts,
                    "lease held; backing off",
                );
                attempts = attempts.saturating_add(1);
                if let Some(max) = cfg.lease_retry_max_attempts {
                    if attempts >= max {
                        return Err(Error::LeaseHeld {
                            holder: holder_id,
                            expires_at,
                        });
                    }
                }
                tokio::time::sleep(cfg.lease_retry_interval).await;
            }
        }
    }
}
