use super::CoordRuntime;
use crate::errors::{Error, Result};
use crate::lease::{self, LeaseHandle};

impl CoordRuntime {
    /// Replace the current lease handle (after a refresh).
    /// Background-tick-only API; HTTP handlers do not call this.
    pub async fn update_lease(&self, new_handle: LeaseHandle) {
        let mut guard = self.inner.lock().await;
        guard.lease = new_handle;
    }

    /// Mark the lease as lost. Subsequent `ingest` calls return
    /// [`Error::LeaseLost`]. Idempotent.
    pub async fn mark_lease_lost(&self) {
        self.inner.lock().await.lease_lost = true;
    }

    pub async fn lease_lost(&self) -> bool {
        self.inner.lock().await.lease_lost
    }

    /// Snapshot the current lease handle (for diagnostics and the
    /// next refresh tick).
    pub async fn lease_handle(&self) -> LeaseHandle {
        self.inner.lock().await.lease.clone()
    }
    /// Graceful shutdown: flush the log, write a final snapshot,
    /// release the lease. Returns the [`crate::lease::release`]
    /// outcome implicitly (release is idempotent — see lease docs).
    ///
    /// Short-circuits with [`Error::LeaseLost`] before any store
    /// write (or the lease release) once the lease is observed
    /// lost: a successor coord has already taken over and replayed;
    /// flushing our buffer or snapshotting our state would corrupt
    /// its log. The caller decides how loudly to surface the drop.
    pub async fn shutdown(&self, history_keep: usize) -> Result<()> {
        if self.lease_lost().await {
            return Err(Error::LeaseLost);
        }
        self.flush_log().await?;
        self.write_snapshot(history_keep).await?;
        let handle = self.lease_handle().await;
        lease::release(self.store.as_ref(), &handle).await?;
        Ok(())
    }
}
