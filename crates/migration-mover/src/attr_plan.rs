//! Pure attribute-application planner (ledger F08).
//!
//! Both attr paths (`Mover::apply_attrs`, sync; and
//! `AsyncBucketedFileMover::apply_async_attrs`, async) talk to
//! concrete NFS contexts, so the *order* of the setattr calls is made
//! a testable artifact here: [`plan_attr_ops`] is a pure function from
//! (row, policy) to an ordered op list, and both executors iterate the
//! plan.
//!
//! ## Order contract: chown → chmod → utimes
//!
//! NFSv3 SETATTR of uid/gid makes Linux-family servers clear
//! S_ISUID/S_ISGID on regular files (VFS kill-priv semantics), so
//! applying mode *before* owner silently lands a `4755` source file as
//! `0755`. rsync/cp order (chown, then chmod, utimes last) exists
//! precisely to avoid this; the plan encodes it.
//!
//! `utimes` stays strictly last: some servers update mtime as a side
//! effect of mode/owner setattrs, so the mtime restore must be the
//! final op.

use migration_core::shard::RowView;

use crate::attrs::{self, AttrPolicy};

/// One attribute-application step. Each op maps 1:1 onto an existing
/// per-attribute NFS call (`ops::chown`/`chmod`/`utimes` on the sync
/// path, `AsyncNfsContext::chown`/`chmod`/`utimes` on the async path)
/// — the plan changes only the order, never the calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttrOp {
    /// Set uid/gid. Must run before `Chmod`: NFSv3 kill-priv clears
    /// S_ISUID/S_ISGID on ownership change.
    Chown { uid: u32, gid: u32 },
    /// Set the mode bits (full `st_mode` from the row, as the
    /// existing calls pass it).
    Chmod { mode: u32 },
    /// Restore atime/mtime as `(sec, nsec)` pairs. Always the last op
    /// in a plan — mode/owner setattrs may bump mtime as a side
    /// effect on some servers.
    Utimes {
        atime: (i64, i32),
        mtime: (i64, i32),
    },
}

impl AttrOp {
    pub fn is_chown(&self) -> bool {
        matches!(self, AttrOp::Chown { .. })
    }

    pub fn is_chmod(&self) -> bool {
        matches!(self, AttrOp::Chmod { .. })
    }

    pub fn is_utimes(&self) -> bool {
        matches!(self, AttrOp::Utimes { .. })
    }

    /// True when this op applies S_ISUID and/or S_ISGID — the bits
    /// NFSv3 kill-priv semantics strip on a subsequent chown, i.e.
    /// exactly the rows the chown-before-chmod order exists for (F08).
    pub fn sets_id_bits(&self) -> bool {
        matches!(self, AttrOp::Chmod { mode } if mode & 0o6000 != 0)
    }
}

/// How the executor resolved a [`AttrOp::Chown`]. Distinguishes the
/// degraded-mode outcome (chown got EPERM, `require_chown` is off, a
/// `NullOwner` downgrade was recorded) from a real success — both
/// continue the plan; a fatal chown error propagates as `Err` from the
/// executor instead and stops the plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChownOutcome {
    /// The uid/gid were applied.
    Applied,
    /// chown failed non-fatally (EPERM in degraded mode); the
    /// executor recorded the downgrade and the plan continues —
    /// notably to the subsequent `Chmod`.
    SkippedDegraded,
}

/// Executor seam for [`execute_plan`]. The sync mover implements this
/// over its `NfsContext`; tests drive it with fakes to pin the
/// continue-on-degraded-chown behavior without an NFS server. The
/// chown-EPERM degraded-mode *policy* lives in the implementor (it
/// needs errno, `require_chown`, and the downgrade sink) — the loop
/// only honors its verdict.
pub trait AttrExec {
    type Err;

    fn chown(&mut self, uid: u32, gid: u32) -> Result<ChownOutcome, Self::Err>;
    fn chmod(&mut self, mode: u32) -> Result<(), Self::Err>;
    fn utimes(&mut self, atime: (i64, i32), mtime: (i64, i32)) -> Result<(), Self::Err>;
}

/// Plan the attribute ops for one row under `policy`, in application
/// order: chown → chmod → utimes (see module docs for why). Ops whose
/// attributes are absent (policy off, or null in the row) are simply
/// not planned; when mtime is present but atime is not, atime falls
/// back to mtime — both exactly as the pre-plan executors behaved.
pub fn plan_attr_ops(row: &RowView, policy: AttrPolicy) -> Vec<AttrOp> {
    let a = attrs::build(row, policy);
    let mut plan = Vec::with_capacity(3);
    if let (Some(uid), Some(gid)) = (a.uid, a.gid) {
        plan.push(AttrOp::Chown { uid, gid });
    }
    if let Some(mode) = a.mode {
        plan.push(AttrOp::Chmod { mode });
    }
    if let Some(mtime) = a.mtime {
        plan.push(AttrOp::Utimes {
            atime: a.atime.unwrap_or(mtime),
            mtime,
        });
    }
    plan
}

/// Run `plan` against `exec` in order. A `SkippedDegraded` chown
/// continues the plan (the following chmod still runs — F08 test 4);
/// any `Err` from an op stops it, matching the pre-plan `?`
/// short-circuit semantics.
pub fn execute_plan<X: AttrExec>(plan: &[AttrOp], exec: &mut X) -> Result<(), X::Err> {
    for op in plan {
        match *op {
            AttrOp::Chown { uid, gid } => match exec.chown(uid, gid)? {
                ChownOutcome::Applied | ChownOutcome::SkippedDegraded => {}
            },
            AttrOp::Chmod { mode } => exec.chmod(mode)?,
            AttrOp::Utimes { atime, mtime } => exec.utimes(atime, mtime)?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration_core::schema::FileTypeTag;
    use migration_core::shard::RowView;

    use crate::attrs::AttrPolicy;

    fn row(
        mode: u32,
        owner: Option<(u32, u32)>,
        mtime_sec: Option<i64>,
        atime_sec: Option<i64>,
    ) -> RowView {
        RowView {
            row_id: 1,
            path: b"/data/f.bin".to_vec(),
            size: 4096,
            mtime_sec,
            mtime_nsec: None,
            atime_sec,
            atime_nsec: None,
            mode,
            uid: owner.map(|(u, _)| u),
            gid: owner.map(|(_, g)| g),
            nlink: None,
            inode: None,
            fsid: None,
            xattr_blob: None,
            symlink_target: None,
            file_type: FileTypeTag::Regular,
        }
    }

    fn policy(owner: bool, mode: bool, times: bool) -> AttrPolicy {
        AttrPolicy {
            preserve_mode: mode,
            preserve_owner: owner,
            preserve_times: times,
            preserve_xattr: false,
        }
    }

    /// F08 acceptance test 1: a row with owner + mode + times to
    /// preserve plans exactly `[Chown, Chmod, Utimes]` — owner strictly
    /// before mode so NFSv3 kill-priv semantics can't strip
    /// setuid/setgid the chmod just applied.
    #[test]
    fn attr_plan_orders_chown_before_chmod() {
        let r = row(0o100644, Some((1000, 100)), Some(1234), Some(999));

        let plan = plan_attr_ops(&r, policy(true, true, true));

        assert_eq!(
            plan,
            vec![
                AttrOp::Chown {
                    uid: 1000,
                    gid: 100
                },
                AttrOp::Chmod { mode: 0o100644 },
                AttrOp::Utimes {
                    atime: (999, 0),
                    mtime: (1234, 0),
                },
            ],
        );
    }

    /// F08 acceptance test 2: no owner preservation configured →
    /// `[Chmod, Utimes]`; no chown op is planned at all.
    #[test]
    fn attr_plan_skips_chown_when_not_preserving() {
        let r = row(0o100755, Some((1000, 100)), Some(1234), Some(999));

        let plan = plan_attr_ops(&r, policy(false, true, true));

        assert_eq!(
            plan,
            vec![
                AttrOp::Chmod { mode: 0o100755 },
                AttrOp::Utimes {
                    atime: (999, 0),
                    mtime: (1234, 0),
                },
            ],
        );
    }

    /// F08 acceptance test 3 (property over config combinations):
    /// for every owner × mode × times on/off combination, any planned
    /// `Utimes` op is strictly last, and `Chown` (when present)
    /// precedes `Chmod` (when present).
    #[test]
    fn attr_plan_utimes_always_last() {
        for owner in [false, true] {
            for mode in [false, true] {
                for times in [false, true] {
                    let r = row(0o104755, Some((0, 0)), Some(1234), Some(999));
                    let plan = plan_attr_ops(&r, policy(owner, mode, times));

                    let combo = format!("owner={owner} mode={mode} times={times}");
                    if let Some(i) = plan.iter().position(AttrOp::is_utimes) {
                        assert_eq!(
                            i,
                            plan.len() - 1,
                            "utimes must be the last op ({combo}); plan: {plan:?}",
                        );
                    }
                    let chown = plan.iter().position(AttrOp::is_chown);
                    let chmod = plan.iter().position(AttrOp::is_chmod);
                    if let (Some(cn), Some(cm)) = (chown, chmod) {
                        assert!(
                            cn < cm,
                            "chown must precede chmod ({combo}); plan: {plan:?}",
                        );
                    }
                    // Presence matches the config: the row has every
                    // attribute, so each enabled preserve maps to
                    // exactly one op.
                    assert_eq!(chown.is_some(), owner, "{combo}; plan: {plan:?}");
                    assert_eq!(chmod.is_some(), mode, "{combo}; plan: {plan:?}");
                    assert_eq!(
                        plan.iter().any(|op| op.is_utimes()),
                        times,
                        "{combo}; plan: {plan:?}",
                    );
                }
            }
        }
    }

    /// F08 acceptance test 4: the chown-EPERM degraded mode (executor
    /// reports the failure as non-fatal `SkippedDegraded`) must not
    /// skip the subsequent chmod — the executor loop continues through
    /// the rest of the plan.
    #[test]
    fn chown_eperm_degraded_still_applies_mode() {
        struct DegradedChownExec {
            calls: Vec<&'static str>,
        }
        impl AttrExec for DegradedChownExec {
            type Err = String;
            fn chown(&mut self, _uid: u32, _gid: u32) -> Result<ChownOutcome, String> {
                self.calls.push("chown");
                // Simulates the EPERM + !require_chown branch: the
                // executor records a NullOwner downgrade and reports
                // the op as skipped, not failed.
                Ok(ChownOutcome::SkippedDegraded)
            }
            fn chmod(&mut self, _mode: u32) -> Result<(), String> {
                self.calls.push("chmod");
                Ok(())
            }
            fn utimes(&mut self, _at: (i64, i32), _mt: (i64, i32)) -> Result<(), String> {
                self.calls.push("utimes");
                Ok(())
            }
        }

        let r = row(0o104755, Some((0, 0)), Some(1234), None);
        let plan = plan_attr_ops(&r, policy(true, true, true));
        let mut exec = DegradedChownExec { calls: Vec::new() };

        execute_plan(&plan, &mut exec).expect("degraded chown must not fail the plan");

        assert_eq!(
            exec.calls,
            vec!["chown", "chmod", "utimes"],
            "degraded chown skip must continue to chmod (and utimes last)",
        );
    }

    /// Companion to test 4: a *fatal* chown (executor returns `Err`,
    /// i.e. `require_chown` mode) must stop the plan before chmod —
    /// same short-circuit semantics the pre-plan code had.
    #[test]
    fn fatal_chown_stops_before_chmod() {
        struct FatalChownExec {
            calls: Vec<&'static str>,
        }
        impl AttrExec for FatalChownExec {
            type Err = String;
            fn chown(&mut self, _uid: u32, _gid: u32) -> Result<ChownOutcome, String> {
                self.calls.push("chown");
                Err("chown: EPERM".to_string())
            }
            fn chmod(&mut self, _mode: u32) -> Result<(), String> {
                self.calls.push("chmod");
                Ok(())
            }
            fn utimes(&mut self, _at: (i64, i32), _mt: (i64, i32)) -> Result<(), String> {
                self.calls.push("utimes");
                Ok(())
            }
        }

        let r = row(0o100644, Some((0, 0)), Some(1234), None);
        let plan = plan_attr_ops(&r, policy(true, true, true));
        let mut exec = FatalChownExec { calls: Vec::new() };

        let err = execute_plan(&plan, &mut exec);

        assert!(err.is_err(), "fatal chown must propagate");
        assert_eq!(
            exec.calls,
            vec!["chown"],
            "no ops may run after a fatal chown"
        );
    }

    /// F08 acceptance test 5 (the regression this item exists for):
    /// rows whose mode carries S_ISUID/S_ISGID are marked by the
    /// planner (`sets_id_bits` on the Chmod op), and the
    /// chown-before-chmod ordering holds for them specifically.
    #[test]
    fn suid_sgid_rows_marked_and_chown_precedes_chmod() {
        for mode in [0o104755u32, 0o102755, 0o106755] {
            let r = row(mode, Some((0, 0)), Some(1234), Some(999));
            let plan = plan_attr_ops(&r, policy(true, true, true));

            let chown = plan
                .iter()
                .position(AttrOp::is_chown)
                .unwrap_or_else(|| panic!("mode {mode:o}: chown must be planned"));
            let chmod = plan
                .iter()
                .position(AttrOp::is_chmod)
                .unwrap_or_else(|| panic!("mode {mode:o}: chmod must be planned"));

            assert!(
                plan[chmod].sets_id_bits(),
                "mode {mode:o} must be marked as setting S_ISUID/S_ISGID",
            );
            assert!(
                chown < chmod,
                "mode {mode:o}: chown must precede chmod; plan: {plan:?}",
            );
        }

        // A plain mode carries no set-id intent.
        let r = row(0o100644, Some((0, 0)), Some(1234), None);
        let plan = plan_attr_ops(&r, policy(true, true, true));
        assert!(
            plan.iter().all(|op| !op.sets_id_bits()),
            "0644 must not be marked set-id; plan: {plan:?}",
        );
    }
}
