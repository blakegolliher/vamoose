# Apply chown before chmod (preserve setuid/setgid)

Status: in review — fix + tests on branch `mover-attr-order`; hardware 4755 stat check (MANUAL_VERIFY.md [5]) pending before `verified`.
Ledger: F08 in `docs/REVIEW_LEDGER.md`.
Priority: high — silent permission corruption on exactly the files
where mode matters most.
Scope: `migration-mover` (`mover.rs::apply_attrs`,
`file_mover.rs::apply_async_attrs`).

## Problem

Both attribute paths apply mode first, then owner
(`file_mover.rs` ~228–269: chmod → chown → utimes; the sync
`Mover::apply_attrs` documents the same order). NFSv3 SETATTR of
uid/gid makes Linux-family servers clear S_ISUID/S_ISGID on regular
files (VFS kill-priv semantics), so a `4755` source file lands `0755`
— silently. rsync/cp order (chown, then chmod, utimes last) exists
precisely to avoid this.

## Required reading

- `crates/migration-mover/src/file_mover.rs::apply_async_attrs`
  (including the chown-EPERM degraded-mode handling — its semantics
  must survive the reorder)
- `crates/migration-mover/src/mover.rs::apply_attrs` (sync twin)
- `crates/migration-mover/src/downgrade.rs` (`NullOwner` downgrade)

## Acceptance tests — write these FIRST

Both attr paths talk to concrete NFS contexts, so make the order a
testable artifact: extract a pure planner, wire both paths through it.

1. `attr_plan_orders_chown_before_chmod` (red before fix — planner
   won't exist) — pure fn `plan_attr_ops(row, cfg) -> Vec<AttrOp>`
   returns `[Chown, Chmod, Utimes]` for a row with owner + mode +
   times to preserve.
2. `attr_plan_skips_chown_when_not_preserving` — no owner
   preservation configured → `[Chmod, Utimes]`.
3. `attr_plan_utimes_always_last` — property-style over the config
   combinations (owner on/off × mode on/off × times on/off).
4. `chown_eperm_degraded_still_applies_mode` — the degraded-mode
   EPERM-skip must not skip the subsequent chmod: drive the executor
   seam with a failing-chown fake if one exists, else assert at the
   planner + executor-loop level that a non-fatal Chown failure
   continues to Chmod.
5. Suid-bit intent test: planner marks rows whose mode has
   S_ISUID/S_ISGID; assert ordering holds for them specifically (the
   regression this item exists for).

## Fix shape

- Introduce `AttrOp` + `plan_attr_ops` in the mover; both
  `apply_attrs` and `apply_async_attrs` iterate the plan (each op
  maps to the existing calls — no FFI changes).
- Keep utimes strictly last (mtime restore must not be disturbed by
  later setattrs — this is why the current order has it last;
  preserve that).
- Update both functions' doc comments (they currently document the
  chmod-first order as the contract).
- Add a line to `crates/migration-mover/MANUAL_VERIFY.md`: create a
  `4755` root-owned file in the test tree; post-migration `stat`
  must show `4755`. (Hardware verification gate for `verified`.)

## Out of scope / do NOT

- No xattr work (walker doesn't capture them yet).
- Do not change the chown-EPERM degraded-mode policy — only its
  position.

## Definition of done

- [ ] Tests written first; 1 observed red.
- [ ] All acceptance tests green; full gate green.
- [ ] MANUAL_VERIFY.md updated; hardware step run before `verified`.
- [ ] Ledger F08 updated; this doc's Status flipped.
