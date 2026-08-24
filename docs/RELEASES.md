# Packages and release bundles

Two artifacts come out of the same tree. The **RPM/deb** (`make rpm`,
`make deb`) is what operators install per [QUICKSTART.md](QUICKSTART.md): it
places the executables in `/usr/bin`, the example configuration and secrets
files in `/etc/vamoose`, and the `vamoose-worker@` and `vamoose-coord` units
in `/usr/lib/systemd/system`. The **bundle** below is the provenance-checked
tarball for sites that deploy with the `ops/` harness or need the pinned
libnfs and glibc verification before activation.

## Release bundles

Production deployment from a bundle starts from one immutable archive. A bundle contains all
four executables, the exact pinned libnfs binary, example configuration and
service files, `build-info.json`, and `SHA256SUMS`.

## Build

The supported lab target is glibc 2.34:

```bash
make bundle \
  TARGET=x86_64-unknown-linux-gnu.2.34 \
  LIBNFS_SO=/path/to/libnfs.so.16.2.0 \
  NFS_WALKER_BIN=/path/to/nfs-walker
```

`NFS_WALKER_BIN` (also honored by `make rpm` / `make deb`) ships the
scanner as `libexec/nfs-walker` so `vamoose prepare` works out of the box;
its SHA256 is recorded in `share/doc/vamoose/NFS_WALKER_SOURCE.txt`. Without
it, `prepare` falls back to `nfs-walker` on PATH or `[prepare] walker_bin`.

The libnfs file must match `packaging/libnfs.lock.json`. Updating libnfs means
updating its source commit and exact artifact digest in that reviewed lock.
The cross compiler versions are pinned in
`packaging/release-toolchain.lock.json`. The Makefile locates a normal `zig`
executable first and falls back to `/snap/zig/current/zig`, because confined
automation cannot execute the `/snap/bin/zig` launcher on the lab build host.
Override discovery with `ZIG=/absolute/path/to/zig` on another build host.
Validate the local tools without compiling by running:

```bash
make check-cross-toolchain TARGET=x86_64-unknown-linux-gnu.2.34
```

The pinned setup is Rust/Cargo 1.98.0, cargo-zigbuild 0.20.1, and Zig 0.16.0.
The Makefile keeps Zig's global and local caches beneath `target/`, so the
documented build does not depend on a writable home-directory cache.
The build refuses a dirty Git tree, ensuring the recorded Git SHA describes
the source exactly. `ALLOW_DIRTY=1` exists only for testing; installation also
rejects dirty bundles unless explicitly overridden.

The output is:

```text
dist/vamoose-VERSION-GIT_SHA-TARGET.tar.gz
dist/vamoose-VERSION-GIT_SHA-TARGET.tar.gz.sha256
```

Set `SOURCE_DATE_EPOCH` to reproduce archive timestamps and the recorded build
time exactly.

## Verify and activate one host

Copy the archive, its `.sha256` sidecar, and `scripts/install-release.sh` plus
`scripts/verify-release.sh` to the host, then run:

```bash
sudo ./install-release.sh vamoose-....tar.gz --prefix /opt/vamoose
```

The installer verifies the archive digest, every bundled file, the clean Git
provenance, the libnfs source and artifact pins, the ELF dependencies, the
maximum required glibc version, and an executable `vamoose --version` smoke
test. Only then does it install under:

```text
/opt/vamoose/releases/RELEASE_ID/
```

An existing release ID is immutable. Activation creates a temporary symlink
and renames it over `/opt/vamoose/current`, making the switch atomic. Old
release directories remain available for rollback.
