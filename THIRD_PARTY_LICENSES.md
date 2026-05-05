# Third-Party Licenses

This file should be regenerated on each release with `cargo about
generate` (or equivalent). Until then, this is the list of direct
dependencies and their licenses, derived from `Cargo.toml`.

## C library dependencies

- **libnfs** — LGPL-2.1-or-later. Linked dynamically (see
  `crates/migration-mover/build.rs`). Distribute the libnfs source or
  a written offer for it alongside any binary distribution. Do not
  switch to static linking without re-reviewing.

## Vendored Rust code

- **nfs-walker libnfs FFI wrappers** (`crates/migration-mover/src/libnfs.rs`)
  — vendored from https://github.com/blakegolliher/nfs-walker. MIT.
  Preserve the original copyright header on any files copied verbatim.

## Rust crate dependencies

| Crate          | License             |
|----------------|---------------------|
| aws-sdk-s3     | Apache-2.0          |
| aws-config     | Apache-2.0          |
| arrow          | Apache-2.0          |
| parquet        | Apache-2.0          |
| tokio          | MIT                 |
| tokio-util     | MIT                 |
| io-uring       | MIT or Apache-2.0   |
| serde          | MIT or Apache-2.0   |
| serde_json     | MIT or Apache-2.0   |
| anyhow         | MIT or Apache-2.0   |
| thiserror      | MIT or Apache-2.0   |
| clap           | MIT or Apache-2.0   |
| tracing        | MIT                 |
| tracing-subscriber | MIT             |
| chrono         | MIT or Apache-2.0   |
| base64         | MIT or Apache-2.0   |
| bytes          | MIT                 |
| uuid           | MIT or Apache-2.0   |
| hex            | MIT or Apache-2.0   |
| memmap2        | MIT or Apache-2.0   |
| crossbeam-channel | MIT or Apache-2.0 |
| crossbeam-deque   | MIT or Apache-2.0 |
| ratatui        | MIT                 |
| crossterm      | MIT                 |
| prometheus     | Apache-2.0          |
| hyper          | MIT                 |
| libc           | MIT or Apache-2.0   |
| async-trait    | MIT or Apache-2.0   |
| pkg-config     | MIT or Apache-2.0   |

For Apache-2.0 dependencies, the corresponding `LICENSE` and (where
present) `NOTICE` files must be reproduced in any binary distribution.
For MIT dependencies, the copyright notice and permission notice must
be preserved.
