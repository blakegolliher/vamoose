use crate::model::FileType;
use crate::store::{BatchCommit, Entry, ObservationIssue, ScannedEntry, Side, Store};
use anyhow::{Context, Result};
use migration_core::records::Endpoint;
use migration_mover::libnfs::{ops, raw, scan, NfsContext};
use regex::Regex;

pub(crate) fn compile_exclusions(patterns: &[String]) -> Result<Vec<Regex>> {
    patterns
        .iter()
        .map(|pattern| {
            Regex::new(pattern)
                .with_context(|| format!("invalid migration exclusion regex {pattern:?}"))
        })
        .collect()
}

pub(crate) fn scan_side(
    store: &mut Store,
    side: Side,
    endpoint: &Endpoint,
    rpc_timeout_ms: u32,
    batch_size: u32,
    exclusions: &[Regex],
) -> Result<()> {
    if store.side_complete(side)? {
        return Ok(());
    }
    let mut ctx = match NfsContext::mount_url(&endpoint.url, rpc_timeout_ms) {
        Ok(ctx) => ctx,
        Err(error) => {
            store.initialize_side_failure(side, "mount", &error.to_string())?;
            return Ok(());
        }
    };

    if store.next_pending(side)?.is_none() {
        let root_path = full_path(&endpoint.root, b"")?;
        match observe_path(&mut ctx, Vec::new(), &root_path) {
            Ok((root, issue)) => {
                let directory_fh = if root.file_type == FileType::Directory {
                    match resolve_directory_fh(&mut ctx, &endpoint.root) {
                        Ok(fh) => Some(fh),
                        Err(error) => {
                            store.initialize_side_failure(side, "lookup_root", &error)?;
                            return Ok(());
                        }
                    }
                } else {
                    None
                };
                let mut issues = Vec::new();
                if let Some(mut issue) = issue {
                    issue.side = side;
                    issues.push(issue);
                }
                store.initialize_side(side, &root, directory_fh.as_deref(), &issues)?;
            }
            Err(error) => {
                store.initialize_side_failure(side, "lstat_root", &error.to_string())?;
                return Ok(());
            }
        }
    }

    while let Some(parent) = store.next_pending(side)? {
        let parent_full = full_path(&endpoint.root, &parent.path)?;
        let batch = match scan::read_dir_batch(
            &mut ctx,
            &parent_full,
            &parent.directory_fh,
            parent.cookie,
            parent.cookie_verifier,
            batch_size,
        ) {
            Ok(batch) => batch,
            Err(error) => {
                store.fail_pending(side, &parent, error.operation, &error.to_string())?;
                continue;
            }
        };

        let mut entries = Vec::with_capacity(batch.entries.len());
        let mut issues = Vec::new();
        for discovered in batch.entries {
            let name = discovered.name;
            if name.is_empty() || name.contains(&b'/') {
                issues.push(ObservationIssue {
                    side,
                    path: child_path(&parent.path, &name),
                    operation: "readdirplus".to_string(),
                    detail: "server returned an invalid empty or slash-containing basename"
                        .to_string(),
                });
                continue;
            }
            let relative = child_path(&parent.path, &name);
            if is_excluded(&relative, exclusions) {
                continue;
            }
            let full = full_path(&endpoint.root, &relative)?;
            let observed = match discovered.attrs {
                Some(attrs) => observe_attributes(&mut ctx, relative.clone(), &full, attrs),
                None => observe_path(&mut ctx, relative.clone(), &full),
            };
            match observed {
                Ok((entry, issue)) => {
                    let directory_fh = if entry.file_type == FileType::Directory {
                        match discovered.fh {
                            Some(fh) => Some(fh),
                            None => match raw::lookup(&mut ctx, &parent.directory_fh, &name) {
                                Ok(fh) => Some(fh),
                                Err(error) => {
                                    issues.push(ObservationIssue {
                                        side,
                                        path: relative.clone(),
                                        operation: "lookup_directory".to_string(),
                                        detail: format!("{}: {}", error.tag, error.detail),
                                    });
                                    None
                                }
                            },
                        }
                    } else {
                        None
                    };
                    entries.push(ScannedEntry {
                        entry,
                        directory_fh,
                    });
                    if let Some(mut issue) = issue {
                        issue.side = side;
                        issues.push(issue);
                    }
                }
                Err(error) => issues.push(ObservationIssue {
                    side,
                    path: relative,
                    operation: error.operation.to_string(),
                    detail: error.to_string(),
                }),
            }
        }

        let post = if batch.complete {
            match observe_path(&mut ctx, parent.path.clone(), &parent_full) {
                Ok((entry, issue)) => {
                    if let Some(mut issue) = issue {
                        issue.side = side;
                        issues.push(issue);
                    }
                    Some(entry)
                }
                Err(error) => {
                    issues.push(ObservationIssue {
                        side,
                        path: parent.path.clone(),
                        operation: "lstat_after_readdir".to_string(),
                        detail: error.to_string(),
                    });
                    None
                }
            }
        } else {
            None
        };

        store.commit_batch(
            side,
            &parent,
            BatchCommit {
                next_cookie: batch.next_cookie,
                cookie_verifier: batch.cookie_verifier,
                complete: batch.complete,
                entries: &entries,
                issues: &issues,
                post: post.as_ref(),
            },
        )?;
    }
    store.finish_side(side)
}

fn resolve_directory_fh(ctx: &mut NfsContext, root: &str) -> std::result::Result<Vec<u8>, String> {
    let mut fh = raw::root_fh(ctx).map_err(|error| format!("{}: {}", error.tag, error.detail))?;
    for component in root.as_bytes().split(|byte| *byte == b'/') {
        if component.is_empty() {
            continue;
        }
        fh = raw::lookup(ctx, &fh, component)
            .map_err(|error| format!("{}: {}", error.tag, error.detail))?;
    }
    Ok(fh)
}

fn observe_path(
    ctx: &mut NfsContext,
    relative: Vec<u8>,
    full: &[u8],
) -> std::result::Result<(Entry, Option<ObservationIssue>), scan::ScanError> {
    let stat = scan::lstat(ctx, full)?;
    let mode =
        u32::try_from(stat.nfs_mode).map_err(|_| decode_error(full, "mode", stat.nfs_mode))?;
    let uid = u32::try_from(stat.nfs_uid).map_err(|_| decode_error(full, "uid", stat.nfs_uid))?;
    let gid = u32::try_from(stat.nfs_gid).map_err(|_| decode_error(full, "gid", stat.nfs_gid))?;
    let nlink = u32::try_from(stat.nfs_nlink)
        .map_err(|_| decode_error(full, "link count", stat.nfs_nlink))?;
    let mtime_sec = i64::try_from(stat.nfs_mtime)
        .map_err(|_| decode_error(full, "mtime seconds", stat.nfs_mtime))?;
    let mtime_nsec = decode_nanoseconds(full, "mtime", stat.nfs_mtime_nsec)?;
    let ctime_sec = i64::try_from(stat.nfs_ctime)
        .map_err(|_| decode_error(full, "ctime seconds", stat.nfs_ctime))?;
    let ctime_nsec = decode_nanoseconds(full, "ctime", stat.nfs_ctime_nsec)?;
    finish_observation(
        ctx,
        full,
        Entry {
            path: relative,
            file_type: file_type_from_mode(mode),
            size: stat.nfs_size,
            mode: mode & 0o7777,
            uid,
            gid,
            mtime_sec,
            mtime_nsec,
            ctime_sec,
            ctime_nsec,
            dev: stat.nfs_dev,
            ino: stat.nfs_ino,
            nlink,
            rdev: stat.nfs_rdev,
            symlink_target: None,
            hardlink_group: None,
        },
    )
}

fn observe_attributes(
    ctx: &mut NfsContext,
    relative: Vec<u8>,
    full: &[u8],
    attrs: raw::NfsAttributes,
) -> std::result::Result<(Entry, Option<ObservationIssue>), scan::ScanError> {
    if attrs.mtime_nsec >= 1_000_000_000 {
        return Err(decode_error(full, "mtime nanoseconds", attrs.mtime_nsec));
    }
    if attrs.ctime_nsec >= 1_000_000_000 {
        return Err(decode_error(full, "ctime nanoseconds", attrs.ctime_nsec));
    }
    finish_observation(
        ctx,
        full,
        Entry {
            path: relative,
            file_type: file_type_from_raw(attrs.file_type),
            size: attrs.size,
            mode: attrs.mode & 0o7777,
            uid: attrs.uid,
            gid: attrs.gid,
            mtime_sec: i64::from(attrs.mtime_sec),
            mtime_nsec: attrs.mtime_nsec,
            ctime_sec: i64::from(attrs.ctime_sec),
            ctime_nsec: attrs.ctime_nsec,
            dev: attrs.fsid,
            ino: attrs.fileid,
            nlink: attrs.nlink,
            rdev: (u64::from(attrs.rdev_major) << 32) | u64::from(attrs.rdev_minor),
            symlink_target: None,
            hardlink_group: None,
        },
    )
}

fn finish_observation(
    ctx: &mut NfsContext,
    full: &[u8],
    mut entry: Entry,
) -> std::result::Result<(Entry, Option<ObservationIssue>), scan::ScanError> {
    let issue = if entry.file_type == FileType::Symlink {
        match ops::readlink(ctx, full) {
            Ok(target) => {
                entry.symlink_target = Some(target);
                None
            }
            Err(error) => Some(ObservationIssue {
                side: Side::Source,
                path: entry.path.clone(),
                operation: "readlink".to_string(),
                detail: error.to_string(),
            }),
        }
    } else {
        None
    };
    Ok((entry, issue))
}

fn decode_nanoseconds(
    path: &[u8],
    field: &'static str,
    value: u64,
) -> std::result::Result<u32, scan::ScanError> {
    if value >= 1_000_000_000 {
        return Err(decode_error(path, field, value));
    }
    u32::try_from(value).map_err(|_| decode_error(path, field, value))
}

fn decode_error(
    path: &[u8],
    field: &'static str,
    value: impl std::fmt::Display,
) -> scan::ScanError {
    scan::ScanError {
        operation: "decode_attributes",
        path: String::from_utf8_lossy(path).into_owned(),
        detail: format!("invalid {field} value {value}"),
    }
}

pub(crate) fn file_type_from_mode(mode: u32) -> FileType {
    match (mode as libc::mode_t) & libc::S_IFMT {
        libc::S_IFREG => FileType::Regular,
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFIFO => FileType::Fifo,
        libc::S_IFSOCK => FileType::Socket,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFCHR => FileType::CharacterDevice,
        _ => FileType::Unknown,
    }
}

fn file_type_from_raw(file_type: raw::NfsFileType) -> FileType {
    match file_type {
        raw::NfsFileType::Regular => FileType::Regular,
        raw::NfsFileType::Directory => FileType::Directory,
        raw::NfsFileType::Symlink => FileType::Symlink,
        raw::NfsFileType::Fifo => FileType::Fifo,
        raw::NfsFileType::Socket => FileType::Socket,
        raw::NfsFileType::BlockDevice => FileType::BlockDevice,
        raw::NfsFileType::CharacterDevice => FileType::CharacterDevice,
        raw::NfsFileType::Unknown => FileType::Unknown,
    }
}

fn child_path(parent: &[u8], name: &[u8]) -> Vec<u8> {
    if parent.is_empty() {
        return name.to_vec();
    }
    let mut path = Vec::with_capacity(parent.len() + 1 + name.len());
    path.extend_from_slice(parent);
    path.push(b'/');
    path.extend_from_slice(name);
    path
}

pub(crate) fn full_path(root: &str, relative: &[u8]) -> Result<Vec<u8>> {
    if !root.starts_with('/') {
        anyhow::bail!("endpoint root must be absolute, got {root:?}");
    }
    if root.as_bytes().contains(&0) {
        anyhow::bail!("endpoint root contains NUL");
    }
    let normalized = root.trim_end_matches('/');
    if relative.is_empty() {
        return Ok(if normalized.is_empty() {
            b"/".to_vec()
        } else {
            normalized.as_bytes().to_vec()
        });
    }
    let mut path = Vec::with_capacity(normalized.len() + 1 + relative.len());
    if normalized.is_empty() {
        path.push(b'/');
    } else {
        path.extend_from_slice(normalized.as_bytes());
        path.push(b'/');
    }
    path.extend_from_slice(relative);
    Ok(path)
}

fn is_excluded(relative: &[u8], exclusions: &[Regex]) -> bool {
    if exclusions.is_empty() {
        return false;
    }
    let mut walker_path = Vec::with_capacity(relative.len() + 1);
    walker_path.push(b'/');
    walker_path.extend_from_slice(relative);
    let walker_path = String::from_utf8_lossy(&walker_path);
    exclusions.iter().any(|regex| regex.is_match(&walker_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_roots_and_raw_relative_paths_without_loss() {
        assert_eq!(full_path("/", b"").unwrap(), b"/");
        assert_eq!(full_path("/data/", b"a/\xff").unwrap(), b"/data/a/\xff");
        assert_eq!(child_path(b"a", b"b"), b"a/b");
    }

    #[test]
    fn exclusions_match_the_walkers_root_relative_shape() {
        let exclusions = compile_exclusions(&[r"^/\.snapshot(?:/|$)".to_string()]).unwrap();
        assert!(is_excluded(b".snapshot", &exclusions));
        assert!(is_excluded(b".snapshot/hourly", &exclusions));
        assert!(!is_excluded(b"data/.snapshot-name", &exclusions));
    }

    #[test]
    fn mode_mapping_covers_v1_types() {
        assert_eq!(file_type_from_mode(libc::S_IFREG), FileType::Regular);
        assert_eq!(file_type_from_mode(libc::S_IFDIR), FileType::Directory);
        assert_eq!(file_type_from_mode(libc::S_IFLNK), FileType::Symlink);
        assert_eq!(file_type_from_mode(0), FileType::Unknown);
    }
}
