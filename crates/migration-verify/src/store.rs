use crate::model::{FileType, ObservationCounts, ObservedMetadata};
use anyhow::{Context, Result};
use base64::Engine;
use rusqlite::{params, Connection, OptionalExtension, Row, Transaction};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    Source = 0,
    Destination = 1,
}

impl Side {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Destination => "destination",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Entry {
    pub path: Vec<u8>,
    pub file_type: FileType,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime_sec: i64,
    pub mtime_nsec: u32,
    pub ctime_sec: i64,
    pub ctime_nsec: u32,
    pub dev: u64,
    pub ino: u64,
    pub nlink: u32,
    pub rdev: u64,
    pub symlink_target: Option<Vec<u8>>,
    pub hardlink_group: Option<Vec<u8>>,
}

impl Entry {
    pub(crate) fn observed(&self) -> ObservedMetadata {
        let b64 = base64::engine::general_purpose::STANDARD;
        ObservedMetadata {
            file_type: self.file_type,
            size: self.size,
            mode: self.mode & 0o7777,
            uid: self.uid,
            gid: self.gid,
            mtime_sec: self.mtime_sec,
            mtime_nsec: self.mtime_nsec,
            symlink_target_b64: self.symlink_target.as_ref().map(|v| b64.encode(v)),
            hardlink_group_b64: self.hardlink_group.as_ref().map(|v| b64.encode(v)),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PendingDir {
    pub path: Vec<u8>,
    pub directory_fh: Vec<u8>,
    pub cookie: u64,
    pub cookie_verifier: [u8; 8],
    pub baseline: Entry,
}

#[derive(Debug, Clone)]
pub(crate) struct ScannedEntry {
    pub entry: Entry,
    pub directory_fh: Option<Vec<u8>>,
}

pub(crate) struct BatchCommit<'a> {
    pub next_cookie: u64,
    pub cookie_verifier: [u8; 8],
    pub complete: bool,
    pub entries: &'a [ScannedEntry],
    pub issues: &'a [ObservationIssue],
    pub post: Option<&'a Entry>,
}

#[derive(Debug, Clone)]
pub(crate) struct ObservationIssue {
    pub side: Side,
    pub path: Vec<u8>,
    pub operation: String,
    pub detail: String,
}

pub(crate) struct Store {
    conn: Connection,
}

impl Store {
    pub(crate) fn open(path: &Path, identity: &str, started_utc: &str) -> Result<(Self, bool)> {
        let existed = path.exists();
        let conn = Connection::open(path)
            .with_context(|| format!("opening verification database {}", path.display()))?;
        conn.busy_timeout(std::time::Duration::from_secs(1))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA temp_store=FILE;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS meta (
               key TEXT PRIMARY KEY,
               value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS scan_state (
               side INTEGER PRIMARY KEY,
               complete INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS pending_dirs (
               side INTEGER NOT NULL,
               path BLOB NOT NULL,
               directory_fh BLOB NOT NULL,
               cookie INTEGER NOT NULL,
               cookie_verifier BLOB NOT NULL,
               baseline_mode INTEGER NOT NULL,
               baseline_size INTEGER NOT NULL,
               baseline_uid INTEGER NOT NULL,
               baseline_gid INTEGER NOT NULL,
               baseline_nlink INTEGER NOT NULL,
               baseline_mtime_sec INTEGER NOT NULL,
               baseline_mtime_nsec INTEGER NOT NULL,
               baseline_ctime_sec INTEGER NOT NULL,
               baseline_ctime_nsec INTEGER NOT NULL,
               baseline_dev INTEGER NOT NULL,
               baseline_ino INTEGER NOT NULL,
               PRIMARY KEY(side, path)
             ) WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS entries (
               side INTEGER NOT NULL,
               path BLOB NOT NULL,
               file_type INTEGER NOT NULL,
               size INTEGER NOT NULL,
               mode INTEGER NOT NULL,
               uid INTEGER NOT NULL,
               gid INTEGER NOT NULL,
               mtime_sec INTEGER NOT NULL,
               mtime_nsec INTEGER NOT NULL,
               ctime_sec INTEGER NOT NULL,
               ctime_nsec INTEGER NOT NULL,
               dev INTEGER NOT NULL,
               ino INTEGER NOT NULL,
               nlink INTEGER NOT NULL,
               rdev INTEGER NOT NULL,
               symlink_target BLOB,
               PRIMARY KEY(side, path)
             ) WITHOUT ROWID;
             CREATE INDEX IF NOT EXISTS entries_inode
               ON entries(side, dev, ino, path);
             CREATE TABLE IF NOT EXISTS issues (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               side INTEGER,
               path BLOB NOT NULL,
               operation TEXT NOT NULL,
               detail TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS unstable (
               side INTEGER NOT NULL,
               path BLOB NOT NULL,
               detail TEXT NOT NULL,
               PRIMARY KEY(side, path)
             ) WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS hardlinks (
               side INTEGER NOT NULL,
               path BLOB NOT NULL,
               group_root BLOB NOT NULL,
               PRIMARY KEY(side, path)
             ) WITHOUT ROWID;",
        )?;

        let existing_identity: Option<String> = conn
            .query_row("SELECT value FROM meta WHERE key='identity'", [], |r| {
                r.get(0)
            })
            .optional()?;
        match existing_identity {
            Some(existing) if existing != identity => anyhow::bail!(
                "verification work database belongs to a different request; choose a new verification id"
            ),
            Some(_) => {}
            None => {
                let tx = conn.unchecked_transaction()?;
                tx.execute(
                    "INSERT INTO meta(key,value) VALUES('identity',?1)",
                    [identity],
                )?;
                tx.execute(
                    "INSERT INTO meta(key,value) VALUES('started_utc',?1)",
                    [started_utc],
                )?;
                tx.commit()?;
            }
        }
        Ok((Self { conn }, existed))
    }

    pub(crate) fn started_utc(&self) -> Result<String> {
        Ok(self
            .conn
            .query_row("SELECT value FROM meta WHERE key='started_utc'", [], |r| {
                r.get(0)
            })?)
    }

    pub(crate) fn side_complete(&self, side: Side) -> Result<bool> {
        Ok(self
            .conn
            .query_row(
                "SELECT complete FROM scan_state WHERE side=?1",
                [side as i64],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .is_some_and(|v| v != 0))
    }

    pub(crate) fn initialize_side(
        &mut self,
        side: Side,
        root: &Entry,
        directory_fh: Option<&[u8]>,
        issues: &[ObservationIssue],
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO scan_state(side,complete) VALUES(?1,0)",
            [side as i64],
        )?;
        insert_entry(&tx, side, root)?;
        if root.file_type == FileType::Directory {
            let directory_fh = directory_fh
                .ok_or_else(|| anyhow::anyhow!("directory root has no NFS filehandle"))?;
            insert_pending(&tx, side, root, directory_fh)?;
        } else {
            tx.execute(
                "UPDATE scan_state SET complete=1 WHERE side=?1",
                [side as i64],
            )?;
        }
        for issue in issues {
            tx.execute(
                "INSERT INTO issues(side,path,operation,detail) VALUES(?1,?2,?3,?4)",
                params![
                    issue.side as i64,
                    &issue.path,
                    &issue.operation,
                    &issue.detail
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn initialize_side_failure(
        &mut self,
        side: Side,
        operation: &str,
        detail: &str,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT OR REPLACE INTO scan_state(side,complete) VALUES(?1,1)",
            [side as i64],
        )?;
        tx.execute(
            "INSERT INTO issues(side,path,operation,detail) VALUES(?1,?2,?3,?4)",
            params![side as i64, Vec::<u8>::new(), operation, detail],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn next_pending(&self, side: Side) -> Result<Option<PendingDir>> {
        self.conn
            .query_row(
                "SELECT path,directory_fh,cookie,cookie_verifier,baseline_mode,baseline_size,
                        baseline_uid,baseline_gid,baseline_nlink,baseline_mtime_sec,
                        baseline_mtime_nsec,baseline_ctime_sec,baseline_ctime_nsec,
                        baseline_dev,baseline_ino
                   FROM pending_dirs WHERE side=?1 ORDER BY path LIMIT 1",
                [side as i64],
                |row| {
                    let path: Vec<u8> = row.get(0)?;
                    Ok(PendingDir {
                        path: path.clone(),
                        directory_fh: row.get(1)?,
                        cookie: decode_u64(row.get(2)?),
                        cookie_verifier: decode_verifier(row.get(3)?)?,
                        baseline: Entry {
                            path,
                            file_type: FileType::Directory,
                            mode: row.get::<_, i64>(4)? as u32,
                            size: decode_u64(row.get(5)?),
                            uid: row.get::<_, i64>(6)? as u32,
                            gid: row.get::<_, i64>(7)? as u32,
                            nlink: row.get::<_, i64>(8)? as u32,
                            mtime_sec: row.get(9)?,
                            mtime_nsec: row.get::<_, i64>(10)? as u32,
                            ctime_sec: row.get(11)?,
                            ctime_nsec: row.get::<_, i64>(12)? as u32,
                            dev: decode_u64(row.get(13)?),
                            ino: decode_u64(row.get(14)?),
                            rdev: 0,
                            symlink_target: None,
                            hardlink_group: None,
                        },
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub(crate) fn commit_batch(
        &mut self,
        side: Side,
        parent: &PendingDir,
        batch: BatchCommit<'_>,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        for scanned in batch.entries {
            insert_entry(&tx, side, &scanned.entry)?;
            if let Some(directory_fh) = scanned.directory_fh.as_deref() {
                insert_pending(&tx, side, &scanned.entry, directory_fh)?;
            }
        }
        for issue in batch.issues {
            tx.execute(
                "INSERT INTO issues(side,path,operation,detail) VALUES(?1,?2,?3,?4)",
                params![
                    issue.side as i64,
                    &issue.path,
                    &issue.operation,
                    &issue.detail
                ],
            )?;
        }
        if batch.complete {
            if let Some(post) = batch.post {
                if directory_changed(&parent.baseline, post) {
                    tx.execute(
                        "INSERT OR REPLACE INTO unstable(side,path,detail) VALUES(?1,?2,?3)",
                        params![
                            side as i64,
                            &parent.path,
                            "directory attributes changed while it was enumerated"
                        ],
                    )?;
                }
            }
            tx.execute(
                "DELETE FROM pending_dirs WHERE side=?1 AND path=?2",
                params![side as i64, &parent.path],
            )?;
        } else {
            tx.execute(
                "UPDATE pending_dirs SET cookie=?3,cookie_verifier=?4
                   WHERE side=?1 AND path=?2",
                params![
                    side as i64,
                    &parent.path,
                    encode_u64(batch.next_cookie),
                    batch.cookie_verifier.as_slice()
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn fail_pending(
        &mut self,
        side: Side,
        parent: &PendingDir,
        operation: &str,
        detail: &str,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO issues(side,path,operation,detail) VALUES(?1,?2,?3,?4)",
            params![side as i64, &parent.path, operation, detail],
        )?;
        tx.execute(
            "DELETE FROM pending_dirs WHERE side=?1 AND path=?2",
            params![side as i64, &parent.path],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn finish_side(&mut self, side: Side) -> Result<()> {
        self.conn.execute(
            "UPDATE scan_state SET complete=1 WHERE side=?1",
            [side as i64],
        )?;
        Ok(())
    }

    pub(crate) fn materialize_hardlinks(&mut self) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM hardlinks", [])?;
        tx.execute_batch(
            "INSERT INTO hardlinks(side,path,group_root)
             SELECT e.side,e.path,g.group_root
               FROM entries e
               JOIN (
                 SELECT side,dev,ino,MIN(path) AS group_root,COUNT(*) AS members
                   FROM entries
                  WHERE file_type=1 AND nlink>1
                  GROUP BY side,dev,ino
                 HAVING members>1
               ) g
                 ON e.side=g.side AND e.dev=g.dev AND e.ino=g.ino
              WHERE e.file_type=1;",
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn for_each_joined(
        &self,
        mut visit: impl FnMut(Option<Entry>, Option<Entry>) -> Result<()>,
    ) -> Result<()> {
        const QUERY: &str =
            "SELECT e.path,e.file_type,e.size,e.mode,e.uid,e.gid,e.mtime_sec,e.mtime_nsec,
                    e.ctime_sec,e.ctime_nsec,e.dev,e.ino,e.nlink,e.rdev,e.symlink_target,
                    h.group_root
               FROM entries e
               LEFT JOIN hardlinks h ON h.side=e.side AND h.path=e.path
              WHERE e.side=?1 ORDER BY e.path";
        let mut source_stmt = self.conn.prepare(QUERY)?;
        let mut destination_stmt = self.conn.prepare(QUERY)?;
        let mut source_rows = source_stmt.query([Side::Source as i64])?;
        let mut destination_rows = destination_stmt.query([Side::Destination as i64])?;
        let mut source = next_entry(&mut source_rows)?;
        let mut destination = next_entry(&mut destination_rows)?;
        while source.is_some() || destination.is_some() {
            match (&source, &destination) {
                (Some(s), Some(d)) => match s.path.cmp(&d.path) {
                    std::cmp::Ordering::Less => {
                        visit(source.take(), None)?;
                        source = next_entry(&mut source_rows)?;
                    }
                    std::cmp::Ordering::Greater => {
                        visit(None, destination.take())?;
                        destination = next_entry(&mut destination_rows)?;
                    }
                    std::cmp::Ordering::Equal => {
                        visit(source.take(), destination.take())?;
                        source = next_entry(&mut source_rows)?;
                        destination = next_entry(&mut destination_rows)?;
                    }
                },
                (Some(_), None) => {
                    visit(source.take(), None)?;
                    source = next_entry(&mut source_rows)?;
                }
                (None, Some(_)) => {
                    visit(None, destination.take())?;
                    destination = next_entry(&mut destination_rows)?;
                }
                (None, None) => break,
            }
        }
        Ok(())
    }

    pub(crate) fn issues(&self) -> Result<Vec<ObservationIssue>> {
        let mut stmt = self
            .conn
            .prepare("SELECT side,path,operation,detail FROM issues ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            Ok(ObservationIssue {
                side: if row.get::<_, Option<i64>>(0)? == Some(Side::Destination as i64) {
                    Side::Destination
                } else {
                    Side::Source
                },
                path: row.get(1)?,
                operation: row.get(2)?,
                detail: row.get(3)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub(crate) fn unstable(&self) -> Result<Vec<(Side, Vec<u8>, String)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT side,path,detail FROM unstable ORDER BY side,path")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                if row.get::<_, i64>(0)? == Side::Destination as i64 {
                    Side::Destination
                } else {
                    Side::Source
                },
                row.get(1)?,
                row.get(2)?,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub(crate) fn counts(&self) -> Result<ObservationCounts> {
        let mut result = ObservationCounts::default();
        for side in [Side::Source, Side::Destination] {
            let count = self.conn.query_row(
                "SELECT COUNT(*) FROM entries WHERE side=?1",
                [side as i64],
                |r| r.get::<_, i64>(0),
            )?;
            let count = u64::try_from(count).context("negative SQLite entry count")?;
            let mut by_type = BTreeMap::new();
            let mut stmt = self.conn.prepare(
                "SELECT file_type,COUNT(*) FROM entries WHERE side=?1 GROUP BY file_type",
            )?;
            let mut rows = stmt.query([side as i64])?;
            while let Some(row) = rows.next()? {
                let kind = FileType::from_i64(row.get(0)?);
                let count = u64::try_from(row.get::<_, i64>(1)?)
                    .context("negative SQLite file-type count")?;
                by_type.insert(format!("{kind:?}").to_lowercase(), count);
            }
            match side {
                Side::Source => {
                    result.source_entries = count;
                    result.source_by_type = by_type;
                }
                Side::Destination => {
                    result.destination_entries = count;
                    result.destination_by_type = by_type;
                }
            }
        }
        let entries_on_both_sides = self.conn.query_row(
            "SELECT COUNT(*) FROM entries s JOIN entries d ON s.path=d.path
              WHERE s.side=0 AND d.side=1",
            [],
            |r| r.get::<_, i64>(0),
        )?;
        result.entries_on_both_sides =
            u64::try_from(entries_on_both_sides).context("negative SQLite joined-entry count")?;
        Ok(result)
    }

    #[cfg(test)]
    pub(crate) fn insert_test_entry(&mut self, side: Side, entry: &Entry) -> Result<()> {
        let tx = self.conn.transaction()?;
        insert_entry(&tx, side, entry)?;
        tx.commit()?;
        Ok(())
    }
}

fn insert_entry(tx: &Transaction<'_>, side: Side, entry: &Entry) -> Result<()> {
    tx.execute(
        "INSERT OR REPLACE INTO entries(
           side,path,file_type,size,mode,uid,gid,mtime_sec,mtime_nsec,
           ctime_sec,ctime_nsec,dev,ino,nlink,rdev,symlink_target
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
        params![
            side as i64,
            &entry.path,
            entry.file_type.as_i64(),
            encode_u64(entry.size),
            entry.mode as i64,
            entry.uid as i64,
            entry.gid as i64,
            entry.mtime_sec,
            entry.mtime_nsec as i64,
            entry.ctime_sec,
            entry.ctime_nsec as i64,
            encode_u64(entry.dev),
            encode_u64(entry.ino),
            entry.nlink as i64,
            encode_u64(entry.rdev),
            &entry.symlink_target,
        ],
    )?;
    Ok(())
}

fn insert_pending(
    tx: &Transaction<'_>,
    side: Side,
    entry: &Entry,
    directory_fh: &[u8],
) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO pending_dirs(
           side,path,directory_fh,cookie,cookie_verifier,baseline_mode,
           baseline_size,baseline_uid,baseline_gid,baseline_nlink,
           baseline_mtime_sec,baseline_mtime_nsec,baseline_ctime_sec,
           baseline_ctime_nsec,baseline_dev,baseline_ino
         ) VALUES(?1,?2,?3,0,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![
            side as i64,
            &entry.path,
            directory_fh,
            [0_u8; 8].as_slice(),
            entry.mode as i64,
            encode_u64(entry.size),
            entry.uid as i64,
            entry.gid as i64,
            entry.nlink as i64,
            entry.mtime_sec,
            entry.mtime_nsec as i64,
            entry.ctime_sec,
            entry.ctime_nsec as i64,
            encode_u64(entry.dev),
            encode_u64(entry.ino),
        ],
    )?;
    Ok(())
}

fn next_entry(rows: &mut rusqlite::Rows<'_>) -> Result<Option<Entry>> {
    rows.next()?
        .map(row_to_entry)
        .transpose()
        .map_err(Into::into)
}

fn row_to_entry(row: &Row<'_>) -> rusqlite::Result<Entry> {
    Ok(Entry {
        path: row.get(0)?,
        file_type: FileType::from_i64(row.get(1)?),
        size: decode_u64(row.get(2)?),
        mode: row.get::<_, i64>(3)? as u32,
        uid: row.get::<_, i64>(4)? as u32,
        gid: row.get::<_, i64>(5)? as u32,
        mtime_sec: row.get(6)?,
        mtime_nsec: row.get::<_, i64>(7)? as u32,
        ctime_sec: row.get(8)?,
        ctime_nsec: row.get::<_, i64>(9)? as u32,
        dev: decode_u64(row.get(10)?),
        ino: decode_u64(row.get(11)?),
        nlink: row.get::<_, i64>(12)? as u32,
        rdev: decode_u64(row.get(13)?),
        symlink_target: row.get(14)?,
        hardlink_group: row.get(15)?,
    })
}

fn directory_changed(before: &Entry, after: &Entry) -> bool {
    before.file_type != after.file_type
        || before.dev != after.dev
        || before.ino != after.ino
        || before.mode != after.mode
        || before.uid != after.uid
        || before.gid != after.gid
        || before.nlink != after.nlink
        || before.size != after.size
        || before.mtime_sec != after.mtime_sec
        || before.mtime_nsec != after.mtime_nsec
        || before.ctime_sec != after.ctime_sec
        || before.ctime_nsec != after.ctime_nsec
}

fn encode_u64(value: u64) -> i64 {
    value as i64
}

fn decode_u64(value: i64) -> u64 {
    value as u64
}

fn decode_verifier(value: Vec<u8>) -> rusqlite::Result<[u8; 8]> {
    value.try_into().map_err(|value: Vec<u8>| {
        rusqlite::Error::FromSqlConversionFailure(
            value.len(),
            rusqlite::types::Type::Blob,
            "cookie verifier is not exactly 8 bytes".into(),
        )
    })
}
