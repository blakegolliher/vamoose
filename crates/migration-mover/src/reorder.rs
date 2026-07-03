//! Pure reorder/EOF/bounds state machine for [`pipelined_copy`].
//!
//! Extracted from the read-pump / reorder / deliver loop in
//! `pipelined_copy.rs` so the state transitions are testable without
//! the FFI-coupled loop around them (ledger F06/F07,
//! `docs/work-items/MOVER_EOF_REORDER_BOUNDS.md`).
//!
//! The owning loop is responsible for:
//!
//! - issuing preads for the `(offset, wanted)` pairs handed out by
//!   [`ReorderState::take_read`] (gated by [`ReorderState::may_issue`]),
//! - routing each completed read either to
//!   [`ReorderState::on_completion`] (non-empty bytes) or to
//!   [`ReorderState::on_eof_clamp`] (zero-byte read = EOF), and
//! - exiting only when nothing is in flight **and**
//!   [`ReorderState::drained`] is true.
//!
//! Delivery stays strictly in-order: [`ReorderState::on_completion`]
//! only releases chunks whose offset is exactly the next-to-deliver
//! cursor (plus any directly following buffered chunks).
//!
//! [`pipelined_copy`]: crate::pipelined_copy::pipelined_copy

use std::collections::BTreeMap;

/// One in-order chunk released for hashing + writing at `offset`.
#[derive(Debug, PartialEq, Eq)]
pub struct Chunk {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

/// What the loop must do after routing one completed read.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct CompletionOutcome {
    /// In-order chunks now deliverable (hash, then submit to the
    /// write pipeline, in this exact order).
    pub deliver: Vec<Chunk>,
    /// Short-read gap-fill the loop must issue: `(offset, len)`.
    /// NFSv3 permits short reads mid-file; the reorder buffer holds
    /// both halves and delivery walks through them.
    pub reissue: Option<(u64, u64)>,
}

/// Reorder/EOF/bounds state for one file copy.
///
/// Tracks the two cursors (`next_issue` for the read pump,
/// `next_deliver` for the in-order write handoff), the reorder
/// buffer, the EOF clamp, and the buffered-byte total.
#[derive(Debug)]
pub struct ReorderState {
    /// Next offset the read pump will issue at.
    next_issue: u64,
    /// Next offset the writer cursor expects.
    next_deliver: u64,
    /// `size` from the shard row, clamped down when a read observes
    /// EOF earlier than predicted (source shrank mid-copy).
    effective_size: u64,
    /// Total bytes currently held in `buf`.
    buffered_bytes: u64,
    /// True once any read returned 0 bytes (EOF observed).
    hit_eof: bool,
    /// Completed-but-not-yet-deliverable chunks, keyed by offset.
    buf: BTreeMap<u64, Vec<u8>>,
}

impl ReorderState {
    pub fn new(size: u64) -> Self {
        Self {
            next_issue: 0,
            next_deliver: 0,
            effective_size: size,
            buffered_bytes: 0,
            hit_eof: false,
            buf: BTreeMap::new(),
        }
    }

    /// May the pump issue another read right now?
    ///
    /// `inflight` is the loop's current in-flight read count (the
    /// state machine does not own the `FuturesUnordered`).
    pub fn may_issue(&self, inflight: usize, read_depth: usize, _max_buffered_bytes: u64) -> bool {
        !self.hit_eof && inflight < read_depth && self.next_issue < self.effective_size
    }

    /// Hand out the next `(offset, wanted)` read and advance the
    /// issue cursor. Only call when [`Self::may_issue`] is true.
    pub fn take_read(&mut self, rsize: u64) -> (u64, u64) {
        debug_assert!(self.next_issue < self.effective_size);
        let off = self.next_issue;
        let want = rsize.min(self.effective_size - off);
        self.next_issue += want;
        (off, want)
    }

    /// Route one completed read carrying bytes. Empty completions
    /// (EOF) must go to [`Self::on_eof_clamp`] instead.
    pub fn on_completion(&mut self, offset: u64, wanted: u64, bytes: Vec<u8>) -> CompletionOutcome {
        let actual = bytes.len() as u64;
        debug_assert!(actual > 0, "empty completions must route to on_eof_clamp");

        // Short read mid-file: the loop must issue the gap-fill at
        // `offset + actual` for the remaining bytes.
        let reissue = if actual < wanted {
            Some((offset + actual, wanted - actual))
        } else {
            None
        };

        self.buffered_bytes += actual;
        self.buf.insert(offset, bytes);

        // Release every chunk that is now contiguous with the
        // deliver cursor.
        let mut deliver = Vec::new();
        while let Some(bytes) = self.buf.remove(&self.next_deliver) {
            let len = bytes.len() as u64;
            if len == 0 {
                // Defensive: an empty entry can neither advance the
                // cursor nor be delivered. Drop it.
                continue;
            }
            self.buffered_bytes -= len;
            deliver.push(Chunk {
                offset: self.next_deliver,
                bytes,
            });
            self.next_deliver += len;
        }

        CompletionOutcome { deliver, reissue }
    }

    /// A read at `offset` returned 0 bytes: the file ends at (or
    /// before) `offset`. Clamp `effective_size` so the pump stops
    /// issuing past the real end.
    pub fn on_eof_clamp(&mut self, offset: u64) {
        if offset < self.effective_size {
            self.effective_size = offset;
        }
        self.hit_eof = true;
    }

    /// True when nothing is buffered and no further reads are needed:
    /// the loop may exit once its in-flight set is also empty.
    pub fn drained(&self) -> bool {
        self.buf.is_empty()
            && (self.hit_eof
                || self.next_deliver >= self.effective_size
                || self.next_issue >= self.effective_size)
    }

    /// Bytes handed to the writer so far (the delivery cursor).
    pub fn bytes_delivered(&self) -> u64 {
        self.next_deliver
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn chunk(offset: u64, len: u64) -> Chunk {
        Chunk {
            offset,
            bytes: vec![0xAB; len as usize],
        }
    }

    /// Drive the pump like the real loop: issue while allowed.
    fn pump(state: &mut ReorderState, inflight: &mut Vec<(u64, u64)>, depth: usize, rsize: u64) {
        while state.may_issue(inflight.len(), depth, u64::MAX) {
            inflight.push(state.take_read(rsize));
        }
    }

    // Acceptance test 1: completions arriving in order deliver
    // immediately; drained() once everything is through.
    #[test]
    fn in_order_delivery_roundtrip() {
        let rsize = MIB;
        let size = 3 * MIB;
        let mut state = ReorderState::new(size);
        let mut inflight: Vec<(u64, u64)> = Vec::new();

        pump(&mut state, &mut inflight, 2, rsize);
        assert_eq!(inflight, vec![(0, MIB), (MIB, MIB)]);
        assert!(!state.drained());

        // Complete them in issue order: each delivers immediately.
        for expected_off in [0, MIB, 2 * MIB] {
            pump(&mut state, &mut inflight, 2, rsize);
            let (off, want) = inflight.remove(0);
            assert_eq!(off, expected_off);
            let out = state.on_completion(off, want, vec![0xAB; want as usize]);
            assert_eq!(out.reissue, None);
            assert_eq!(out.deliver, vec![chunk(off, want)]);
        }

        assert!(inflight.is_empty());
        assert!(state.drained());
        assert_eq!(state.bytes_delivered(), size);
        // Nothing further to issue.
        assert!(!state.may_issue(0, 2, u64::MAX));
    }

    // Acceptance test 2: completions 2,0,1 deliver 0,1,2.
    #[test]
    fn out_of_order_completions_reorder() {
        let rsize = MIB;
        let size = 3 * MIB;
        let mut state = ReorderState::new(size);
        let mut inflight: Vec<(u64, u64)> = Vec::new();

        pump(&mut state, &mut inflight, 3, rsize);
        assert_eq!(inflight.len(), 3);

        // Chunk 2 completes first: nothing deliverable yet.
        let out = state.on_completion(2 * MIB, MIB, vec![0xAB; MIB as usize]);
        assert!(out.deliver.is_empty());
        assert_eq!(out.reissue, None);
        assert!(!state.drained());

        // Chunk 0 completes: delivers exactly chunk 0.
        let out = state.on_completion(0, MIB, vec![0xAB; MIB as usize]);
        assert_eq!(out.deliver, vec![chunk(0, MIB)]);
        assert!(!state.drained());

        // Chunk 1 completes: delivers 1 and the buffered 2, in order.
        let out = state.on_completion(MIB, MIB, vec![0xAB; MIB as usize]);
        assert_eq!(out.deliver, vec![chunk(MIB, MIB), chunk(2 * MIB, MIB)]);

        assert!(state.drained());
        assert_eq!(state.bytes_delivered(), size);
    }

    // Short-read handling is pinned too: it predates this work item
    // and must survive the extraction unchanged.
    #[test]
    fn short_read_reissues_gap_and_delivers_in_order() {
        let rsize = MIB;
        let size = 2 * MIB;
        let mut state = ReorderState::new(size);
        let mut inflight: Vec<(u64, u64)> = Vec::new();
        pump(&mut state, &mut inflight, 2, rsize);

        // Read at 0 comes back short: 256 KiB of 1 MiB.
        let short = 256 * 1024;
        let out = state.on_completion(0, MIB, vec![0xAB; short as usize]);
        assert_eq!(out.reissue, Some((short, MIB - short)));
        assert_eq!(out.deliver, vec![chunk(0, short)]);

        // Chunk at 1 MiB completes: held until the gap fills.
        let out = state.on_completion(MIB, MIB, vec![0xAB; MIB as usize]);
        assert!(out.deliver.is_empty());
        assert!(!state.drained());

        // Gap-fill completes: both remaining chunks deliver in order.
        let out = state.on_completion(short, MIB - short, vec![0xAB; (MIB - short) as usize]);
        assert_eq!(
            out.deliver,
            vec![chunk(short, MIB - short), chunk(MIB, MIB)]
        );
        assert!(state.drained());
        assert_eq!(state.bytes_delivered(), size);
    }

    #[test]
    fn zero_byte_file_drained_immediately() {
        let state = ReorderState::new(0);
        assert!(state.drained());
        assert!(!state.may_issue(0, 2, u64::MAX));
        assert_eq!(state.bytes_delivered(), 0);
    }
}
