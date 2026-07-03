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
    ///
    /// Gated on `buffered_bytes < max_buffered_bytes` (F07): if the
    /// read at the deliver cursor stalls, completed reads pile up in
    /// the reorder buffer, and without this gate the pump would keep
    /// issuing — worst case buffering the whole remaining file in
    /// RAM. The bound cannot deadlock delivery: whenever anything is
    /// buffered, the chunk at the deliver cursor was already issued
    /// (issuance is in offset order), so progress never depends on a
    /// read the bound is blocking.
    pub fn may_issue(&self, inflight: usize, read_depth: usize, max_buffered_bytes: u64) -> bool {
        !self.hit_eof
            && inflight < read_depth
            && self.next_issue < self.effective_size
            && self.buffered_bytes < max_buffered_bytes
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

        // A completion at or past the EOF clamp can never be
        // delivered (the deliver cursor stops at `effective_size`):
        // discard it outright instead of parking it in the buffer
        // forever (F06). This is a discard of the *result*, not a
        // cancellation — the RPC already completed.
        if offset >= self.effective_size {
            return CompletionOutcome::default();
        }

        // Short read mid-file: the loop must issue the gap-fill at
        // `offset + actual` for the remaining bytes — unless the gap
        // starts at or past the EOF clamp, in which case there is
        // nothing left to fetch.
        let gap_off = offset + actual;
        let reissue = if actual < wanted && gap_off < self.effective_size {
            Some((gap_off, wanted - actual))
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
    /// issuing past the real end, and purge buffered chunks at or
    /// past the clamp — delivery can never reach their keys, and
    /// keeping them would wedge the loop forever (F06 livelock).
    ///
    /// A buffered chunk *below* the clamp whose extent crosses it is
    /// kept and delivered whole: its bytes existed at read time (the
    /// source shrank afterwards), and the pre/post stat bracket flags
    /// the copy as torn. This item is about not wedging, not about
    /// trimming torn tails.
    pub fn on_eof_clamp(&mut self, offset: u64) {
        if offset < self.effective_size {
            self.effective_size = offset;
        }
        self.hit_eof = true;
        let purged = self.buf.split_off(&self.effective_size);
        for bytes in purged.values() {
            self.buffered_bytes -= bytes.len() as u64;
        }
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

    // Acceptance test 3 (F06): a completion already buffered past a
    // subsequent EOF clamp must be purged — delivery can never reach
    // its key, so leaving it wedges the loop forever.
    #[test]
    fn eof_clamp_purges_stale_entries() {
        let rsize = 4 * MIB;
        let mut state = ReorderState::new(12 * MIB);
        let mut inflight: Vec<(u64, u64)> = Vec::new();
        pump(&mut state, &mut inflight, 3, rsize);
        assert_eq!(
            inflight,
            vec![(0, rsize), (4 * MIB, rsize), (8 * MIB, rsize)]
        );

        // Read at 8 MiB completes first and is buffered.
        let out = state.on_completion(8 * MIB, rsize, vec![0xAB; rsize as usize]);
        assert!(out.deliver.is_empty());

        // Read at 4 MiB observes EOF: the file shrank to 4 MiB.
        state.on_eof_clamp(4 * MIB);

        // The stale 8 MiB entry must be gone: with only the read at
        // 0 left in flight, the state machine must report drained as
        // soon as that read is routed — nothing can ever deliver the
        // 8 MiB chunk.
        assert!(
            state.drained(),
            "stale reorder entry past the EOF clamp was not purged"
        );

        // The surviving read at 0 delivers normally, and nothing
        // beyond the 4 MiB clamp is ever delivered.
        let out = state.on_completion(0, rsize, vec![0xAB; rsize as usize]);
        assert_eq!(out.deliver, vec![chunk(0, rsize)]);
        assert!(state.drained());
        assert_eq!(state.bytes_delivered(), 4 * MIB);
    }

    // Acceptance test 4 (F06): reads in flight beyond the clamp when
    // EOF is observed complete *after* the clamp; their completions
    // must be discarded, not buffered forever.
    #[test]
    fn eof_clamp_with_inflight_reads_past_eof() {
        let rsize = 4 * MIB;
        let mut state = ReorderState::new(16 * MIB);
        let mut inflight: Vec<(u64, u64)> = Vec::new();
        pump(&mut state, &mut inflight, 4, rsize);
        assert_eq!(inflight.len(), 4); // 0, 4M, 8M, 12M

        // Read at 4 MiB observes EOF while 8 MiB / 12 MiB are still
        // in flight.
        state.on_eof_clamp(4 * MIB);

        // Their completions arrive after the clamp: discard.
        let out = state.on_completion(8 * MIB, rsize, vec![0xAB; rsize as usize]);
        assert!(
            out.deliver.is_empty() && out.reissue.is_none(),
            "completion past the clamp must be discarded outright"
        );
        // Even a short read past the clamp must not reissue its gap.
        let out = state.on_completion(12 * MIB, rsize, vec![0xAB; MIB as usize]);
        assert!(
            out.deliver.is_empty() && out.reissue.is_none(),
            "short completion past the clamp must not reissue"
        );
        assert!(
            state.drained(),
            "discarded completions must not leave the state un-drained"
        );

        // The read below the clamp still delivers.
        let out = state.on_completion(0, rsize, vec![0xAB; rsize as usize]);
        assert_eq!(out.deliver, vec![chunk(0, rsize)]);
        assert!(state.drained());
        assert_eq!(state.bytes_delivered(), 4 * MIB);
    }

    // Acceptance test 5 (F07): one stalled read at the deliver cursor
    // must not let the pump buffer without bound. `may_issue` goes
    // false once `buffered_bytes` reaches the cap and flips back once
    // the stall clears and chunks deliver.
    #[test]
    fn buffered_bytes_bounded() {
        let rsize = MIB;
        let depth = 8;
        let cap = 4 * MIB;
        let mut state = ReorderState::new(64 * MIB);
        let mut inflight: Vec<(u64, u64)> = Vec::new();

        // Issue up to depth: offsets 0..8 MiB. The read at 0 stalls.
        while state.may_issue(inflight.len(), depth, cap) {
            inflight.push(state.take_read(rsize));
        }
        assert_eq!(inflight.len(), depth);

        // Completions for offsets 1..=3 MiB arrive: 3 MiB buffered,
        // still under the cap, so the pump may keep issuing.
        for i in 1..=3u64 {
            let out = state.on_completion(i * MIB, MIB, vec![0xAB; MIB as usize]);
            assert!(out.deliver.is_empty());
            inflight.retain(|&(off, _)| off != i * MIB);
        }
        assert!(state.may_issue(inflight.len(), depth, cap));

        // Fourth buffered chunk reaches the 4 MiB cap: pump must stop
        // even though in-flight count is below depth.
        let out = state.on_completion(4 * MIB, MIB, vec![0xAB; MIB as usize]);
        assert!(out.deliver.is_empty());
        inflight.retain(|&(off, _)| off != 4 * MIB);
        assert!(inflight.len() < depth);
        assert!(
            !state.may_issue(inflight.len(), depth, cap),
            "pump kept issuing past the buffered-bytes cap"
        );

        // The stall clears: chunk 0 arrives, everything buffered
        // delivers in order, and the pump may issue again.
        let out = state.on_completion(0, MIB, vec![0xAB; MIB as usize]);
        assert_eq!(
            out.deliver
                .iter()
                .map(|c| (c.offset, c.bytes.len() as u64))
                .collect::<Vec<_>>(),
            (0..=4).map(|i| (i * MIB, MIB)).collect::<Vec<_>>()
        );
        assert_eq!(state.bytes_delivered(), 5 * MIB);
        assert!(state.may_issue(inflight.len(), depth, cap));
    }

    // Acceptance test 6 (F06): drive the exact livelock interleaving
    // through a harness shaped like the real loop and prove it
    // terminates. The real loop in `pipelined_copy.rs` exits on
    // `reads_inflight.is_empty() && state.drained()` and returns an
    // error — rather than spinning — if nothing is in flight and the
    // state is not drained, so termination of this state-machine
    // harness implies termination of the loop. (The real loop cannot
    // be driven hermetically: it is coupled to the concrete
    // `AsyncNfsContext` FFI surface.)
    #[test]
    fn livelock_regression_loop_exits() {
        let rsize = 4 * MIB;
        let depth = 3;
        let cap = 2 * depth as u64 * rsize;
        let mut state = ReorderState::new(12 * MIB);
        let mut inflight: Vec<(u64, u64)> = Vec::new();

        // Scripted completion order: 8 MiB (data, buffered), 4 MiB
        // (EOF → clamp), 0 (data, delivers). Exactly the F06
        // interleaving: a stale entry sits past the clamp.
        let mut script: Vec<(u64, u64, bool)> = vec![
            (0, rsize, false),
            (4 * MIB, rsize, true),
            (8 * MIB, rsize, false),
        ];

        let mut iterations = 0;
        loop {
            iterations += 1;
            assert!(
                iterations <= 100,
                "livelock: loop did not exit after the EOF clamp"
            );

            while state.may_issue(inflight.len(), depth, cap) {
                inflight.push(state.take_read(rsize));
            }
            if inflight.is_empty() {
                if state.drained() {
                    break;
                }
                // Mirror of the real loop's wedged-state arm: with
                // nothing in flight and not drained the real loop
                // returns an error instead of spinning.
                panic!("wedged: nothing in flight and not drained");
            }
            let (off, want, is_eof) = script.pop().expect("completion for in-flight read");
            inflight.retain(|&(o, _)| o != off);
            if is_eof {
                state.on_eof_clamp(off);
            } else {
                let out = state.on_completion(off, want, vec![0xAB; want as usize]);
                assert!(out.reissue.is_none());
            }
        }

        assert!(state.drained());
        assert_eq!(state.bytes_delivered(), 4 * MIB);
    }
}
