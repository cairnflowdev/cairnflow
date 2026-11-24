// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Initialising a Db's shared WAL writer: `SharedWriterInit`, `FramesIter`.

use super::*;

/// The shared WAL's `WriterInit` for one range.
pub(super) struct SharedWriterInit {
    pub(super) stripe: Arc<Stripe>,
    pub(super) range: u32,
    pub(super) frames: Vec<Frame>,
    pub(super) floor_id: Option<u64>,
    pub(super) skipped_own: bool,
    pub(super) first: Arc<OnceLock<u64>>,
}

/// Replay iterator over frames read in advance.
pub(super) struct FramesIter(VecDeque<WalRows>);

#[async_trait]
impl WalIterator for FramesIter {
    async fn next(&mut self) -> std::result::Result<Option<WalRows>, WalError> {
        Ok(self.0.pop_front())
    }
}

#[async_trait]
impl WriterInit for SharedWriterInit {
    /// The WAL fence is done before the Db opens — the previous owner's logs are already
    /// sealed ([`NodeWal::take_over`]); here we only build the replay iterator over
    /// `(replay_after_wal_id, …]` and attach the range to the stream at the first id.
    /// No fence object is written (#2151).
    async fn fence_and_init(
        &self,
        manifest: &mut WriterManifest,
    ) -> std::result::Result<WriterInitResult, WalError> {
        let replay_after = manifest.replay_after_wal_id();
        let next_in_manifest = manifest.manifest().next_wal_sst_id();
        let replay: VecDeque<WalRows> = self
            .frames
            .iter()
            .filter(|f| f.wal_file_id > replay_after)
            .map(|f| WalRows {
                rows: f.rows.clone(),
                last_consumed_wal_file_id: f.wal_file_id,
            })
            .collect();
        let max_seen = self.frames.iter().map(|f| f.wal_file_id).max().unwrap_or(0);
        let first = [
            next_in_manifest,
            max_seen + 1,
            self.floor_id.map_or(0, |f| f + 1),
            replay_after + 1,
        ]
        .into_iter()
        .max()
        .unwrap_or(1);
        let (first, gen) =
            self.stripe
                .register(self.range, first, manifest.epoch(), self.skipped_own)?;
        let _ = self.first.set(first);
        Ok(WriterInitResult {
            replay_iterator: Box::new(FramesIter(replay)),
            wal_writer: Box::new(RangeWal {
                stripe: Arc::clone(&self.stripe),
                key: (self.range, gen),
            }),
        })
    }

    fn wal_name(&self) -> Option<&str> {
        Some(SHARED_WAL_NAME)
    }
}
