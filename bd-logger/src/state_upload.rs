// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! State snapshot upload coordination for log uploads.
//!
//! This module provides the [`StateUploadHandle`] which tracks the coordination between log
//! and state snapshots. The server associates logs with state by timestamp - logs at time T use the
//! most recent state snapshot uploaded before time T.
//!
//! The [`StateUploadHandle`] ensures that:
//! - State snapshots are uploaded before logs that depend on them
//! - Duplicate snapshot uploads are avoided across multiple buffers
//! - Snapshot coverage is tracked across process restarts via persistence
//!
//! ## Architecture
//!
//! Upload coordination is split into two parts:
//!
//! - [`StateUploadHandle`] — a cheap, cloneable handle held by each buffer uploader. Callers
//!   fire-and-forget upload requests via [`StateUploadHandle::notify_upload_needed`], which
//!   coalesces ranges in shared state and emits best-effort wake signals without blocking.
//!
//! - [`StateUploadWorker`] — a single background task that owns all snapshot creation and upload
//!   logic. Because only one task processes requests, deduplication and cooldown enforcement happen
//!   naturally without any locking between callers.

#[cfg(test)]
#[path = "./state_upload_test.rs"]
mod tests;

use bd_artifact_upload::{Client as ArtifactClient, EnqueueError, UploadSource};
use bd_client_stats_store::{Counter, Scope};
use bd_log_primitives::LogFields;
use bd_resilient_kv::SnapshotFilename;
use bd_state::{RetentionHandle, RetentionRegistry};
use bd_time::{OffsetDateTimeExt, TimeProvider};
use protobuf::well_known_types::struct_::{Struct as ProtoStruct, Value as ProtoValue, value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};

/// Capacity of the worker wake channel used to nudge processing of coalesced pending ranges.
const UPLOAD_CHANNEL_CAPACITY: usize = 1;
const BACKPRESSURE_RETRY_INTERVAL: Duration = Duration::from_secs(1);
static PENDING_UPLOAD_RANGE_KEY: bd_key_value::Key<ProtoStruct> =
  bd_key_value::Key::new("state_upload.pending_range.1");


/// A reference to a state snapshot that should be uploaded.
#[derive(Debug, Clone)]
pub struct SnapshotRef {
  /// The timestamp of the snapshot (microseconds since epoch).
  pub timestamp_micros: u64,
  /// Path to the snapshot file.
  pub path: PathBuf,
}

#[derive(Clone, Copy)]
struct PendingRange {
  oldest_micros: u64,
  newest_micros: u64,
}

impl PendingRange {
  fn merge(&mut self, other: Self) {
    self.oldest_micros = self.oldest_micros.min(other.oldest_micros);
    self.newest_micros = self.newest_micros.max(other.newest_micros);
  }
}

#[derive(Default)]
struct PendingAccumulator {
  range: Option<PendingRange>,
  version: u64,
  wake_queued: bool,
}

struct Stats {
  snapshots_uploaded: Counter,
  snapshots_skipped: Counter,
  upload_failures: Counter,
  backpressure_pauses: Counter,
}

impl Stats {
  fn new(scope: &Scope) -> Self {
    Self {
      snapshots_uploaded: scope.counter("snapshots_uploaded"),
      snapshots_skipped: scope.counter("snapshots_skipped"),
      upload_failures: scope.counter("upload_failures"),
      backpressure_pauses: scope.counter("backpressure_pauses"),
    }
  }
}

/// Coordinates state snapshot uploads before log uploads.
///
/// This is a lightweight, cloneable coalescing handle. Buffer uploaders call
/// [`notify_upload_needed`][Self::notify_upload_needed] in a fire-and-forget manner —
/// the call is non-blocking and never waits for the snapshot to be created or uploaded.
///
/// All actual snapshot creation and upload logic is handled by the companion
/// [`StateUploadWorker`], which runs as a single background task.
pub struct StateUploadHandle {
  /// Best-effort wake channel for nudging the background worker.
  wake_tx: mpsc::Sender<()>,
  /// Shared pending-range accumulator.
  pending_accumulator: Arc<parking_lot::Mutex<PendingAccumulator>>,
}

impl StateUploadHandle {
  /// Creates a new handle and its companion worker.
  ///
  /// The returned [`StateUploadWorker`] must be spawned (e.g. via `tokio::spawn` or included in a
  /// `try_join!`) for snapshot uploads to be processed. The handle can be cloned and
  /// shared across multiple buffer uploaders.
  pub async fn new(
    state_store_path: Option<PathBuf>,
    store: Arc<bd_key_value::Store>,
    retention_registry: Option<Arc<RetentionRegistry>>,
    state_store: Option<Arc<bd_state::Store>>,
    snapshot_creation_interval_ms: u32,
    time_provider: Arc<dyn TimeProvider>,
    artifact_client: Arc<dyn ArtifactClient>,
    stats_scope: &Scope,
  ) -> (Self, StateUploadWorker) {
    let stats = Stats::new(&stats_scope.scope("state_upload"));

    let (wake_tx, wake_rx) = mpsc::channel(UPLOAD_CHANNEL_CAPACITY);
    let pending_accumulator = Arc::new(parking_lot::Mutex::new(PendingAccumulator::default()));
    let retention_handle = match &retention_registry {
      Some(registry) => Some(registry.create_handle().await),
      None => None,
    };

    let handle = Self {
      wake_tx,
      pending_accumulator: pending_accumulator.clone(),
    };

    let worker = StateUploadWorker {
      last_snapshot_creation_micros: AtomicU64::new(0),
      snapshot_creation_interval_micros: u64::from(snapshot_creation_interval_ms) * 1000,
      state_store_path,
      store,
      retention_handle,
      state_store,
      time_provider,
      artifact_client,
      wake_rx,
      pending_accumulator,
      pending_version_seen: 0,
      pending_range: None,
      stats,
    };

    (handle, worker)
  }

  /// Notifies the background worker that a state snapshot upload may be needed for a log batch.
  ///
  /// This is non-blocking. The range is first merged into a shared accumulator, then the worker is
  /// nudged via a best-effort wake channel.
  pub fn notify_upload_needed(&self, batch_oldest_micros: u64, batch_newest_micros: u64) {
    let should_wake = {
      let mut pending = self.pending_accumulator.lock();
      let incoming = PendingRange {
        oldest_micros: batch_oldest_micros,
        newest_micros: batch_newest_micros,
      };
      if let Some(existing) = &mut pending.range {
        existing.merge(incoming);
      } else {
        pending.range = Some(incoming);
      }
      pending.version = pending.version.wrapping_add(1);
      if pending.wake_queued {
        false
      } else {
        pending.wake_queued = true;
        true
      }
    };

    if should_wake {
      // If this fails there is already a pending wake in the channel so we don't have to worry
      // about nudging the worker later - it will process the updated pending range when it wakes
      // up.
      let _ = self.wake_tx.try_send(());
    }
  }
}



//
// StateUploadWorker
//

/// Background task that processes state snapshot upload requests.
///
/// There is exactly one worker per logger instance. Because all upload logic runs in a single
/// task, deduplication and cooldown enforcement require no synchronization.
///
/// Obtain via [`StateUploadHandle::new`] and spawn with `tokio::spawn` or `try_join!`.
pub struct StateUploadWorker {
  /// Timestamp of the last snapshot creation (microseconds since epoch).
  last_snapshot_creation_micros: AtomicU64,
  /// Minimum interval between snapshot creations (microseconds).
  snapshot_creation_interval_micros: u64,

  state_store_path: Option<PathBuf>,
  store: Arc<bd_key_value::Store>,
  retention_handle: Option<RetentionHandle>,
  state_store: Option<Arc<bd_state::Store>>,
  time_provider: Arc<dyn TimeProvider>,
  artifact_client: Arc<dyn ArtifactClient>,

  /// Used to coordinate updates to the pending range and best-effort wake signals from the handle.
  wake_rx: mpsc::Receiver<()>,
  pending_accumulator: Arc<parking_lot::Mutex<PendingAccumulator>>,
  pending_version_seen: u64,
  pending_range: Option<PendingRange>,

  stats: Stats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessResult {
  Progress,
  Backpressure,
  DeferredCooldown,
  Skipped,
  Error,
}

enum UploadPreflight {
  Skipped,
  DeferredCooldown,
  Ready(Vec<SnapshotRef>),
}


impl StateUploadWorker {
  /// Returns the path to the state store directory, if configured.
  #[must_use]
  pub fn state_store_path(&self) -> Option<&Path> {
    self.state_store_path.as_deref()
  }

  /// Runs the worker event loop, processing upload requests until the channel is closed.
  pub async fn run(mut self) {
    log::debug!("state upload worker started");
    self.refresh_retention_handle();
    self.pending_range = self.read_persisted_pending_range();
    if self.pending_range.is_some() {
      self.process_pending().await;
    }

    loop {
      tokio::select! {
        Some(()) = self.wake_rx.recv() => {
          self.drain_pending_accumulator();
          self.process_pending().await;
          while self.pending_version_changed() {
            self.drain_pending_accumulator();
            self.process_pending().await;
          }
        }
        () = sleep(BACKPRESSURE_RETRY_INTERVAL), if self.pending_range.is_some() => {
          self.drain_pending_accumulator();
          self.process_pending().await;
        }
        else => break,
      }
    }
  }

  fn drain_pending_accumulator(&mut self) {
    let mut pending = self.pending_accumulator.lock();
    if let Some(incoming) = pending.range.take() {
      if let Some(existing) = &mut self.pending_range {
        existing.merge(incoming);
      } else {
        self.pending_range = Some(incoming);
      }
    }
    self.pending_version_seen = pending.version;
    pending.wake_queued = false;
    self.persist_pending_range();
  }

  fn pending_version_changed(&self) -> bool {
    let pending = self.pending_accumulator.lock();
    pending.version != self.pending_version_seen
  }

  async fn process_pending(&mut self) {
    let Some(pending) = self.pending_range else {
      return;
    };
    let result = self
      .process_upload(pending.oldest_micros, pending.newest_micros)
      .await;
    if matches!(
      result,
      ProcessResult::Backpressure | ProcessResult::DeferredCooldown
    ) {
      self.stats.backpressure_pauses.inc();
    }

    if matches!(result, ProcessResult::Progress | ProcessResult::Skipped) {
      self.pending_range = None;
    }
    self.persist_pending_range();
    self.refresh_retention_handle();
  }

  // State upload flow:
  // 1) Build an upload plan in `plan_upload_attempt` by checking coverage/last-change state,
  //    finding in-range snapshots, and deciding whether on-demand snapshot creation is needed.
  // 2) Handle preflight outcomes:
  //    - `Skipped`: no work required for current coverage.
  //    - `DeferredCooldown`: uncovered changes exist but snapshot creation is rate-limited.
  //    - `Ready`: concrete snapshots should be uploaded now.
  // 3) For each ready snapshot, enqueue and wait for persistence ack.
  // 4) On success, count the snapshot upload and continue; on failure, keep pending work for retry.
  async fn process_upload(
    &self,
    batch_oldest_micros: u64,
    batch_newest_micros: u64,
  ) -> ProcessResult {
    let last_change = self
      .state_store
      .as_ref()
      .map_or(0, |s| s.last_change_micros());

    let snapshots = match self
      .plan_upload_attempt(batch_oldest_micros, batch_newest_micros, last_change)
      .await
    {
      UploadPreflight::Skipped => return ProcessResult::Skipped,
      UploadPreflight::DeferredCooldown => return ProcessResult::DeferredCooldown,
      UploadPreflight::Ready(snapshots) => snapshots,
    };
    self.refresh_retention_handle();

    // Upload each snapshot in order, advancing the watermark after each confirmed upload.
    for snapshot_ref in snapshots {
      log::debug!(
        "uploading state snapshot {} for log batch (oldest={}, newest={})",
        snapshot_ref.timestamp_micros,
        batch_oldest_micros,
        batch_newest_micros
      );

      let timestamp = OffsetDateTime::from_unix_timestamp_micros(
        snapshot_ref.timestamp_micros.try_into().unwrap_or_default(),
      )
      .ok();

      let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
      match self.artifact_client.enqueue_upload(
        UploadSource::Path(snapshot_ref.path.clone()),
        "state_snapshot".to_string(),
        LogFields::new(),
        timestamp,
        "state_snapshot".to_string(),
        vec![],
        Some(persisted_tx),
      ) {
        Ok(_uuid) => match persisted_rx.await {
          Ok(Ok(())) => {
            log::debug!(
              "state snapshot persisted to artifact queue for timestamp {}",
              snapshot_ref.timestamp_micros
            );
            self.stats.snapshots_uploaded.inc();
          },
          Ok(Err(e)) => {
            log::warn!("failed to persist state snapshot upload entry: {e}");
            self.stats.upload_failures.inc();
            if matches!(e, EnqueueError::QueueFull) {
              return ProcessResult::Backpressure;
            }
            return ProcessResult::Error;
          },
          Err(e) => {
            log::warn!("state snapshot persistence ack channel dropped: {e}");
            self.stats.upload_failures.inc();
            return ProcessResult::Error;
          },
        },
        Err(e) => {
          log::warn!("failed to enqueue state snapshot upload: {e}");
          self.stats.upload_failures.inc();
          if matches!(e, EnqueueError::QueueFull) {
            return ProcessResult::Backpressure;
          }
          return ProcessResult::Error;
        },
      }
    }
    ProcessResult::Progress
  }

  async fn plan_upload_attempt(
    &self,
    batch_oldest_micros: u64,
    batch_newest_micros: u64,
    last_change: u64,
  ) -> UploadPreflight {
    if last_change == 0 {
      log::debug!("state upload: last_change=0, skipping (batch_newest={batch_newest_micros})");
      return UploadPreflight::Skipped;
    }

    let snapshots = self.find_snapshots_in_range(batch_oldest_micros, batch_newest_micros);
    if !snapshots.is_empty() {
      return UploadPreflight::Ready(snapshots);
    }

    let now_micros = self
      .time_provider
      .now()
      .unix_timestamp_micros()
      .cast_unsigned();
    if self.snapshot_creation_on_cooldown(now_micros) {
      self.stats.snapshots_skipped.inc();
      log::debug!(
        "deferring snapshot creation due to cooldown (last={}, now={now_micros}, interval={})",
        self.last_snapshot_creation_micros.load(Ordering::Relaxed),
        self.snapshot_creation_interval_micros
      );
      return UploadPreflight::DeferredCooldown;
    }

    self
      .create_snapshot_if_needed(last_change)
      .await
      .map_or(UploadPreflight::DeferredCooldown, |snapshot| {
        UploadPreflight::Ready(vec![snapshot])
      })
  }

  fn find_snapshots_in_range(
    &self,
    batch_oldest_micros: u64,
    batch_newest_micros: u64,
  ) -> Vec<SnapshotRef> {
    self
      .find_all_snapshots()
      .into_iter()
      .filter(|snapshot| {
        snapshot.timestamp_micros >= batch_oldest_micros
          && snapshot.timestamp_micros <= batch_newest_micros
      })
      .collect()
  }

  /// Finds all snapshot files, sorted oldest first.
  pub(crate) fn find_all_snapshots(&self) -> Vec<SnapshotRef> {
    let Some(state_path) = self.state_store_path.as_ref() else {
      return vec![];
    };
    let snapshots_dir = state_path.join("snapshots");

    let Ok(entries) = std::fs::read_dir(&snapshots_dir) else {
      return vec![];
    };

    let mut found: Vec<SnapshotRef> = entries
      .flatten()
      .filter_map(|entry| {
        let path = entry.path();
        let filename = path.file_name().and_then(|f| f.to_str())?.to_owned();
        let parsed = SnapshotFilename::parse(&filename)?;
        Some(SnapshotRef {
          timestamp_micros: parsed.timestamp_micros,
          path,
        })
      })
      .collect();

    found.sort_by_key(|s| s.timestamp_micros);
    found
  }

  /// Creates a new snapshot for uncovered state changes, if needed.
  ///
  /// Implements cooldown logic to prevent excessive snapshot creation during high-volume log
  /// streaming. If a snapshot was created recently (within
  /// `snapshot_creation_interval_micros`), returns `None` to defer creation for a later retry.
  pub(crate) async fn create_snapshot_if_needed(
    &self,
    min_uncovered_micros: u64,
  ) -> Option<SnapshotRef> {
    let state_store = self.state_store.as_ref()?;
    if let Some(handle) = &self.retention_handle {
      // Ensure cleanup during rotation doesn't remove the newly created snapshot before it can be
      // enqueued for upload.
      handle.update_retention_micros(min_uncovered_micros);
    }

    let now_micros = {
      let now = self.time_provider.now();
      now.unix_timestamp_micros().cast_unsigned()
    };
    if self.snapshot_creation_on_cooldown(now_micros) {
      log::debug!(
        "skipping snapshot creation due to cooldown (last={}, now={now_micros}, interval={})",
        self.last_snapshot_creation_micros.load(Ordering::Relaxed),
        self.snapshot_creation_interval_micros
      );
      self.stats.snapshots_skipped.inc();
      return None;
    }

    log::debug!(
      "creating snapshot for uncovered state changes (min_uncovered_micros={min_uncovered_micros})"
    );

    let Some(snapshot_path) = state_store.rotate_journal().await else {
      log::debug!("snapshot creation failed or not supported");
      self.stats.upload_failures.inc();
      return None;
    };

    self
      .last_snapshot_creation_micros
      .store(now_micros, Ordering::Relaxed);

    let filename = snapshot_path.file_name().and_then(|f| f.to_str())?;
    let Some(parsed) = SnapshotFilename::parse(filename) else {
      log::debug!("failed to parse snapshot filename: {filename}");
      self.stats.upload_failures.inc();
      return None;
    };

    Some(SnapshotRef {
      timestamp_micros: parsed.timestamp_micros,
      path: snapshot_path,
    })
  }

  fn snapshot_creation_on_cooldown(&self, now_micros: u64) -> bool {
    let last_creation = self.last_snapshot_creation_micros.load(Ordering::Relaxed);
    last_creation > 0
      && now_micros.saturating_sub(last_creation) < self.snapshot_creation_interval_micros
  }

  fn persist_pending_range(&self) {
    match self.pending_range {
      Some(range) => self
        .store
        .set(&PENDING_UPLOAD_RANGE_KEY, &pending_range_to_proto(range)),
      None => self
        .store
        .set(&PENDING_UPLOAD_RANGE_KEY, &ProtoStruct::default()),
    }
  }

  fn read_persisted_pending_range(&self) -> Option<PendingRange> {
    self
      .store
      .get(&PENDING_UPLOAD_RANGE_KEY)
      .and_then(|proto| pending_range_from_proto(&proto))
  }

  fn refresh_retention_handle(&self) {
    let Some(handle) = &self.retention_handle else {
      return;
    };
    let oldest_snapshot = self
      .find_all_snapshots()
      .into_iter()
      .map(|s| s.timestamp_micros)
      .min();
    match oldest_snapshot {
      Some(oldest) => handle.update_retention_micros(oldest),
      None => handle.update_retention_micros(RetentionHandle::RETENTION_NONE),
    }
  }
}

fn pending_range_to_proto(range: PendingRange) -> ProtoStruct {
  let mut proto = ProtoStruct::new();
  proto.fields.insert(
    "oldest_micros".to_string(),
    ProtoValue {
      kind: Some(value::Kind::StringValue(range.oldest_micros.to_string())),
      ..Default::default()
    },
  );
  proto.fields.insert(
    "newest_micros".to_string(),
    ProtoValue {
      kind: Some(value::Kind::StringValue(range.newest_micros.to_string())),
      ..Default::default()
    },
  );
  proto
}

fn pending_range_from_proto(proto: &ProtoStruct) -> Option<PendingRange> {
  let oldest = proto
    .fields
    .get("oldest_micros")
    .and_then(proto_string_value_to_u64)?;
  let newest = proto
    .fields
    .get("newest_micros")
    .and_then(proto_string_value_to_u64)?;
  Some(PendingRange {
    oldest_micros: oldest,
    newest_micros: newest,
  })
}

fn proto_string_value_to_u64(value: &ProtoValue) -> Option<u64> {
  let value::Kind::StringValue(v) = value.kind.as_ref()? else {
    return None;
  };
  v.parse::<u64>().ok()
}
