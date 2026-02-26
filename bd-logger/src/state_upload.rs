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
//!   fire-and-forget upload requests via [`StateUploadHandle::notify_upload_needed`], which sends
//!   to a bounded channel without blocking.
//!
//! - [`StateUploadWorker`] — a single background task that owns all snapshot creation and upload
//!   logic. Because only one task processes requests, deduplication and cooldown enforcement happen
//!   naturally without any locking between callers.

#[cfg(test)]
#[path = "./state_upload_test.rs"]
mod tests;

use bd_artifact_upload::{Client as ArtifactClient, EnqueueError};
use bd_client_stats_store::{Counter, Scope};
use bd_log_primitives::LogFields;
use bd_resilient_kv::SnapshotFilename;
use bd_state::{RetentionHandle, RetentionRegistry};
use bd_time::{OffsetDateTimeExt, TimeProvider};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};

/// Capacity of the upload request channel. Requests beyond this are silently dropped — the worker
/// will process the queued requests which already cover the needed state range.
const UPLOAD_CHANNEL_CAPACITY: usize = 8;
const BACKPRESSURE_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Key for persisting the state upload index via bd-key-value.
static STATE_UPLOAD_KEY: bd_key_value::Key<String> =
  bd_key_value::Key::new("state_upload.uploaded_through.1");


/// A reference to a state snapshot that should be uploaded.
#[derive(Debug, Clone)]
pub struct SnapshotRef {
  /// The timestamp of the snapshot (microseconds since epoch).
  pub timestamp_micros: u64,
  /// Path to the snapshot file.
  pub path: PathBuf,
}

/// A request from a buffer uploader to upload a state snapshot if needed.
#[derive(Clone, Copy)]
struct StateUploadRequest {
  batch_oldest_micros: u64,
  batch_newest_micros: u64,
}

#[derive(Clone, Copy)]
struct PendingRange {
  oldest_micros: u64,
  newest_micros: u64,
}

impl PendingRange {
  const fn from_request(request: &StateUploadRequest) -> Self {
    Self {
      oldest_micros: request.batch_oldest_micros,
      newest_micros: request.batch_newest_micros,
    }
  }

  fn merge(&mut self, other: Self) {
    self.oldest_micros = self.oldest_micros.min(other.oldest_micros);
    self.newest_micros = self.newest_micros.max(other.newest_micros);
  }
}

/// Statistics for state upload operations.
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
/// This is a lightweight, cloneable sender handle. Buffer uploaders call
/// [`notify_upload_needed`][Self::notify_upload_needed] in a fire-and-forget manner —
/// the call is non-blocking and never waits for the snapshot to be created or uploaded.
///
/// All actual snapshot creation and upload logic is handled by the companion
/// [`StateUploadWorker`], which runs as a single background task.
pub struct StateUploadHandle {
  /// Channel for sending upload requests to the background worker.
  upload_tx: mpsc::Sender<StateUploadRequest>,
}

impl StateUploadHandle {
  /// Creates a new handle and its companion worker.
  ///
  /// The returned [`StateUploadWorker`] must be spawned (e.g. via `tokio::spawn` or included in a
  /// `try_join!`) for snapshot uploads to be processed. The handle can be cloned and
  /// shared across multiple buffer uploaders.
  ///
  /// # Arguments
  /// * `state_store_path` - Path to the state store directory containing snapshot files
  /// * `store` - Key-value store for persisting upload index
  /// * `retention_registry` - Registry for managing snapshot retention to prevent cleanup
  /// * `state_store` - Optional state store for triggering on-demand snapshots
  /// * `snapshot_creation_interval_ms` - Minimum interval between snapshot creations (ms)
  /// * `time_provider` - Time provider for getting current time
  /// * `artifact_client` - Client for uploading snapshot artifacts
  /// * `stats_scope` - Stats scope for metrics
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

    let retention_handle = match &retention_registry {
      Some(registry) => Some(registry.create_handle().await),
      None => None,
    };

    let uploaded_through = store
      .get_string(&STATE_UPLOAD_KEY)
      .and_then(|s| s.parse::<u64>().ok())
      .unwrap_or(0);

    if uploaded_through > 0 {
      log::debug!("loaded state upload coverage through {uploaded_through}");
    }

    if let Some(handle) = &retention_handle
      && uploaded_through > 0
    {
      handle.update_retention_micros(uploaded_through);
    }

    let (upload_tx, upload_rx) = mpsc::channel(UPLOAD_CHANNEL_CAPACITY);

    let state_uploaded_through_micros = Arc::new(AtomicU64::new(uploaded_through));

    let handle = Self { upload_tx };

    let worker = StateUploadWorker {
      state_uploaded_through_micros,
      last_snapshot_creation_micros: AtomicU64::new(0),
      snapshot_creation_interval_micros: u64::from(snapshot_creation_interval_ms) * 1000,
      state_store_path,
      store,
      retention_handle,
      state_store,
      time_provider,
      artifact_client,
      upload_rx,
      pending_range: None,
      stats,
    };

    (handle, worker)
  }

  /// Notifies the background worker that a state snapshot upload may be needed for a log batch.
  ///
  /// This is non-blocking: the request is queued in a bounded channel and the worker processes it
  /// asynchronously. If the channel is full, the request is silently dropped — the worker will
  /// still cover the needed state range via already-queued requests.
  pub fn notify_upload_needed(&self, batch_oldest_micros: u64, batch_newest_micros: u64) {
    let result = self.upload_tx.try_send(StateUploadRequest {
      batch_oldest_micros,
      batch_newest_micros,
    });
    if let Err(e) = result {
      log::warn!("dropping state upload request due to full channel: {e}");
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
  /// Shared with the handle — updated after successful uploads.
  state_uploaded_through_micros: Arc<AtomicU64>,

  /// Timestamp of the last snapshot creation (microseconds since epoch).
  last_snapshot_creation_micros: AtomicU64,

  /// Minimum interval between snapshot creations (microseconds).
  snapshot_creation_interval_micros: u64,

  /// Path to the state store directory (for finding snapshot files).
  state_store_path: Option<PathBuf>,

  /// Key-value store for persisting upload index across restarts.
  store: Arc<bd_key_value::Store>,

  /// Retention handle for preventing snapshot cleanup.
  retention_handle: Option<RetentionHandle>,

  /// State store for triggering on-demand snapshot creation before uploads.
  state_store: Option<Arc<bd_state::Store>>,

  time_provider: Arc<dyn TimeProvider>,

  /// Artifact client for uploading snapshots.
  artifact_client: Arc<dyn ArtifactClient>,

  /// Receiver for upload requests from handle instances.
  upload_rx: mpsc::Receiver<StateUploadRequest>,
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
    loop {
      tokio::select! {
        Some(request) = self.upload_rx.recv() => {
          self.ingest_request(request);
          self.drain_pending_requests();
          self.process_pending().await;
        }
        () = sleep(BACKPRESSURE_RETRY_INTERVAL), if self.pending_range.is_some() => {
          self.process_pending().await;
        }
        else => break,
      }
    }
  }

  fn ingest_request(&mut self, request: StateUploadRequest) {
    let incoming = PendingRange::from_request(&request);
    if let Some(existing) = &mut self.pending_range {
      existing.merge(incoming);
    } else {
      self.pending_range = Some(incoming);
    }
  }

  fn drain_pending_requests(&mut self) {
    while let Ok(request) = self.upload_rx.try_recv() {
      self.ingest_request(request);
    }
  }

  async fn process_pending(&mut self) {
    let Some(pending) = self.pending_range else {
      return;
    };
    match self
      .process_upload(pending.oldest_micros, pending.newest_micros)
      .await
    {
      ProcessResult::Backpressure | ProcessResult::DeferredCooldown => {
        self.stats.backpressure_pauses.inc();
      },
      _ => {
        // A "Skipped" result can still mean we're done with this pending range:
        // process_upload first recomputes current coverage from persisted state/upload watermark.
        // If another earlier upload already advanced coverage past `pending.oldest_micros`, there
        // is nothing left to do for this range and we clear it here.
        if self.pending_satisfied(pending.oldest_micros) {
          self.pending_range = None;
        }
      },
    }
  }

  /// Pending work is satisfied once uploaded coverage reaches the oldest timestamp that required
  /// state; newer pending windows are merged before this check.
  fn pending_satisfied(&self, pending_oldest_micros: u64) -> bool {
    let uploaded_through = self.state_uploaded_through_micros.load(Ordering::Relaxed);
    uploaded_through >= pending_oldest_micros
  }

  // State upload flow:
  // 1) Build an upload plan in `plan_upload_attempt` by checking coverage/last-change state,
  //    finding in-range snapshots, and deciding whether on-demand snapshot creation is needed.
  // 2) Handle preflight outcomes:
  //    - `Skipped`: no work required for current coverage.
  //    - `DeferredCooldown`: uncovered changes exist but snapshot creation is rate-limited.
  //    - `Ready`: concrete snapshots should be uploaded now.
  // 3) For each ready snapshot, enqueue and wait for persistence ack.
  // 4) Advance `state_uploaded_through_micros` only after a successful persistence ack via
  //    `on_state_uploaded`, so deferred/failed attempts never move the watermark.
  async fn process_upload(
    &self,
    batch_oldest_micros: u64,
    batch_newest_micros: u64,
  ) -> ProcessResult {
    let uploaded_through = self.state_uploaded_through_micros.load(Ordering::Relaxed);
    let last_change = self
      .state_store
      .as_ref()
      .map_or(0, |s| s.last_change_micros());

    let snapshots = match self
      .plan_upload_attempt(
        batch_oldest_micros,
        batch_newest_micros,
        uploaded_through,
        last_change,
      )
      .await
    {
      UploadPreflight::Skipped => return ProcessResult::Skipped,
      UploadPreflight::DeferredCooldown => return ProcessResult::DeferredCooldown,
      UploadPreflight::Ready(snapshots) => snapshots,
    };

    // Upload each snapshot in order, advancing the watermark after each confirmed upload.
    for snapshot_ref in snapshots {
      log::debug!(
        "uploading state snapshot {} for log batch (oldest={}, newest={})",
        snapshot_ref.timestamp_micros,
        batch_oldest_micros,
        batch_newest_micros
      );

      // Open the snapshot file.
      let file = match File::open(&snapshot_ref.path) {
        Ok(f) => f,
        Err(e) => {
          log::warn!(
            "failed to open snapshot file {}: {e}",
            snapshot_ref.path.display()
          );
          self.stats.upload_failures.inc();
          return ProcessResult::Error;
        },
      };

      let timestamp = OffsetDateTime::from_unix_timestamp_micros(
        snapshot_ref.timestamp_micros.try_into().unwrap_or_default(),
      )
      .ok();

      let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
      match self.artifact_client.enqueue_upload(
        file,
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
            self.on_state_uploaded(snapshot_ref.timestamp_micros);
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
    uploaded_through: u64,
    last_change: u64,
  ) -> UploadPreflight {
    if last_change == 0 {
      log::debug!(
        "state upload: last_change=0, skipping (uploaded_through={uploaded_through}, \
         batch_oldest={batch_oldest_micros})"
      );
      return UploadPreflight::Skipped;
    }

    if uploaded_through >= batch_oldest_micros {
      self.stats.snapshots_skipped.inc();
      return UploadPreflight::Skipped;
    }

    if last_change <= uploaded_through {
      self.stats.snapshots_skipped.inc();
      return UploadPreflight::Skipped;
    }

    let mut snapshots = self.find_snapshots_in_range(uploaded_through, batch_newest_micros);
    let effective_coverage = snapshots
      .last()
      .map_or(uploaded_through, |snapshot| snapshot.timestamp_micros);

    if last_change > effective_coverage {
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

      if let Some(snapshot) = self
        .create_snapshot_if_needed(effective_coverage.saturating_add(1))
        .await
        && snapshot.timestamp_micros > effective_coverage
      {
        snapshots.push(snapshot);
      }
    }

    if snapshots.is_empty() {
      UploadPreflight::Skipped
    } else {
      UploadPreflight::Ready(snapshots)
    }
  }

  /// Called after a state snapshot has been successfully uploaded.
  fn on_state_uploaded(&self, snapshot_timestamp_micros: u64) {
    self
      .state_uploaded_through_micros
      .fetch_max(snapshot_timestamp_micros, Ordering::Relaxed);
    self.stats.snapshots_uploaded.inc();

    // Update retention handle to allow cleanup of snapshots older than what we've uploaded.
    if let Some(handle) = &self.retention_handle {
      handle.update_retention_micros(snapshot_timestamp_micros);
    }

    // Persist the updated coverage.
    self
      .store
      .set_string(&STATE_UPLOAD_KEY, &snapshot_timestamp_micros.to_string());
  }

  /// Finds all snapshot files in the range `(after_micros, up_to_micros]`, sorted oldest first.
  ///
  /// This ensures we upload every state change that occurred during the batch window, not just the
  /// most recent one.
  pub(crate) fn find_snapshots_in_range(
    &self,
    after_micros: u64,
    up_to_micros: u64,
  ) -> Vec<SnapshotRef> {
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
        if parsed.timestamp_micros > after_micros && parsed.timestamp_micros <= up_to_micros {
          Some(SnapshotRef {
            timestamp_micros: parsed.timestamp_micros,
            path,
          })
        } else {
          None
        }
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
}
