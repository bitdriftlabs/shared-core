// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! State snapshot upload coordination for log uploads.
//!
//! This module provides the [`StateLogCorrelator`] which tracks the correlation between log uploads
//! and state snapshots. The server correlates logs with state by timestamp - logs at time T use the
//! most recent state snapshot uploaded before time T.
//!
//! The correlator ensures that:
//! - State snapshots are uploaded before logs that depend on them
//! - Duplicate snapshot uploads are avoided across multiple buffers
//! - Snapshot coverage is tracked across process restarts via persistence
//!
//! ## Architecture
//!
//! Upload coordination is split into two parts:
//!
//! - [`StateLogCorrelator`] — a cheap, cloneable handle held by each buffer uploader. Callers
//!   fire-and-forget upload requests via [`StateLogCorrelator::notify_upload_needed`], which sends
//!   to a bounded channel without blocking.
//!
//! - [`StateUploadWorker`] — a single background task that owns all snapshot creation and upload
//!   logic. Because only one task processes requests, deduplication and cooldown enforcement happen
//!   naturally without any locking between callers.

#[cfg(test)]
#[path = "./state_upload_test.rs"]
mod tests;

use bd_artifact_upload::Client as ArtifactClient;
use bd_client_stats_store::{Counter, Scope};
use bd_log_primitives::LogFields;
use bd_resilient_kv::SnapshotFilename;
use bd_state::{RetentionHandle, RetentionRegistry};
use bd_time::TimeProvider;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use time::OffsetDateTime;
use tokio::sync::mpsc;

/// Capacity of the upload request channel. Requests beyond this are silently dropped — the worker
/// will process the queued requests which already cover the needed state range.
const UPLOAD_CHANNEL_CAPACITY: usize = 8;

/// Key for persisting the state upload index via bd-key-value.
static STATE_UPLOAD_KEY: bd_key_value::Key<String> =
  bd_key_value::Key::new("state_upload.uploaded_through.1");

//
// SnapshotCreator
//

/// Trait for creating state snapshots on demand.
///
/// This is implemented by `bd_state::Store` to allow the correlator to trigger journal rotation
/// and create snapshot files when needed for log uploads.
#[async_trait::async_trait]
pub trait SnapshotCreator: Send + Sync {
  /// Triggers a journal rotation to create a snapshot file.
  ///
  /// Returns the path to the created snapshot file, or `None` if snapshot creation failed or is
  /// not supported (e.g., in-memory store).
  async fn create_snapshot(&self) -> Option<PathBuf>;
}

/// A reference to a state snapshot that should be uploaded.
#[derive(Debug, Clone)]
pub struct SnapshotRef {
  /// The timestamp of the snapshot (microseconds since epoch).
  pub timestamp_micros: u64,
  /// The generation number of the snapshot file.
  pub generation: u64,
  /// Path to the snapshot file.
  pub path: PathBuf,
}

/// A request from a buffer uploader to upload a state snapshot if needed.
struct StateUploadRequest {
  batch_oldest_micros: u64,
  batch_newest_micros: u64,
}

/// Statistics for state upload operations.
struct Stats {
  snapshots_uploaded: Counter,
  snapshots_skipped: Counter,
  upload_failures: Counter,
}

impl Stats {
  fn new(scope: &Scope) -> Self {
    Self {
      snapshots_uploaded: scope.counter("snapshots_uploaded"),
      snapshots_skipped: scope.counter("snapshots_skipped"),
      upload_failures: scope.counter("upload_failures"),
    }
  }
}

/// Tracks correlation between log uploads and state snapshot coverage.
///
/// This is a lightweight, cloneable sender handle. Buffer uploaders call
/// [`notify_upload_needed`][Self::notify_upload_needed] in a fire-and-forget manner —
/// the call is non-blocking and never waits for the snapshot to be created or uploaded.
///
/// All actual snapshot creation and upload logic is handled by the companion
/// [`StateUploadWorker`], which runs as a single background task.
pub struct StateLogCorrelator {
  /// Timestamp of the most recent state change (microseconds since epoch).
  pub(crate) last_state_change_micros: Arc<AtomicU64>,

  /// Channel for sending upload requests to the background worker.
  upload_tx: mpsc::Sender<StateUploadRequest>,
}

impl StateLogCorrelator {
  /// Creates a new correlator and its companion worker.
  ///
  /// The returned [`StateUploadWorker`] must be spawned (e.g. via `tokio::spawn` or included in a
  /// `try_join!`) for snapshot uploads to be processed. The correlator handle can be cloned and
  /// shared across multiple buffer uploaders.
  ///
  /// # Arguments
  /// * `state_store_path` - Path to the state store directory containing snapshot files
  /// * `store` - Key-value store for persisting upload index
  /// * `retention_registry` - Registry for managing snapshot retention to prevent cleanup
  /// * `snapshot_creator` - Optional snapshot creator for triggering on-demand snapshots
  /// * `snapshot_creation_interval_ms` - Minimum interval between snapshot creations (ms)
  /// * `time_provider` - Time provider for getting current time
  /// * `artifact_client` - Client for uploading snapshot artifacts
  /// * `stats_scope` - Stats scope for metrics
  pub async fn new(
    state_store_path: Option<PathBuf>,
    store: Arc<bd_key_value::Store>,
    retention_registry: Option<Arc<RetentionRegistry>>,
    snapshot_creator: Option<Arc<dyn SnapshotCreator>>,
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

    let last_state_change_micros = Arc::new(AtomicU64::new(0));
    let state_uploaded_through_micros = Arc::new(AtomicU64::new(uploaded_through));

    let correlator = Self {
      last_state_change_micros: last_state_change_micros.clone(),
      upload_tx,
    };

    let worker = StateUploadWorker {
      state_uploaded_through_micros,
      last_state_change_micros,
      last_snapshot_creation_micros: AtomicU64::new(0),
      snapshot_creation_interval_micros: u64::from(snapshot_creation_interval_ms) * 1000,
      state_store_path,
      store,
      retention_handle,
      snapshot_creator,
      time_provider,
      artifact_client,
      upload_rx,
      stats,
    };

    (correlator, worker)
  }

  /// Called when state changes. Updates the tracked last state change timestamp.
  ///
  /// This should be called from the state store's change listener to notify the correlator that
  /// state has changed and a new snapshot may need to be uploaded.
  pub fn on_state_change(&self, timestamp: OffsetDateTime) {
    let timestamp_micros =
      timestamp.unix_timestamp().cast_unsigned() * 1_000_000 + u64::from(timestamp.microsecond());
    self
      .last_state_change_micros
      .fetch_max(timestamp_micros, Ordering::Relaxed);
  }


  /// Notifies the background worker that a state snapshot upload may be needed for a log batch.
  ///
  /// This is non-blocking: the request is queued in a bounded channel and the worker processes it
  /// asynchronously. If the channel is full, the request is silently dropped — the worker will
  /// still cover the needed state range via already-queued requests.
  pub fn notify_upload_needed(&self, batch_oldest_micros: u64, batch_newest_micros: u64) {
    let _ = self.upload_tx.try_send(StateUploadRequest {
      batch_oldest_micros,
      batch_newest_micros,
    });
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
/// Obtain via [`StateLogCorrelator::new`] and spawn with `tokio::spawn` or `try_join!`.
pub struct StateUploadWorker {
  /// Shared with the correlator — updated after successful uploads.
  state_uploaded_through_micros: Arc<AtomicU64>,

  /// Shared with the correlator — read to check for new state changes.
  last_state_change_micros: Arc<AtomicU64>,
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

  /// Snapshot creator for triggering on-demand snapshot creation before uploads.
  snapshot_creator: Option<Arc<dyn SnapshotCreator>>,

  time_provider: Arc<dyn TimeProvider>,

  /// Artifact client for uploading snapshots.
  artifact_client: Arc<dyn ArtifactClient>,

  /// Receiver for upload requests from correlator handles.
  upload_rx: mpsc::Receiver<StateUploadRequest>,

  stats: Stats,
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
    while let Some(request) = self.upload_rx.recv().await {
      // Drain any additional pending requests to coalesce: keep the widest timestamp range
      // across all queued requests so we do the minimum number of snapshots.
      let mut oldest = request.batch_oldest_micros;
      let mut newest = request.batch_newest_micros;

      while let Ok(extra) = self.upload_rx.try_recv() {
        oldest = oldest.min(extra.batch_oldest_micros);
        newest = newest.max(extra.batch_newest_micros);
      }

      self.process_upload(oldest, newest).await;
    }
  }

  async fn process_upload(&self, batch_oldest_micros: u64, batch_newest_micros: u64) {
    let uploaded_through = self.state_uploaded_through_micros.load(Ordering::Relaxed);
    let last_change = self.last_state_change_micros.load(Ordering::Relaxed);

    // If we've never seen any state changes, no upload needed.
    if last_change == 0 {
      log::debug!(
        "state upload: last_change=0, skipping (uploaded_through={uploaded_through}, \
         batch_oldest={batch_oldest_micros})"
      );
      return;
    }

    // If we've already uploaded state that covers this batch, no upload needed.
    if uploaded_through >= batch_oldest_micros {
      self.stats.snapshots_skipped.inc();
      return;
    }

    // If there are no pending state changes since our last upload, no upload needed.
    if last_change <= uploaded_through {
      self.stats.snapshots_skipped.inc();
      return;
    }

    // Find all snapshot files in (uploaded_through, batch_newest_micros] that need uploading.
    let mut snapshots = self.find_snapshots_in_range(uploaded_through, batch_newest_micros);

    // If no existing snapshots cover the range, create one on-demand.
    if snapshots.is_empty() {
      if let Some(snapshot) = self.get_or_create_snapshot(batch_oldest_micros).await {
        snapshots.push(snapshot);
      } else {
        return;
      }
    }

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
          return;
        },
      };

      // Convert timestamp from microseconds to OffsetDateTime.
      let timestamp =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(snapshot_ref.timestamp_micros) * 1000)
          .ok();

      // Create a oneshot channel so we know when the upload is confirmed or dropped.
      let (completion_tx, completion_rx) = tokio::sync::oneshot::channel::<bool>();

      // Enqueue the upload via artifact uploader (skip_intent=true for immediate upload).
      match self.artifact_client.enqueue_upload(
        file,
        LogFields::new(),
        timestamp,
        "state_snapshot".to_string(),
        vec![],
        "state_snapshot".to_string(),
        true,
        Some(completion_tx),
      ) {
        Ok(_uuid) => {
          log::debug!(
            "state snapshot upload enqueued for timestamp {}",
            snapshot_ref.timestamp_micros
          );
          if completion_rx.await == Ok(true) {
            self.on_state_uploaded(snapshot_ref.timestamp_micros);
          } else {
            log::warn!("state snapshot upload failed or was dropped — watermark not advanced");
            self.stats.upload_failures.inc();
            // Watermark NOT advanced — will retry on next notify.
            return;
          }
        },
        Err(e) => {
          log::warn!("failed to enqueue state snapshot upload: {e}");
          self.stats.upload_failures.inc();
          return;
        },
      }
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
            generation: parsed.generation,
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

  /// Returns the most recently created snapshot with timestamp ≤ `up_to_micros`, if any.
  fn find_most_recent_snapshot(&self, up_to_micros: u64) -> Option<SnapshotRef> {
    let state_path = self.state_store_path.as_ref()?;
    let snapshots_dir = state_path.join("snapshots");
    let entries = std::fs::read_dir(&snapshots_dir).ok()?;
    entries
      .flatten()
      .filter_map(|entry| {
        let path = entry.path();
        let filename = path.file_name().and_then(|f| f.to_str())?.to_owned();
        let parsed = SnapshotFilename::parse(&filename)?;
        if parsed.timestamp_micros <= up_to_micros {
          Some(SnapshotRef {
            timestamp_micros: parsed.timestamp_micros,
            generation: parsed.generation,
            path,
          })
        } else {
          None
        }
      })
      .max_by_key(|s| s.timestamp_micros)
  }

  /// Finds an existing snapshot or creates a new one for the given timestamp.
  ///
  /// Implements cooldown logic to prevent excessive snapshot creation during high-volume log
  /// streaming. If a snapshot was created recently (within `snapshot_creation_interval_micros`),
  /// returns `None` to skip this upload cycle.
  pub(crate) async fn get_or_create_snapshot(
    &self,
    batch_oldest_micros: u64,
  ) -> Option<SnapshotRef> {
    let creator = self.snapshot_creator.as_ref()?;

    let now_micros = {
      let now = self.time_provider.now();
      now.unix_timestamp().cast_unsigned() * 1_000_000 + u64::from(now.microsecond())
    };
    let last_creation = self.last_snapshot_creation_micros.load(Ordering::Relaxed);
    if last_creation > 0
      && now_micros.saturating_sub(last_creation) < self.snapshot_creation_interval_micros
    {
      log::debug!(
        "skipping snapshot creation due to cooldown (last={last_creation}, now={now_micros}, \
         interval={})",
        self.snapshot_creation_interval_micros
      );
      self.stats.snapshots_skipped.inc();
      // Return the most recent existing snapshot instead of creating a new one.
      return self.find_most_recent_snapshot(now_micros);
    }

    log::debug!(
      "no existing snapshot covers log batch (oldest={batch_oldest_micros}), creating new snapshot"
    );

    let Some(snapshot_path) = creator.create_snapshot().await else {
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
      generation: parsed.generation,
      path: snapshot_path,
    })
  }
}

#[async_trait::async_trait]
impl SnapshotCreator for bd_state::Store {
  async fn create_snapshot(&self) -> Option<PathBuf> {
    let path = self.rotate_journal().await?;
    if path.exists() {
      Some(path)
    } else {
      log::debug!(
        "rotate_journal returned path but file does not exist: {}",
        path.display()
      );
      None
    }
  }
}
