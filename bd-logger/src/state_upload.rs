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

#[cfg(test)]
#[path = "./state_upload_test.rs"]
mod tests;

use bd_artifact_upload::Client as ArtifactClient;
use bd_client_common::file::{read_checksummed_data, write_checksummed_data};
use bd_client_common::file_system::FileSystem;
use bd_client_stats_store::{Counter, Scope};
use bd_log_primitives::LogFields;
use bd_resilient_kv::SnapshotFilename;
use bd_state::{RetentionHandle, RetentionRegistry};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use time::OffsetDateTime;
use tokio::sync::RwLock;

/// Directory for storing state upload index.
const STATE_UPLOAD_DIR: &str = "state_upload";

/// Index file name for tracking uploaded snapshots.
const STATE_UPLOAD_INDEX_FILE: &str = "upload_index.bin";

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
/// This correlator coordinates state snapshot uploads with log uploads to ensure the server has
/// the state context needed to hydrate logs. It tracks:
/// - The timestamp of the most recent state change
/// - The timestamp through which state has been uploaded to the server
///
/// The correlator is shared across all buffer uploaders to avoid duplicate uploads.
pub struct StateLogCorrelator {
  /// Timestamp of the most recent state change (microseconds since epoch).
  last_state_change_micros: AtomicU64,

  /// Timestamp through which state has been uploaded to server (microseconds since epoch).
  /// Any logs with timestamps before this value have their state coverage already uploaded.
  state_uploaded_through_micros: AtomicU64,

  /// Timestamp of the last snapshot creation (microseconds since epoch).
  /// Used to implement batching/cooldown between snapshot creations.
  ///
  /// This cooldown is primarily necessary for **log streaming** configurations where potentially
  /// all logs are streamed to the server in rapid succession. Since logs are batched for upload
  /// (e.g., every few seconds), we want to ensure state snapshots are also batched to avoid
  /// creating a new snapshot for every log batch. Without this cooldown, a high-volume streaming
  /// configuration could trigger dozens of snapshot creations per minute, wasting disk I/O and
  /// upload bandwidth.
  last_snapshot_creation_micros: AtomicU64,

  /// Minimum interval between snapshot creations (microseconds).
  /// Prevents excessive snapshot creation on rapid log uploads. See
  /// `last_snapshot_creation_micros` for details on why batching is necessary.
  snapshot_creation_interval_micros: u64,

  /// Path to the state store directory (for finding snapshot files).
  state_store_path: Option<PathBuf>,

  /// Path to the SDK directory (for persisting upload index).
  sdk_directory: PathBuf,

  /// File system for persistence operations.
  file_system: Arc<dyn FileSystem>,

  /// Lock for persisting the upload index.
  persist_lock: RwLock<()>,

  /// Retention handle for preventing snapshot cleanup. Updated as state is uploaded to allow
  /// cleanup of old snapshots that have already been uploaded.
  retention_handle: Option<RetentionHandle>,

  /// Snapshot creator for triggering on-demand snapshot creation before uploads.
  snapshot_creator: Option<Arc<dyn SnapshotCreator>>,

  /// Statistics.
  stats: Stats,
}

impl StateLogCorrelator {
  /// Creates a new correlator.
  ///
  /// # Arguments
  /// * `state_store_path` - Path to the state store directory containing snapshot files
  /// * `sdk_directory` - Path to the SDK directory for persisting the upload index
  /// * `file_system` - File system for persistence operations
  /// * `retention_registry` - Registry for managing snapshot retention to prevent cleanup
  /// * `snapshot_creator` - Optional snapshot creator for triggering on-demand snapshots
  /// * `snapshot_creation_interval_ms` - Minimum interval between snapshot creations (milliseconds)
  /// * `stats_scope` - Stats scope for metrics
  pub async fn new(
    state_store_path: Option<PathBuf>,
    sdk_directory: PathBuf,
    file_system: Arc<dyn FileSystem>,
    retention_registry: Option<Arc<RetentionRegistry>>,
    snapshot_creator: Option<Arc<dyn SnapshotCreator>>,
    snapshot_creation_interval_ms: u32,
    stats_scope: &Scope,
  ) -> Self {
    let stats = Stats::new(&stats_scope.scope("state_upload"));

    let retention_handle = match &retention_registry {
      Some(registry) => Some(registry.create_handle().await),
      None => None,
    };

    let correlator = Self {
      last_state_change_micros: AtomicU64::new(0),
      state_uploaded_through_micros: AtomicU64::new(0),
      last_snapshot_creation_micros: AtomicU64::new(0),
      snapshot_creation_interval_micros: u64::from(snapshot_creation_interval_ms) * 1000,
      state_store_path,
      sdk_directory,
      file_system,
      persist_lock: RwLock::new(()),
      retention_handle,
      snapshot_creator,
      stats,
    };

    correlator.load_persisted_state().await;

    if let Some(handle) = &correlator.retention_handle {
      let uploaded_through = correlator
        .state_uploaded_through_micros
        .load(Ordering::Relaxed);
      if uploaded_through > 0 {
        handle.update_retention_micros(uploaded_through);
      }
    }

    correlator
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

  /// Checks if a state snapshot upload is needed before uploading a batch of logs.
  ///
  /// Returns `Some(SnapshotRef)` if a snapshot should be uploaded before the logs, or `None` if
  /// the server already has sufficient state coverage for the log timestamps.
  ///
  /// # Arguments
  /// * `batch_oldest_micros` - Timestamp of the oldest log in the batch (microseconds)
  /// * `_batch_newest_micros` - Timestamp of the newest log in the batch (microseconds)
  #[must_use]
  pub fn should_upload_state(
    &self,
    batch_oldest_micros: u64,
    _batch_newest_micros: u64,
  ) -> Option<SnapshotRef> {
    let state_uploaded_through = self.state_uploaded_through_micros.load(Ordering::Relaxed);
    let last_state_change = self.last_state_change_micros.load(Ordering::Relaxed);

    // If we've never seen any state changes, no upload needed
    if last_state_change == 0 {
      return None;
    }

    // If we've already uploaded state that covers this batch, no upload needed
    if state_uploaded_through >= batch_oldest_micros {
      self.stats.snapshots_skipped.inc();
      return None;
    }

    // Find the most recent snapshot that covers the batch
    self.find_snapshot_for_timestamp(batch_oldest_micros)
  }

  /// Called after a state snapshot has been successfully uploaded.
  ///
  /// Updates the coverage tracking so future log batches won't trigger redundant uploads.
  pub async fn on_state_uploaded(&self, snapshot_timestamp_micros: u64) {
    self
      .state_uploaded_through_micros
      .fetch_max(snapshot_timestamp_micros, Ordering::Relaxed);
    self.stats.snapshots_uploaded.inc();

    // Update retention handle to allow cleanup of snapshots older than what we've uploaded
    if let Some(handle) = &self.retention_handle {
      handle.update_retention_micros(snapshot_timestamp_micros);
    }

    // Persist the updated coverage
    self.persist_state().await;
  }

  /// Records that an upload attempt failed.
  pub fn on_upload_failed(&self) {
    self.stats.upload_failures.inc();
  }

  /// Uploads a state snapshot if needed before uploading logs.
  ///
  /// This method checks if a state snapshot upload is needed for the given log batch timestamps.
  /// If state has changed since our last upload, it triggers snapshot creation via the
  /// `SnapshotCreator` and then uploads the snapshot via the artifact uploader.
  pub async fn upload_state_if_needed(
    &self,
    batch_oldest_micros: u64,
    batch_newest_micros: u64,
    artifact_client: &dyn ArtifactClient,
    session_id: &str,
  ) {
    let state_uploaded_through = self.state_uploaded_through_micros.load(Ordering::Relaxed);
    let last_state_change = self.last_state_change_micros.load(Ordering::Relaxed);

    // If we've never seen any state changes, no upload needed
    if last_state_change == 0 {
      return;
    }

    // If we've already uploaded state that covers this batch, no upload needed
    if state_uploaded_through >= batch_oldest_micros {
      self.stats.snapshots_skipped.inc();
      return;
    }

    // If there are no pending state changes since our last upload, no upload needed
    if last_state_change <= state_uploaded_through {
      self.stats.snapshots_skipped.inc();
      return;
    }

    // Try to find an existing snapshot or create one
    let Some(snapshot_ref) = self.get_or_create_snapshot(batch_oldest_micros).await else {
      return;
    };

    log::debug!(
      "uploading state snapshot {} for log batch (oldest={}, newest={})",
      snapshot_ref.timestamp_micros,
      batch_oldest_micros,
      batch_newest_micros
    );

    // Open the snapshot file
    let file = match File::open(&snapshot_ref.path) {
      Ok(f) => f,
      Err(e) => {
        log::warn!(
          "failed to open snapshot file {}: {e}",
          snapshot_ref.path.display()
        );
        self.on_upload_failed();
        return;
      },
    };

    // Convert timestamp from microseconds to OffsetDateTime
    let timestamp =
      OffsetDateTime::from_unix_timestamp_nanos(i128::from(snapshot_ref.timestamp_micros) * 1000)
        .ok();

    // Enqueue the upload via artifact uploader
    // skip_intent=true since we want to upload immediately without negotiation
    match artifact_client.enqueue_upload(
      file,
      LogFields::new(),
      timestamp,
      session_id.to_string(),
      vec![],
      "state_snapshot".to_string(),
      true, // skip_intent - upload immediately
    ) {
      Ok(_uuid) => {
        log::debug!(
          "state snapshot upload enqueued for timestamp {}",
          snapshot_ref.timestamp_micros
        );
        // Mark as uploaded - the artifact uploader will handle retries
        self.on_state_uploaded(snapshot_ref.timestamp_micros).await;
      },
      Err(e) => {
        log::warn!("failed to enqueue state snapshot upload: {e}");
        self.on_upload_failed();
      },
    }
  }

  /// Returns the path to the state store directory, if configured.
  #[must_use]
  pub fn state_store_path(&self) -> Option<&Path> {
    self.state_store_path.as_deref()
  }

  /// Finds a snapshot file that covers the given timestamp.
  ///
  /// Looks for .zz snapshot files in the state store's snapshot directory and returns the most
  /// recent one that was created before the given timestamp.
  fn find_snapshot_for_timestamp(&self, timestamp_micros: u64) -> Option<SnapshotRef> {
    let state_path = self.state_store_path.as_ref()?;
    let snapshots_dir = state_path.join("snapshots");

    let entries = std::fs::read_dir(&snapshots_dir).ok()?;

    let mut best_match: Option<SnapshotRef> = None;

    for entry in entries.flatten() {
      let path = entry.path();
      if let Some(filename) = path.file_name().and_then(|f| f.to_str())
        && let Some(parsed) = SnapshotFilename::parse(filename)
        && parsed.timestamp_micros <= timestamp_micros
        && best_match
          .as_ref()
          .is_none_or(|b| parsed.timestamp_micros > b.timestamp_micros)
      {
        best_match = Some(SnapshotRef {
          timestamp_micros: parsed.timestamp_micros,
          generation: parsed.generation,
          path,
        });
      }
    }

    best_match
  }

  /// Finds an existing snapshot or creates a new one for the given timestamp.
  ///
  /// Implements cooldown logic to prevent excessive snapshot creation during high-volume log
  /// streaming. If a snapshot was created recently (within `snapshot_creation_interval_micros`),
  /// returns `None` to skip this upload cycle.
  async fn get_or_create_snapshot(&self, batch_oldest_micros: u64) -> Option<SnapshotRef> {
    if let Some(snapshot) = self.find_snapshot_for_timestamp(batch_oldest_micros) {
      return Some(snapshot);
    }

    let creator = self.snapshot_creator.as_ref()?;

    let now_micros = {
      let now = time::OffsetDateTime::now_utc();
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
      return None;
    }

    log::debug!(
      "no existing snapshot covers log batch (oldest={batch_oldest_micros}), creating new snapshot"
    );

    let Some(snapshot_path) = creator.create_snapshot().await else {
      log::debug!("snapshot creation failed or not supported");
      self.on_upload_failed();
      return None;
    };

    self
      .last_snapshot_creation_micros
      .store(now_micros, Ordering::Relaxed);

    let filename = snapshot_path.file_name().and_then(|f| f.to_str())?;
    let Some(parsed) = SnapshotFilename::parse(filename) else {
      log::debug!("failed to parse snapshot filename: {filename}");
      self.on_upload_failed();
      return None;
    };

    Some(SnapshotRef {
      timestamp_micros: parsed.timestamp_micros,
      generation: parsed.generation,
      path: snapshot_path,
    })
  }

  /// Loads persisted state from disk.
  async fn load_persisted_state(&self) {
    let index_path = self
      .sdk_directory
      .join(STATE_UPLOAD_DIR)
      .join(STATE_UPLOAD_INDEX_FILE);

    let Ok(contents) = self.file_system.read_file(&index_path).await else {
      return;
    };

    let Ok(data) = read_checksummed_data(&contents) else {
      log::debug!("state upload index checksum validation failed, starting fresh");
      return;
    };

    // Simple binary format: just a u64 for state_uploaded_through_micros
    if data.len() >= 8 {
      let uploaded_through = u64::from_le_bytes(data[.. 8].try_into().unwrap_or_default());
      self
        .state_uploaded_through_micros
        .store(uploaded_through, Ordering::Relaxed);
      log::debug!("loaded state upload coverage through {uploaded_through}");
    }
  }

  /// Persists state to disk.
  async fn persist_state(&self) {
    let _lock = self.persist_lock.write().await;

    let dir_path = self.sdk_directory.join(STATE_UPLOAD_DIR);
    let index_path = dir_path.join(STATE_UPLOAD_INDEX_FILE);

    // Ensure directory exists
    if let Err(e) = self.file_system.create_dir(&dir_path).await {
      log::debug!("failed to create state upload directory: {e}");
      return;
    }

    // Simple binary format: just a u64 for state_uploaded_through_micros
    let uploaded_through = self.state_uploaded_through_micros.load(Ordering::Relaxed);
    let data = uploaded_through.to_le_bytes().to_vec();

    let checksummed = write_checksummed_data(&data);

    if let Err(e) = self.file_system.write_file(&index_path, &checksummed).await {
      log::debug!("failed to persist state upload index: {e}");
    }
  }
}

#[async_trait::async_trait]
impl SnapshotCreator for bd_state::Store {
  async fn create_snapshot(&self) -> Option<PathBuf> {
    self.rotate_journal().await
  }
}
