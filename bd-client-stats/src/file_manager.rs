// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_api::upload::TrackedStatsUploadRequest;
use bd_client_common::error::InvariantError;
use bd_client_common::file::{read_compressed_protobuf, write_compressed_protobuf};
use bd_client_common::file_system::FileSystem;
use bd_proto::protos::client::api::StatsUploadRequest;
use bd_proto::protos::client::api::stats_upload_request::Snapshot;
use bd_proto::protos::client::api::stats_upload_request::snapshot::{Aggregated, Occurred_at};
use bd_proto::protos::client::metric::pending_aggregation_index::{
  PendingFile,
  PendingStatsPipelineAnalyticsReport,
};
use bd_proto::protos::client::metric::{PendingAggregationIndex, StatsPipelineAnalytics};
use bd_runtime::runtime::stats::{
  GlobalMinimumUploadIntervalFlag,
  MaxAggregatedFilesFlag,
  MaxAggregationWindowPerFileFlag,
};
use bd_runtime::runtime::{ConfigLoader, Watch};
use bd_time::{OffsetDateTimeExt, TimeProvider, TimestampExt};
use protobuf::Message;
use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::result::Result::Ok;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use time::{Duration, OffsetDateTime};
use tokio::sync::Mutex;
use tokio::time::Instant;

/// Root directory for all files used for storage and uploading.
pub static STATS_DIRECTORY: LazyLock<PathBuf> = LazyLock::new(|| "stats_uploads".into());

/// The index file used for tracking all of the individual files.
pub static PENDING_AGGREGATION_INDEX_FILE: LazyLock<PathBuf> =
  LazyLock::new(|| "pending_aggregation_index.pb".into());

/// Maximum number of persisted snapshots combined into one upload request.
pub const MAX_SNAPSHOTS_PER_UPLOAD: usize = 10;

//
// StatsUploadRequestHandle
//

pub struct StatsUploadRequestHandle {
  index: usize,
  stats_upload_request: StatsUploadRequest,
}

pub struct PendingUpload {
  pub request: StatsUploadRequest,
  pub source_file_ids: Vec<String>,
}

//
// StatsPipelineAnalyticsReport
//

#[derive(Clone, Debug)]
pub struct StatsPipelineAnalyticsReport {
  pub report_id: String,
  pub analytics: StatsPipelineAnalytics,
}

impl StatsUploadRequestHandle {
  pub fn snapshot(&mut self) -> Option<Snapshot> {
    // Disk snapshots are still written as one-snapshot files. Pending upload batching may later
    // combine multiple files into one request, but the writable handle always exposes the single
    // snapshot owned by the file currently being updated.
    if self.stats_upload_request.snapshot.is_empty() {
      None
    } else {
      Some(self.stats_upload_request.snapshot.remove(0))
    }
  }
}

//
// FileManager
//

struct InitializedInner {
  file_system: Arc<dyn FileSystem>,
  index: VecDeque<PendingFile>,
  next_client_stats_sequence: u64,
  in_flight_uploads: HashSet<String>,
  unreported_stats_pipeline_analytics: StatsPipelineAnalytics,
  pending_stats_pipeline_analytics_report: Option<PendingStatsPipelineAnalyticsReport>,
  last_stats_upload_attempt_at: Option<OffsetDateTime>,
  stats_upload_attempt_in_progress: bool,
  outstanding_stats_uploads: usize,
  stats_upload_elapsed: Option<(Instant, Duration)>,
}
enum Inner {
  NotInitialized(Option<Arc<dyn FileSystem>>),
  Initialized(Box<InitializedInner>),
}
pub struct FileManager {
  inner: Mutex<Inner>,
  time_provider: Arc<dyn TimeProvider>,
  started_at: (Instant, OffsetDateTime),
  max_aggregated_files: Watch<u32, MaxAggregatedFilesFlag>,
  max_aggregation_window_per_file: Watch<Duration, MaxAggregationWindowPerFileFlag>,
  global_minimum_upload_interval: Watch<Duration, GlobalMinimumUploadIntervalFlag>,
  upload_history_error_reported: AtomicBool,
}

impl InitializedInner {
  fn analytics_has_values(analytics: &StatsPipelineAnalytics) -> bool {
    analytics != StatsPipelineAnalytics::default_instance()
  }

  fn record_upload_ack(&mut self, success: bool) {
    if success {
      self
        .unreported_stats_pipeline_analytics
        .stats_uploads_acknowledged_successfully += 1;
    } else {
      self
        .unreported_stats_pipeline_analytics
        .stats_uploads_acknowledged_unsuccessfully += 1;
    }
  }

  fn record_rotation_drop(&mut self) {
    self
      .unreported_stats_pipeline_analytics
      .stats_files_dropped_due_to_rotation += 1;
  }

  fn record_active_snapshot_corruption_drop(&mut self) {
    self
      .unreported_stats_pipeline_analytics
      .stats_files_dropped_due_to_active_snapshot_corruption += 1;
  }

  fn record_pending_snapshot_corruption_drop(&mut self) {
    self
      .unreported_stats_pipeline_analytics
      .stats_files_dropped_due_to_pending_snapshot_corruption += 1;
  }

  fn find_index(
    index: &VecDeque<PendingFile>,
    predicate: impl Fn(&PendingFile) -> bool,
  ) -> Option<usize> {
    index.iter().position(predicate)
  }

  fn increment_retry_counts(&mut self, source_file_ids: &[String]) {
    for source_file_id in source_file_ids {
      if let Some(index) = Self::find_index(&self.index, |file| file.name == *source_file_id) {
        self.index[index].retry_count = self.index[index].retry_count.saturating_add(1);
      } else {
        log::debug!("pending upload {source_file_id} not found in index");
      }
    }
  }

  fn release_pending_uploads(&mut self, source_file_ids: &[String]) {
    for source_file_id in source_file_ids {
      self.in_flight_uploads.remove(source_file_id);
    }
  }

  fn finish_stats_upload(&mut self, now: OffsetDateTime) {
    self.last_stats_upload_attempt_at = Some(now);
    self.outstanding_stats_uploads = self.outstanding_stats_uploads.saturating_sub(1);
    self.stats_upload_attempt_in_progress = self.outstanding_stats_uploads != 0;
    self.stats_upload_elapsed = Some((Instant::now(), Duration::ZERO));
  }

  fn allocate_client_stats_sequence(&mut self) -> anyhow::Result<u64> {
    let client_stats_sequence = self.next_client_stats_sequence;
    self.next_client_stats_sequence = client_stats_sequence
      .checked_add(1)
      .ok_or_else(|| anyhow::anyhow!("client stats sequence exhausted"))?;
    Ok(client_stats_sequence)
  }

  // Persist the index back to the filesystem.
  async fn write_index(&self) -> anyhow::Result<()> {
    let index = PendingAggregationIndex {
      pending_files: self.index.iter().cloned().collect(),
      unreported_stats_pipeline_analytics: Some(self.unreported_stats_pipeline_analytics.clone())
        .into(),
      pending_stats_pipeline_analytics_report: self
        .pending_stats_pipeline_analytics_report
        .clone()
        .into(),
      next_client_stats_sequence: self.next_client_stats_sequence,
      last_stats_upload_attempt_at: self
        .last_stats_upload_attempt_at
        .and_then(|timestamp| timestamp.into_proto().into_option())
        .into(),
      stats_upload_attempt_in_progress: self.stats_upload_attempt_in_progress,
      ..Default::default()
    };

    let compressed = write_compressed_protobuf(&index)?;
    let path = STATS_DIRECTORY.join(&*PENDING_AGGREGATION_INDEX_FILE);
    let temporary_path = path.with_extension("tmp");
    self
      .file_system
      .as_ref()
      .write_file(&temporary_path, &compressed)
      .await?;
    self.file_system.rename_file(&temporary_path, &path).await?;

    Ok(())
  }

  async fn delete_snapshot(&mut self, index: usize) -> anyhow::Result<()> {
    self
      .file_system
      .delete_file(&STATS_DIRECTORY.join(&self.index[index].name))
      .await?;
    self.index.remove(index);

    Ok(())
  }

  async fn delete_pending_uploads(&mut self, names: &[String]) -> anyhow::Result<()> {
    let mut remaining_names: HashSet<String> = names.iter().cloned().collect();
    let mut index = 0;

    while index < self.index.len() {
      let file_name = self.index[index].name.clone();
      if remaining_names.remove(&file_name) {
        log::debug!("deleting pending upload: {file_name}");
        self.delete_snapshot(index).await?;
      } else {
        index += 1;
      }
    }

    for name in remaining_names {
      // A completion can race with max-files eviction removing an older in-flight entry before the
      // upload ack arrives. We could teach eviction to preserve in-flight files, but that is more
      // complicated than treating a missing entry here as an already-cleaned-up upload.
      log::debug!("pending upload {name} not found in index");
    }

    self.write_index().await
  }

  fn eligible_pending_upload_ids(
    &self,
    only_if_file_is_old: bool,
    now: time::OffsetDateTime,
    max_aggregation_window_per_file: Duration,
  ) -> Vec<String> {
    let mut eligible = Vec::new();

    for file in &self.index {
      if self.in_flight_uploads.contains(&file.name) {
        continue;
      }

      let is_old = file.period_start.to_offset_date_time() + max_aggregation_window_per_file <= now;
      if only_if_file_is_old {
        if !is_old {
          break;
        }
        eligible.push(file.name.clone());
        if eligible.len() == MAX_SNAPSHOTS_PER_UPLOAD {
          break;
        }
      } else {
        eligible.push(file.name.clone());
        break;
      }
    }

    eligible
  }

  fn file_is_stats_snapshot(file: &str) -> bool {
    let Some(file_name) = Path::new(file).file_name().and_then(|name| name.to_str()) else {
      return false;
    };

    file_name != PENDING_AGGREGATION_INDEX_FILE.as_os_str()
      && !file_name.starts_with("pending_aggregation_index.")
  }
}

impl Inner {
  // Initialize or get an already initialized file manager.
  async fn get_initialized(&mut self) -> anyhow::Result<&mut InitializedInner> {
    // Due to the way logger startup works we delay initializing the index until first use. First we
    // try to load an existing file index. If this doesn't exist or is corrupted we remove the
    // entire directory as there is no reasonable way to manage the contents.
    match self {
      Self::Initialized(inner) => Ok(inner),
      Self::NotInitialized(file_system) => {
        let file_system_ref = file_system.as_ref().ok_or(InvariantError::Invariant)?;
        let path = STATS_DIRECTORY.join(&*PENDING_AGGREGATION_INDEX_FILE);
        log::debug!("initializing pending aggregation index: {}", path.display());
        let stats_directory_existed = file_system_ref.exists(&STATS_DIRECTORY).await?;
        let mut recovered_from_existing_directory = false;
        let index = match file_system_ref
          .read_file(&path)
          .await
          .and_then(|contents| read_compressed_protobuf(&contents))
        {
          Ok(index) => index,
          Err(e) => {
            log::debug!("unable to open pending aggregation index: {e}");
            log::debug!("creating new aggregation index");

            let dropped_snapshot_files = if stats_directory_existed {
              recovered_from_existing_directory = true;
              match file_system_ref.list_files(&STATS_DIRECTORY).await {
                Ok(files) => files
                  .iter()
                  .filter(|file| InitializedInner::file_is_stats_snapshot(file))
                  .count(),
                Err(error) => {
                  log::debug!("unable to list stats files during index recovery: {error}");
                  0
                },
              }
            } else {
              0
            };

            file_system_ref.remove_dir(&STATS_DIRECTORY).await?;
            file_system_ref.create_dir(&STATS_DIRECTORY).await?;

            PendingAggregationIndex {
              unreported_stats_pipeline_analytics: recovered_from_existing_directory
                .then(|| StatsPipelineAnalytics {
                  stats_files_dropped_due_to_index_recovery: dropped_snapshot_files as u64,
                  stats_index_recovery_events: 1,
                  ..Default::default()
                })
                .into(),
              ..Default::default()
            }
          },
        };

        *self = Self::Initialized(Box::new(InitializedInner {
          file_system: file_system.take().ok_or(InvariantError::Invariant)?,
          index: index.pending_files.into(),
          next_client_stats_sequence: index.next_client_stats_sequence.max(1),
          in_flight_uploads: HashSet::new(),
          unreported_stats_pipeline_analytics: index
            .unreported_stats_pipeline_analytics
            .into_option()
            .unwrap_or_default(),
          pending_stats_pipeline_analytics_report: index
            .pending_stats_pipeline_analytics_report
            .into_option(),
          last_stats_upload_attempt_at: index
            .last_stats_upload_attempt_at
            .as_ref()
            .map(TimestampExt::to_offset_date_time),
          stats_upload_attempt_in_progress: index.stats_upload_attempt_in_progress,
          outstanding_stats_uploads: 0,
          stats_upload_elapsed: None,
        }));

        if recovered_from_existing_directory && let Self::Initialized(inner) = self {
          inner.write_index().await?;
        }

        Ok(match self {
          Self::Initialized(inner) => inner,
          Self::NotInitialized(_) => return Err(InvariantError::Invariant.into()),
        })
      },
    }
  }
}

impl FileManager {
  pub fn new(
    file_system: Box<dyn FileSystem>,
    time_provider: Arc<dyn TimeProvider>,
    runtime_loader: &ConfigLoader,
  ) -> Self {
    let started_at = (Instant::now(), time_provider.now());
    Self {
      inner: Mutex::new(Inner::NotInitialized(Some(Arc::from(file_system)))),
      time_provider,
      started_at,
      max_aggregated_files: runtime_loader.register_int_watch(),
      max_aggregation_window_per_file: runtime_loader.register_duration_watch(),
      global_minimum_upload_interval: runtime_loader.register_duration_watch(),
      upload_history_error_reported: AtomicBool::new(false),
    }
  }

  // Hold the reservation until ACK or tracker closure so queued requests cannot accumulate and
  // later be sent together, even after their preparation interval has elapsed.
  pub async fn prepare_stats_upload(&self, source_file_ids: &[String]) -> bool {
    match self.try_begin_stats_upload_attempt(source_file_ids).await {
      Ok(allowed) => allowed,
      Err(error) => {
        self.report_upload_history_error(&error);
        true
      },
    }
  }

  pub async fn finish_stats_upload(&self) {
    self.observe_upload_history_result(&self.finish_stats_upload_attempt().await);
  }

  fn observe_upload_history_result(&self, result: &anyhow::Result<()>) {
    match result {
      Ok(()) => self
        .upload_history_error_reported
        .store(false, Ordering::Relaxed),
      Err(error) => self.report_upload_history_error(error),
    }
  }

  fn report_upload_history_error(&self, error: &anyhow::Error) {
    if !self
      .upload_history_error_reported
      .swap(true, Ordering::Relaxed)
    {
      log::warn!("stats upload history persistence failed; allowing uploads: {error}");
    }
  }

  /// Reserves an upload and persists its history before exposing the payload.
  pub async fn try_begin_stats_upload_attempt(
    &self,
    source_file_ids: &[String],
  ) -> anyhow::Result<bool> {
    let mut inner = self.inner.lock().await;
    let initialized = inner.get_initialized().await?;
    let now = self.time_provider.now();
    let mut recovery_error = None;
    let mut recovered_unresolved = false;

    // Credit downtime from construction, then use monotonic elapsed time, including before the
    // first candidate. Unresolved persisted reservations recover with a full cooldown.
    if initialized.stats_upload_elapsed.is_none() {
      if initialized.stats_upload_attempt_in_progress {
        initialized.last_stats_upload_attempt_at = Some(now);
        initialized.stats_upload_attempt_in_progress = false;
        initialized.stats_upload_elapsed = Some((Instant::now(), Duration::ZERO));
        recovered_unresolved = true;
        log::debug!("recovered unresolved stats upload attempt with a full cooldown");
      } else if let Some(last_attempt) = initialized.last_stats_upload_attempt_at {
        initialized.stats_upload_elapsed = Some((
          self.started_at.0,
          (self.started_at.1 - last_attempt).max(Duration::ZERO),
        ));
      }
    }

    if self.stats_upload_is_throttled(initialized) {
      if !recovered_unresolved {
        return Ok(false);
      }
      match initialized.write_index().await {
        Ok(()) => return Ok(false),
        Err(error) => recovery_error = Some(error),
      }
    }

    initialized.last_stats_upload_attempt_at = Some(now);
    initialized.stats_upload_attempt_in_progress = true;
    initialized.outstanding_stats_uploads += 1;
    initialized.stats_upload_elapsed = Some((Instant::now(), Duration::ZERO));
    // Count at preparation, even when pacing is disabled, to share the reservation's index write.
    // A closed API channel, or SDK shutdown/process death while waiting for channel capacity, can
    // leave this attempt counted without the request ever being handed to the API or sent.
    initialized.increment_retry_counts(source_file_ids);
    // A recovery write failure still exposes a fail-open payload, so reserve and count it before
    // returning the error, just as for an admission write failure.
    if let Some(error) = recovery_error {
      return Err(error);
    }
    initialized.write_index().await?;
    Ok(true)
  }

  /// Starts the next cooldown after an ACK or response-channel closure.
  pub async fn finish_stats_upload_attempt(&self) -> anyhow::Result<()> {
    let mut inner = self.inner.lock().await;
    let initialized = inner.get_initialized().await?;
    initialized.finish_stats_upload(self.time_provider.now());
    initialized.write_index().await
  }

  fn stats_upload_is_throttled(&self, initialized: &InitializedInner) -> bool {
    let minimum_interval = *self.global_minimum_upload_interval.read();
    if minimum_interval <= Duration::ZERO {
      return false;
    }
    // An unresolved restart marker must reach admission once so it can be recovered and persisted.
    if initialized.stats_upload_elapsed.is_none() && initialized.stats_upload_attempt_in_progress {
      return false;
    }
    let elapsed = initialized
      .stats_upload_elapsed
      .or_else(|| {
        initialized
          .last_stats_upload_attempt_at
          .map(|last_attempt| {
            (
              self.started_at.0,
              (self.started_at.1 - last_attempt).max(Duration::ZERO),
            )
          })
      })
      .map(|(reference, credit)| {
        credit.saturating_add(reference.elapsed().try_into().unwrap_or(Duration::MAX))
      });
    let throttled = initialized.stats_upload_attempt_in_progress
      || elapsed.is_some_and(|elapsed| elapsed < minimum_interval);
    if throttled {
      log::debug!(
        "throttling stats upload: outstanding={}, elapsed={elapsed:?}, minimum={minimum_interval}",
        initialized.outstanding_stats_uploads
      );
    }
    throttled
  }

  /// Completes a reservation and its source files with one index write. None means no ACK arrived.
  pub async fn complete_stats_upload(
    &self,
    source_file_ids: &[String],
    success: Option<bool>,
  ) -> anyhow::Result<()> {
    let mut inner = self.inner.lock().await;
    let initialized = inner.get_initialized().await?;
    initialized.finish_stats_upload(self.time_provider.now());
    initialized.release_pending_uploads(source_file_ids);
    if let Some(success) = success {
      initialized.record_upload_ack(success);
    }
    let result = if success == Some(true) {
      initialized.delete_pending_uploads(source_file_ids).await
    } else {
      initialized.write_index().await
    };
    self.observe_upload_history_result(&result);
    result
  }

  // Read an existing snapshot from disk to merge into, or create a new one.
  pub async fn get_or_create_snapshot(&self) -> anyhow::Result<StatsUploadRequestHandle> {
    let mut inner = self.inner.lock().await;
    let initialized_inner = inner.get_initialized().await?;

    let create_new_snapshot = initialized_inner.index.back_mut().is_none_or(|file| {
      if file.period_end.is_some() {
        log::debug!("snapshot is ready to upload, creating new snapshot");
        true
      } else if file.period_start.to_offset_date_time()
        + *self.max_aggregation_window_per_file.read()
        <= self.time_provider.now()
      {
        log::debug!("snapshot is too old, creating new snapshot");
        file.period_end = self.time_provider.now().into_proto();
        true
      } else {
        false
      }
    });

    if create_new_snapshot {
      if *self.max_aggregated_files.read() <= u32::try_from(initialized_inner.index.len())? {
        log::debug!("max files reached, popping oldest snapshot");
        initialized_inner.delete_snapshot(0).await?;
        initialized_inner.record_rotation_drop();
      }

      let pending_file = PendingFile {
        name: TrackedStatsUploadRequest::upload_uuid(),
        period_start: self.time_provider.now().into_proto(),
        client_stats_sequence: initialized_inner.allocate_client_stats_sequence()?,
        ..Default::default()
      };
      log::debug!("creating new snapshot in index: {}", pending_file.name);
      initialized_inner.index.push_back(pending_file);
      initialized_inner.write_index().await?;
    }

    // Read the file back or make a new one. We don't count an error reading the file or file
    // corruption as a fatal error.
    let path = STATS_DIRECTORY.join(
      &initialized_inner
        .index
        .back()
        .ok_or(InvariantError::Invariant)?
        .name,
    );
    let stats_upload_request = if create_new_snapshot {
      None
    } else {
      match initialized_inner
        .file_system
        .read_file(&path)
        .await
        .and_then(|contents| read_compressed_protobuf::<StatsUploadRequest>(&contents))
      {
        Ok(request) => Some(request),
        Err(error) => {
          log::debug!(
            "unable to read snapshot {}, creating default: {error}",
            path.display()
          );
          initialized_inner.record_active_snapshot_corruption_drop();
          initialized_inner.write_index().await?;
          None
        },
      }
    }
    .map_or_else(
      || {
        Ok(StatsUploadRequest {
          upload_uuid: initialized_inner
            .index
            .back()
            .ok_or(InvariantError::Invariant)?
            .name
            .clone(),
          ..Default::default()
        })
      },
      Ok::<_, anyhow::Error>,
    )?;

    Ok(StatsUploadRequestHandle {
      index: initialized_inner.index.len() - 1,
      stats_upload_request,
    })
  }

  // Called if a merge results in no metrics. The file is removed from the index as it is never
  // written to avoid empty uploads.
  pub async fn remove_empty_snapshot(&self) -> anyhow::Result<()> {
    let mut inner = self.inner.lock().await;
    let initialized_inner = inner.get_initialized().await?;

    debug_assert!(
      !initialized_inner
        .index
        .back()
        .ok_or(InvariantError::Invariant)?
        .period_end
        .is_some()
    );
    log::debug!(
      "removing empty snapshot from index: {}",
      initialized_inner
        .index
        .back()
        .ok_or(InvariantError::Invariant)?
        .name
    );
    initialized_inner.index.pop_back();
    initialized_inner.write_index().await?;

    Ok(())
  }

  // Write the snapshot returned from `get_or_create_snapshot` back to disk.
  pub async fn write_snapshot(
    &self,
    mut handle: StatsUploadRequestHandle,
    snapshot: Snapshot,
  ) -> anyhow::Result<()> {
    let mut inner = self.inner.lock().await;
    let initialized_inner = inner.get_initialized().await?;

    handle.stats_upload_request.snapshot = vec![snapshot];
    let path = STATS_DIRECTORY.join(&initialized_inner.index[handle.index].name);
    log::debug!("writing snapshot: {}", path.display());
    let compressed = write_compressed_protobuf(&handle.stats_upload_request)?;

    initialized_inner
      .file_system
      .write_file(&path, &compressed)
      .await
  }

  // Determine whether there is a pending upload ready to go. If so read it back.
  pub async fn get_or_create_pending_upload(
    &self,
    only_if_file_is_old: bool,
  ) -> anyhow::Result<Option<PendingUpload>> {
    let now = self.time_provider.now();
    let max_aggregation_window_per_file = *self.max_aggregation_window_per_file.read();

    loop {
      let mut inner = self.inner.lock().await;
      let initialized_inner = inner.get_initialized().await?;

      // Avoid sealing, reading, and decompressing snapshots that cannot be admitted. Admission
      // checks again after selection because another producer may reserve while disk reads run.
      if self.stats_upload_is_throttled(initialized_inner) {
        return Ok(None);
      }

      if initialized_inner.index.is_empty() {
        log::debug!("no pending upload: index is empty");
        return Ok(None);
      }

      let eligible_file_ids = initialized_inner.eligible_pending_upload_ids(
        only_if_file_is_old,
        now,
        max_aggregation_window_per_file,
      );

      let Some(first_file_id) = eligible_file_ids.first() else {
        if only_if_file_is_old {
          log::debug!("no pending upload: file is not old enough");
        } else {
          log::debug!("no pending upload: all files are in flight");
        }
        return Ok(None);
      };

      debug_assert!(
        initialized_inner
          .index
          .iter()
          .any(|file| &file.name == first_file_id)
      );

      let mut should_write_index = false;
      let file_system = initialized_inner.file_system.clone();
      let mut pending_files = Vec::new();

      for file_id in &eligible_file_ids {
        let Some(index) =
          InitializedInner::find_index(&initialized_inner.index, |file| file.name == *file_id)
        else {
          continue;
        };

        // Mark the selected files in flight under the lock before we release it for disk reads.
        if initialized_inner.index[index].period_end.is_none() {
          log::debug!(
            "marking entry as ready to upload: {}",
            initialized_inner.index[index].name
          );
          initialized_inner.index[index].period_end = now.into_proto();
          should_write_index = true;
        }

        initialized_inner
          .in_flight_uploads
          .insert(initialized_inner.index[index].name.clone());
        pending_files.push(initialized_inner.index[index].clone());
      }

      if should_write_index {
        initialized_inner.write_index().await?;
      }
      drop(inner);

      let mut pending_request = StatsUploadRequest::default();
      let mut source_file_ids = Vec::new();
      let mut bad_file_ids = Vec::new();

      for pending_file in pending_files {
        let path = STATS_DIRECTORY.join(&pending_file.name);
        match file_system
          .read_file(&path)
          .await
          .and_then(|contents| read_compressed_protobuf::<StatsUploadRequest>(&contents))
        {
          Ok(mut request_from_disk) => {
            // Each pending file still contains one snapshot on disk. Batch uploads preserve those
            // per-file aggregation windows by carrying each snapshot forward separately.
            debug_assert_eq!(1, request_from_disk.snapshot.len());
            if let Some(snapshot) = request_from_disk.snapshot.first_mut() {
              snapshot.occurred_at = Some(Occurred_at::Aggregated(Aggregated {
                period_start: pending_file.period_start.clone(),
                period_end: pending_file.period_end.clone(),
                ..Default::default()
              }));
              snapshot.retry_count = pending_file.retry_count;
              snapshot.client_stats_sequence = pending_file.client_stats_sequence;
            }

            pending_request.snapshot.extend(request_from_disk.snapshot);
            source_file_ids.push(pending_file.name);
          },
          Err(e) => {
            // We failed to read the data, so the file must be bad. This could happen if we change
            // the schema in an incompatible way or if the file is corrupt. Delete the file and
            // accept the loss of this upload.
            log::debug!("unable to read pending upload {}: {e}", path.display());
            bad_file_ids.push(pending_file.name);
          },
        }
      }

      if !bad_file_ids.is_empty() {
        let mut inner = self.inner.lock().await;
        let initialized_inner = inner.get_initialized().await?;
        for file_id in &bad_file_ids {
          initialized_inner.in_flight_uploads.remove(file_id);
          initialized_inner.record_pending_snapshot_corruption_drop();
        }
        initialized_inner
          .delete_pending_uploads(&bad_file_ids)
          .await?;
      }

      if source_file_ids.is_empty() {
        continue;
      }

      return Ok(Some(PendingUpload {
        request: pending_request,
        source_file_ids,
      }));
    }
  }

  // Called when a pending upload returned from `get_or_create_pending_upload` is successfully
  // uploaded
  pub async fn complete_pending_upload(
    &self,
    source_file_ids: &[String],
    success: bool,
  ) -> anyhow::Result<()> {
    // We should always have an entry to complete if this code runs.
    let mut inner = self.inner.lock().await;
    let initialized_inner = inner.get_initialized().await?;

    initialized_inner.record_upload_ack(success);

    if !success {
      log::debug!("not completing pending upload batch {source_file_ids:?} due to failure");
      initialized_inner.release_pending_uploads(source_file_ids);
      initialized_inner.write_index().await?;
      return Ok(());
    }

    initialized_inner.release_pending_uploads(source_file_ids);

    for uuid in source_file_ids {
      if let Some(index) =
        InitializedInner::find_index(&initialized_inner.index, |file| file.name == *uuid)
      {
        debug_assert!(initialized_inner.index[index].period_end.is_some());
      }
    }

    initialized_inner
      .delete_pending_uploads(source_file_ids)
      .await
  }

  // Releases a claimed upload that was never handed off to the transport.
  pub async fn release_pending_upload(&self, source_file_ids: &[String]) -> anyhow::Result<()> {
    let mut inner = self.inner.lock().await;
    let initialized_inner = inner.get_initialized().await?;

    initialized_inner.release_pending_uploads(source_file_ids);

    Ok(())
  }

  pub async fn prepare_stats_pipeline_analytics_report(
    &self,
  ) -> anyhow::Result<Option<StatsPipelineAnalyticsReport>> {
    let mut inner = self.inner.lock().await;
    let initialized_inner = inner.get_initialized().await?;

    if let Some(report) = &initialized_inner.pending_stats_pipeline_analytics_report {
      return Ok(Some(StatsPipelineAnalyticsReport {
        report_id: report.report_id.clone(),
        analytics: report.analytics.clone().unwrap_or_default(),
      }));
    }

    if !InitializedInner::analytics_has_values(
      &initialized_inner.unreported_stats_pipeline_analytics,
    ) {
      return Ok(None);
    }

    let report = PendingStatsPipelineAnalyticsReport {
      report_id: TrackedStatsUploadRequest::upload_uuid(),
      analytics: Some(std::mem::take(
        &mut initialized_inner.unreported_stats_pipeline_analytics,
      ))
      .into(),
      ..Default::default()
    };
    let prepared_report = StatsPipelineAnalyticsReport {
      report_id: report.report_id.clone(),
      analytics: report.analytics.clone().unwrap_or_default(),
    };
    initialized_inner.pending_stats_pipeline_analytics_report = Some(report);
    initialized_inner.write_index().await?;

    Ok(Some(prepared_report))
  }

  pub async fn acknowledge_stats_pipeline_analytics_report(
    &self,
    report_id: &str,
  ) -> anyhow::Result<()> {
    let mut inner = self.inner.lock().await;
    let initialized_inner = inner.get_initialized().await?;

    if initialized_inner
      .pending_stats_pipeline_analytics_report
      .as_ref()
      .is_some_and(|report| report.report_id == report_id)
    {
      initialized_inner.pending_stats_pipeline_analytics_report = None;
      initialized_inner.write_index().await?;
    }

    Ok(())
  }
}
