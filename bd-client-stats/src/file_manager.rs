// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use async_trait::async_trait;
use bd_api::upload::TrackedStatsUploadRequest;
use bd_client_common::file::{read_compressed_protobuf, write_compressed_protobuf};
use bd_proto::protos::client::api::StatsUploadRequest;
use bd_proto::protos::client::api::stats_upload_request::Snapshot;
use bd_proto::protos::client::api::stats_upload_request::snapshot::{Aggregated, Occurred_at};
use bd_proto::protos::client::metric::PendingAggregationIndex;
use bd_proto::protos::client::metric::pending_aggregation_index::PendingFile;
use bd_runtime::runtime::stats::{MaxAggregatedFilesFlag, MaxAggregationWindowPerFileFlag};
use bd_runtime::runtime::{ConfigLoader, Watch};
use bd_time::{OffsetDateTimeExt, TimeProvider, TimestampExt};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use time::Duration;
use tokio::sync::Mutex;

/// Root directory for all files used for storage and uploading.
pub static STATS_DIRECTORY: LazyLock<PathBuf> = LazyLock::new(|| "stats_uploads".into());

/// The index file used for tracking all of the individual files.
pub static PENDING_AGGREGATION_INDEX_FILE: LazyLock<PathBuf> =
  LazyLock::new(|| "pending_aggregation_index.pb".into());

//
// FileSystem
//

/// A filesystem scoped to the SDK directory. This allows for mocking and abstracting away the
/// relative root of the data directory used by the SDK.
#[async_trait]
pub trait FileSystem: Send + Sync {
  async fn read_file(&self, path: &Path) -> anyhow::Result<Vec<u8>>;
  async fn write_file(&self, path: &Path, data: &[u8]) -> anyhow::Result<()>;
  async fn delete_file(&self, path: &Path) -> anyhow::Result<()>;
  async fn remove_dir(&self, path: &Path) -> anyhow::Result<()>;
  async fn create_dir(&self, path: &Path) -> anyhow::Result<()>;
}

//
// RealFileSystem
//

/// The real filesystem implementation which delegates to `tokio::fs`, joining the relative paths
/// provided in the calls with the SDK directory.
pub struct RealFileSystem {
  directory: PathBuf,
}

impl RealFileSystem {
  #[must_use]
  pub const fn new(directory: PathBuf) -> Self {
    Self { directory }
  }
}

#[async_trait]
impl FileSystem for RealFileSystem {
  async fn read_file(&self, path: &Path) -> anyhow::Result<Vec<u8>> {
    Ok(tokio::fs::read(self.directory.join(path)).await?)
  }

  async fn write_file(&self, path: &Path, data: &[u8]) -> anyhow::Result<()> {
    Ok(tokio::fs::write(self.directory.join(path), data).await?)
  }

  async fn delete_file(&self, path: &Path) -> anyhow::Result<()> {
    // There are edge cases where the index might have been written but the file was not. We don't
    // fail if the file is not found.
    match tokio::fs::remove_file(self.directory.join(path)).await {
      Ok(()) => Ok(()),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
      Err(e) => Err(e.into()),
    }
  }

  async fn remove_dir(&self, path: &Path) -> anyhow::Result<()> {
    match tokio::fs::remove_dir_all(self.directory.join(path)).await {
      Ok(()) => Ok(()),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
      Err(e) => Err(e.into()),
    }
  }

  async fn create_dir(&self, path: &Path) -> anyhow::Result<()> {
    Ok(tokio::fs::create_dir(self.directory.join(path)).await?)
  }
}

//
// StatsUploadRequestHandle
//

pub struct StatsUploadRequestHandle {
  index: usize,
  stats_upload_request: StatsUploadRequest,
}

impl StatsUploadRequestHandle {
  pub fn snapshot(&mut self) -> Option<Snapshot> {
    // TODO(mattklein123): Currently we only support a single snapshot per upload. We could now
    // support multiple per upload, but we would need to handle merging into a single request at
    // pending upload time. We can consider this as a follow up.
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
  file_system: Box<dyn FileSystem>,
  index: VecDeque<PendingFile>,
}
enum Inner {
  NotInitialized(Option<Box<dyn FileSystem>>),
  Initialized(InitializedInner),
}
pub struct FileManager {
  inner: Mutex<Inner>,
  time_provider: Arc<dyn TimeProvider>,
  max_aggregated_files: Watch<u32, MaxAggregatedFilesFlag>,
  max_aggregation_window_per_file: Watch<Duration, MaxAggregationWindowPerFileFlag>,
}

impl InitializedInner {
  // Persist the index back to the filesystem.
  async fn write_index(&self) -> anyhow::Result<()> {
    let index = PendingAggregationIndex {
      pending_files: self.index.iter().cloned().collect(),
      ..Default::default()
    };

    let compressed = write_compressed_protobuf(&index);
    self
      .file_system
      .as_ref()
      .write_file(
        &STATS_DIRECTORY.join(&*PENDING_AGGREGATION_INDEX_FILE),
        &compressed,
      )
      .await?;

    Ok(())
  }

  async fn delete_oldest_snapshot(&mut self) -> anyhow::Result<()> {
    self
      .file_system
      .delete_file(&STATS_DIRECTORY.join(&self.index[0].name))
      .await?;
    self.index.pop_front();

    Ok(())
  }

  async fn delete_pending_upload(&mut self) -> anyhow::Result<()> {
    log::debug!("deleting pending upload: {}", self.index[0].name);
    self.delete_oldest_snapshot().await?;
    self.write_index().await?;

    Ok(())
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
        let file_system = file_system.take().unwrap();
        let path = STATS_DIRECTORY.join(&*PENDING_AGGREGATION_INDEX_FILE);
        log::debug!("initializing pending aggregation index: {}", path.display());
        let index = match file_system
          .read_file(&path)
          .await
          .and_then(|contents| read_compressed_protobuf(&contents))
        {
          Ok(index) => index,
          Err(e) => {
            log::debug!("unable to open pending aggregation index: {e}");
            log::debug!("creating new aggregation index");

            file_system.remove_dir(&STATS_DIRECTORY).await?;
            file_system.create_dir(&STATS_DIRECTORY).await?;
            PendingAggregationIndex::default()
          },
        };

        *self = Self::Initialized(InitializedInner {
          file_system,
          index: index.pending_files.into(),
        });

        Ok(match self {
          Self::Initialized(inner) => inner,
          Self::NotInitialized(_) => unreachable!(),
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
  ) -> anyhow::Result<Self> {
    Ok(Self {
      inner: Mutex::new(Inner::NotInitialized(Some(file_system))),
      time_provider,
      max_aggregated_files: runtime_loader.register_watch()?,
      max_aggregation_window_per_file: runtime_loader.register_watch()?,
    })
  }

  // Read an existing snapshot from disk to merge into, or create a new one.
  pub async fn get_or_create_snapshot(&self) -> anyhow::Result<StatsUploadRequestHandle> {
    let mut inner = self.inner.lock().await;
    let initialized_inner = inner.get_initialized().await?;

    let create_new_snapshot = initialized_inner.index.back_mut().map_or(true, |file| {
      if file.period_end.is_some() {
        log::debug!("snapshot is ready to upload, creating new snapshot");
        true
      } else if file.period_start.to_offset_date_time()
        + self.max_aggregation_window_per_file.read()
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
      if self.max_aggregated_files.read() <= u32::try_from(initialized_inner.index.len()).unwrap() {
        log::debug!("max files reached, popping oldest snapshot");
        initialized_inner.delete_oldest_snapshot().await?;
      }

      let pending_file = PendingFile {
        name: TrackedStatsUploadRequest::upload_uuid(),
        period_start: self.time_provider.now().into_proto(),
        ..Default::default()
      };
      log::debug!("creating new snapshot in index: {}", pending_file.name);
      initialized_inner.index.push_back(pending_file);
      initialized_inner.write_index().await?;
    }

    // Read the file back or make a new one. We don't count an error reading the file or file
    // corruption as a fatal error.
    let path = STATS_DIRECTORY.join(&initialized_inner.index.back().unwrap().name);
    let stats_upload_request = if create_new_snapshot {
      None
    } else {
      initialized_inner
        .file_system
        .read_file(&path)
        .await
        .and_then(|contents| read_compressed_protobuf::<StatsUploadRequest>(&contents))
        .inspect_err(|e| {
          log::debug!(
            "unable to read snapshot {}, creating default: {e}",
            path.display()
          );
        })
        .ok()
    }
    .unwrap_or_else(|| StatsUploadRequest {
      upload_uuid: initialized_inner.index.back().unwrap().name.clone(),
      ..Default::default()
    });

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

    debug_assert!(!initialized_inner.index.back().unwrap().period_end.is_some());
    log::debug!(
      "removing empty snapshot from index: {}",
      initialized_inner.index.back().unwrap().name
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
    let compressed = write_compressed_protobuf(&handle.stats_upload_request);

    initialized_inner
      .file_system
      .write_file(&path, &compressed)
      .await
  }

  // Determine whether there is a pending upload ready to go. If so read it back.
  pub async fn get_or_create_pending_upload(
    &self,
    only_if_file_is_old: bool,
  ) -> anyhow::Result<Option<StatsUploadRequest>> {
    let mut inner = self.inner.lock().await;
    let initialized_inner = inner.get_initialized().await?;

    loop {
      if initialized_inner.index.is_empty() {
        log::debug!("no pending upload: index is empty");
        return Ok(None);
      };

      if only_if_file_is_old
        && initialized_inner.index[0]
          .period_start
          .to_offset_date_time()
          + self.max_aggregation_window_per_file.read()
          > self.time_provider.now()
      {
        log::debug!("no pending upload: file is not old enough");
        return Ok(None);
      }

      // If there is a pending upload, first attempt to re-upload. Otherwise, mark the first entry
      // as ready to upload and return it.
      if initialized_inner.index[0].period_end.is_none() {
        log::debug!(
          "marking entry as ready to upload: {}",
          initialized_inner.index[0].name
        );
        initialized_inner.index[0].period_end = self.time_provider.now().into_proto();
        initialized_inner.write_index().await?;
      }

      let path = STATS_DIRECTORY.join(&initialized_inner.index[0].name);

      match initialized_inner
        .file_system
        .read_file(&path)
        .await
        .and_then(|contents| read_compressed_protobuf::<StatsUploadRequest>(&contents))
      {
        Ok(mut pending_request) => {
          // At the time of creation period_end was not known so we set both start and end here.
          // In the future if we support multiple snapshots per upload we would need to handle that
          // here as well.
          debug_assert_eq!(1, pending_request.snapshot.len());
          if !pending_request.snapshot.is_empty() {
            pending_request.snapshot[0].occurred_at = Some(Occurred_at::Aggregated(Aggregated {
              period_start: initialized_inner.index[0].period_start.clone(),
              period_end: initialized_inner.index[0].period_end.clone(),
              ..Default::default()
            }));
          }

          return Ok(Some(pending_request));
        },
        Err(e) => {
          // We failed to read the data, so the file must be bad. This could happen if we change
          // the schema in an incompatible way or if the file is corrupt. Delete the file and
          // accept the loss of this upload.
          log::debug!("unable to read pending upload {}: {e}", path.display());
          initialized_inner.delete_pending_upload().await?;
        },
      }
    }
  }

  // Called when a pending upload returned from `get_or_create_pending_upload` is successfully
  // uploaded
  pub async fn complete_pending_upload(&self, uuid: &str) -> anyhow::Result<()> {
    // We should always have an entry to complete if this code runs.
    let mut inner = self.inner.lock().await;
    let initialized_inner = inner.get_initialized().await?;

    if initialized_inner.index[0].name != uuid {
      // There is a race condition in which we could theoretically have reached max files, but
      // there is an upload in flight that comes back after we already popped the first entry.
      // We could handle this by having the max file code not pop inflight uploads, but that is
      // more complicated than just ignoring the response here.
      log::debug!(
        "pending upload {} does not match expected {}",
        initialized_inner.index[0].name,
        uuid
      );
      return Ok(());
    }

    log::debug!(
      "completing pending upload: {}",
      initialized_inner.index[0].name
    );
    debug_assert!(initialized_inner.index[0].period_end.is_some());
    initialized_inner.delete_pending_upload().await?;
    Ok(())
  }
}
