// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./stats_test.rs"]
mod stats_test;

use crate::{FlushTriggerCompletionSender, Stats};
use anyhow::Context;
use async_trait::async_trait;
use bd_api::upload::TrackedStatsUploadRequest;
use bd_api::DataUpload;
use bd_client_common::error::handle_unexpected;
use bd_client_stats_store::{BoundedCollector, Histogram, MetricData};
use bd_proto::protos::client::api::stats_upload_request::snapshot::{
  Aggregated,
  Occurred_at,
  Snapshot_type,
};
use bd_proto::protos::client::api::stats_upload_request::Snapshot as StatsSnapshot;
use bd_proto::protos::client::api::StatsUploadRequest;
use bd_proto::protos::client::metric::{Metric as ProtoMetric, MetricsList};
use bd_runtime::runtime::stats::{DirectStatFlushIntervalFlag, UploadStatFlushIntervalFlag};
use bd_runtime::runtime::Watch;
use bd_shutdown::ComponentShutdown;
use bd_stats_common::Id;
use bd_time::{OffsetDateTimeExt, TimeProvider, TimestampExt};
use flate2::read::{ZlibDecoder, ZlibEncoder};
use flate2::Compression;
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;

/// The file to aggregate stats to during each flush interval.
static AGGREGATED_STATS_FILE: LazyLock<PathBuf> = LazyLock::new(|| "aggregated_stats.pb".into());

/// The file to write each upload request to, providing persistence until the upload has been
/// completed successfully.
static PENDING_STATS_UPLOAD_FILE: LazyLock<PathBuf> =
  LazyLock::new(|| "pending_stats_upload.pb".into());

//
// SerializedFileSystem
//

/// A filesystem scoped to the SDK directory. This allows for mocking and abstracting away the
/// relative root of the data directory used by the SDK. All operations are serialized to avoid
/// concurrent read/writes. This works for the current usage but may need to be relaxed in the
/// future.
#[async_trait]
pub trait SerializedFileSystem: Sync {
  async fn read_file(&self, path: impl AsRef<Path> + Send) -> anyhow::Result<Vec<u8>>;
  async fn write_file(
    &self,
    path: impl AsRef<Path> + Send,
    data: impl AsRef<[u8]> + Send,
  ) -> anyhow::Result<()>;
  async fn delete_file(&self, path: impl AsRef<Path> + Send) -> anyhow::Result<()>;
  async fn exists(&self, path: impl AsRef<Path> + Send) -> bool;

  async fn write_compressed_protobuf_file<T: protobuf::Message>(
    &self,
    path: impl AsRef<Path> + Send,
    message: &T,
  ) -> anyhow::Result<()> {
    let bytes = message.write_to_bytes().unwrap();
    let mut encoder = ZlibEncoder::new(&bytes[..], Compression::new(5));
    let mut compressed_bytes = Vec::new();
    encoder.read_to_end(&mut compressed_bytes).unwrap();
    self.write_file(path, compressed_bytes).await?;
    Ok(())
  }

  async fn read_compressed_protobuf_file<T: protobuf::Message>(
    &self,
    path: impl AsRef<Path> + Send,
  ) -> anyhow::Result<T> {
    // The files are likely not large enough to deal with streaming decompression on top of flate2.
    // For now we just read the entire thing and then decompress it in memory. We can consider
    // streaming later.
    let compressed_bytes = self.read_file(path).await?;
    let mut decoder = ZlibDecoder::new(&compressed_bytes[..]);
    // In the future if/when we switch to using Bytes/Chars in the compiled proto it may be more
    // efficient to read out the uncompressed into Bytes and then parse from that.
    Ok(T::parse_from_reader(&mut decoder)?)
  }
}

//
// RealSerializedFileSystem
//

/// The real filesystem implementation which delegates to `tokio::fs`, joining the relative paths
/// provided in the calls with the SDK directory.
pub struct RealSerializedFileSystem {
  directory: PathBuf,
  semaphore: Semaphore,
}

impl RealSerializedFileSystem {
  #[must_use]
  pub fn new(directory: PathBuf) -> Self {
    Self {
      directory,
      semaphore: Semaphore::new(1),
    }
  }
}

#[async_trait]
impl SerializedFileSystem for RealSerializedFileSystem {
  async fn read_file(&self, path: impl AsRef<Path> + Send) -> anyhow::Result<Vec<u8>> {
    let _permit = self.semaphore.acquire().await;
    Ok(tokio::fs::read(self.directory.join(path)).await?)
  }

  async fn write_file(
    &self,
    path: impl AsRef<Path> + Send,
    data: impl AsRef<[u8]> + Send,
  ) -> anyhow::Result<()> {
    let _permit = self.semaphore.acquire().await;
    Ok(tokio::fs::write(self.directory.join(path), data).await?)
  }

  async fn delete_file(&self, path: impl AsRef<Path> + Send) -> anyhow::Result<()> {
    let _permit = self.semaphore.acquire().await;
    Ok(tokio::fs::remove_file(self.directory.join(path)).await?)
  }

  async fn exists(&self, path: impl AsRef<Path> + Send) -> bool {
    let _permit = self.semaphore.acquire().await;
    tokio::fs::metadata(self.directory.join(path)).await.is_ok()
  }
}

//
// StatsUploader
//

/// Responsible for periodically preparing an upload request from the aggregated stats file to
/// a "pending upload" file and periodically retrying this upload.
pub struct Uploader<T: TimeProvider, F: SerializedFileSystem> {
  shutdown: ComponentShutdown,
  upload_interval_flag: Watch<u32, UploadStatFlushIntervalFlag>,
  data_flush_tx: Sender<DataUpload>,
  time_provider: Arc<T>,
  fs: Arc<F>,
}

impl<T: TimeProvider, F: SerializedFileSystem> Uploader<T, F> {
  pub const fn new(
    shutdown: ComponentShutdown,
    upload_interval_flag: Watch<u32, UploadStatFlushIntervalFlag>,
    data_flush_tx: Sender<DataUpload>,
    time_provider: Arc<T>,
    fs: Arc<F>,
  ) -> Self {
    Self {
      shutdown,
      upload_interval_flag,
      data_flush_tx,
      time_provider,
      fs,
    }
  }

  pub async fn upload_stats(mut self) -> anyhow::Result<()> {
    loop {
      let upload_in = tokio::time::sleep(Duration::from_millis(
        self.upload_interval_flag.read().into(),
      ));

      tokio::select! {
        _ = self.upload_interval_flag.changed() => continue,
        () = upload_in => self.upload_from_disk().await?,
        () = self.shutdown.cancelled() => return Ok(()),
      }
    }
  }

  async fn upload_from_disk(&mut self) -> anyhow::Result<()> {
    // Note on error handling: while we could probably gracefully handle some of the failing I/O
    // operations, it is likely to result in inaccurate stats (double submission of stats, missing
    // aggregations, etc.), so we bail on failure. As we start seeing this out in the wild we may
    // get a better understanding of why things are failing at which point we can do more targeted
    // error handling.

    // If there is a pending upload, first attempt to re-upload.
    let pending_upload = if self.fs.exists(&*PENDING_STATS_UPLOAD_FILE).await {
      if let Ok(pending_request) = self
        .fs
        .read_compressed_protobuf_file::<StatsUploadRequest>(&*PENDING_STATS_UPLOAD_FILE)
        .await
      {
        Some(pending_request)
      } else {
        // We failed to read the data, so the file must be bad. This could happen if we change
        // the schema in an incompatible way or if the file is corrupt. Delete the file and
        // accept the loss of this upload.

        // TODO(snowp): Technically we could end up in a situation here where we are stuck
        // trying to delete a bad file which then spams the error handler.
        // TODO(snowp): Track how often we delete the file here.
        handle_unexpected(
          self.fs.delete_file(&*PENDING_STATS_UPLOAD_FILE).await,
          "delete pending stats upload",
        );
        None
      }
    } else {
      None
    };

    if let Some(pending_upload) = pending_upload {
      return self.process_pending_upload(pending_upload).await;
    }

    // If there is no pending upload, create one from the current aggregated file.

    // TODO(snowp): Consider doing an open -> read vs exist check -> open and read.
    if !self.fs.exists(&*AGGREGATED_STATS_FILE).await {
      return Ok(());
    }

    let Ok(aggregated_stats) = self
      .fs
      .read_compressed_protobuf_file::<StatsSnapshot>(&*AGGREGATED_STATS_FILE)
      .await
    else {
      log::warn!("failed to read aggregated stats file due to data corruption, deleting file");
      let _ignored = self.fs.delete_file(&*AGGREGATED_STATS_FILE).await;
      return Ok(());
    };

    let stats_request = StatsUploadRequest {
      upload_uuid: TrackedStatsUploadRequest::upload_uuid(),
      snapshot: vec![aggregated_stats],
      ..Default::default()
    };

    // First write the new pending upload to disk, ensuring that if we shut down before the upload
    // completes the data is not lost. If writing the file fails (e.g. no space available) we bail
    // out, leaving the aggregated stats file on disk for the next iteration.
    // TODO(snowp): Consider how we might record stats for this - if stats flushing is broken we
    // might not be able to propagate the stats values.
    if self
      .fs
      .write_compressed_protobuf_file(&*PENDING_STATS_UPLOAD_FILE, &stats_request)
      .await
      .is_err()
    {
      return Ok(());
    }

    // Once the pending data has been written, wipe the aggregated stats file.
    // TODO(snowp): Technically if we shut down right here we'll end up double reporting. We could
    // avoid this by doing a file move, but then we need some mechanism to associated the pending
    // upload with a uuid, which we now embed into pending upload file.
    self
      .fs
      .delete_file(&*AGGREGATED_STATS_FILE)
      .await
      .context("deleting {AGGREGATED_STATS_FILE}")?;

    self.process_pending_upload(stats_request).await
  }

  // Attempts to upload the provided stats request. Upon success, the file containing the pending
  // request will be deleted.
  async fn process_pending_upload(&mut self, request: StatsUploadRequest) -> anyhow::Result<()> {
    let mut request = request;
    request.sent_at = self.time_provider.now().into_proto();

    let (stats, response_rx) = TrackedStatsUploadRequest::new(request.upload_uuid.clone(), request);

    log::debug!(
      "sending pending flush upload: {} with {} metrics",
      stats.payload.upload_uuid,
      stats
        .payload
        .snapshot
        .iter()
        .map(|s| s.metrics().metric.len())
        .sum::<usize>(),
    );

    let tracked_upload = DataUpload::StatsUploadRequest(stats);

    // If this errors out the other end of the channel has closed, indicating that we are shutting
    // down.
    if self.data_flush_tx.send(tracked_upload).await.is_err() {
      return Ok(());
    }

    let stats_uploaded = tokio::select! {
      r = response_rx => r.unwrap_or(false),
      () = self.shutdown.cancelled() => return Ok(()),
    };

    log::debug!("stat upload attempt complete, success: {}", stats_uploaded);

    if stats_uploaded {
      self
        .fs
        .delete_file(&*PENDING_STATS_UPLOAD_FILE)
        .await
        .context("deleting {PENDING_STATS_UPLOAD_FILE}")?;
    }

    Ok(())
  }
}

//
// Flusher
//

/// Responsible for periodically flushing the stats store to a locally aggregated file.
pub struct Flusher<T: TimeProvider, F: SerializedFileSystem> {
  stats: Arc<Stats>,
  shutdown: ComponentShutdown,
  flush_interval_flag: Watch<u32, DirectStatFlushIntervalFlag>,
  flush_rx: tokio::sync::mpsc::Receiver<FlushTriggerCompletionSender>,
  time_provider: Arc<T>,

  flush_time_histogram: Histogram,
  fs: Arc<F>,
}

impl<T: TimeProvider, F: SerializedFileSystem> Flusher<T, F> {
  pub fn new(
    stats: Arc<Stats>,
    shutdown: ComponentShutdown,
    flush_interval_flag: Watch<u32, DirectStatFlushIntervalFlag>,
    flush_rx: tokio::sync::mpsc::Receiver<FlushTriggerCompletionSender>,
    time_provider: Arc<T>,
    flush_time_histogram: Histogram,
    fs: Arc<F>,
  ) -> Self {
    Self {
      stats,
      shutdown,
      flush_interval_flag,
      flush_rx,
      time_provider,
      flush_time_histogram,
      fs,
    }
  }

  pub async fn periodic_flush(mut self) -> anyhow::Result<()> {
    loop {
      let flush_in = tokio::time::sleep(Duration::from_millis(
        self.flush_interval_flag.read().into(),
      ));

      // If the flush interval changes, reset the timer. This ensures that if we are currently
      // operating at a high timeout, we can reset it down to a lower one with runtime. This is
      // helpful for testing.
      tokio::select! {
        _ = self.flush_interval_flag.changed() => continue,
        Some(completion_tx) = self.flush_rx.recv() => {
          log::debug!("received a signal to flush stats to disk");
          self.flush_to_disk().await?;
          log::debug!("stats flushed");

          if let Some(completion_tx) = completion_tx {
            completion_tx.send(());
          }
        },
        () = self.shutdown.cancelled() => return Ok(()),
        () = flush_in => self.flush_to_disk().await?,
      };
    }
  }

  // Merges a delta snapshot to disk. This contains the difference in metrics since the last time
  // stats were flushed to disk.
  async fn merge_delta_snapshot_to_disk(
    &self,
    delta_snapshot: SnapshotHelper,
  ) -> anyhow::Result<()> {
    // Use either the snapshot cached to disk or a new one that records starting point of this
    // aggregation window.
    // TODO(snowp): Track these as unusual but not impossible errors.
    log::debug!("starting merge of delta snapshot to disk");
    let mut new_or_existing_snapshot = self
      .fs
      .read_compressed_protobuf_file::<StatsSnapshot>(&*AGGREGATED_STATS_FILE)
      .await
      .ok()
      .and_then(|snapshot| {
        log::debug!("found existing snapshot, merging in delta");
        SnapshotHelper::new_from_snapshot(snapshot)
      })
      .unwrap_or_else(|| {
        log::debug!("no existing snapshot found or corrupted, creating new snapshot");
        SnapshotHelper::new(&self.time_provider)
      });

    for (id, metric) in delta_snapshot.metrics {
      let Some(cached_metric) = new_or_existing_snapshot.mut_metric(&id) else {
        log::trace!("adding new metric to snapshot: {:?}", id);
        new_or_existing_snapshot.add_metric(id, metric);
        continue;
      };

      // If the metric already exists in the cached snapshot, sum the values together.
      match (&metric, &cached_metric) {
        (MetricData::Counter(c), MetricData::Counter(cached_counter)) => {
          log::trace!("merging counter {id:?} with value {}", c.get());
          cached_counter.inc_by(c.get());
        },
        (MetricData::Histogram(h), MetricData::Histogram(cached_histogram)) => {
          log::trace!("merging histogram {id:?}");
          cached_histogram.merge_from(h);
        },
        _ => {
          // We don't support metrics changing type ever, so do nothing but record an error so we
          // know if this happens.
          bd_client_common::error::handle_unexpected::<(), anyhow::Error>(
            Err(anyhow::anyhow!("metrics inconsistency")),
            "stats merging",
          );
        },
      }
    }

    // If there are no metrics in the snapshot after merging in the latest delta, skip writing the
    // aggregated snapshot to prevent empty stats uploads.
    if new_or_existing_snapshot.metrics.is_empty() {
      return Ok(());
    }

    // Write the updated snapshot back to disk. This will either be read back up on the next
    // iteration of this task or converted into an upload payload by the upload task.
    log::debug!(
      "updating aggregated snapshot file with {} metrics",
      new_or_existing_snapshot.metrics.len()
    );

    // This might fail due to us being out of space or other I/O errors.
    // TODO(snowp): Consider how we might record stats for this - if stats flushing is broken we
    // might not be able to propagate the stats values.
    self
      .fs
      .write_compressed_protobuf_file(
        &*AGGREGATED_STATS_FILE,
        &new_or_existing_snapshot.into_proto(),
      )
      .await
  }

  async fn flush_to_disk(&self) -> anyhow::Result<()> {
    let _timer = self.flush_time_histogram.start_timer();
    // To support flushing stats between multiple process lifetimes, we go through a few steps to
    // apply the diff to the disk-cached snapshot:
    // 1. Gather the current set of delta metrics from the stats registry and convert this into a
    //    StatsSnapshot. This is referred to as the delta snapshot.
    // 2. Attempt to write the new delta snapshot to disk.
    let delta_snapshot = self.create_delta_snapshot();

    // Because we have snapped deltas out of the collectors, if we fail to write to disk we will
    // lose the stats. Given that we will lose the stats anyway if the process terminates, this
    // seems not completely terrible. If we want to slightly improve this in the future we could
    // decide to re-merge the deltas back into the collectors if we fail to write to disk.
    if let Err(e) = self.merge_delta_snapshot_to_disk(delta_snapshot).await {
      bd_client_common::error::handle_unexpected::<(), anyhow::Error>(
        Err(e),
        "writing stats to disk",
      );
    }

    Ok(())
  }

  fn create_delta_snapshot(&self) -> SnapshotHelper {
    let mut snapshot = SnapshotHelper::new(&self.time_provider);
    Self::snap_collector_to_snapshot(self.stats.collector.inner(), &mut snapshot);
    Self::snap_collector_to_snapshot(&self.stats.dynamic_stats.dynamic_collector, &mut snapshot);
    snapshot
  }

  fn snap_collector_to_snapshot(collector: &BoundedCollector, snapshot: &mut SnapshotHelper) {
    // During iteration if a metric has data, we retain it, since it is likely to be used again.
    // If there is no data we drop it if there are no outstanding references. This iteration
    // occurs under the collector lock so it is serialized with respect to new fetches.
    collector.retain(|id, metric| {
      metric.snap().map_or_else(
        || metric.multiple_references(),
        |metric| {
          snapshot.add_metric(id.clone(), metric);
          true
        },
      )
    });
  }
}

//
// SnapshotHelper
//

struct SnapshotHelper {
  time: OffsetDateTime,
  metrics: HashMap<Id, MetricData>,
}

impl SnapshotHelper {
  fn new<T: TimeProvider>(time_provider: &Arc<T>) -> Self {
    Self {
      time: time_provider.now(),
      metrics: HashMap::new(),
    }
  }

  fn new_from_snapshot(snapshot: StatsSnapshot) -> Option<Self> {
    let Some(Snapshot_type::Metrics(metrics)) = snapshot.snapshot_type else {
      return None;
    };

    let Some(Occurred_at::Aggregated(Aggregated { period_start, .. })) = snapshot.occurred_at
    else {
      return None;
    };

    let mut new_metrics = HashMap::new();
    for metric in metrics.metric {
      let id = Id::new(metric.name, metric.tags.into_iter().collect());
      let metric = MetricData::from_proto(metric.data?)?;
      let existing = new_metrics.insert(id, metric);
      debug_assert!(existing.is_none());
    }

    Some(Self {
      time: period_start.to_offset_date_time(),
      metrics: new_metrics,
    })
  }

  fn mut_metric(&mut self, id: &Id) -> Option<&mut MetricData> {
    self.metrics.get_mut(id)
  }

  fn add_metric(&mut self, id: Id, metric: MetricData) {
    let existing = self.metrics.insert(id, metric);
    debug_assert!(existing.is_none());
  }

  fn into_proto(self) -> StatsSnapshot {
    let proto_metrics: Vec<ProtoMetric> = self
      .metrics
      .into_iter()
      .map(|(id, metric)| ProtoMetric {
        name: id.name,
        tags: id.labels.into_iter().collect(),
        data: Some(metric.to_proto()),
        ..Default::default()
      })
      .collect();

    StatsSnapshot {
      snapshot_type: Some(Snapshot_type::Metrics(MetricsList {
        metric: proto_metrics,
        ..Default::default()
      })),
      occurred_at: Some(Occurred_at::Aggregated(Aggregated {
        period_start: self.time.into_proto(),
        ..Default::default()
      })),
      ..Default::default()
    }
  }
}
