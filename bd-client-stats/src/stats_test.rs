// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::Stats;
use crate::file_manager::{FileManager, PENDING_AGGREGATION_INDEX_FILE, STATS_DIRECTORY};
use crate::stats::{IntervalCreator, SleepModeAwareRuntimeWatchTicker, Ticker};
use crate::test::TestTicker;
use assert_matches::assert_matches;
use bd_api::DataUpload;
use bd_api::upload::{Tracked, UploadResponse};
use bd_client_common::file::write_compressed_protobuf;
use bd_client_common::file_system::{FileSystem, RealFileSystem, TestFileSystem};
use bd_client_stats_store::Collector;
use bd_proto::protos::client::api::StatsUploadRequest;
use bd_proto::protos::client::api::stats_upload_request::Snapshot as StatsSnapshot;
use bd_proto::protos::client::api::stats_upload_request::snapshot::{
  Aggregated,
  Occurred_at,
  Snapshot_type,
};
use bd_proto::protos::client::metric::metric::{Data as MetricData, Metric_name_type};
use bd_proto::protos::client::metric::pending_aggregation_index::PendingFile;
use bd_proto::protos::client::metric::{Counter, Metric, MetricsList, PendingAggregationIndex};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_shutdown::ComponentShutdownTrigger;
use bd_stats_common::labels;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_test_helpers::stats::StatsRequestHelper;
use bd_time::{OffsetDateTimeExt, TestTimeProvider, TimeDurationExt, TimeProvider};
use futures_util::poll;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tempfile::TempDir;
use time::ext::{NumericalDuration, NumericalStdDuration};
use time::{Duration, OffsetDateTime};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinError;
use tokio::time::{MissedTickBehavior, sleep, timeout};

async fn write_test_index(fs: &dyn FileSystem, ready_to_upload: bool) {
  let index = PendingAggregationIndex {
    pending_files: vec![PendingFile {
      name: "test".to_string(),
      period_end: if ready_to_upload {
        OffsetDateTime::UNIX_EPOCH.into_proto()
      } else {
        None.into()
      },
      ..Default::default()
    }],
    ..Default::default()
  };
  fs.create_dir(&STATS_DIRECTORY).await.unwrap();
  let compressed = write_compressed_protobuf(&index);
  fs.write_file(
    &STATS_DIRECTORY.join(&*PENDING_AGGREGATION_INDEX_FILE),
    &compressed,
  )
  .await
  .unwrap();
}

//
// TestHooks
//

pub struct TestHooksSender {
  pub upload_complete_tx: mpsc::Sender<()>,
  pub flush_complete_tx: mpsc::Sender<()>,
}
pub struct TestHooksReceiver {
  pub upload_complete_rx: mpsc::Receiver<()>,
  pub flush_complete_rx: mpsc::Receiver<()>,
}
pub struct TestHooks {
  pub sender: TestHooksSender,
  pub receiver: Option<TestHooksReceiver>,
}

impl Default for TestHooks {
  fn default() -> Self {
    let (upload_complete_tx, upload_complete_rx) = mpsc::channel(1);
    let (flush_complete_tx, flush_complete_rx) = mpsc::channel(1);
    Self {
      sender: TestHooksSender {
        upload_complete_tx,
        flush_complete_tx,
      },
      receiver: Some(TestHooksReceiver {
        upload_complete_rx,
        flush_complete_rx,
      }),
    }
  }
}

//
// Setup
//

struct Setup {
  stats: Arc<Stats>,
  _directory: TempDir,
  shutdown_trigger: ComponentShutdownTrigger,
  _runtime_loader: Arc<ConfigLoader>,
  flush_handle: tokio::task::JoinHandle<()>,
  data_rx: mpsc::Receiver<DataUpload>,
  test_time: Arc<TestTimeProvider>,
  flush_tick_tx: mpsc::Sender<()>,
  upload_tick_tx: mpsc::Sender<()>,
  test_hooks: TestHooksReceiver,
}

impl Setup {
  async fn new() -> Self {
    Self::new_with_filesystem(Box::new(TestFileSystem::new()), None, 500).await
  }

  async fn new_with_directory(directory: TempDir) -> Self {
    Self::new_with_filesystem(
      Box::new(RealFileSystem::new(directory.path().to_path_buf())),
      Some(directory),
      500,
    )
    .await
  }

  async fn new_with_filesystem(
    fs: Box<dyn FileSystem>,
    directory: Option<TempDir>,
    limit: u32,
  ) -> Self {
    let directory = directory.unwrap_or_else(|| TempDir::new().unwrap());
    let test_time = Arc::new(TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
    let shutdown_trigger = ComponentShutdownTrigger::default();
    let runtime_loader = ConfigLoader::new(directory.path());
    runtime_loader
      .update_snapshot(&make_simple_update(vec![(
        bd_runtime::runtime::stats::MaxAggregatedFilesFlag::path(),
        ValueKind::Int(2),
      )]))
      .await;

    let stats = Stats::new(Collector::new(Some(watch::channel(limit).1)));
    let (data_tx, data_rx) = mpsc::channel(1);
    let (flush_tick_tx, flush_ticker) = TestTicker::new();
    let (upload_tick_tx, upload_ticker) = TestTicker::new();
    let mut flush_handles = stats.flush_handle_helper(
      Box::new(flush_ticker),
      Box::new(upload_ticker),
      shutdown_trigger.make_shutdown(),
      data_tx,
      Arc::new(FileManager::new(fs, test_time.clone(), &runtime_loader).unwrap()),
    );

    let test_hooks = flush_handles.flusher.test_hooks();
    let flush_handle = tokio::spawn(async move {
      flush_handles.flusher.periodic_flush().await;
    });

    Self {
      test_time,
      stats,
      _directory: directory,
      shutdown_trigger,
      _runtime_loader: runtime_loader,
      flush_handle,
      data_rx,
      flush_tick_tx,
      upload_tick_tx,
      test_hooks,
    }
  }

  async fn do_flush(&mut self) {
    self.flush_tick_tx.send(()).await.unwrap();
    self.test_hooks.flush_complete_rx.recv().await.unwrap();
  }

  async fn next_stat_upload(&mut self) -> Tracked<StatsUploadRequest, UploadResponse> {
    let stats_upload = self.data_rx.recv().await.unwrap();

    let DataUpload::StatsUpload(stats) = stats_upload else {
      panic!("unexpected upload type");
    };

    stats
  }

  async fn with_next_stats_upload_with_result(
    &mut self,
    upload_ok: bool,
    f: impl FnOnce(StatsRequestHelper),
  ) {
    // In order for the tests to be deterministic we have to do this dance carefully to make sure
    // that the events are fully complete before continuing. This is the basic sequence. Some tests
    // use more complicated interleavings.
    self.do_flush().await;

    self.upload_tick_tx.send(()).await.unwrap();
    let stats = self.next_stat_upload().await;
    f(StatsRequestHelper::new(stats.payload.clone()));

    stats
      .response_tx
      .send(UploadResponse {
        success: upload_ok,
        uuid: stats.uuid,
      })
      .unwrap();
    self.test_hooks.upload_complete_rx.recv().await.unwrap();
  }

  async fn with_next_stats_upload(&mut self, f: impl FnOnce(StatsRequestHelper)) {
    self.with_next_stats_upload_with_result(true, f).await;
  }

  async fn shutdown(self) -> Result<(), JoinError> {
    self.shutdown_trigger.shutdown().await;
    self.flush_handle.await
  }
}

#[tokio::test(start_paused = true)]
async fn overflow() {
  let mut setup = Setup::new_with_filesystem(Box::new(TestFileSystem::new()), None, 1).await;

  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "bar"), "id1", 1);
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "bar"), "id1", 2);
  // Overflow 1 for id1.
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "baz"), "id1", 2);
  // Overflow 2 for id1.
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "baz"), "id1", 3);
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "baz"), "id2", 10);
  setup.do_flush().await;

  // This will cause the unused metrics to get freed from RAM.
  setup.do_flush().await;

  // Make sure we don't overflow when we merge into the disk snapshot and upload and also make sure
  // we add up all of the overflows properly.

  // This will go into RAM, but overflow when inserted into the disk snapshot, so #3.
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "blah"), "id1", 100);
  // Overflow 4 for id1.
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "blah2"), "id1", 100);
  // This will go into RAM, but overflow when inserted into the disk snapshot, so #1 for id2.
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "blah"), "id2", 100);
  // Overflow 2 for id2.
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "blah2"), "id2", 100);
  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.overflows().len(), 2);
      assert_eq!(upload.overflows()["id1"], 4);
      assert_eq!(upload.overflows()["id2"], 2);
      assert_eq!(
        upload.get_workflow_counter("id1", labels!("foo" => "bar")),
        Some(3)
      );
      assert_eq!(
        upload.get_workflow_counter("id2", labels!("foo" => "baz")),
        Some(10)
      );
    })
    .await;

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn report() {
  let mut setup = Setup::new().await;

  let c = setup
    .stats
    .collector
    .scope("test")
    .counter_with_labels("test", labels!("buffer" => "continuous"));

  c.inc();

  assert!(poll!(Box::pin(setup.data_rx.recv())).is_pending());

  // Time of first flush.
  let t0 = setup.test_time.now();
  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.aggregation_window_start(), t0);
      assert_eq!(upload.number_of_metrics(), 1);
      assert_eq!(
        upload.get_counter("test:test", labels! {"buffer" => "continuous"}),
        Some(1)
      );
    })
    .await;

  setup.test_time.advance(Duration::seconds(1));

  // Time of second flush.
  let t1 = setup.test_time.now();
  assert_ne!(t0, t1);

  c.inc_by(5);

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.aggregation_window_start(), t1);
      assert_eq!(upload.number_of_metrics(), 2);
      assert_eq!(
        upload.get_counter("test:test", labels! {"buffer" => "continuous"}),
        Some(5)
      );
    })
    .await;

  // Add a second metric and make sure that we only flush the new one.

  let second_counter = setup
    .stats
    .collector
    .scope("second")
    .counter_with_labels("test", labels!("another" => "tag"));

  second_counter.inc();

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.number_of_metrics(), 2);
      assert_eq!(
        upload.get_counter("second:test", labels! {"another" => "tag"}),
        Some(1)
      );
    })
    .await;

  // Update both counters and make sure we send the correct diff.
  second_counter.inc();
  c.inc();

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.number_of_metrics(), 3);
      assert_eq!(
        upload.get_counter("second:test", labels! {"another" => "tag"}),
        Some(1)
      );
      assert_eq!(
        upload.get_counter("test:test", labels! {"buffer" => "continuous"}),
        Some(1)
      );
    })
    .await;

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn max_files_upload_race() {
  let mut setup = Setup::new().await;

  let counter = setup.stats.collector.scope("test").counter("test");
  counter.inc();
  setup.do_flush().await;

  // Advance 5 minutes and flush. This will start a new file.
  setup.test_time.advance(5.minutes());
  counter.inc_by(10);
  setup.do_flush().await;

  // Start the upload but don't complete it.
  setup.upload_tick_tx.send(()).await.unwrap();
  let stats = setup.next_stat_upload().await;

  // Advance 5 minutes and flush. This will remove the file that is currently being uploaded.
  setup.test_time.advance(5.minutes());
  counter.inc_by(100);
  setup.do_flush().await;

  // Complete the upload and make sure we ignore the removed file from previous overflow.
  stats
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: stats.uuid,
    })
    .unwrap();

  // Since we have a "too old" snapshot sitting around, it should get sent immediately.
  let stats = setup.next_stat_upload().await;
  assert_eq!(
    StatsRequestHelper::new(stats.payload).get_counter("test:test", labels! {}),
    Some(10)
  );

  // Responding to this one should not kick off a new upload of the last snapshot.
  stats
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: stats.uuid,
    })
    .unwrap();
  assert!(
    timeout(1.std_seconds(), setup.next_stat_upload())
      .await
      .is_err()
  );
}

#[tokio::test(start_paused = true)]
async fn max_files() {
  let mut setup = Setup::new().await;

  let counter = setup.stats.collector.scope("test").counter("test");
  counter.inc();

  // Fail the first upload then increment and flush again which will make a second file because
  // the first file is ready for upload.
  setup
    .with_next_stats_upload_with_result(false, |upload| {
      assert_eq!(upload.number_of_metrics(), 1);
    })
    .await;

  counter.inc_by(10);
  setup.do_flush().await;

  // Now advance 5 minutes, increment, and flush again. This will exceed max files.
  setup.test_time.advance(5.minutes());
  counter.inc_by(100);
  setup.do_flush().await;

  // Now upload, we should get the 2nd counter increment.
  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.get_counter("test:test", labels! {}), Some(10));
    })
    .await;

  // Upload again. We should get the 3rd counter increment.
  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.get_counter("test:test", labels! {}), Some(100));
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn earliest_aggregation_start_end_maintained() {
  // This test simulates a few upload failures interleaved with stats being recorded, and verifies
  // that the aggregation start/end window is set according to when an aggregation period starts and
  // is not updated every time more data is merged into the aggregated data.
  let mut setup = Setup::new().await;

  let t0 = setup.test_time.now();
  let counter = setup.stats.collector.scope("test").counter("test");
  counter.inc();

  let t1 = setup.test_time.now();
  // Fail the first attempt to upload. At this point we'll have one cached upload request and an
  // empty aggregated snapshot. We know this since both aggregation and uploads will have been
  // processed by the time we get the upload.
  setup
    .with_next_stats_upload_with_result(false, |upload| {
      assert_eq!(upload.aggregation_window_start(), t0);
      assert_eq!(upload.aggregation_window_end(), t1);
      assert_eq!(upload.number_of_metrics(), 1);
    })
    .await;

  setup.test_time.advance(Duration::seconds(1));
  let t2 = setup.test_time.now();

  counter.inc();

  // Fail the second attempt. This attempt should still report for t0, as it's a retry of the old
  // one. At this point we should have started another aggregation window starting at t2.
  setup
    .with_next_stats_upload_with_result(false, |upload| {
      assert_eq!(upload.aggregation_window_start(), t0);
      assert_eq!(upload.aggregation_window_end(), t1);
      assert_eq!(upload.number_of_metrics(), 1);
    })
    .await;

  setup.test_time.advance(Duration::seconds(1));
  let t3 = setup.test_time.now();
  assert_ne!(t3, t2);
  counter.inc();

  // The first stat upload is yet another retry of the one from t0.
  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.aggregation_window_start(), t0);
      assert_eq!(upload.aggregation_window_end(), t1);
      assert_eq!(upload.number_of_metrics(), 1);
      assert_eq!(upload.get_counter("test:test", labels! {}), Some(1));
    })
    .await;

  // The next upload should contain the aggregation window which started at t2, but contains the
  // increments that happened at t3.
  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.aggregation_window_start(), t2);
      assert_eq!(upload.aggregation_window_end(), t3);
      assert_eq!(upload.number_of_metrics(), 2);
      assert_eq!(upload.get_counter("test:test", labels! {}), Some(2));
    })
    .await;

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn existing_pending_upload() {
  let directory = TempDir::new().unwrap();
  let fs = RealFileSystem::new(directory.path().to_path_buf());
  write_test_index(&fs, true).await;
  let req = StatsUploadRequest {
    upload_uuid: "test".to_string(),
    snapshot: vec![StatsSnapshot::default()],
    ..Default::default()
  };
  let compressed = write_compressed_protobuf(&req);
  fs.write_file(&STATS_DIRECTORY.join("test"), &compressed)
    .await
    .unwrap();

  let mut setup = Setup::new_with_directory(directory).await;

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.request.upload_uuid, "test");
      // Normally we won't get an empty snapshot list, but because this was already written as a
      // pending upload we get an unconditional upload.
      assert_eq!(upload.number_of_metrics(), 0);
    })
    .await;
  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn existing_empty_pending_upload() {
  let directory = TempDir::new().unwrap();

  // Write empty data to the upload file. This should then be ignored on startup, so the first
  // upload should be based on fresh stats.
  let fs = RealFileSystem::new(directory.path().to_path_buf());
  write_test_index(&fs, true).await;
  fs.write_file(&STATS_DIRECTORY.join("test"), &[])
    .await
    .unwrap();

  let mut setup = Setup::new_with_directory(directory).await;

  let counter = setup.stats.collector.scope("test").counter("test");
  counter.inc();

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.get_counter("test:test", labels! {}), Some(1));
    })
    .await;
  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn existing_corrupted_pending_upload() {
  let directory = TempDir::new().unwrap();
  let fs = RealFileSystem::new(directory.path().to_path_buf());
  write_test_index(&fs, true).await;
  // Write garbage data to the upload file. This should then be ignored on startup, so the first
  // upload should be based on fresh stats.
  fs.write_file(&STATS_DIRECTORY.join("test"), b"not a proto")
    .await
    .unwrap();

  let mut setup = Setup::new_with_directory(directory).await;

  let counter = setup.stats.collector.scope("test").counter("test");
  counter.inc();

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.get_counter("test:test", labels! {}), Some(1));
    })
    .await;
  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn existing_aggregated_file() {
  let directory = TempDir::new().unwrap();
  let fs = RealFileSystem::new(directory.path().to_path_buf());
  write_test_index(&fs, false).await;
  let snapshot = StatsSnapshot {
    snapshot_type: Some(Snapshot_type::Metrics(MetricsList {
      metric: vec![Metric {
        metric_name_type: Some(Metric_name_type::Name("test:blah".to_string())),
        tags: HashMap::new(),
        data: Some(MetricData::Counter(Counter {
          value: 1,
          ..Default::default()
        })),
        ..Default::default()
      }],
      ..Default::default()
    })),
    occurred_at: Some(Occurred_at::Aggregated(Aggregated::default())),
    ..Default::default()
  };
  let req = StatsUploadRequest {
    upload_uuid: "test".to_string(),
    snapshot: vec![snapshot],
    ..Default::default()
  };
  let compressed = write_compressed_protobuf(&req);
  fs.write_file(&STATS_DIRECTORY.join("test"), &compressed)
    .await
    .unwrap();

  let mut setup = Setup::new_with_directory(directory).await;

  let counter = setup.stats.collector.scope("test").counter("blah");
  counter.inc();

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.get_counter("test:blah", labels! {}), Some(2));
    })
    .await;

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn empty_aggregated_file() {
  let directory = TempDir::new().unwrap();
  let fs = RealFileSystem::new(directory.path().to_path_buf());
  write_test_index(&fs, false).await;
  fs.write_file(&STATS_DIRECTORY.join("test"), &[])
    .await
    .unwrap();

  let mut setup = Setup::new_with_directory(directory).await;

  let counter = setup.stats.collector.scope("test").counter("foo");
  counter.inc();

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.get_counter("test:foo", labels! {}), Some(1));
    })
    .await;
  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn corrupted_aggregated_file() {
  let directory = TempDir::new().unwrap();
  let fs = RealFileSystem::new(directory.path().to_path_buf());
  write_test_index(&fs, false).await;
  fs.write_file(&STATS_DIRECTORY.join("test"), b"not a proto")
    .await
    .unwrap();

  let mut setup = Setup::new_with_directory(directory).await;

  let counter = setup.stats.collector.scope("test").counter("foo");
  counter.inc();

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.get_counter("test:foo", labels! {}), Some(1));
    })
    .await;
  setup.shutdown().await.unwrap();
}

// Basic test which exercises happy path flushing with test filesystem.
#[tokio::test(start_paused = true)]
async fn stat_flush_test_fs() {
  let mut setup = Setup::new().await;

  let counter = setup.stats.collector.scope("test").counter("foo");

  counter.inc();

  setup
    .with_next_stats_upload(|upload| {
      assert_matches!(upload.get_counter("test:foo", labels! {}), Some(1));
    })
    .await;

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn different_histogram_merges() {
  let mut setup = Setup::new().await;

  // Fail first upload with an inline histogram.
  let histogram = setup.stats.collector.scope("test").histogram("blah");
  for i in &[1.0, 2.0, 3.0, 4.0, 5.0] {
    histogram.observe(*i);
  }
  setup
    .with_next_stats_upload_with_result(false, |_| {})
    .await;

  // This next upload will start another inline histogram but not upload as there is an upload
  // pending.
  for i in &[1.0, 2.0, 3.0, 4.0, 5.0] {
    histogram.observe(*i);
  }
  setup
    .with_next_stats_upload_with_result(false, |_| {})
    .await;

  // This next merge should take us from inline to sketch as we are over the limit.
  for i in &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0] {
    histogram.observe(*i);
  }
  setup
    .with_next_stats_upload_with_result(false, |_| {})
    .await;

  // Make sure we can merge inline into an existing sketch in the snapshot.
  for i in &[1.0, 2.0, 3.0, 4.0, 5.0] {
    histogram.observe(*i);
  }

  setup
    .with_next_stats_upload_with_result(true, |upload| {
      upload.expect_inline_histogram("test:blah", labels! {}, &[1.0, 2.0, 3.0, 4.0, 5.0]);
    })
    .await;
  setup
    .with_next_stats_upload_with_result(true, |upload| {
      upload.expect_ddsketch_histogram("test:blah", labels! {}, 51.126_736_746_918_93, 16);
    })
    .await;

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn different_histogram_merges2() {
  let mut setup = Setup::new().await;

  // Fail first upload with an inline histogram.
  let histogram = setup.stats.collector.scope("test").histogram("blah");
  for i in &[1.0, 2.0, 3.0, 4.0, 5.0] {
    histogram.observe(*i);
  }
  setup
    .with_next_stats_upload_with_result(false, |_| {})
    .await;

  // This next upload will start another inline histogram but not upload as there is an upload
  // pending.
  for i in &[1.0, 2.0, 3.0, 4.0, 5.0] {
    histogram.observe(*i);
  }
  setup
    .with_next_stats_upload_with_result(false, |_| {})
    .await;

  // This should swap over to a sketch which we will have to merge into the inline.
  for i in &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0] {
    histogram.observe(*i);
  }

  setup
    .with_next_stats_upload_with_result(true, |upload| {
      upload.expect_inline_histogram("test:blah", labels! {}, &[1.0, 2.0, 3.0, 4.0, 5.0]);
    })
    .await;
  setup
    .with_next_stats_upload_with_result(true, |upload| {
      upload.expect_ddsketch_histogram("test:blah", labels! {}, 36.061_183_799_491_74, 11);
    })
    .await;

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn histograms() {
  let mut setup = Setup::new().await;

  let histogram = setup.stats.collector.scope("test").histogram("blah");

  histogram.observe(2.0);

  setup
    .with_next_stats_upload(|upload| {
      upload.expect_inline_histogram("test:blah", labels! {}, &[2.0]);
    })
    .await;

  histogram.observe(1.0);
  histogram.observe(3.0);

  setup
    .with_next_stats_upload(|upload| {
      upload.expect_inline_histogram("test:blah", labels! {}, &[1.0, 3.0]);
    })
    .await;

  for i in &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0] {
    histogram.observe(*i);
  }

  setup
    .with_next_stats_upload(|upload| {
      upload.expect_ddsketch_histogram("test:blah", labels! {}, 20.995_630_852_064_536, 6);
    })
    .await;

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn dynamic_stats() {
  let mut setup = Setup::new().await;

  setup.stats.record_dynamic_counter(
    BTreeMap::from([
      ("k1".to_string(), "v1".to_string()),
      ("k2".to_string(), "v2".to_string()),
    ]),
    "id",
    5,
  );

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.number_of_metrics(), 1);
      assert_eq!(
        upload.get_workflow_counter("id", labels! { "k1" => "v1", "k2" => "v2"}),
        Some(5)
      );
    })
    .await;

  // Record the same metric multiple times between a flush interval.
  setup.stats.record_dynamic_counter(
    BTreeMap::from([
      ("k1".to_string(), "v1".to_string()),
      ("k2".to_string(), "v2".to_string()),
    ]),
    "id",
    3,
  );

  setup.stats.record_dynamic_counter(
    BTreeMap::from([
      ("k1".to_string(), "v1".to_string()),
      ("k2".to_string(), "v2".to_string()),
    ]),
    "id",
    4,
  );

  // Record a similar metric to ensure that we properly disambiguate metrics by tag values.
  setup.stats.record_dynamic_counter(
    BTreeMap::from([
      ("k1".to_string(), "v1".to_string()),
      ("k2".to_string(), "foo".to_string()),
    ]),
    "id",
    3,
  );

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.number_of_metrics(), 3);
      assert_eq!(
        upload.get_workflow_counter("id", labels! { "k1" => "v1", "k2" => "v2"}),
        Some(7)
      );
      assert_eq!(
        upload.get_workflow_counter("id", labels! { "k1" => "v1", "k2" => "foo"}),
        Some(3)
      );
    })
    .await;

  // During this cycle only record one of the metrics, the other should be dropped.
  setup.stats.record_dynamic_counter(
    BTreeMap::from([
      ("k1".to_string(), "v1".to_string()),
      ("k2".to_string(), "foo".to_string()),
    ]),
    "id",
    5,
  );

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.number_of_metrics(), 2);
      assert_eq!(
        upload.get_workflow_counter("id", labels! { "k1" => "v1", "k2" => "foo"}),
        Some(5)
      );
    })
    .await;

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn sleep_mode_aware_runtime_watch_ticker() {
  struct TestIntervalCreator;
  impl IntervalCreator for TestIntervalCreator {
    fn interval(duration: Duration) -> tokio::time::Interval {
      duration.interval_at(MissedTickBehavior::Delay)
    }
  }

  let (live_mode_tx, live_mode_rx) = watch::channel(60.seconds());
  let (sleep_mode_tx, sleep_mode_rx) = watch::channel(15.minutes());
  let (sleep_mode_active_tx, sleep_mode_active_rx) = watch::channel(false);

  let mut ticker = SleepModeAwareRuntimeWatchTicker::<TestIntervalCreator>::new(
    live_mode_rx,
    sleep_mode_rx,
    sleep_mode_active_rx,
  );

  let mut tick_future = ticker.tick();
  assert!(poll!(&mut tick_future).is_pending());
  sleep(59.std_seconds()).await;
  assert!(poll!(&mut tick_future).is_pending());
  sleep(1.std_seconds()).await;
  assert!(poll!(tick_future).is_ready());

  // Now switch to sleep mode outside of an active tick.
  sleep_mode_active_tx.send(true).unwrap();
  let mut tick_future = ticker.tick();
  assert!(poll!(&mut tick_future).is_pending());
  sleep(14.std_minutes()).await;
  assert!(poll!(&mut tick_future).is_pending());
  sleep(1.std_minutes()).await;
  assert!(poll!(tick_future).is_ready());

  // Start a new tick in sleep mode, and switch to live mode during the tick.
  let mut tick_future = ticker.tick();
  sleep(3.std_minutes()).await;
  assert!(poll!(&mut tick_future).is_pending());
  sleep_mode_active_tx.send(false).unwrap();
  assert!(poll!(&mut tick_future).is_pending());
  sleep(59.std_seconds()).await;
  assert!(poll!(&mut tick_future).is_pending());
  sleep(1.std_seconds()).await;
  assert!(poll!(tick_future).is_ready());

  // Start a new tick and update sleep mode time which shouldn't do anything until we switch to
  // sleep mode.
  let mut tick_future = ticker.tick();
  sleep_mode_tx.send(30.minutes()).unwrap();
  sleep(59.std_seconds()).await;
  assert!(poll!(&mut tick_future).is_pending());
  sleep(1.std_seconds()).await;
  assert!(poll!(tick_future).is_ready());

  // Start a tick, switch to sleep mode, and verify we get the new time.
  let mut tick_future = ticker.tick();
  sleep_mode_active_tx.send(true).unwrap();
  assert!(poll!(&mut tick_future).is_pending());
  sleep(29.std_minutes()).await;
  assert!(poll!(&mut tick_future).is_pending());
  sleep(1.std_minutes()).await;
  assert!(poll!(tick_future).is_ready());

  // Start a tick, change live mode duration, switch to live, and verify we get the new time.
  let mut tick_future = ticker.tick();
  live_mode_tx.send(30.seconds()).unwrap();
  sleep_mode_active_tx.send(false).unwrap();
  assert!(poll!(&mut tick_future).is_pending());
  sleep(29.std_seconds()).await;
  assert!(poll!(&mut tick_future).is_pending());
  sleep(1.std_seconds()).await;
  assert!(poll!(tick_future).is_ready());
}
