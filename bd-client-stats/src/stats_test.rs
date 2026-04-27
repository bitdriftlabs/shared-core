// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::file_manager::{FileManager, PENDING_AGGREGATION_INDEX_FILE, STATS_DIRECTORY};
use crate::stats::{PeriodicAction, PeriodicSchedule, RuntimePeriodicSchedule};
use crate::test::TestTickerBackedSchedule;
use crate::{FlushTrigger, FlushTriggerRequest, Stats};
use assert_matches::assert_matches;
use async_trait::async_trait;
use bd_api::DataUpload;
use bd_api::upload::{Tracked, UploadResponse};
use bd_client_common::file::write_compressed_protobuf;
use bd_client_common::file_system::{FileSystem, RealFileSystem};
use bd_client_common::test::TestFileSystem;
use bd_client_stats_store::Collector;
use bd_proto::protos::client::api::StatsUploadRequest;
use bd_proto::protos::client::api::stats_upload_request::snapshot::{
  Aggregated,
  Occurred_at,
  Snapshot_type,
};
use bd_proto::protos::client::api::stats_upload_request::{
  Snapshot as StatsSnapshot,
  UploadReason,
};
use bd_proto::protos::client::metric::metric::{Data as MetricData, Metric_name_type};
use bd_proto::protos::client::metric::pending_aggregation_index::PendingFile;
use bd_proto::protos::client::metric::{Counter, Metric, MetricsList, PendingAggregationIndex};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_shutdown::ComponentShutdownTrigger;
use bd_stats_common::labels;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_test_helpers::stats::StatsRequestHelper;
use bd_time::test::TestTicker;
use bd_time::{OffsetDateTimeExt, TestTimeProvider, TimeProvider};
use futures_util::poll;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tempfile::TempDir;
use time::ext::{NumericalDuration, NumericalStdDuration};
use time::{Duration, OffsetDateTime};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinError;
use tokio::time::timeout;

async fn write_test_index(fs: &dyn FileSystem, ready_to_upload: bool) {
  write_test_index_with_start(fs, OffsetDateTime::UNIX_EPOCH, ready_to_upload).await;
}

async fn write_test_index_with_start(
  fs: &dyn FileSystem,
  period_start: OffsetDateTime,
  ready_to_upload: bool,
) {
  let index = PendingAggregationIndex {
    pending_files: vec![PendingFile {
      name: "test".to_string(),
      period_start: period_start.into_proto(),
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
  let compressed = write_compressed_protobuf(&index).unwrap();
  fs.write_file(
    &STATS_DIRECTORY.join(&*PENDING_AGGREGATION_INDEX_FILE),
    &compressed,
  )
  .await
  .unwrap();
}

async fn write_test_upload_request(fs: &dyn FileSystem, request: StatsUploadRequest) {
  let compressed = write_compressed_protobuf(&request).unwrap();
  fs.write_file(&STATS_DIRECTORY.join("test"), &compressed)
    .await
    .unwrap();
}

#[derive(Clone)]
struct PausedScheduleTimeProvider {
  now: Arc<Mutex<OffsetDateTime>>,
}

impl PausedScheduleTimeProvider {
  fn new(now: OffsetDateTime) -> Self {
    Self {
      now: Arc::new(Mutex::new(now)),
    }
  }

  async fn advance(&self, duration: Duration) {
    *self.now.lock() += duration;
    tokio::time::advance(duration.unsigned_abs()).await;
  }
}

#[async_trait]
impl TimeProvider for PausedScheduleTimeProvider {
  fn now(&self) -> OffsetDateTime {
    *self.now.lock()
  }

  async fn sleep(&self, duration: Duration) {
    tokio::time::sleep(duration.unsigned_abs()).await;
  }
}

fn runtime_periodic_schedule(
  flush_interval: Duration,
  live_upload_interval: Duration,
  sleep_upload_interval: Duration,
  sleep_mode_active: bool,
  time_provider: Arc<dyn TimeProvider>,
) -> (
  RuntimePeriodicSchedule,
  watch::Sender<Duration>,
  watch::Sender<Duration>,
  watch::Sender<Duration>,
  watch::Sender<bool>,
) {
  let (flush_tx, flush_rx) = watch::channel(flush_interval);
  let (live_tx, live_rx) = watch::channel(live_upload_interval);
  let (sleep_tx, sleep_rx) = watch::channel(sleep_upload_interval);
  let (sleep_mode_tx, sleep_mode_rx) = watch::channel(sleep_mode_active);

  (
    RuntimePeriodicSchedule::new(flush_rx, live_rx, sleep_rx, sleep_mode_rx, time_provider),
    flush_tx,
    live_tx,
    sleep_tx,
    sleep_mode_tx,
  )
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
  runtime_loader: Arc<ConfigLoader>,
  flush_handle: tokio::task::JoinHandle<()>,
  data_rx: mpsc::Receiver<DataUpload>,
  test_time: Arc<TestTimeProvider>,
  periodic_flush_tick_tx: mpsc::Sender<()>,
  upload_tick_tx: mpsc::Sender<()>,
  test_hooks: TestHooksReceiver,
  explicit_flush_trigger: FlushTrigger,
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
      .update_snapshot(make_simple_update(vec![(
        bd_runtime::runtime::stats::MaxAggregatedFilesFlag::path(),
        ValueKind::Int(2),
      )]))
      .await
      .unwrap();

    let stats = Stats::new(Collector::new(Some(watch::channel(limit).1)));
    let (data_tx, data_rx) = mpsc::channel(1);
    let (periodic_flush_tick_tx, periodic_flush_ticker) = TestTicker::new();
    let (upload_tick_tx, upload_ticker) = TestTicker::new();
    let minimum_upload_interval = runtime_loader.register_duration_watch();
    let mut flush_handles = stats.flush_handle_helper(
      Box::new(TestTickerBackedSchedule::new(
        Box::new(periodic_flush_ticker),
        Box::new(upload_ticker),
      )),
      shutdown_trigger.make_shutdown(),
      data_tx,
      Arc::new(FileManager::new(fs, test_time.clone(), &runtime_loader)),
      test_time.clone(),
      minimum_upload_interval,
    );

    let test_hooks = flush_handles.flusher.test_hooks();
    let explicit_flush_trigger = flush_handles.flush_trigger.clone();
    let flush_handle = tokio::spawn(async move {
      flush_handles.flusher.periodic_flush().await;
    });

    Self {
      test_time,
      stats,
      _directory: directory,
      shutdown_trigger,
      runtime_loader,
      flush_handle,
      data_rx,
      periodic_flush_tick_tx,
      upload_tick_tx,
      test_hooks,
      explicit_flush_trigger,
    }
  }

  async fn do_periodic_flush(&mut self) {
    self.periodic_flush_tick_tx.send(()).await.unwrap();
    self.test_hooks.flush_complete_rx.recv().await.unwrap();
  }

  async fn do_explicit_flush(&self, completion: bd_completion::Sender<()>) {
    self
      .explicit_flush_trigger
      .flush(FlushTriggerRequest {
        do_upload: true,
        completion_tx: Some(completion),
      })
      .await
      .unwrap();
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
    // use more complicated interleavings. Periodic upload ticks now flush to disk before they
    // attempt the upload, so the helper only needs to drive the upload tick and wait for that
    // embedded flush.
    self.upload_tick_tx.send(()).await.unwrap();
    self.test_hooks.flush_complete_rx.recv().await.unwrap();
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
async fn runtime_periodic_schedule_flushes_before_upload_when_divisible() {
  let time_provider = Arc::new(PausedScheduleTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let (mut schedule, _flush_tx, _live_tx, _sleep_tx, _sleep_mode_tx) = runtime_periodic_schedule(
    30.seconds(),
    90.seconds(),
    15.minutes(),
    false,
    time_provider.clone(),
  );

  let mut next_action = Box::pin(schedule.next_action());
  assert!(poll!(&mut next_action).is_pending());
  time_provider.advance(29.seconds()).await;
  assert!(poll!(&mut next_action).is_pending());
  time_provider.advance(1.seconds()).await;
  assert_eq!(
    poll!(next_action),
    std::task::Poll::Ready(PeriodicAction::Flush)
  );

  let mut next_action = Box::pin(schedule.next_action());
  time_provider.advance(29.seconds()).await;
  assert!(poll!(&mut next_action).is_pending());
  time_provider.advance(1.seconds()).await;
  assert_eq!(
    poll!(next_action),
    std::task::Poll::Ready(PeriodicAction::Flush)
  );

  let mut next_action = Box::pin(schedule.next_action());
  time_provider.advance(29.seconds()).await;
  assert!(poll!(&mut next_action).is_pending());
  time_provider.advance(1.seconds()).await;
  assert_eq!(
    poll!(next_action),
    std::task::Poll::Ready(PeriodicAction::Upload)
  );
}

#[tokio::test(start_paused = true)]
async fn runtime_periodic_schedule_falls_back_to_upload_when_flush_is_not_divisor() {
  let time_provider = Arc::new(PausedScheduleTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let (mut schedule, _flush_tx, _live_tx, _sleep_tx, _sleep_mode_tx) = runtime_periodic_schedule(
    40.seconds(),
    90.seconds(),
    15.minutes(),
    false,
    time_provider.clone(),
  );

  let mut next_action = Box::pin(schedule.next_action());
  assert!(poll!(&mut next_action).is_pending());
  time_provider.advance(89.seconds()).await;
  assert!(poll!(&mut next_action).is_pending());
  time_provider.advance(1.seconds()).await;
  assert_eq!(
    poll!(next_action),
    std::task::Poll::Ready(PeriodicAction::Upload)
  );
}

#[tokio::test(start_paused = true)]
async fn runtime_periodic_schedule_upload_interval_change_rebuilds_to_new_deadline() {
  let time_provider = Arc::new(PausedScheduleTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let (mut schedule, _flush_tx, live_tx, _sleep_tx, _sleep_mode_tx) = runtime_periodic_schedule(
    120.seconds(),
    120.seconds(),
    15.minutes(),
    false,
    time_provider.clone(),
  );

  let mut next_action = Box::pin(schedule.next_action());
  assert!(poll!(&mut next_action).is_pending());
  time_provider.advance(30.seconds()).await;
  assert!(poll!(&mut next_action).is_pending());

  live_tx.send(40.seconds()).unwrap();

  match poll!(&mut next_action) {
    std::task::Poll::Ready(action) => assert_eq!(action, PeriodicAction::Upload),
    std::task::Poll::Pending => {
      time_provider.advance(40.seconds()).await;
      assert_eq!(
        poll!(next_action),
        std::task::Poll::Ready(PeriodicAction::Upload)
      );
    },
  }
}

#[tokio::test(start_paused = true)]
async fn runtime_periodic_schedule_sleep_mode_transition_uses_sleep_interval() {
  let time_provider = Arc::new(PausedScheduleTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let (mut schedule, _flush_tx, _live_tx, _sleep_tx, sleep_mode_tx) = runtime_periodic_schedule(
    120.seconds(),
    120.seconds(),
    50.seconds(),
    false,
    time_provider.clone(),
  );

  let mut next_action = Box::pin(schedule.next_action());
  assert!(poll!(&mut next_action).is_pending());
  time_provider.advance(30.seconds()).await;
  assert!(poll!(&mut next_action).is_pending());

  sleep_mode_tx.send(true).unwrap();

  match poll!(&mut next_action) {
    std::task::Poll::Ready(action) => assert_eq!(action, PeriodicAction::Upload),
    std::task::Poll::Pending => {
      time_provider.advance(50.seconds()).await;
      assert_eq!(
        poll!(next_action),
        std::task::Poll::Ready(PeriodicAction::Upload)
      );
    },
  }
}

#[tokio::test(start_paused = true)]
async fn runtime_periodic_schedule_flush_only_change_keeps_upload_deadline() {
  let time_provider = Arc::new(PausedScheduleTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let (mut schedule, flush_tx, _live_tx, _sleep_tx, _sleep_mode_tx) = runtime_periodic_schedule(
    30.seconds(),
    90.seconds(),
    15.minutes(),
    false,
    time_provider.clone(),
  );

  let mut next_action = Box::pin(schedule.next_action());
  assert!(poll!(&mut next_action).is_pending());
  time_provider.advance(10.seconds()).await;
  assert!(poll!(&mut next_action).is_pending());

  flush_tx.send(200.seconds()).unwrap();
  assert!(poll!(&mut next_action).is_pending());

  time_provider.advance(79.seconds()).await;
  assert!(poll!(&mut next_action).is_pending());
  time_provider.advance(1.seconds()).await;
  assert_eq!(
    poll!(next_action),
    std::task::Poll::Ready(PeriodicAction::Upload)
  );
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
  setup.do_periodic_flush().await;

  // This will cause the unused metrics to get freed from RAM.
  setup.do_periodic_flush().await;

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
      assert_eq!(upload.upload_reason(), UploadReason::UPLOAD_REASON_PERIODIC);
    })
    .await;

  setup.test_time.advance(Duration::seconds(31));

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

  setup.test_time.advance(Duration::seconds(31));
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

  setup.test_time.advance(Duration::seconds(31));
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
  setup.do_periodic_flush().await;

  // Advance 5 minutes and flush. This will start a new file.
  setup.test_time.advance(5.minutes());
  counter.inc_by(10);
  setup.do_periodic_flush().await;

  // Start the upload but don't complete it.
  setup.upload_tick_tx.send(()).await.unwrap();
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();
  let stats = setup.next_stat_upload().await;

  // Advance 5 minutes and flush. This will remove the file that is currently being uploaded.
  setup.test_time.advance(5.minutes());
  counter.inc_by(100);
  setup.do_periodic_flush().await;

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
  setup.do_periodic_flush().await;

  // Now advance 5 minutes, increment, and flush again. This will exceed max files.
  setup.test_time.advance(5.minutes());
  counter.inc_by(100);
  setup.do_periodic_flush().await;

  // Now upload, we should get the 2nd counter increment.
  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.get_counter("test:test", labels! {}), Some(10));
    })
    .await;

  // Upload again. We should get the 3rd counter increment.
  setup.test_time.advance(Duration::seconds(31));
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
  setup.test_time.advance(Duration::seconds(31));
  let t4 = setup.test_time.now();
  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.aggregation_window_start(), t2);
      assert_eq!(upload.aggregation_window_end(), t4);
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
  let compressed = write_compressed_protobuf(&req).unwrap();
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
async fn existing_old_aggregated_file_uploads_immediately_on_startup() {
  let directory = TempDir::new().unwrap();
  let fs = RealFileSystem::new(directory.path().to_path_buf());
  write_test_index_with_start(&fs, OffsetDateTime::UNIX_EPOCH - 10.minutes(), false).await;
  write_test_upload_request(
    &fs,
    StatsUploadRequest {
      upload_uuid: "test".to_string(),
      snapshot: vec![StatsSnapshot::default()],
      ..Default::default()
    },
  )
  .await;

  let mut setup = Setup::new_with_directory(directory).await;

  let upload = timeout(1.std_seconds(), setup.next_stat_upload())
    .await
    .unwrap();
  let helper = StatsRequestHelper::new(upload.payload.clone());
  assert_eq!(upload.payload.upload_uuid, "test");
  assert_eq!(helper.number_of_metrics(), 0);

  upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn fresh_pending_upload_does_not_start_on_startup() {
  let directory = TempDir::new().unwrap();
  let fs = RealFileSystem::new(directory.path().to_path_buf());
  write_test_index_with_start(&fs, OffsetDateTime::UNIX_EPOCH, true).await;
  write_test_upload_request(
    &fs,
    StatsUploadRequest {
      upload_uuid: "test".to_string(),
      snapshot: vec![StatsSnapshot::default()],
      ..Default::default()
    },
  )
  .await;

  let mut setup = Setup::new_with_directory(directory).await;

  assert!(
    timeout(1.std_seconds(), setup.next_stat_upload())
      .await
      .is_err()
  );

  setup.upload_tick_tx.send(()).await.unwrap();
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();
  let upload = setup.next_stat_upload().await;
  assert_eq!(upload.payload.upload_uuid, "test");

  upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();

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
  let compressed = write_compressed_protobuf(&req).unwrap();
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
  setup.test_time.advance(Duration::seconds(31));
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
  setup.test_time.advance(Duration::seconds(31));
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

  setup.test_time.advance(Duration::seconds(31));
  setup
    .with_next_stats_upload(|upload| {
      upload.expect_inline_histogram("test:blah", labels! {}, &[1.0, 3.0]);
    })
    .await;

  for i in &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0] {
    histogram.observe(*i);
  }

  setup.test_time.advance(Duration::seconds(31));
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

  setup.test_time.advance(Duration::seconds(31));
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

  setup.test_time.advance(Duration::seconds(31));
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
async fn flush_during_periodic_upload() {
  let mut setup = Setup::new().await;

  // 1. Record Metric A
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "bar"), "id1", 1);

  // 2. Flush to disk (periodic flush)
  setup.do_periodic_flush().await;

  // 3. Trigger periodic upload
  setup.upload_tick_tx.send(()).await.unwrap();
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();

  // 4. Receive Upload 1
  let upload1 = setup.next_stat_upload().await;
  {
    let helper = StatsRequestHelper::new(upload1.payload.clone());
    assert_eq!(
      helper.get_workflow_counter("id1", labels!("foo" => "bar")),
      Some(1)
    );
  }

  // 5. Record Metric B
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "baz"), "id2", 2);

  // 6. Trigger explicit flush. Pretend periodic is taking a very long time to come back so that
  // the minimum time between uploads has elapsed.
  setup.test_time.advance(Duration::seconds(31));
  let (tx, rx) = bd_completion::Sender::new();
  setup.do_explicit_flush(tx).await;

  // 7. Receive Upload 2 (triggered by flush)
  let upload2 = setup.next_stat_upload().await;
  {
    let helper = StatsRequestHelper::new(upload2.payload.clone());
    assert_eq!(
      helper.get_workflow_counter("id2", labels!("foo" => "baz")),
      Some(2)
    );
  }

  // 8. Complete Upload 1
  upload1
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload1.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();

  // 9. Complete Upload 2
  upload2
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload2.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();

  rx.recv().await.unwrap();

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn periodic_upload_does_not_block_immediate_explicit_flush_upload() {
  let mut setup = Setup::new().await;

  setup
    .runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::stats::MinimumUploadIntervalFlag::path(),
      ValueKind::Int(60_000),
    )]))
    .await
    .unwrap();

  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "periodic"), "id1", 1);

  setup.do_periodic_flush().await;
  setup.upload_tick_tx.send(()).await.unwrap();
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();

  let periodic_upload = setup.next_stat_upload().await;
  assert_eq!(
    StatsRequestHelper::new(periodic_upload.payload.clone()).upload_reason(),
    UploadReason::UPLOAD_REASON_PERIODIC
  );

  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "explicit"), "id1", 2);

  let (tx, rx) = bd_completion::Sender::new();
  setup.do_explicit_flush(tx).await;
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();

  let explicit_upload = setup.next_stat_upload().await;
  let helper = StatsRequestHelper::new(explicit_upload.payload.clone());
  assert_eq!(
    helper.upload_reason(),
    UploadReason::UPLOAD_REASON_EVENT_TRIGGERED
  );
  assert_eq!(
    helper.get_workflow_counter("id1", labels!("foo" => "explicit")),
    Some(2)
  );

  periodic_upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: periodic_upload.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();

  explicit_upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: explicit_upload.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();
  rx.recv().await.unwrap();

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn periodic_tick_while_upload_is_in_flight_flushes_without_starting_second_upload() {
  let mut setup = Setup::new().await;

  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "first"), "id1", 1);

  setup.do_periodic_flush().await;
  setup.upload_tick_tx.send(()).await.unwrap();
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();

  let upload1 = setup.next_stat_upload().await;

  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "second"), "id2", 2);

  setup.upload_tick_tx.send(()).await.unwrap();
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();
  assert!(poll!(Box::pin(setup.data_rx.recv())).is_pending());

  upload1
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload1.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();

  assert!(poll!(Box::pin(setup.data_rx.recv())).is_pending());

  setup.upload_tick_tx.send(()).await.unwrap();
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();

  let upload2 = setup.next_stat_upload().await;
  assert_eq!(
    StatsRequestHelper::new(upload2.payload.clone())
      .get_workflow_counter("id2", labels!("foo" => "second")),
    Some(2)
  );

  upload2
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload2.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn explicit_flush_triggers_upload_immediately() {
  let mut setup = Setup::new().await;

  // 1. Record Metric
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "bar"), "id1", 1);

  // 2. Trigger explicit flush. This should flush to disk AND trigger upload because there is no
  // upload in flight.
  let (tx, rx) = bd_completion::Sender::new();
  setup.do_explicit_flush(tx).await;

  // 3. Receive Upload
  let upload = setup.next_stat_upload().await;
  {
    let helper = StatsRequestHelper::new(upload.payload.clone());
    assert_eq!(
      helper.get_workflow_counter("id1", labels!("foo" => "bar")),
      Some(1)
    );
    assert_eq!(
      helper.upload_reason(),
      UploadReason::UPLOAD_REASON_EVENT_TRIGGERED
    );
  }

  // 4. Complete Upload
  upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();

  rx.recv().await.unwrap();

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn concurrent_flushes() {
  let mut setup = Setup::new().await;

  // 1. Record Metric A
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "bar"), "id1", 1);

  // 2. Trigger explicit flush 1
  let (tx1, rx1) = bd_completion::Sender::new();
  setup.do_explicit_flush(tx1).await;

  // 3. Wait for Upload 1 to start. The flush triggers an upload. We need to grab it. Also drain the
  // flush complete hook.
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();
  let upload1 = setup.next_stat_upload().await;

  // 4. Record Metric B
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "baz"), "id2", 2);

  // 5. Trigger explicit flush 2 while Upload 1 is in flight
  let (tx2, rx2) = bd_completion::Sender::new();
  setup.do_explicit_flush(tx2).await;

  // 6. Verify behavior of Flush 2. With current code, flush 2 is ignored. The completion tx2 is
  // dropped. So rx2.recv() should return Err.
  let result = rx2.recv().await;
  assert!(result.is_err(), "Expected flush 2 completion to be dropped");

  // 7. Verify Metric B was NOT flushed to disk. If it was flushed, we would see a file or it would
  // be in the next upload. But since flush_to_disk was skipped, it should still be in memory. If we
  // finish upload 1, and then trigger another flush, we can check.
  upload1
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload1.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();
  rx1.recv().await.unwrap(); // Flush 1 complete

  // Now trigger Flush 3.
  setup.test_time.advance(Duration::seconds(31));
  let (tx3, rx3) = bd_completion::Sender::new();
  setup.do_explicit_flush(tx3).await;
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();

  let upload3 = setup.next_stat_upload().await;
  {
    let helper = StatsRequestHelper::new(upload3.payload.clone());
    // It should contain Metric B (id2)
    assert_eq!(
      helper.get_workflow_counter("id2", labels!("foo" => "baz")),
      Some(2)
    );
  }

  upload3
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload3.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();
  rx3.recv().await.unwrap();

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn explicit_flush_no_data() {
  let mut setup = Setup::new().await;

  // Trigger explicit flush with no data
  let (tx, rx) = bd_completion::Sender::new();
  setup.do_explicit_flush(tx).await;

  // Should complete immediately without upload. Note: flush_to_disk is still called, so we must
  // drain the hook.
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();
  rx.recv().await.unwrap();

  // Verify no upload
  assert!(poll!(Box::pin(setup.data_rx.recv())).is_pending());

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn explicit_flush_upload_failure() {
  let mut setup = Setup::new().await;

  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "bar"), "id1", 1);

  let (tx, rx) = bd_completion::Sender::new();
  setup.do_explicit_flush(tx).await;
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();

  let upload = setup.next_stat_upload().await;

  // Fail the upload
  upload
    .response_tx
    .send(UploadResponse {
      success: false,
      uuid: upload.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();

  // Completion should still fire
  rx.recv().await.unwrap();

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn minimum_upload_interval() {
  let mut setup = Setup::new().await;

  // Configure minimum upload interval to 10 seconds
  setup
    .runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::stats::MinimumUploadIntervalFlag::path(),
      ValueKind::Int(10_000),
    )]))
    .await
    .unwrap();

  // Record some data
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "bar"), "id1", 1);

  // First upload should succeed
  let (tx1, rx1) = bd_completion::Sender::new();
  setup.do_explicit_flush(tx1).await;
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();

  let upload1 = setup.next_stat_upload().await;
  upload1
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload1.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();
  rx1.recv().await.unwrap();

  // Immediately trigger another force flush with upload - should be skipped due to minimum interval
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "baz"), "id1", 2);

  let (tx2, rx2) = bd_completion::Sender::new();
  setup.do_explicit_flush(tx2).await;
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();

  // Should complete immediately without upload
  rx2.recv().await.unwrap();

  // Verify no upload occurred
  assert!(poll!(Box::pin(setup.data_rx.recv())).is_pending());

  // Trigger periodic upload tick - should also be skipped
  setup.upload_tick_tx.send(()).await.unwrap();
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();

  // Yield to let the background task process the upload tick
  tokio::task::yield_now().await;

  // Verify no upload occurred
  assert!(poll!(Box::pin(setup.data_rx.recv())).is_pending());

  // Advance time by 10+ seconds
  setup.test_time.advance(Duration::seconds(11));

  // Now force flush should succeed
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "qux"), "id1", 3);

  let (tx3, rx3) = bd_completion::Sender::new();
  setup.do_explicit_flush(tx3).await;
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();

  let upload2 = setup.next_stat_upload().await;
  upload2
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload2.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();
  rx3.recv().await.unwrap();

  // Advance time again
  setup.test_time.advance(Duration::seconds(11));

  // Periodic upload should now succeed
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "quux"), "id1", 4);
  setup.do_periodic_flush().await;

  setup.upload_tick_tx.send(()).await.unwrap();
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();
  let upload3 = setup.next_stat_upload().await;
  upload3
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload3.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();

  // Test failure case: failed uploads should clear the timer and allow immediate retry
  setup.test_time.advance(Duration::seconds(11));
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "fail"), "id1", 5);

  let (tx4, rx4) = bd_completion::Sender::new();
  setup.do_explicit_flush(tx4).await;
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();

  let upload4 = setup.next_stat_upload().await;

  // Fail this upload
  upload4
    .response_tx
    .send(UploadResponse {
      success: false,
      uuid: upload4.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();
  rx4.recv().await.unwrap();

  // Immediately retry should succeed (no minimum interval after failure)
  setup
    .stats
    .record_dynamic_counter(labels!("foo" => "retry"), "id1", 6);

  let (tx5, rx5) = bd_completion::Sender::new();
  setup.do_explicit_flush(tx5).await;
  setup.test_hooks.flush_complete_rx.recv().await.unwrap();

  let upload5 = setup.next_stat_upload().await;
  upload5
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload5.uuid,
    })
    .unwrap();
  setup.test_hooks.upload_complete_rx.recv().await.unwrap();
  rx5.recv().await.unwrap();

  setup.shutdown().await.unwrap();
}
