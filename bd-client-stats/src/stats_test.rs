// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{RealSerializedFileSystem, SerializedFileSystem};
use crate::{DynamicStats, Stats};
use assert_matches::assert_matches;
use async_trait::async_trait;
use bd_api::DataUpload;
use bd_client_stats_store::{make_sketch, Collector};
use bd_proto::protos::client::api::stats_upload_request::snapshot::{
  Aggregated,
  Occurred_at,
  Snapshot_type,
};
use bd_proto::protos::client::api::stats_upload_request::Snapshot as StatsSnapshot;
use bd_proto::protos::client::api::StatsUploadRequest;
use bd_proto::protos::client::metric::metric::Data as MetricData;
use bd_proto::protos::client::metric::{Counter, Metric, MetricsList};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_shutdown::ComponentShutdownTrigger;
use bd_stats_common::labels;
use bd_test_helpers::float_eq;
use bd_time::{TestTimeProvider, TimeDurationExt, TimeProvider, TimestampExt};
use flate2::read::ZlibEncoder;
use flate2::Compression;
use futures_util::poll;
use protobuf::Message;
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use time::{Duration, OffsetDateTime};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinError;

struct Setup<F: SerializedFileSystem + Send + 'static> {
  stats: Arc<Stats>,
  dynamic_stats: Arc<DynamicStats>,
  _directory: tempfile::TempDir,
  shutdown_trigger: ComponentShutdownTrigger,
  _runtime_loader: Arc<ConfigLoader>,
  upload_handle: tokio::task::JoinHandle<()>,
  flush_handle: tokio::task::JoinHandle<()>,
  data_rx: Receiver<DataUpload>,
  test_time: TestTimeProvider,
  phantom: PhantomData<F>,
}

impl Setup<TestSerializedFileSystem> {
  fn new() -> Self {
    Self::new_with_filesystem(Arc::new(TestSerializedFileSystem::new()), None)
  }
}
impl Setup<RealSerializedFileSystem> {
  fn new_with_directory(directory: tempfile::TempDir) -> Self {
    Self::new_with_filesystem(
      Arc::new(RealSerializedFileSystem::new(
        directory.path().to_path_buf(),
      )),
      Some(directory),
    )
  }

  fn compress(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = ZlibEncoder::new(bytes, Compression::new(5));
    let mut compressed_bytes = Vec::new();
    encoder.read_to_end(&mut compressed_bytes).unwrap();
    compressed_bytes
  }

  fn write_pending_upload_file(directory: &Path, bytes: &[u8]) {
    std::fs::write(
      directory.join("pending_stats_upload.pb"),
      Self::compress(bytes),
    )
    .unwrap();
  }

  fn write_aggregated_file(directory: &Path, bytes: &[u8]) {
    std::fs::write(directory.join("aggregated_stats.pb"), Self::compress(bytes)).unwrap();
  }
}

impl<F: SerializedFileSystem + Send + 'static> Setup<F> {
  fn new_with_filesystem(fs: Arc<F>, directory: Option<tempfile::TempDir>) -> Self {
    let directory = directory.unwrap_or_else(|| tempfile::TempDir::with_prefix("sdk").unwrap());
    let test_time = TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH);
    let shutdown_trigger = ComponentShutdownTrigger::default();
    let runtime_loader = ConfigLoader::new(directory.path());
    let dynamic_stats = Arc::new(DynamicStats::new(
      &Collector::default().scope(""),
      &runtime_loader,
    ));
    let stats = Stats::new(Collector::default(), dynamic_stats.clone());
    let (data_tx, data_rx) = tokio::sync::mpsc::channel(1);
    let flush_handles = stats
      .flush_handle_helper(
        &runtime_loader,
        shutdown_trigger.make_shutdown(),
        test_time.clone(),
        data_tx,
        fs,
      )
      .unwrap();

    let flush_handle = tokio::spawn(async move {
      flush_handles.flusher.periodic_flush().await.unwrap();
    });

    let upload_handle =
      tokio::spawn(async move { flush_handles.uploader.upload_stats().await.unwrap() });

    Self {
      test_time,
      stats,
      dynamic_stats,
      _directory: directory,
      shutdown_trigger,
      _runtime_loader: runtime_loader,
      upload_handle,
      flush_handle,
      data_rx,
      phantom: PhantomData,
    }
  }

  async fn with_next_stats_upload_with_result(
    &mut self,
    upload_ok: bool,
    f: impl FnOnce(StatRequestHelper),
  ) {
    wait_for_next_flush().await;
    wait_for_next_flush().await;
    let stats_upload = self.data_rx.recv().await.unwrap();

    let DataUpload::StatsUploadRequest(stats) = stats_upload else {
      panic!("unexpected upload type");
    };

    f(StatRequestHelper::new(stats.payload.clone()));

    stats.response_tx.send(upload_ok).unwrap();
  }

  async fn with_next_stats_upload(&mut self, f: impl FnOnce(StatRequestHelper)) {
    self.with_next_stats_upload_with_result(true, f).await;
  }

  async fn shutdown(self) -> Result<((), ()), JoinError> {
    self.shutdown_trigger.shutdown().await;

    tokio::try_join!(self.flush_handle, self.upload_handle)
  }
}

//
// TestSerializedFileSystem
//

/// An in-memory test implementation of a file system, meant to somewhat mimic the behavior of a
/// real filesystem.
#[derive(Clone)]
struct TestSerializedFileSystem {
  files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
  disk_full: Arc<AtomicBool>,
}

#[async_trait]
impl SerializedFileSystem for TestSerializedFileSystem {
  async fn read_file(&self, path: impl AsRef<Path> + Send) -> anyhow::Result<Vec<u8>> {
    let l = self.files.lock().unwrap();

    l.get(&Self::path_as_str(path))
      .cloned()
      .ok_or_else(|| anyhow::anyhow!("file not found"))
  }

  async fn write_file(
    &self,
    path: impl AsRef<Path> + Send,
    data: impl AsRef<[u8]> + Send,
  ) -> anyhow::Result<()> {
    if self.disk_full.load(Ordering::Relaxed) {
      anyhow::bail!("disk full");
    }

    self
      .files
      .lock()
      .unwrap()
      .insert(Self::path_as_str(path), data.as_ref().to_vec());

    Ok(())
  }

  async fn delete_file(&self, path: impl AsRef<Path> + Send) -> anyhow::Result<()> {
    self.files.lock().unwrap().remove(&Self::path_as_str(path));

    Ok(())
  }

  async fn exists(&self, path: impl AsRef<Path> + Send) -> bool {
    self
      .files
      .lock()
      .unwrap()
      .contains_key(&Self::path_as_str(path))
  }
}

impl TestSerializedFileSystem {
  fn new() -> Self {
    Self {
      files: Arc::new(Mutex::new(HashMap::new())),
      disk_full: Arc::new(AtomicBool::new(false)),
    }
  }

  fn path_as_str(path: impl AsRef<Path>) -> String {
    path.as_ref().as_os_str().to_str().unwrap().to_string()
  }
}

#[tokio::test(start_paused = true)]
async fn report() {
  let mut setup = Setup::new();

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
async fn earliest_aggregation_start_maintained() {
  // This test simulates a few upload failures interleaved with stats being recorded, and verifies
  // that the aggregation start window is set according to when an aggregation period starts and is
  // not updated every time more data is merged into the aggregated data.

  let directory = tempfile::TempDir::with_prefix("stats").unwrap();

  let mut setup = Setup::new_with_directory(directory);

  let counter = setup.stats.collector.scope("test").counter("test");
  counter.inc();

  let t0 = setup.test_time.now();
  // Fail the first attempt to upload. At this point we'll have one cached upload request and an
  // empty aggregated snapshot. We know this since both aggregation and uploads will have been
  // processed by the time we get the upload.
  setup
    .with_next_stats_upload_with_result(false, |upload| {
      assert_eq!(upload.aggregation_window_start(), t0);
      assert_eq!(upload.number_of_metrics(), 1);
    })
    .await;

  setup.test_time.advance(Duration::seconds(1));
  let t1 = setup.test_time.now();

  counter.inc();

  // Fail the second attempt. This attempt should still report for t0, as it's a retry of the old
  // one. At this point we should have started another aggregation window starting at t1.
  setup
    .with_next_stats_upload_with_result(false, |upload| {
      assert_eq!(upload.aggregation_window_start(), t0);
      assert_eq!(upload.number_of_metrics(), 1);
    })
    .await;

  setup.test_time.advance(Duration::seconds(1));
  let t2 = setup.test_time.now();
  assert_ne!(t2, t1);
  counter.inc();

  // The first stat upload is yet another retry of the one from t0.
  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.aggregation_window_start(), t0);
      assert_eq!(upload.number_of_metrics(), 1);
      assert_eq!(upload.get_counter("test:test", labels! {}), Some(1));
    })
    .await;

  // The next upload should contain the aggregation window which started at t1, but contains the
  // increments that happened at t2.
  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.aggregation_window_start(), t1);
      assert_eq!(upload.number_of_metrics(), 2);
      assert_eq!(upload.get_counter("test:test", labels! {}), Some(2));
    })
    .await;

  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn existing_pending_upload() {
  let directory = tempfile::TempDir::with_prefix("stats").unwrap();

  let req = StatsUploadRequest {
    upload_uuid: "test".to_string(),
    ..Default::default()
  };

  Setup::write_pending_upload_file(directory.path(), &req.write_to_bytes().unwrap());

  let mut setup = Setup::new_with_directory(directory);

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.request.upload_uuid, "test");
      // Normally we won't get an empty snapshot list, but because this was already written as a
      // pending upload we get an uncondtional upload.
      assert_eq!(upload.number_of_metrics(), 0);
    })
    .await;
  setup.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn existing_corrupted_pending_upload() {
  let directory = tempfile::TempDir::with_prefix("stats").unwrap();

  // Write garbage data to the upload file. This should then be ignored on startup, so the first
  // upload should be based on fresh stats.
  Setup::write_pending_upload_file(directory.path(), b"not a proto");

  let mut setup = Setup::new_with_directory(directory);

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
  let directory = tempfile::TempDir::with_prefix("stats").unwrap();

  let snapshot = StatsSnapshot {
    snapshot_type: Some(Snapshot_type::Metrics(MetricsList {
      metric: vec![Metric {
        name: "test:blah".to_string(),
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

  Setup::write_aggregated_file(directory.path(), &snapshot.write_to_bytes().unwrap());

  let mut setup = Setup::new_with_directory(directory);

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
async fn corrupted_aggregated_file() {
  let directory = tempfile::TempDir::with_prefix("stats").unwrap();

  Setup::write_aggregated_file(directory.path(), b"not a proto");

  let mut setup = Setup::new_with_directory(directory);

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
  let mut setup = Setup::<TestSerializedFileSystem>::new();

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
  let mut setup = Setup::new();

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
  let mut setup = Setup::new();

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
  let mut setup = Setup::new();

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
  let mut setup = Setup::new();

  setup.dynamic_stats.record_dynamic_counter(
    "foo",
    BTreeMap::from([
      ("k1".to_string(), "v1".to_string()),
      ("k2".to_string(), "v2".to_string()),
    ]),
    5,
  );

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.number_of_metrics(), 1);
      assert_eq!(
        upload.get_counter("foo", labels! { "k1" => "v1", "k2" => "v2"}),
        Some(5)
      );
    })
    .await;

  // Record the same metric multiple times between a flush interval.
  setup.dynamic_stats.record_dynamic_counter(
    "foo",
    BTreeMap::from([
      ("k1".to_string(), "v1".to_string()),
      ("k2".to_string(), "v2".to_string()),
    ]),
    3,
  );

  setup.dynamic_stats.record_dynamic_counter(
    "foo",
    BTreeMap::from([
      ("k1".to_string(), "v1".to_string()),
      ("k2".to_string(), "v2".to_string()),
    ]),
    4,
  );

  // Record a similar metric to ensure that we properly disambiguate metrics by tag values.
  setup.dynamic_stats.record_dynamic_counter(
    "foo",
    BTreeMap::from([
      ("k1".to_string(), "v1".to_string()),
      ("k2".to_string(), "foo".to_string()),
    ]),
    3,
  );

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.number_of_metrics(), 3);
      assert_eq!(
        upload.get_counter("foo", labels! { "k1" => "v1", "k2" => "v2"}),
        Some(7)
      );
      assert_eq!(
        upload.get_counter("foo", labels! { "k1" => "v1", "k2" => "foo"}),
        Some(3)
      );
    })
    .await;

  // During this cycle only record one of the metrics, the other should be dropped.
  setup.dynamic_stats.record_dynamic_counter(
    "foo",
    BTreeMap::from([
      ("k1".to_string(), "v1".to_string()),
      ("k2".to_string(), "foo".to_string()),
    ]),
    5,
  );

  setup
    .with_next_stats_upload(|upload| {
      assert_eq!(upload.number_of_metrics(), 2);
      assert_eq!(
        upload.get_counter("foo", labels! { "k1" => "v1", "k2" => "foo"}),
        Some(5)
      );
    })
    .await;

  setup.shutdown().await.unwrap();
}

async fn wait_for_next_flush() {
  Duration::milliseconds(bd_runtime::runtime::stats::DirectStatFlushIntervalFlag::default().into())
    .advance()
    .await;
}

#[derive(Debug)]
struct StatRequestHelper {
  request: StatsUploadRequest,
}

impl StatRequestHelper {
  fn new(request: StatsUploadRequest) -> Self {
    for snapshot in &request.snapshot {
      assert_matches!(snapshot.occurred_at, Some(Occurred_at::Aggregated(_)));
    }
    Self { request }
  }

  fn aggregation_window_start(&self) -> OffsetDateTime {
    assert_eq!(self.request.snapshot.len(), 1);
    assert_matches!(
      &self.request.snapshot[0].occurred_at,
      Some(Occurred_at::Aggregated(Aggregated {
        period_start,
        special_fields: _
      })) => period_start.to_offset_date_time()
    )
  }

  fn number_of_metrics(&self) -> usize {
    assert!(self.request.snapshot.len() <= 1);
    if self.request.snapshot.is_empty() {
      return 0;
    }

    self.request.snapshot[0].metrics().metric.len()
  }

  fn get_counter(&self, name: &str, fields: BTreeMap<&str, &str>) -> Option<u64> {
    self.get_metric(name, fields).map(|c| c.counter().value)
  }

  #[allow(clippy::float_cmp, clippy::cast_precision_loss)]
  fn expect_ddsketch_histogram(
    &self,
    name: &str,
    fields: BTreeMap<&str, &str>,
    sum: f64,
    count: u64,
  ) {
    let histogram = self
      .get_metric(name, fields)
      .and_then(|c| c.has_ddsketch_histogram().then(|| c.ddsketch_histogram()))
      .expect("missing histogram");

    let mut sketch = make_sketch();
    sketch.decode_and_merge_with(&histogram.serialized).unwrap();
    assert_eq!(sketch.get_count(), count as f64);
    assert!(
      float_eq!(sketch.get_sum().unwrap(), sum),
      "sum: {}, sketch sum: {}",
      sum,
      sketch.get_sum().unwrap()
    );

    // TODO(mattklein123): Add verification for the actual quantiles?
  }

  fn expect_inline_histogram(&self, name: &str, fields: BTreeMap<&str, &str>, values: &[f64]) {
    let histogram = self
      .get_metric(name, fields)
      .and_then(|c| {
        c.has_inline_histogram_values()
          .then(|| c.inline_histogram_values())
      })
      .expect("missing histogram");

    assert_eq!(histogram.values, values);
  }

  #[allow(clippy::needless_pass_by_value)]
  fn get_metric(&self, name: &str, fields: BTreeMap<&str, &str>) -> Option<&Metric> {
    assert_eq!(self.request.snapshot.len(), 1);
    let fields_str = fields
      .iter()
      .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
      .collect();

    self.request.snapshot[0]
      .metrics()
      .metric
      .iter()
      .find(|metric| metric.name == name && metric.tags == fields_str)
  }
}
