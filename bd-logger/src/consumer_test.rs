// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{BufferUploadManager, ContinuousBufferUploader, Flags, service};
use crate::consumer::StreamedBufferUpload;
use assert_matches::assert_matches;
use bd_api::upload::{Tracked, UploadResponse};
use bd_api::{DataUpload, TriggerUpload};
use bd_buffer::{Buffer, BufferEvent, BufferEventWithResponse, RingBuffer, RingBufferStats};
use bd_client_common::fb::{make_log, root_as_log};
use bd_client_stats_store::test::StatsHelper;
use bd_client_stats_store::{Collector, Counter};
use bd_log_primitives::{LogType, log_level};
use bd_proto::protos::client::api::ApiRequest;
use bd_proto::protos::client::api::api_request::Request_type;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_shutdown::ComponentShutdownTrigger;
use bd_stats_common::labels;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_time::{OffsetDateTimeExt as _, TimeDurationExt};
use core::panic;
use flatbuffers::FlatBufferBuilder;
use futures_util::poll;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use time::ext::NumericalDuration;
use tokio::sync::mpsc::{Receiver, Sender, channel};

struct SetupSingleConsumer {
  _global_shutdown_trigger: ComponentShutdownTrigger,
  shutdown_trigger: ComponentShutdownTrigger,
  data_upload_rx: Receiver<DataUpload>,
  buffer: Arc<Buffer>,
  producer: bd_buffer::Producer,
  batch_deadline: Duration,
  records_written_counter: Counter,
  runtime_loader: Arc<ConfigLoader>,
  collector: Collector,

  _sdk_directory: tempfile::TempDir,
}

impl SetupSingleConsumer {
  fn new() -> Self {
    tokio::time::pause();
    let sdk_directory = tempfile::TempDir::with_prefix("sdk").unwrap();

    let records_written_counter = Counter::default();

    let (buffer, producer) = create_continuous_buffer(
      sdk_directory.path().join("buffer").as_path(),
      records_written_counter.clone(),
    );

    let (data_upload_tx, data_upload_rx) = tokio::sync::mpsc::channel(1);

    let shutdown_trigger = ComponentShutdownTrigger::default();
    let global_shutdown_trigger = ComponentShutdownTrigger::default();
    let runtime_loader = ConfigLoader::new(sdk_directory.path());
    let runtime_loader_clone = runtime_loader.clone();
    let collector = Collector::default();

    let uploader = ContinuousBufferUploader::new(
      buffer.create_continous_consumer().unwrap(),
      service::new(
        data_upload_tx,
        global_shutdown_trigger.make_shutdown(),
        &runtime_loader,
        &collector.scope(""),
      ),
      Flags {
        max_batch_size_logs: runtime_loader_clone.register_int_watch(),
        max_match_size_bytes: runtime_loader_clone.register_int_watch(),
        batch_deadline_watch: runtime_loader_clone.register_int_watch(),
        upload_lookback_window_feature_flag: runtime_loader_clone.register_duration_watch(),
        streaming_batch_size: runtime_loader_clone.register_int_watch(),
      },
      shutdown_trigger.make_shutdown(),
      "buffer".to_string(),
    );

    tokio::spawn(async move { uploader.consume_continuous_logs().await });

    Self {
      _global_shutdown_trigger: global_shutdown_trigger,
      shutdown_trigger,
      buffer,
      producer,
      runtime_loader,
      data_upload_rx,
      batch_deadline: Duration::from_secs(3),
      records_written_counter,
      collector,
      _sdk_directory: sdk_directory,
    }
  }

  // Waits until we've recorded N logs written to the non-volatile buffer.
  async fn await_logs_flushed(&self, n: u64) {
    loop {
      if self.records_written_counter.get() == n {
        break;
      }
      tokio::time::sleep(Duration::from_millis(100)).await;
    }
  }

  async fn next_upload(&mut self) -> Tracked<ApiRequest, UploadResponse> {
    match self.data_upload_rx.recv().await.unwrap() {
      DataUpload::LogsUpload(upload) => upload.map_payload(|payload| ApiRequest {
        request_type: Request_type::LogUpload(payload).into(),
        ..Default::default()
      }),
      DataUpload::StatsUpload(upload) => upload.map_payload(|payload| ApiRequest {
        request_type: Request_type::StatsUpload(payload).into(),
        ..Default::default()
      }),
      DataUpload::LogsUploadIntent(_) => panic!("unexpected intent"),
      DataUpload::SankeyPathUploadIntent(_) => panic!("unexpected sankey upload intent"),
      DataUpload::SankeyPathUpload(_) => panic!("unexpected sankey upload"),
      DataUpload::ArtifactUploadIntent(_) => panic!("unexpected artifact upload intent"),
      DataUpload::ArtifactUpload(_) => panic!("unexpected artifact upload"),
    }
  }

  async fn advance_time_to_deadline(&self) {
    tokio::time::advance(self.batch_deadline).await;
  }

  async fn shutdown(self) {
    self.shutdown_trigger.shutdown().await;
  }
}

#[tokio::test]
async fn upload_retries() {
  let mut setup = SetupSingleConsumer::new();

  // Set the batch size to 10 before writing 10 logs that should be uploaded.
  setup
    .runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
      ValueKind::Int(10),
    )]))
    .await;

  // Write logs for one batch upload.
  for _ in 0 .. 10 {
    setup.producer.write(b"a").unwrap();
  }
  setup.buffer.flush();

  // We retry uploads 11 times (unrelated to batch size, 1 + 10 retries), so verify that we get 10
  // upload attempts.
  for _ in 0 .. 11 {
    let log_upload = setup.next_upload().await;
    assert_eq!(log_upload.payload.log_upload().logs.len(), 10);
    log_upload
      .response_tx
      .send(UploadResponse {
        success: false,
        uuid: log_upload.uuid,
      })
      .unwrap();
  }

  tokio::select! {
    () = 1.seconds().sleep() => {},
    _ = setup.data_upload_rx.recv() => panic!("received unexpected upload"),
  }

  // Write another batch of logs.
  for _ in 0 .. 10 {
    setup.producer.write(b"b").unwrap();
  }
  setup.buffer.flush();

  // We should now receive an upload with the logs from the second batch, as we already gave up on
  // the first one.
  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().logs[0], b"b");
  log_upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: log_upload.uuid,
    })
    .unwrap();

  // Now update the max retries to 1 via runtime. The next upload should only retry once.
  setup
    .runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::log_upload::RetryCountFlag::path(),
      ValueKind::Int(1),
    )]))
    .await;

  for _ in 0 .. 10 {
    setup.producer.write(b"c").unwrap();
  }
  setup.buffer.flush();

  for _ in 0 .. 2 {
    let log_upload = setup.next_upload().await;
    log_upload
      .response_tx
      .send(UploadResponse {
        success: false,
        uuid: log_upload.uuid,
      })
      .unwrap();
  }

  tokio::select! {
    () = 1.seconds().sleep() => {},
    _ = setup.data_upload_rx.recv() => panic!("received unexpected upload"),
  }

  setup
    .collector
    .assert_counter_eq(2, "uploader:retry_limit_exceeded", labels! {});
  setup.collector.assert_counter_eq(
    14,
    "uploader:log_upload_attempt",
    labels! {"buffer_id" => "buffer"},
  );
  setup.collector.assert_counter_eq(
    13,
    "uploader:log_upload_failure",
    labels! {"buffer_id" => "buffer"},
  );
  setup
    .collector
    .assert_counter_eq(20, "uploader:retry_limit_exceeded_dropped_logs", labels! {});
}

// Validates that we are limiting the total byte size of the batch upload for the continuous
// buffers.
#[tokio::test]
async fn continuous_buffer_upload_byte_limit() {
  let mut setup = SetupSingleConsumer::new();

  // Set the byte limit per batch to 10. The log limit remains at the default of 1,000.
  setup
    .runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::log_upload::BatchSizeBytesFlag::path(),
      ValueKind::Int(10),
    )]))
    .await;

  for _ in 0 .. 2 {
    setup.producer.write(&[0; 150]).unwrap();
  }

  // The first upload should just one log, as the 150 byte log exceeds the 100 limit.
  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().logs.len(), 1);
  assert_eq!(log_upload.payload.log_upload().logs[0].len(), 150);

  log_upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: log_upload.uuid,
    })
    .unwrap();

  // We should then receive a second upload with the second log line.
  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().logs.len(), 1);
  assert_eq!(log_upload.payload.log_upload().logs[0].len(), 150);
}

// Verifies that we shut down the continuous buffer even if there is a pending log upload.
#[tokio::test]
async fn continuous_buffer_upload_shutdown() {
  let mut setup = SetupSingleConsumer::new();

  // Set the byte limit per batch to 10. The log limit remains at the default of 1,000.
  setup
    .runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::log_upload::BatchSizeBytesFlag::path(),
      ValueKind::Int(10),
    )]))
    .await;

  setup.producer.write(&[0; 150]).unwrap();

  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().logs.len(), 1);

  // Without responding to the upload, we shut down the continuous buffer.
  setup.shutdown().await;
}

// Validates the behavior when the first upload is a full batch, followed
// by a partial batch after the full batch retries once.
#[tokio::test]
async fn uploading_full_batch_failure() {
  let mut setup = SetupSingleConsumer::new();

  // Set the batch size to 10 before writing 11 logs that should be uploaded.
  setup
    .runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
      ValueKind::Int(10),
    )]))
    .await;

  for i in 0 .. 11 {
    setup.producer.write(&[i]).unwrap();
  }

  setup.await_logs_flushed(11).await;

  // The first upload should contain 10 (batch size) logs, starting at the start of the buffer.
  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().logs.len(), 10);
  assert_eq!(log_upload.payload.log_upload().logs[0], &[0]);

  let first_uuid = log_upload.uuid.clone();

  // We signal that the upload failed, which should cause the uploader to re-upload the same logs.
  log_upload
    .response_tx
    .send(UploadResponse {
      success: false,
      uuid: log_upload.uuid,
    })
    .unwrap();

  // The second upload should be the same as the previous one, with the same uuid as before.
  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().logs.len(), 10);
  assert_eq!(log_upload.payload.log_upload().logs[0], &[0]);
  assert_eq!(log_upload.uuid, first_uuid);

  // This time we signal that the upload was ack'd.
  log_upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: log_upload.uuid,
    })
    .unwrap();

  // At this point the uploader is preparing another batch, but this batch is not big enough to
  // flush on its own. Advance the time beyond the deadline period to trigger a partial batch
  // upload.
  setup.advance_time_to_deadline().await;

  // The third upload should only contain one log (11 - 10 = 1).
  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().logs.len(), 1);
  assert_eq!(log_upload.payload.log_upload().logs[0], &[10]);

  // Since this is not retrying the first one, ensure that the uuid is different.
  assert_ne!(log_upload.uuid, first_uuid);

  log_upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: log_upload.uuid,
    })
    .unwrap();

  setup.shutdown().await;
}

#[tokio::test]
async fn uploading_partial_batch_failure() {
  let mut setup = SetupSingleConsumer::new();

  for i in 0 .. 4 {
    setup.producer.write(&[i]).unwrap();
  }

  setup.await_logs_flushed(4).await;

  // We haven't reached the batch limit, but awaiting will have us hit the time deadline once there
  // are no more logs to read.
  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().logs.len(), 4);

  let uuid = log_upload.uuid;

  // Write some more logs into the buffer. These should not be included in the retry attempt.
  for i in 4 .. 10 {
    setup.producer.write(&[i]).unwrap();
  }

  10.seconds().advance().await;

  log_upload
    .response_tx
    .send(UploadResponse {
      success: false,
      uuid: uuid.clone(),
    })
    .unwrap();

  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.uuid, uuid);
  assert_eq!(log_upload.payload.log_upload().logs.len(), 4);

  log_upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: log_upload.uuid,
    })
    .unwrap();

  setup.shutdown().await;
}

// Verifies that the batch deadline works as a total timeout, not an idle timeout.
#[tokio::test]
async fn total_batch_upload_timeout() {
  let mut setup = SetupSingleConsumer::new();

  setup.producer.write(&[1]).unwrap();

  (setup.batch_deadline - 10.milliseconds()).advance().await;

  setup.producer.write(&[1]).unwrap();

  setup.await_logs_flushed(2).await;

  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().logs.len(), 2);

  setup.shutdown().await;
}

#[tokio::test]
async fn uploading_never_succeeds() {
  let mut setup = SetupSingleConsumer::new();

  // Log a single log and advance time far enough to trigger a log upload of the single log.
  setup.producer.write(&[0]).unwrap();

  setup.advance_time_to_deadline().await;

  // Fail the first upload.
  let log_upload = setup.next_upload().await;
  log_upload
    .response_tx
    .send(UploadResponse {
      success: false,
      uuid: log_upload.uuid,
    })
    .unwrap();

  // Fail the second upload.
  let log_upload = setup.next_upload().await;
  log_upload
    .response_tx
    .send(UploadResponse {
      success: false,
      uuid: log_upload.uuid,
    })
    .unwrap();

  // Tear down the structure
  setup.shutdown().await;
}

#[tokio::test]
async fn age_limit_log_uploads() {
  let mut setup = SetupMultiConsumer::new(1, 1000).await;
  let (buffer, mut producer) =
    create_trigger_buffer(setup.temp_directory.path().join("buffer").as_path());

  setup
    .buffer_event_tx
    .send(
      BufferEventWithResponse::new(BufferEvent::TriggerBufferCreated(
        "buffer".to_string(),
        buffer.clone(),
      ))
      .0,
    )
    .await
    .unwrap();

  // Yield to allow processing the above buffer creation event at the right time.
  tokio::task::yield_now().await;

  setup
    .runtime_loader
    .update_snapshot(make_simple_update(vec![
      (
        bd_runtime::runtime::log_upload::FlushBufferLookbackWindow::path(),
        ValueKind::Int(2.minutes().whole_milliseconds().try_into().unwrap()),
      ),
      (
        bd_runtime::runtime::log_upload::BatchDeadlineFlag::path(),
        ValueKind::Int(10),
      ),
    ]))
    .await;

  let now = time::OffsetDateTime::now_utc().floor(1.minutes());
  for i in (0 .. 5).rev() {
    producer
      .write(&make_test_log(now - time::Duration::minutes(i)))
      .unwrap();
  }

  setup.trigger_buffer_upload("buffer").await;

  // We should only get the 2 most recent logs.
  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().logs.len(), 2);
  assert_eq!(
    time::OffsetDateTime::from_unix_timestamp(
      root_as_log(&log_upload.payload.log_upload().logs[0])
        .unwrap()
        .timestamp()
        .unwrap()
        .seconds()
    )
    .unwrap(),
    now - time::Duration::minutes(1)
  );

  setup
    .stats
    .assert_counter_eq(3, "consumer:old_logs_dropped", labels! {});

  // Tear down the structure
  setup.shutdown().await;
}

fn make_test_log(t: time::OffsetDateTime) -> Vec<u8> {
  let mut fbb = FlatBufferBuilder::new();

  let mut log = Vec::new();

  make_log(
    &mut fbb,
    log_level::INFO,
    LogType::Normal,
    &"".into(),
    &[].into(),
    "",
    t,
    std::iter::empty(),
    std::iter::empty(),
    |l| {
      log = l.to_vec();
      Ok(())
    },
  )
  .unwrap();

  log
}

struct SetupMultiConsumer {
  log_upload_rx: Receiver<DataUpload>,
  shutdown_trigger: ComponentShutdownTrigger,
  buffer_event_tx: Sender<BufferEventWithResponse>,
  trigger_upload_tx: Sender<TriggerUpload>,
  runtime_loader: Arc<ConfigLoader>,
  stats: Collector,

  temp_directory: tempfile::TempDir,
}

impl SetupMultiConsumer {
  async fn new(batch_size: u32, byte_limit: u32) -> Self {
    tokio::time::pause();

    let stats = Collector::default();
    let temp_directory = TempDir::with_prefix("consumertest").unwrap();
    let (buffer_event_tx, buffer_event_rx) = channel(1);

    let (log_upload_tx, log_upload_rx) = tokio::sync::mpsc::channel(1);
    let (trigger_upload_tx, trigger_upload_rx) = tokio::sync::mpsc::channel(1);

    let shutdown_trigger = ComponentShutdownTrigger::default();
    let config_loader = ConfigLoader::new(temp_directory.path());
    config_loader
      .update_snapshot(make_simple_update(vec![
        (
          bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
          ValueKind::Int(batch_size),
        ),
        (
          bd_runtime::runtime::log_upload::BatchSizeBytesFlag::path(),
          ValueKind::Int(byte_limit),
        ),
      ]))
      .await;
    let shutdown = shutdown_trigger.make_shutdown();
    let config_loader_clone = config_loader.clone();
    let collector_clone = stats.clone();
    tokio::spawn(async move {
      BufferUploadManager::new(
        log_upload_tx,
        &config_loader_clone,
        shutdown,
        buffer_event_rx,
        trigger_upload_rx,
        &collector_clone.scope("consumer"),
        bd_internal_logging::NoopLogger::new(),
      )
      .run()
      .await
      .unwrap();
    });

    Self {
      log_upload_rx,
      shutdown_trigger,
      buffer_event_tx,
      trigger_upload_tx,
      temp_directory,
      runtime_loader: config_loader,
      stats,
    }
  }

  async fn shutdown(self) {
    self.shutdown_trigger.shutdown().await;
  }

  async fn trigger_buffer_upload(&self, buffer: &str) -> tokio::sync::oneshot::Receiver<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let upload = TriggerUpload::new(vec![buffer.to_string()], tx);
    self.trigger_upload_tx.send(upload).await.unwrap();

    rx
  }

  async fn next_upload(&mut self) -> Tracked<ApiRequest, UploadResponse> {
    match self.log_upload_rx.recv().await.unwrap() {
      DataUpload::LogsUpload(upload) => upload.map_payload(|p| ApiRequest {
        request_type: Some(Request_type::LogUpload(p)),
        ..Default::default()
      }),
      _ => panic!("unexpected upload"),
    }
  }
}

#[tokio::test]
async fn upload_multiple_continuous_buffers() {
  let directory = tempfile::TempDir::with_prefix("consumer-").unwrap();
  let mut setup = SetupMultiConsumer::new(1, 1000).await;

  let (buffer_a, mut producer_a) = create_continuous_buffer(
    &directory.path().join(PathBuf::from("buffer.a")),
    Counter::default(),
  );

  let (buffer_b, mut producer_b) = create_continuous_buffer(
    &directory.path().join(PathBuf::from("buffer.b")),
    Counter::default(),
  );

  setup
    .buffer_event_tx
    .send(
      BufferEventWithResponse::new(BufferEvent::ContinuousBufferCreated(
        "buffer_a".to_string(),
        buffer_a.clone(),
      ))
      .0,
    )
    .await
    .unwrap();
  setup
    .buffer_event_tx
    .send(
      BufferEventWithResponse::new(BufferEvent::ContinuousBufferCreated(
        "buffer_b".to_string(),
        buffer_b.clone(),
      ))
      .0,
    )
    .await
    .unwrap();

  producer_a.write(b"a").unwrap();
  producer_b.write(b"b").unwrap();

  1.seconds().advance().await;
  let upload_1 = setup.next_upload().await;

  1.seconds().advance().await;
  let upload_2 = setup.next_upload().await;

  let upload_1_payload = upload_1.payload;
  // The order is unspecified, so figure out which one is which.
  if *upload_1_payload.log_upload().logs == vec![b"a".to_vec()] {
    assert_eq!(*upload_2.payload.log_upload().logs, vec![b"b".to_vec()]);
    upload_1
      .response_tx
      .send(UploadResponse {
        success: true,
        uuid: upload_1.uuid,
      })
      .unwrap();
  } else {
    assert_eq!(*upload_1_payload.log_upload().logs, vec![b"b".to_vec()]);
    assert_eq!(*upload_2.payload.log_upload().logs, vec![b"a".to_vec()]);
    upload_2
      .response_tx
      .send(UploadResponse {
        success: true,
        uuid: upload_2.uuid,
      })
      .unwrap();
  }

  // Remove one of the buffers, making sure that uploads still work.
  setup
    .buffer_event_tx
    .send(BufferEventWithResponse::new(BufferEvent::BufferRemoved("buffer_b".to_string())).0)
    .await
    .unwrap();

  producer_a.write(b"a2").unwrap();

  10.seconds().advance().await;

  let upload_3 = setup.next_upload().await;
  assert_eq!(*upload_3.payload.log_upload().logs, vec![b"a2".to_vec()]);

  // We intentionally leave both the response for buffer b and a open to validate that we don't
  // require an ack for proper shutdown.

  setup.shutdown().await;
}

#[tokio::test]
async fn remove_trigger_buffer() {
  let setup = SetupMultiConsumer::new(1, 1000).await;

  // Verify that nothing bad happens if we remove a buffer that hasn't been added as a continous
  // one.
  setup
    .buffer_event_tx
    .send(BufferEventWithResponse::new(BufferEvent::BufferRemoved("buffer".to_string())).0)
    .await
    .unwrap();

  setup.shutdown().await;
}

#[tokio::test]
async fn trigger_upload_byte_size_limit() {
  // Allow 10 logs or 100 bytes per upload.
  let mut setup = SetupMultiConsumer::new(10, 100).await;
  let (buffer, mut producer) =
    create_trigger_buffer(setup.temp_directory.path().join("buffer").as_path());

  setup
    .buffer_event_tx
    .send(
      BufferEventWithResponse::new(BufferEvent::TriggerBufferCreated(
        "buffer".to_string(),
        buffer.clone(),
      ))
      .0,
    )
    .await
    .unwrap();

  tokio::task::yield_now().await;

  producer.write(&[0; 150]).unwrap();
  producer.write(&[0; 150]).unwrap();

  setup.trigger_buffer_upload("buffer").await;

  log::debug!("waiting for upload");
  let upload = setup.next_upload().await;
  assert_eq!(upload.payload.log_upload().logs.len(), 1);
}

#[tokio::test]
async fn dropped_trigger() {
  let mut setup = SetupMultiConsumer::new(1, 1000).await;
  let (buffer, mut producer) =
    create_trigger_buffer(setup.temp_directory.path().join("buffer").as_path());

  setup
    .buffer_event_tx
    .send(
      BufferEventWithResponse::new(BufferEvent::TriggerBufferCreated(
        "buffer".to_string(),
        buffer.clone(),
      ))
      .0,
    )
    .await
    .unwrap();

  // Yield to allow processing the above buffer creation event at the right time.
  tokio::task::yield_now().await;

  producer.write(b"one").unwrap();
  producer.write(b"two").unwrap();

  setup.trigger_buffer_upload("buffer").await;

  let pinned = Box::pin(setup.log_upload_rx.recv());
  assert!(poll!(pinned).is_pending());
}

#[tokio::test]
async fn uploaded_trigger() {
  let mut setup = SetupMultiConsumer::new(2, 1000).await;
  let (buffer, mut producer) =
    create_trigger_buffer(setup.temp_directory.path().join("buffer").as_path());

  setup
    .buffer_event_tx
    .send(
      BufferEventWithResponse::new(BufferEvent::TriggerBufferCreated(
        "buffer".to_string(),
        buffer.clone(),
      ))
      .0,
    )
    .await
    .unwrap();

  // Yield to allow processing the above buffer creation event at the right time.
  tokio::task::yield_now().await;

  producer.write(b"one").unwrap();
  producer.write(b"two").unwrap();

  setup.trigger_buffer_upload("buffer").await;

  let upload = setup.next_upload().await;

  assert_eq!(upload.payload.log_upload().logs.len(), 2);
}

#[tokio::test]
async fn log_streaming() {
  let buffer = bd_buffer::buffer::VolatileRingBuffer::new(
    "test".to_string(),
    1024 * 1024,
    Arc::new(RingBufferStats::default()),
  );

  let mut producer = buffer.clone().register_producer().unwrap();

  let consumer = buffer.clone().register_consumer().unwrap();

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let runtime_loader = ConfigLoader::new(&PathBuf::from("."));

  let (log_upload_tx, mut log_upload_rx) = tokio::sync::mpsc::channel(1);

  let upload_service = service::new(
    log_upload_tx,
    shutdown_trigger.make_shutdown(),
    &runtime_loader,
    &Collector::default().scope(""),
  );

  tokio::task::spawn(async move {
    StreamedBufferUpload {
      consumer,
      log_upload_service: upload_service,
      shutdown: shutdown_trigger.make_shutdown(),
      batch_size: runtime_loader.register_int_watch(),
    }
    .start()
    .await
    .unwrap();
  });

  producer.write(b"data").unwrap();
  producer.write(b"more data").unwrap();

  assert_matches!(log_upload_rx.recv().await.unwrap(), DataUpload::LogsUpload(upload) => {
      assert_eq!(upload.payload.logs.len(), 2);
    assert_eq!(upload.payload.logs[0], b"data");
    assert_eq!(upload.payload.logs[1], b"more data");
    upload.response_tx.send(UploadResponse {
      success: true,
      uuid: upload.uuid,
    }).unwrap();
  });
}

#[tokio::test]
async fn streaming_batch_size_flag() {
  use bd_buffer::buffer::VolatileRingBuffer;
  use bd_client_stats_store::Collector;
  use std::sync::Arc;

  let buffer = VolatileRingBuffer::new(
    "test_stream_batch".to_string(),
    1024 * 1024,
    Arc::new(bd_buffer::RingBufferStats::default()),
  );

  let mut producer = buffer.clone().register_producer().unwrap();
  let consumer = buffer.clone().register_consumer().unwrap();
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let runtime_loader = ConfigLoader::new(&PathBuf::from("."));
  let (log_upload_tx, mut log_upload_rx) = tokio::sync::mpsc::channel(1);
  // Set streaming batch size to 2
  runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::log_upload::StreamingBatchSizeFlag::path(),
      ValueKind::Int(2),
    )]))
    .await;

  let upload_service = service::new(
    log_upload_tx,
    shutdown_trigger.make_shutdown(),
    &runtime_loader,
    &Collector::default().scope(""),
  );

  tokio::task::spawn(async move {
    StreamedBufferUpload {
      consumer,
      log_upload_service: upload_service,
      batch_size: runtime_loader.register_int_watch(),
      shutdown: shutdown_trigger.make_shutdown(),
    }
    .start()
    .await
    .unwrap();
  });

  // Write three logs; expect the first upload to contain two logs due to batch size
  producer.write(b"foo").unwrap();
  producer.write(b"bar").unwrap();
  producer.write(b"baz").unwrap();

  // Should batch foo+bar (due to batch size = 2)
  assert_matches!(log_upload_rx.recv().await.unwrap(), DataUpload::LogsUpload(upload) => {
    assert_eq!(upload.payload.logs, vec![b"foo".to_vec(), b"bar".to_vec()]);
    upload.response_tx.send(UploadResponse {
      success: true,
      uuid: upload.uuid,
    }).unwrap();
  });
  // Next upload should contain "baz"
  assert_matches!(log_upload_rx.recv().await.unwrap(), DataUpload::LogsUpload(upload) => {
    assert_eq!(upload.payload.logs, vec![b"baz".to_vec()]);
    upload.response_tx.send(UploadResponse {
      success: true,
      uuid: upload.uuid,
    }).unwrap();
  });
}

#[tokio::test]
async fn log_streaming_shutdown() {
  let buffer = bd_buffer::buffer::VolatileRingBuffer::new(
    "test".to_string(),
    1024 * 1024,
    Arc::new(RingBufferStats::default()),
  );

  let mut producer = buffer.clone().register_producer().unwrap();

  let consumer = buffer.clone().register_consumer().unwrap();

  // During real usage the service and the consumer task have different shutdown handles. We want
  // to make sure that the per-consumer task shutdown works right.
  let global_shutdown_trigger = ComponentShutdownTrigger::default();
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let runtime_loader = ConfigLoader::new(&PathBuf::from("."));

  let (log_upload_tx, mut log_upload_rx) = tokio::sync::mpsc::channel(1);

  let upload_service = service::new(
    log_upload_tx,
    global_shutdown_trigger.make_shutdown(),
    &runtime_loader,
    &Collector::default().scope(""),
  );

  let shutdown = shutdown_trigger.make_shutdown();

  let task_handle = tokio::task::spawn(async move {
    StreamedBufferUpload {
      consumer,
      log_upload_service: upload_service,
      shutdown,
      batch_size: runtime_loader.register_int_watch(),
    }
    .start()
    .await
    .unwrap();
  });

  producer.write(b"data").unwrap();

  // Receive the upload request but do not complete it.
  assert_matches!(log_upload_rx.recv().await.unwrap(), DataUpload::LogsUpload(upload) => {
    assert_eq!(upload.payload.logs[0], b"data");
  });

  // Perform a shutdown, making sure that we complete the shutdown even if there is a pending
  // upload.
  shutdown_trigger.shutdown().await;
  task_handle.await.unwrap();
}

fn create_continuous_buffer(
  buffer: &Path,
  records_written: Counter,
) -> (Arc<Buffer>, bd_buffer::Producer) {
  create_buffer(false, buffer, records_written)
}

fn create_trigger_buffer(buffer: &Path) -> (Arc<Buffer>, bd_buffer::Producer) {
  create_buffer(true, buffer, Counter::default())
}

fn create_buffer(
  trigger_buffer: bool,
  buffer: &Path,
  records_written: Counter,
) -> (Arc<Buffer>, bd_buffer::Producer) {
  let (generic_ring_buffer, _) = Buffer::new(
    "test",
    1000,
    buffer.to_path_buf(),
    100_000,
    trigger_buffer,
    Counter::default(),
    Counter::default(),
    Counter::default(),
    Counter::default(),
    Counter::default(),
    Some(records_written),
  )
  .unwrap();

  (
    generic_ring_buffer.clone(),
    generic_ring_buffer.new_thread_local_producer().unwrap(),
  )
}
