// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{BufferUploadManager, ContinuousBufferUploader, Flags, service};
use crate::consumer::{BatchBuilder, RemoteFlushStreamingRequest, StreamedBufferUpload};
use crate::flush_registry::{
  PendingTriggerUploadsStore,
  PersistedTriggerUpload,
  PersistedTriggerUploadBufferLifecycle,
  PersistedTriggerUploadBufferProgress,
  PersistedTriggerUploadLifecycle,
  PersistedTriggerUploadSource,
  PersistedTriggerUploadStreaming,
};
use crate::trigger_upload_artifact::TriggerUploadArtifactStore;
use action_flush_buffers::Streaming as FlushStreaming;
use assert_matches::assert_matches;
use bd_api::upload::{Tracked, UploadResponse};
use bd_api::{DataUpload, TriggerUpload, TriggerUploadSource};
use bd_buffer::{Buffer, BufferEvent, BufferEventWithResponse, RingBuffer, RingBufferStats};
use bd_client_stats_store::test::StatsHelper;
use bd_client_stats_store::{Collector, Counter};
use bd_log_primitives::{EncodableLog, Log, log_level};
use bd_proto::protos::client::api::ApiRequest;
use bd_proto::protos::client::api::api_request::Request_type;
use bd_proto::protos::logging::payload::{Log as ProtoLog, LogType};
use bd_proto::protos::workflow::workflow::workflow::action::action_flush_buffers;
use bd_resilient_kv::{RetentionHandle, RetentionRegistry};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_shutdown::ComponentShutdownTrigger;
use bd_stats_common::labels;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_time::{OffsetDateTimeExt as _, TimeDurationExt};
use bd_workflows::config::FlushBufferId;
use bd_workflows::engine::FlushCompletionTracker;
use core::panic;
use futures_util::poll;
use protobuf::{CodedOutputStream, Message};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use time::ext::NumericalDuration;
use tokio::sync::mpsc::{Receiver, Sender, channel};

fn make_flags(runtime_loader: &ConfigLoader) -> Flags {
  Flags {
    max_batch_size_logs: runtime_loader.register_int_watch(),
    max_match_size_bytes: runtime_loader.register_int_watch(),
    batch_deadline: runtime_loader.register_duration_watch(),
    upload_lookback_window_feature_flag: runtime_loader.register_duration_watch(),
    streaming_batch_deadline: runtime_loader.register_duration_watch(),
  }
}

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
  async fn new() -> Self {
    tokio::time::pause();
    let sdk_directory = tempfile::TempDir::with_prefix("sdk").unwrap();

    let records_written_counter = Counter::default();

    let (buffer, producer) = create_continuous_buffer(
      sdk_directory.path().join("buffer").as_path(),
      records_written_counter.clone(),
    )
    .await;

    let (data_upload_tx, data_upload_rx) = tokio::sync::mpsc::channel(1);

    let shutdown_trigger = ComponentShutdownTrigger::default();
    let global_shutdown_trigger = ComponentShutdownTrigger::default();
    let runtime_loader = ConfigLoader::new(sdk_directory.path());
    let collector = Collector::default();

    let consumer = buffer.create_continous_consumer().unwrap();
    let retention_handle = consumer.retention_handle();
    let uploader = ContinuousBufferUploader::new(
      consumer,
      retention_handle,
      service::new(
        data_upload_tx,
        global_shutdown_trigger.make_shutdown(),
        &runtime_loader,
        &collector.scope(""),
      ),
      make_flags(&runtime_loader),
      shutdown_trigger.make_shutdown(),
      "buffer".to_string(),
      None,
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
      DataUpload::AcklessLogsUpload(_) => panic!("unexpected ackless upload"),
      DataUpload::DebugData(_) => panic!("unexpected debug data upload"),
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
  let mut setup = SetupSingleConsumer::new().await;

  // Set the batch size to 10 before writing 10 logs that should be uploaded.
  setup
    .runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
      ValueKind::Int(10),
    )]))
    .await
    .unwrap();

  // Write logs for one batch upload.
  for _ in 0 .. 10 {
    setup.producer.write(b"a").unwrap();
  }
  setup.buffer.flush();

  // We retry uploads 11 times (unrelated to batch size, 1 + 10 retries), so verify that we get 10
  // upload attempts.
  for _ in 0 .. 11 {
    let log_upload = setup.next_upload().await;
    assert_eq!(log_upload.payload.log_upload().proto_logs.len(), 10);
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
  assert_eq!(log_upload.payload.log_upload().proto_logs[0], b"b");
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
    .await
    .unwrap();

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
  let mut setup = SetupSingleConsumer::new().await;

  // Set the byte limit per batch to 10. The log limit remains at the default of 1,000.
  setup
    .runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::log_upload::BatchSizeBytesFlag::path(),
      ValueKind::Int(10),
    )]))
    .await
    .unwrap();

  for _ in 0 .. 2 {
    setup.producer.write(&[0; 150]).unwrap();
  }

  // The first upload should just one log, as the 150 byte log exceeds the 100 limit.
  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().proto_logs.len(), 1);
  assert_eq!(log_upload.payload.log_upload().proto_logs[0].len(), 150);

  log_upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: log_upload.uuid,
    })
    .unwrap();

  // We should then receive a second upload with the second log line.
  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().proto_logs.len(), 1);
  assert_eq!(log_upload.payload.log_upload().proto_logs[0].len(), 150);
}

// Verifies that we shut down the continuous buffer even if there is a pending log upload.
#[tokio::test]
async fn continuous_buffer_upload_shutdown() {
  let mut setup = SetupSingleConsumer::new().await;

  // Set the byte limit per batch to 10. The log limit remains at the default of 1,000.
  setup
    .runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::log_upload::BatchSizeBytesFlag::path(),
      ValueKind::Int(10),
    )]))
    .await
    .unwrap();

  setup.producer.write(&[0; 150]).unwrap();

  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().proto_logs.len(), 1);

  // Without responding to the upload, we shut down the continuous buffer.
  setup.shutdown().await;
}

#[tokio::test]
async fn continuous_buffer_sets_retention_none_when_batch_drains_buffer() {
  tokio::time::pause();
  let sdk_directory = tempfile::TempDir::with_prefix("sdk").unwrap();
  let runtime_loader = Arc::new(ConfigLoader::new(sdk_directory.path()));
  runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
      ValueKind::Int(1),
    )]))
    .await
    .unwrap();

  let retention_registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let retention_handle = retention_registry.create_handle().await;
  retention_handle.update_retention_micros(RetentionHandle::RETENTION_NONE);

  let (buffer, mut producer) = create_continuous_buffer_with_retention(
    sdk_directory.path().join("buffer").as_path(),
    Counter::default(),
    retention_handle.clone(),
  );

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let global_shutdown_trigger = ComponentShutdownTrigger::default();
  let (data_upload_tx, mut data_upload_rx) = tokio::sync::mpsc::channel(1);
  let consumer = buffer.create_continous_consumer().unwrap();
  let uploader_retention_handle = consumer.retention_handle();
  let uploader = ContinuousBufferUploader::new(
    consumer,
    uploader_retention_handle,
    service::new(
      data_upload_tx,
      global_shutdown_trigger.make_shutdown(),
      &runtime_loader,
      &Collector::default().scope(""),
    ),
    make_flags(&runtime_loader),
    shutdown_trigger.make_shutdown(),
    "buffer".to_string(),
    None,
  );
  tokio::spawn(async move { uploader.consume_continuous_logs().await.unwrap() });

  producer
    .write(&make_test_log(time::OffsetDateTime::now_utc()))
    .unwrap();
  buffer.flush();

  let upload = match data_upload_rx.recv().await.unwrap() {
    DataUpload::LogsUpload(upload) => upload,
    other => panic!("unexpected upload kind: {other:?}"),
  };

  assert_ne!(
    retention_handle.get_retention(),
    RetentionHandle::RETENTION_NONE
  );
  upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload.uuid,
    })
    .unwrap();

  for _ in 0 .. 20 {
    if retention_handle.get_retention() == RetentionHandle::RETENTION_NONE {
      break;
    }
    tokio::task::yield_now().await;
  }
  assert_eq!(
    retention_handle.get_retention(),
    RetentionHandle::RETENTION_NONE
  );

  shutdown_trigger.shutdown().await;
}

// Validates the behavior when the first upload is a full batch, followed
// by a partial batch after the full batch retries once.
#[tokio::test]
async fn uploading_full_batch_failure() {
  let mut setup = SetupSingleConsumer::new().await;

  // Set the batch size to 10 before writing 11 logs that should be uploaded.
  setup
    .runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
      ValueKind::Int(10),
    )]))
    .await
    .unwrap();

  for i in 0 .. 11 {
    setup.producer.write(&[i]).unwrap();
  }

  setup.await_logs_flushed(11).await;

  // The first upload should contain 10 (batch size) logs, starting at the start of the buffer.
  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().proto_logs.len(), 10);
  assert_eq!(log_upload.payload.log_upload().proto_logs[0], &[0]);

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
  assert_eq!(log_upload.payload.log_upload().proto_logs.len(), 10);
  assert_eq!(log_upload.payload.log_upload().proto_logs[0], &[0]);
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
  assert_eq!(log_upload.payload.log_upload().proto_logs.len(), 1);
  assert_eq!(log_upload.payload.log_upload().proto_logs[0], &[10]);

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
  let mut setup = SetupSingleConsumer::new().await;

  for i in 0 .. 4 {
    setup.producer.write(&[i]).unwrap();
  }

  setup.await_logs_flushed(4).await;

  // We haven't reached the batch limit, but awaiting will have us hit the time deadline once there
  // are no more logs to read.
  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().proto_logs.len(), 4);

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
  assert_eq!(log_upload.payload.log_upload().proto_logs.len(), 4);

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
  let mut setup = SetupSingleConsumer::new().await;

  setup.producer.write(&[1]).unwrap();

  (setup.batch_deadline - 10.milliseconds()).advance().await;

  setup.producer.write(&[1]).unwrap();

  setup.await_logs_flushed(2).await;

  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().proto_logs.len(), 2);

  setup.shutdown().await;
}

#[tokio::test]
async fn uploading_never_succeeds() {
  let mut setup = SetupSingleConsumer::new().await;

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
    create_trigger_buffer(setup.sdk_directory.join("buffer").as_path()).await;

  setup.add_trigger_buffer("buffer", buffer.clone()).await;

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
    .await
    .unwrap();

  let now = time::OffsetDateTime::now_utc().floor(1.minutes());
  for i in (0 .. 5).rev() {
    producer
      .write(&make_test_log(now - time::Duration::minutes(i)))
      .unwrap();
  }

  setup.trigger_buffer_upload("buffer").await;

  // We should only get the 2 most recent logs.
  let log_upload = setup.next_upload().await;
  assert_eq!(log_upload.payload.log_upload().proto_logs.len(), 2);
  assert_eq!(
    time::OffsetDateTime::from_unix_timestamp_nanos(
      (ProtoLog::parse_from_bytes(&log_upload.payload.log_upload().proto_logs[0])
        .unwrap()
        .timestamp_unix_micro
        * 1_000)
        .into()
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
  let mut log = EncodableLog::new(
    Log {
      log_level: log_level::INFO,
      log_type: LogType::NORMAL,
      message: "".into(),
      fields: [].into(),
      matching_fields: [].into(),
      session_id: String::new(),
      occurred_at: t,
      capture_session: None,
    },
    u64::MAX,
  );
  let mut output = Vec::with_capacity(log.compute_size(&[], &[]).unwrap().try_into().unwrap());
  let mut os = CodedOutputStream::vec(&mut output);
  log.serialize_to_stream(&[], &[], &mut os).unwrap();
  drop(os);
  output
}

struct SetupMultiConsumer {
  log_upload_rx: Receiver<DataUpload>,
  shutdown_trigger: ComponentShutdownTrigger,
  buffer_event_tx: Sender<BufferEventWithResponse>,
  trigger_upload_tx: Sender<TriggerUpload>,
  flush_completion_tracker: Arc<FlushCompletionTracker>,
  runtime_loader: Arc<ConfigLoader>,
  stats: Collector,
  sdk_directory: PathBuf,

  temp_directory: Option<tempfile::TempDir>,
}

impl SetupMultiConsumer {
  async fn new_with_sdk_directory(
    batch_size: u32,
    byte_limit: u32,
    sdk_directory: PathBuf,
    temp_directory: Option<tempfile::TempDir>,
    remote_flush_streaming_tx: Sender<super::RemoteFlushStreamingRequest>,
  ) -> Self {
    let stats = Collector::default();
    let (buffer_event_tx, buffer_event_rx) = channel(1);

    let (log_upload_tx, log_upload_rx) = tokio::sync::mpsc::channel(1);
    let (trigger_upload_tx, trigger_upload_rx) = tokio::sync::mpsc::channel(1);
    let flush_completion_tracker = Arc::new(FlushCompletionTracker::default());

    let shutdown_trigger = ComponentShutdownTrigger::default();
    let config_loader = ConfigLoader::new(&sdk_directory);
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
      .await
      .unwrap();
    let shutdown = shutdown_trigger.make_shutdown();
    let config_loader_clone = config_loader.clone();
    let collector_clone = stats.clone();
    let sdk_directory_clone = sdk_directory.clone();
    let flush_completion_tracker_clone = flush_completion_tracker.clone();
    tokio::spawn(async move {
      BufferUploadManager::new(
        log_upload_tx,
        &config_loader_clone,
        &sdk_directory_clone,
        shutdown,
        buffer_event_rx,
        trigger_upload_rx,
        remote_flush_streaming_tx,
        &collector_clone.scope("consumer"),
        bd_internal_logging::NoopLogger::new(),
        None,
        PendingTriggerUploadsStore::new(&sdk_directory_clone),
        flush_completion_tracker_clone,
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
      flush_completion_tracker,
      runtime_loader: config_loader,
      stats,
      sdk_directory,
      temp_directory,
    }
  }

  async fn new_with_remote_streaming_channel(
    batch_size: u32,
    byte_limit: u32,
    temp_directory: tempfile::TempDir,
    remote_flush_streaming_tx: Sender<super::RemoteFlushStreamingRequest>,
  ) -> Self {
    let sdk_directory = temp_directory.path().to_path_buf();
    Self::new_with_sdk_directory(
      batch_size,
      byte_limit,
      sdk_directory,
      Some(temp_directory),
      remote_flush_streaming_tx,
    )
    .await
  }

  async fn new_with_state(
    batch_size: u32,
    byte_limit: u32,
    temp_directory: tempfile::TempDir,
  ) -> Self {
    let (remote_flush_streaming_tx, _remote_flush_streaming_rx) = tokio::sync::mpsc::channel(1);

    Self::new_with_remote_streaming_channel(
      batch_size,
      byte_limit,
      temp_directory,
      remote_flush_streaming_tx,
    )
    .await
  }

  async fn new(batch_size: u32, byte_limit: u32) -> Self {
    tokio::time::pause();

    Self::new_with_state(
      batch_size,
      byte_limit,
      TempDir::with_prefix("consumertest").unwrap(),
    )
    .await
  }

  async fn shutdown(self) {
    self.shutdown_trigger.shutdown().await;
  }

  async fn shutdown_persisting_directory(self) -> PathBuf {
    let sdk_directory = self.sdk_directory.clone();
    self.shutdown_trigger.shutdown().await;
    if let Some(temp_directory) = self.temp_directory {
      let _persisted = temp_directory.keep();
    }
    sdk_directory
  }

  async fn trigger_buffer_upload(&self, buffer: &str) {
    let upload = TriggerUpload::new(
      vec![buffer.to_string()],
      None,
      TriggerUploadSource::ExplicitSessionCapture("session-capture".to_string()),
      "session-1".to_string(),
    );
    self.trigger_upload_tx.send(upload).await.unwrap();
  }

  async fn add_trigger_buffer(&self, buffer_id: &str, buffer: Arc<Buffer>) {
    let (event, processed_rx) = BufferEventWithResponse::new(BufferEvent::TriggerBufferCreated(
      buffer_id.to_string(),
      buffer,
    ));
    self.buffer_event_tx.send(event).await.unwrap();
    processed_rx.await.unwrap();
  }

  async fn sync_trigger_buffer_config(&self, buffer_ids: &[&str]) {
    let (event, processed_rx) =
      BufferEventWithResponse::new(BufferEvent::TriggerBufferConfigUpdated(
        buffer_ids
          .iter()
          .map(|buffer_id| (*buffer_id).to_string())
          .collect(),
      ));
    self.buffer_event_tx.send(event).await.unwrap();
    processed_rx.await.unwrap();
  }

  async fn pending_trigger_uploads(&self) -> Vec<PersistedTriggerUpload> {
    PendingTriggerUploadsStore::new(&self.sdk_directory)
      .pending_uploads()
      .await
  }

  fn flush_is_pending(&self, flush_id: &FlushBufferId) -> bool {
    self.flush_completion_tracker.is_pending(flush_id)
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
  )
  .await;

  let (buffer_b, mut producer_b) = create_continuous_buffer(
    &directory.path().join(PathBuf::from("buffer.b")),
    Counter::default(),
  )
  .await;

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
  if *upload_1_payload.log_upload().proto_logs == vec![b"a".to_vec()] {
    assert_eq!(
      *upload_2.payload.log_upload().proto_logs,
      vec![b"b".to_vec()]
    );
    upload_1
      .response_tx
      .send(UploadResponse {
        success: true,
        uuid: upload_1.uuid,
      })
      .unwrap();
  } else {
    assert_eq!(
      *upload_1_payload.log_upload().proto_logs,
      vec![b"b".to_vec()]
    );
    assert_eq!(
      *upload_2.payload.log_upload().proto_logs,
      vec![b"a".to_vec()]
    );
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
  assert_eq!(
    *upload_3.payload.log_upload().proto_logs,
    vec![b"a2".to_vec()]
  );

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
    create_trigger_buffer(setup.sdk_directory.join("buffer").as_path()).await;

  setup.add_trigger_buffer("buffer", buffer.clone()).await;

  producer.write(&[0; 150]).unwrap();
  producer.write(&[0; 150]).unwrap();

  setup.trigger_buffer_upload("buffer").await;

  log::debug!("waiting for upload");
  let upload = setup.next_upload().await;
  assert_eq!(upload.payload.log_upload().proto_logs.len(), 1);
}

#[tokio::test]
async fn dropped_trigger() {
  let mut setup = SetupMultiConsumer::new(1, 1000).await;
  let (buffer, mut producer) =
    create_trigger_buffer(setup.sdk_directory.join("buffer").as_path()).await;

  setup.add_trigger_buffer("buffer", buffer.clone()).await;

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
    create_trigger_buffer(setup.sdk_directory.join("buffer").as_path()).await;

  setup.add_trigger_buffer("buffer", buffer.clone()).await;

  producer.write(b"one").unwrap();
  producer.write(b"two").unwrap();

  setup.trigger_buffer_upload("buffer").await;

  let upload = setup.next_upload().await;

  assert_eq!(upload.payload.log_upload().proto_logs.len(), 2);
}

#[tokio::test]
async fn trigger_upload_with_empty_buffer_list_flushes_all_trigger_buffers() {
  let mut setup = SetupMultiConsumer::new(1, 1000).await;
  let (buffer_a, mut producer_a) =
    create_trigger_buffer(setup.sdk_directory.join("buffer_a").as_path()).await;
  let (buffer_b, mut producer_b) =
    create_trigger_buffer(setup.sdk_directory.join("buffer_b").as_path()).await;

  setup.add_trigger_buffer("buffer_a", buffer_a).await;
  setup.add_trigger_buffer("buffer_b", buffer_b).await;

  producer_a.write(b"a").unwrap();
  producer_b.write(b"b").unwrap();

  setup
    .trigger_upload_tx
    .send(TriggerUpload::new(
      Vec::new(),
      None,
      TriggerUploadSource::ExplicitSessionCapture("session-capture".to_string()),
      "session-1".to_string(),
    ))
    .await
    .unwrap();

  let upload_a = setup.next_upload().await;
  let upload_b = setup.next_upload().await;

  let uploaded_payloads = [upload_a, upload_b]
    .into_iter()
    .map(|upload| upload.payload.log_upload().proto_logs[0].clone())
    .collect::<std::collections::BTreeSet<_>>();

  assert_eq!(
    uploaded_payloads,
    std::collections::BTreeSet::from([b"a".to_vec(), b"b".to_vec(),])
  );
}

#[tokio::test]
async fn trigger_upload_is_persisted_until_completion() {
  let mut setup = SetupMultiConsumer::new(1, 1000).await;
  let (buffer, mut producer) =
    create_trigger_buffer(setup.sdk_directory.join("buffer").as_path()).await;

  setup.add_trigger_buffer("buffer", buffer).await;
  producer.write(b"one").unwrap();

  setup
    .trigger_upload_tx
    .send(TriggerUpload::new(
      vec!["buffer".to_string()],
      None,
      TriggerUploadSource::ExplicitSessionCapture("flush-1".to_string()),
      "session-1".to_string(),
    ))
    .await
    .unwrap();

  let upload = setup.next_upload().await;
  assert_eq!(
    setup.pending_trigger_uploads().await,
    vec![PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::ExplicitSessionCapture("flush-1".to_string()),
      session_id: "session-1".to_string(),
      buffers: vec![PersistedTriggerUploadBufferProgress {
        buffer_id: "buffer".to_string(),
        lifecycle: PersistedTriggerUploadBufferLifecycle::UploadingFromArtifact,
        uploaded_batches_count: 0,
        uploaded_logs_count: 0,
      }],
      streaming: None,
      lifecycle: PersistedTriggerUploadLifecycle::UploadingFromArtifact,
    }]
  );

  upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload.uuid,
    })
    .unwrap();

  for _ in 0 .. 100 {
    if setup.pending_trigger_uploads().await.is_empty() {
      return;
    }

    tokio::task::yield_now().await;
  }

  panic!("expected trigger upload persistence to clear after upload completion");
}

#[tokio::test]
async fn persisted_trigger_upload_replays_after_restart() {
  tokio::time::pause();

  let temp_directory = TempDir::with_prefix("consumertest").unwrap();
  PendingTriggerUploadsStore::new(temp_directory.path())
    .upsert(PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::ExplicitSessionCapture("flush-1".to_string()),
      session_id: "session-1".to_string(),
      buffers: vec![PersistedTriggerUploadBufferProgress::new(
        "buffer".to_string(),
      )],
      streaming: None,
      lifecycle: PersistedTriggerUploadLifecycle::ReadyToUpload,
    })
    .await;

  let buffer_path = temp_directory.path().join("buffer");
  let (buffer, mut producer) = create_trigger_buffer(buffer_path.as_path()).await;
  producer.write(b"one").unwrap();

  let mut setup = SetupMultiConsumer::new_with_state(1, 1000, temp_directory).await;
  setup.add_trigger_buffer("buffer", buffer).await;
  setup.sync_trigger_buffer_config(&["buffer"]).await;

  let recovered_upload = setup.next_upload().await;
  assert_eq!(recovered_upload.payload.log_upload().proto_logs.len(), 1);

  recovered_upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: recovered_upload.uuid,
    })
    .unwrap();

  for _ in 0 .. 100 {
    if setup.pending_trigger_uploads().await.is_empty() {
      return;
    }

    tokio::task::yield_now().await;
  }

  panic!("expected recovered trigger upload to be cleared from persistence");
}

#[tokio::test]
async fn in_flight_trigger_upload_replays_after_restart() {
  tokio::time::pause();

  let temp_directory = TempDir::with_prefix("consumertest").unwrap();
  PendingTriggerUploadsStore::new(temp_directory.path())
    .upsert(PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::ExplicitSessionCapture("flush-1".to_string()),
      session_id: "session-1".to_string(),
      buffers: vec![PersistedTriggerUploadBufferProgress {
        buffer_id: "buffer".to_string(),
        lifecycle: PersistedTriggerUploadBufferLifecycle::UploadingFromArtifact,
        uploaded_batches_count: 0,
        uploaded_logs_count: 0,
      }],
      streaming: None,
      lifecycle: PersistedTriggerUploadLifecycle::UploadingFromArtifact,
    })
    .await;

  TriggerUploadArtifactStore::new(
    temp_directory.path().join("state").join("logger"),
    "flush-1",
    "buffer",
  )
  .stage_batch(vec![b"one".to_vec()])
  .await
  .unwrap();
  let persisted_batch = TriggerUploadArtifactStore::new(
    temp_directory.path().join("state").join("logger"),
    "flush-1",
    "buffer",
  )
  .promote_queued_batch_to_inflight()
  .await
  .unwrap()
  .unwrap();

  let buffer_path = temp_directory.path().join("buffer");
  let (buffer, _producer) = create_trigger_buffer(buffer_path.as_path()).await;

  let mut setup = SetupMultiConsumer::new_with_state(1, 1000, temp_directory).await;
  setup.add_trigger_buffer("buffer", buffer).await;
  setup.sync_trigger_buffer_config(&["buffer"]).await;

  let recovered_upload = setup.next_upload().await;
  assert_eq!(recovered_upload.uuid, persisted_batch.upload_uuid);
  assert_eq!(recovered_upload.payload.log_upload().proto_logs.len(), 1);
  assert_eq!(recovered_upload.payload.log_upload().proto_logs[0], b"one");

  recovered_upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: recovered_upload.uuid,
    })
    .unwrap();

  for _ in 0 .. 100 {
    if setup.pending_trigger_uploads().await.is_empty() {
      return;
    }

    tokio::task::yield_now().await;
  }

  panic!("expected recovered in-flight trigger upload to be cleared from persistence");
}

#[tokio::test]
async fn queued_trigger_upload_artifact_prefix_is_deduped_against_buffer_on_restart() {
  tokio::time::pause();

  let temp_directory = TempDir::with_prefix("consumertest").unwrap();
  PendingTriggerUploadsStore::new(temp_directory.path())
    .upsert(PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::ExplicitSessionCapture("flush-1".to_string()),
      session_id: "session-1".to_string(),
      buffers: vec![PersistedTriggerUploadBufferProgress {
        buffer_id: "buffer".to_string(),
        lifecycle: PersistedTriggerUploadBufferLifecycle::UploadingFromBuffer,
        uploaded_batches_count: 0,
        uploaded_logs_count: 0,
      }],
      streaming: None,
      lifecycle: PersistedTriggerUploadLifecycle::UploadingFromBuffer,
    })
    .await;

  TriggerUploadArtifactStore::new(
    temp_directory.path().join("state").join("logger"),
    "flush-1",
    "buffer",
  )
  .stage_batch(vec![b"one".to_vec()])
  .await
  .unwrap();

  let buffer_path = temp_directory.path().join("buffer");
  let (buffer, mut producer) = create_trigger_buffer(buffer_path.as_path()).await;
  producer.write(b"one").unwrap();

  let mut setup = SetupMultiConsumer::new_with_state(1, 1000, temp_directory).await;
  setup.add_trigger_buffer("buffer", buffer).await;
  setup.sync_trigger_buffer_config(&["buffer"]).await;

  let recovered_upload = setup.next_upload().await;
  assert_eq!(recovered_upload.payload.log_upload().proto_logs.len(), 1);
  assert_eq!(recovered_upload.payload.log_upload().proto_logs[0], b"one");

  recovered_upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: recovered_upload.uuid,
    })
    .unwrap();

  let pinned = Box::pin(setup.log_upload_rx.recv());
  assert!(poll!(pinned).is_pending());
}

#[tokio::test]
async fn recovered_trigger_upload_only_discards_matching_buffer_prefix_on_restart() {
  tokio::time::pause();

  let temp_directory = TempDir::with_prefix("consumertest").unwrap();
  PendingTriggerUploadsStore::new(temp_directory.path())
    .upsert(PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::ExplicitSessionCapture("flush-1".to_string()),
      session_id: "session-1".to_string(),
      buffers: vec![PersistedTriggerUploadBufferProgress {
        buffer_id: "buffer".to_string(),
        lifecycle: PersistedTriggerUploadBufferLifecycle::UploadingFromArtifact,
        uploaded_batches_count: 0,
        uploaded_logs_count: 0,
      }],
      streaming: None,
      lifecycle: PersistedTriggerUploadLifecycle::UploadingFromArtifact,
    })
    .await;

  let artifact_store = TriggerUploadArtifactStore::new(
    temp_directory.path().join("state").join("logger"),
    "flush-1",
    "buffer",
  );
  artifact_store
    .stage_batch(vec![b"one".to_vec(), b"two".to_vec()])
    .await
    .unwrap();
  let persisted_batch = artifact_store
    .promote_queued_batch_to_inflight()
    .await
    .unwrap()
    .unwrap();

  let buffer_path = temp_directory.path().join("buffer");
  let (buffer, mut producer) = create_trigger_buffer(buffer_path.as_path()).await;
  producer.write(b"one").unwrap();
  producer.write(b"three").unwrap();

  let mut setup = SetupMultiConsumer::new_with_state(1, 1000, temp_directory).await;
  setup.add_trigger_buffer("buffer", buffer).await;
  setup.sync_trigger_buffer_config(&["buffer"]).await;

  let recovered_upload = setup.next_upload().await;
  assert_eq!(recovered_upload.uuid, persisted_batch.upload_uuid);
  assert_eq!(
    *recovered_upload.payload.log_upload().proto_logs,
    vec![b"one".to_vec(), b"two".to_vec()]
  );

  recovered_upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: recovered_upload.uuid,
    })
    .unwrap();

  for _ in 0 .. 100 {
    if setup.pending_trigger_uploads().await.is_empty() {
      break;
    }

    tokio::task::yield_now().await;
  }
  assert!(setup.pending_trigger_uploads().await.is_empty());

  setup.trigger_buffer_upload("buffer").await;

  let later_upload = setup.next_upload().await;
  assert_eq!(later_upload.payload.log_upload().proto_logs.len(), 1);
  assert_eq!(later_upload.payload.log_upload().proto_logs[0], b"three");
}

#[tokio::test]
async fn persisted_trigger_upload_prunes_missing_buffers_on_recovery() {
  tokio::time::pause();

  let temp_directory = TempDir::with_prefix("consumertest").unwrap();
  PendingTriggerUploadsStore::new(temp_directory.path())
    .upsert(PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::ExplicitSessionCapture("flush-1".to_string()),
      session_id: "stale-session".to_string(),
      buffers: vec![
        PersistedTriggerUploadBufferProgress::new("buffer-a".to_string()),
        PersistedTriggerUploadBufferProgress::new("buffer-b".to_string()),
      ],
      streaming: None,
      lifecycle: PersistedTriggerUploadLifecycle::ReadyToUpload,
    })
    .await;

  let buffer_path = temp_directory.path().join("buffer-a");
  let (buffer, mut producer) = create_trigger_buffer(buffer_path.as_path()).await;
  producer.write(b"one").unwrap();

  let mut setup = SetupMultiConsumer::new_with_state(1, 1000, temp_directory).await;
  setup.add_trigger_buffer("buffer-a", buffer).await;
  setup.sync_trigger_buffer_config(&["buffer-a"]).await;

  let pending_uploads = setup.pending_trigger_uploads().await;
  assert_eq!(pending_uploads.len(), 1);
  assert_eq!(pending_uploads[0].session_id, "stale-session");
  assert_eq!(
    pending_uploads[0].buffer_ids(),
    vec!["buffer-a".to_string()]
  );

  let recovered_upload = setup.next_upload().await;
  assert_eq!(recovered_upload.payload.log_upload().proto_logs.len(), 1);
  assert_eq!(recovered_upload.payload.log_upload().proto_logs[0], b"one");

  recovered_upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: recovered_upload.uuid,
    })
    .unwrap();

  for _ in 0 .. 100 {
    if setup.pending_trigger_uploads().await.is_empty() {
      return;
    }

    tokio::task::yield_now().await;
  }

  panic!("expected pruned trigger upload to be cleared from persistence");
}

#[tokio::test]
async fn persisted_trigger_upload_with_no_configured_buffers_is_abandoned() {
  tokio::time::pause();

  let temp_directory = TempDir::with_prefix("consumertest").unwrap();
  PendingTriggerUploadsStore::new(temp_directory.path())
    .upsert(PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::ExplicitSessionCapture("flush-1".to_string()),
      session_id: "session-1".to_string(),
      buffers: vec![PersistedTriggerUploadBufferProgress::new(
        "missing-buffer".to_string(),
      )],
      streaming: None,
      lifecycle: PersistedTriggerUploadLifecycle::ReadyToUpload,
    })
    .await;

  let artifact_store = TriggerUploadArtifactStore::new(
    temp_directory.path().join("state").join("logger"),
    "flush-1",
    "missing-buffer",
  );
  artifact_store
    .stage_batch(vec![b"one".to_vec()])
    .await
    .unwrap();

  let mut setup = SetupMultiConsumer::new_with_state(1, 1000, temp_directory).await;
  setup.sync_trigger_buffer_config(&[]).await;

  let artifact_store = TriggerUploadArtifactStore::new(
    setup.sdk_directory.join("state").join("logger"),
    "flush-1",
    "missing-buffer",
  );

  assert!(setup.pending_trigger_uploads().await.is_empty());
  assert!(artifact_store.queued_batch().await.unwrap().is_none());
  assert!(artifact_store.inflight_batch().await.unwrap().is_none());

  let pinned = Box::pin(setup.log_upload_rx.recv());
  assert!(poll!(pinned).is_pending());
}

#[tokio::test]
async fn live_remote_trigger_upload_persists_across_shutdown() {
  tokio::time::pause();

  let temp_directory = TempDir::with_prefix("consumertest").unwrap();
  let (remote_flush_streaming_tx, mut remote_flush_streaming_rx) = tokio::sync::mpsc::channel(1);

  let mut setup = SetupMultiConsumer::new_with_remote_streaming_channel(
    1,
    1000,
    temp_directory,
    remote_flush_streaming_tx,
  )
  .await;
  let (buffer, mut producer) =
    create_trigger_buffer(setup.sdk_directory.join("buffer").as_path()).await;

  setup.add_trigger_buffer("buffer", buffer).await;
  setup.sync_trigger_buffer_config(&["buffer"]).await;
  producer.write(b"one").unwrap();

  setup
    .trigger_upload_tx
    .send(TriggerUpload::new(
      vec!["buffer".to_string()],
      Some(FlushStreaming {
        destination_streaming_buffer_ids: vec!["default".to_string()],
        ..Default::default()
      }),
      TriggerUploadSource::RemoteCommand("flush-1".to_string()),
      "session-1".to_string(),
    ))
    .await
    .unwrap();

  let upload = setup.next_upload().await;
  assert_eq!(upload.payload.log_upload().proto_logs.len(), 1);
  assert_eq!(setup.pending_trigger_uploads().await.len(), 1);
  assert!(matches!(
    remote_flush_streaming_rx.try_recv(),
    Err(tokio::sync::mpsc::error::TryRecvError::Empty)
  ));

  let sdk_directory = setup.shutdown_persisting_directory().await;

  let pending_uploads = PendingTriggerUploadsStore::new(&sdk_directory)
    .pending_uploads()
    .await;
  assert_eq!(pending_uploads.len(), 1);
  assert_eq!(pending_uploads[0].id, "flush-1");
  assert_eq!(
    pending_uploads[0].source,
    PersistedTriggerUploadSource::RemoteCommand("flush-1".to_string())
  );
  assert_eq!(pending_uploads[0].buffer_ids(), vec!["buffer".to_string()]);
  assert_eq!(
    pending_uploads[0].streaming,
    Some(PersistedTriggerUploadStreaming {
      destination_buffer_ids: vec!["default".to_string()],
      max_logs_count: None,
    })
  );

  let artifact_store = TriggerUploadArtifactStore::new(
    sdk_directory.join("state").join("logger"),
    "flush-1",
    "buffer",
  );
  assert!(artifact_store.inflight_batch().await.unwrap().is_some());

  std::fs::remove_dir_all(&sdk_directory).unwrap();
}

#[tokio::test]
async fn remote_streaming_activation_channel_closure_preserves_flush_completion() {
  tokio::time::pause();

  let temp_directory = TempDir::with_prefix("consumertest").unwrap();
  let (remote_flush_streaming_tx, remote_flush_streaming_rx) = tokio::sync::mpsc::channel(1);
  drop(remote_flush_streaming_rx);

  let mut setup = SetupMultiConsumer::new_with_remote_streaming_channel(
    1,
    1000,
    temp_directory,
    remote_flush_streaming_tx,
  )
  .await;
  let (buffer, mut producer) =
    create_trigger_buffer(setup.sdk_directory.join("buffer").as_path()).await;

  setup.add_trigger_buffer("buffer", buffer).await;
  setup.sync_trigger_buffer_config(&["buffer"]).await;
  producer.write(b"one").unwrap();

  let flush_id = FlushBufferId::RemoteCommand("flush-1".to_string());
  setup
    .trigger_upload_tx
    .send(TriggerUpload::new(
      vec!["buffer".to_string()],
      Some(FlushStreaming {
        destination_streaming_buffer_ids: vec!["default".to_string()],
        ..Default::default()
      }),
      TriggerUploadSource::RemoteCommand("flush-1".to_string()),
      "session-1".to_string(),
    ))
    .await
    .unwrap();

  for _ in 0 .. 100 {
    if setup.flush_is_pending(&flush_id) {
      break;
    }

    tokio::task::yield_now().await;
  }
  assert!(setup.flush_is_pending(&flush_id));

  let upload = setup.next_upload().await;
  assert!(setup.flush_is_pending(&flush_id));

  upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload.uuid,
    })
    .unwrap();

  for _ in 0 .. 100 {
    tokio::task::yield_now().await;
  }

  assert!(setup.flush_is_pending(&flush_id));
  assert_eq!(setup.pending_trigger_uploads().await.len(), 1);

  setup.shutdown().await;
}

#[tokio::test]
async fn remote_streaming_activation_waits_for_channel_capacity_before_completing_flush() {
  tokio::time::pause();

  let temp_directory = TempDir::with_prefix("consumertest").unwrap();
  let (remote_flush_streaming_tx, mut remote_flush_streaming_rx) = tokio::sync::mpsc::channel(1);
  remote_flush_streaming_tx
    .send(RemoteFlushStreamingRequest::new(
      "existing-request".to_string(),
      vec!["prefill".to_string()],
      "session-0".to_string(),
      FlushStreaming::default(),
    ))
    .await
    .unwrap();

  let mut setup = SetupMultiConsumer::new_with_remote_streaming_channel(
    1,
    1000,
    temp_directory,
    remote_flush_streaming_tx,
  )
  .await;
  let (buffer, mut producer) =
    create_trigger_buffer(setup.sdk_directory.join("buffer").as_path()).await;

  setup.add_trigger_buffer("buffer", buffer).await;
  setup.sync_trigger_buffer_config(&["buffer"]).await;
  producer.write(b"one").unwrap();

  let flush_id = FlushBufferId::RemoteCommand("flush-1".to_string());
  setup
    .trigger_upload_tx
    .send(TriggerUpload::new(
      vec!["buffer".to_string()],
      Some(FlushStreaming {
        destination_streaming_buffer_ids: vec!["default".to_string()],
        ..Default::default()
      }),
      TriggerUploadSource::RemoteCommand("flush-1".to_string()),
      "session-1".to_string(),
    ))
    .await
    .unwrap();

  for _ in 0 .. 100 {
    if setup.flush_is_pending(&flush_id) {
      break;
    }

    tokio::task::yield_now().await;
  }
  assert!(setup.flush_is_pending(&flush_id));

  let upload = setup.next_upload().await;
  upload
    .response_tx
    .send(UploadResponse {
      success: true,
      uuid: upload.uuid,
    })
    .unwrap();

  for _ in 0 .. 100 {
    tokio::task::yield_now().await;
  }
  assert!(setup.flush_is_pending(&flush_id));

  let queued_request = remote_flush_streaming_rx.recv().await.unwrap();
  assert_eq!(queued_request.id, "existing-request");

  let activated_request = remote_flush_streaming_rx.recv().await.unwrap();
  assert_eq!(activated_request.id, "flush-1");
  assert_eq!(activated_request.buffer_ids, vec!["buffer".to_string()]);

  for _ in 0 .. 100 {
    if !setup.flush_is_pending(&flush_id) {
      setup.shutdown().await;
      return;
    }

    tokio::task::yield_now().await;
  }

  panic!("expected flush completion tracker to clear once remote streaming activation was sent");
}

#[tokio::test]
async fn log_streaming() {
  let buffer = bd_buffer::buffer::VolatileRingBuffer::new(
    "test".to_string(),
    1024 * 1024,
    Arc::new(RingBufferStats::default()),
    |_| {},
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
      batch_builder: BatchBuilder::new(make_flags(&runtime_loader)),
    }
    .start()
    .await
    .unwrap();
  });

  producer.write(b"data").unwrap();
  producer.write(b"more data").unwrap();

  assert_matches!(log_upload_rx.recv().await.unwrap(), DataUpload::AcklessLogsUpload(upload) => {
      assert_eq!(upload.proto_logs.len(), 2);
    assert_eq!(upload.proto_logs[0], b"data");
    assert_eq!(upload.proto_logs[1], b"more data");
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
    |_| {},
  );

  let mut producer = buffer.clone().register_producer().unwrap();
  let consumer = buffer.clone().register_consumer().unwrap();
  let shutdown_trigger = ComponentShutdownTrigger::default();
  let runtime_loader = ConfigLoader::new(&PathBuf::from("."));
  let (log_upload_tx, mut log_upload_rx) = tokio::sync::mpsc::channel(1);
  // Set streaming batch size to 2
  runtime_loader
    .update_snapshot(make_simple_update(vec![(
      bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
      ValueKind::Int(2),
    )]))
    .await
    .unwrap();

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
      batch_builder: BatchBuilder::new(make_flags(&runtime_loader)),
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
  assert_matches!(log_upload_rx.recv().await.unwrap(), DataUpload::AcklessLogsUpload(upload) => {
    assert_eq!(upload.proto_logs, vec![b"foo".to_vec(), b"bar".to_vec()]);
  });

  // Next upload should contain "baz"
  assert_matches!(log_upload_rx.recv().await.unwrap(), DataUpload::AcklessLogsUpload(upload) => {
    assert_eq!(upload.proto_logs, vec![b"baz".to_vec()]);
  });
}

#[tokio::test]
async fn log_streaming_shutdown() {
  let buffer = bd_buffer::buffer::VolatileRingBuffer::new(
    "test".to_string(),
    1024 * 1024,
    Arc::new(RingBufferStats::default()),
    |_| {},
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
      batch_builder: BatchBuilder::new(make_flags(&runtime_loader)),
    }
    .start()
    .await
    .unwrap();
  });

  producer.write(b"data").unwrap();

  // Receive the upload request but do not complete it.
  assert_matches!(log_upload_rx.recv().await.unwrap(), DataUpload::AcklessLogsUpload(upload) => {
    assert_eq!(upload.proto_logs[0], b"data");
  });

  // Perform a shutdown, making sure that we complete the shutdown even if there is a pending
  // upload.
  shutdown_trigger.shutdown().await;
  task_handle.await.unwrap();
}

async fn create_continuous_buffer(
  buffer: &Path,
  records_written: Counter,
) -> (Arc<Buffer>, bd_buffer::Producer) {
  let retention_registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let retention_handle = retention_registry.create_handle().await;
  retention_handle.update_retention_micros(RetentionHandle::RETENTION_NONE);
  create_buffer(false, buffer, records_written, retention_handle)
}

fn create_continuous_buffer_with_retention(
  buffer: &Path,
  records_written: Counter,
  retention_handle: RetentionHandle,
) -> (Arc<Buffer>, bd_buffer::Producer) {
  create_buffer(false, buffer, records_written, retention_handle)
}

async fn create_trigger_buffer(buffer: &Path) -> (Arc<Buffer>, bd_buffer::Producer) {
  let retention_registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  create_buffer(
    true,
    buffer,
    Counter::default(),
    retention_registry.create_handle().await,
  )
}

fn create_buffer(
  trigger_buffer: bool,
  buffer: &Path,
  records_written: Counter,
  retention_handle: RetentionHandle,
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
    None,
    retention_handle,
  )
  .unwrap();

  (
    generic_ring_buffer.clone(),
    generic_ring_buffer.new_thread_local_producer().unwrap(),
  )
}
