// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::buffer::{
  AllowOverwrite,
  BlockWhenReservingIntoConcurrentRead,
  NonVolatileRingBuffer,
  PerRecordCrc32Check,
  RingBuffer as BufferRingBuffer,
  RingBufferStats,
};
use crate::ring_buffer::{Manager, RingBuffer};
use crate::{AbslCode, Error};
use assert_matches::assert_matches;
use bd_client_stats_store::test::StatsHelper;
use bd_client_stats_store::{Collector, Counter};
use bd_log_primitives::{EncodableLog, Log, log_level};
use bd_proto::protos::config::v1::config::buffer_config::BufferSizes;
use bd_proto::protos::config::v1::config::{BufferConfig, BufferConfigList, buffer_config};
use bd_proto::protos::logging::payload::LogType;
use bd_resilient_kv::RetentionHandle;
use bd_stats_common::{NameType, labels};
use bd_time::OffsetDateTimeExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use time::OffsetDateTime;

fn fake_counter() -> Counter {
  Collector::default().scope("test").counter("test")
}

fn tmp_dir() -> tempfile::TempDir {
  tempfile::TempDir::with_prefix("ring-buffer-").unwrap()
}

fn make_test_log_bytes(t: OffsetDateTime) -> Vec<u8> {
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

  let size = log.compute_size(&[], &[]).unwrap();
  let output_size = usize::try_from(size).expect("serialized log size fits usize");
  let mut output_bytes = vec![0u8; output_size];
  log.serialize_to_bytes(&[], &[], &mut output_bytes).unwrap();
  output_bytes
}

#[tokio::test]
async fn test_create_ring_buffer() {
  let dir = tmp_dir();
  let (buffer, _) = RingBuffer::new(
    "test",
    100,
    dir.path().join(PathBuf::from("buffer")),
    1000,
    false,
    fake_counter(),
    fake_counter(),
    fake_counter(),
    fake_counter(),
    fake_counter(),
    None,
    None,
    None,
  )
  .unwrap();

  let mut producer = buffer.new_thread_local_producer();
  producer.as_mut().unwrap().write(b"hello").unwrap();

  let mut consumer = buffer.create_continous_consumer().unwrap();
  let entry = consumer.read().await;
  assert_eq!(b"hello", entry.unwrap());
}

#[test]
fn test_create_ring_buffer_illegal_path() {
  let buffer_path = PathBuf::from("/buffer");
  let buffer = RingBuffer::new(
    "test",
    100,
    buffer_path.clone(),
    1000,
    false,
    fake_counter(),
    fake_counter(),
    fake_counter(),
    fake_counter(),
    fake_counter(),
    None,
    None,
    None,
  );

  assert_matches!(
    buffer,
    Err(Error::BufferCreation(
      path,
      inner_error)
    ) => {
      assert_eq!(buffer_path, path);
      assert_matches!(
        *inner_error, Error::AbslStatus(AbslCode::InvalidArgument, _)
      );
    }
  );
}

#[test]
fn corrupted_buffer() {
  let dir = tmp_dir();
  let path = dir.path().join(PathBuf::from("buffer"));
  let (_, deleted) = RingBuffer::new(
    "test",
    100,
    path.clone(),
    1000,
    false,
    fake_counter(),
    fake_counter(),
    fake_counter(),
    fake_counter(),
    fake_counter(),
    None,
    None,
    None,
  )
  .unwrap();
  assert!(!deleted);

  let mut raw_buffer = std::fs::read(&path).unwrap();

  raw_buffer[0] = 0;

  std::fs::write(&path, raw_buffer).unwrap();

  let (_, deleted) = RingBuffer::new(
    "test",
    100,
    path,
    1000,
    false,
    fake_counter(),
    fake_counter(),
    fake_counter(),
    fake_counter(),
    fake_counter(),
    None,
    None,
    None,
  )
  .unwrap();
  assert!(deleted);
}

#[tokio::test]
async fn test_ring_buffer_manager() {
  let dir = tmp_dir();
  let (ring_buffer_manager, mut buffer_update_rx) = Manager::new(
    dir.path().to_path_buf(),
    &Collector::default().scope(""),
    &bd_runtime::runtime::ConfigLoader::new(&PathBuf::from(".")),
    Arc::new(bd_resilient_kv::RetentionRegistry::new(
      bd_runtime::runtime::IntWatch::new_for_testing(0),
    )),
  );

  // Make sure we're not letting any buffer events sit in the channel, as this extends the
  // lifetime of removed buffers.
  tokio::spawn(async move { while buffer_update_rx.recv().await.is_some() {} });

  let initial_config = simple_buffer_config(&["some-buffer", "another-buffer"]);
  ring_buffer_manager
    .update_from_config(&initial_config, false)
    .await
    .unwrap();

  assert!(dir.path().join(Path::new("./some-buffer")).exists());
  assert!(dir.path().join(Path::new("./another-buffer")).exists());

  let buffer_handle = ring_buffer_manager
    .buffers()
    .get("some-buffer")
    .unwrap()
    .clone();

  let next_config = simple_buffer_config(&["third_buffer"]);
  ring_buffer_manager
    .update_from_config(&next_config, false)
    .await
    .unwrap();

  // At this point the buffer we retained a handle to should still exist
  // while the other one was removed.
  assert!(dir.path().join(Path::new("./some-buffer")).exists());
  assert!(!dir.path().join(Path::new("./another-buffer")).exists());
  assert!(dir.path().join(Path::new("./third_buffer")).exists());

  // Once we drop the remaining buffer handle the file is cleaned up.
  std::mem::drop(buffer_handle);
  assert!(!dir.path().join(Path::new("./some-buffer")).exists());
}

#[tokio::test]
async fn ring_buffer_stats() {
  let diretory = tempfile::TempDir::with_prefix("ringbuffer").unwrap();

  let collector = Collector::default();
  let (ring_buffer_manager, mut buffer_update_rx) = Manager::new(
    diretory.path().to_owned(),
    &collector.scope(""),
    &bd_runtime::runtime::ConfigLoader::new(&PathBuf::from(".")),
    Arc::new(bd_resilient_kv::RetentionRegistry::new(
      bd_runtime::runtime::IntWatch::new_for_testing(0),
    )),
  );

  // Make sure we're not letting any buffer events sit in the channel, as this extends the
  // lifetime of removed buffers.
  tokio::spawn(async move { while buffer_update_rx.recv().await.is_some() {} });

  let initial_config = simple_buffer_config(&["some-buffer", "another-buffer"]);
  ring_buffer_manager
    .update_from_config(&initial_config, false)
    .await
    .unwrap();

  let buffers = ring_buffer_manager.buffers.lock();
  let (_, buffer_handle) = buffers.0.get("some-buffer").unwrap();

  let mut producer1 = buffer_handle.new_thread_local_producer().unwrap();
  producer1.write(b"data").unwrap();

  let mut producer2 = buffer_handle.new_thread_local_producer().unwrap();
  producer2.write(b"data").unwrap();

  collector.assert_counter_eq(
    0,
    "ring_buffer:record_write",
    labels! { "buffer_id" => "another-buffer"},
  );
  collector.assert_counter_eq(
    2,
    "ring_buffer:record_write",
    labels! { "buffer_id" => "some-buffer" },
  );
  collector.assert_counter_eq(
    0,
    "ring_buffer:record_write_failure",
    labels! { "buffer_id" => "some-buffer" },
  );
  collector.assert_counter_eq(
    0,
    "ring_buffer:record_write_failure",
    labels! { "buffer_id" => "another-buffer" },
  );
  collector.assert_counter_eq(
    0,
    "ring_buffer:volatile_overwrite",
    labels! { "buffer_id" => "some-buffer" },
  );
  collector.assert_counter_eq(
    0,
    "ring_buffer:volatile_overwrite",
    labels! { "buffer_id" => "another-buffer" },
  );
  collector.assert_counter_eq(
    0,
    "ring_buffer:record_corrupted",
    labels! { "buffer_id" => "some-buffer" },
  );
  collector.assert_counter_eq(
    0,
    "ring_buffer:record_corrupted",
    labels! { "buffer_id" => "another-buffer" },
  );
  collector.assert_counter_eq(
    0,
    "ring_buffer:total_data_loss",
    labels! { "buffer_id" => "some-buffer" },
  );
  collector.assert_counter_eq(
    0,
    "ring_buffer:total_data_loss",
    labels! { "buffer_id" => "another-buffer" },
  );

  // Write a bunch of entries that will overflow the small buffers
  for _ in 0 .. 5000 {
    let _ignored = producer1.write(&[0; 50]);
  }

  // Verify that we recorded overwrites in the volatile buffer.
  assert!(
    collector
      .find_counter(
        &NameType::Global("ring_buffer:volatile_overwrite".to_string()),
        &labels! { "buffer_id" => "some-buffer" },
      )
      .unwrap()
      .get()
      > 0
  );
}

#[tokio::test]
async fn trigger_buffer_eviction_updates_retention_handle() {
  let directory = tmp_dir();
  let retention_registry = Arc::new(bd_resilient_kv::RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let retention_handle = retention_registry.create_handle().await;
  let first_time = time::OffsetDateTime::now_utc();
  let second_time = first_time + time::Duration::seconds(1);
  let first_log = make_test_log_bytes(first_time);
  let second_log = make_test_log_bytes(second_time);
  let log_size = u32::try_from(first_log.len()).unwrap();
  let buffer_size = log_size.checked_add(64).unwrap();
  let handle_for_cb = retention_handle.clone();
  let on_record_evicted_cb = move |record_data: &[u8]| {
    if let Some(ts) = EncodableLog::extract_timestamp(record_data)
      && let Some(micros) = u64::try_from(ts.unix_timestamp_micros()).ok()
    {
      handle_for_cb.update_retention_micros(micros);
    }
  };

  let buffer = NonVolatileRingBuffer::new(
    "trigger".to_string(),
    directory.path().join("trigger"),
    buffer_size,
    AllowOverwrite::Yes,
    BlockWhenReservingIntoConcurrentRead::No,
    PerRecordCrc32Check::No,
    Arc::new(RingBufferStats::default()),
    on_record_evicted_cb,
  )
  .unwrap();

  // We need to extend the lifetime of buffer as register_producer moves the Arc.
  let _retained = buffer.clone();
  let mut producer = buffer.register_producer().unwrap();
  for _ in 0 .. 10 {
    producer.write(&first_log).unwrap();
    producer.write(&second_log).unwrap();
    if retention_handle.get_retention() != RetentionHandle::NO_RETENTION_REQUIREMENT {
      break;
    }
  }

  let retained = retention_handle.get_retention();
  let first_micros =
    u64::try_from(first_time.unix_timestamp_micros()).expect("timestamp micros fits u64");
  let second_micros =
    u64::try_from(second_time.unix_timestamp_micros()).expect("timestamp micros fits u64");
  assert!(retained >= first_micros);
  assert!(retained <= second_micros);
}

#[tokio::test]
async fn retention_handle_is_released_on_buffer_removal() {
  let directory = tmp_dir();
  let retention_registry = Arc::new(bd_resilient_kv::RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let ring_buffer_manager = setup_manager(directory.path(), retention_registry.clone());

  let config = single_buffer_with_size("trigger", 1000, 100, buffer_config::Type::TRIGGER);
  ring_buffer_manager
    .update_from_config(&config, false)
    .await
    .unwrap();

  assert!(retention_registry.min_retention_timestamp().await.is_some());

  let removed_config = BufferConfigList::default();
  ring_buffer_manager
    .update_from_config(&removed_config, false)
    .await
    .unwrap();

  assert!(retention_registry.min_retention_timestamp().await.is_none());
}

#[tokio::test]
async fn trigger_buffer_retention_initialized_from_oldest_record() {
  let directory = tmp_dir();
  let config = single_buffer_with_size("trigger", 10_000, 1_000, buffer_config::Type::TRIGGER);

  let retention_registry = Arc::new(bd_resilient_kv::RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let ring_buffer_manager = setup_manager(directory.path(), retention_registry);

  ring_buffer_manager
    .update_from_config(&config, false)
    .await
    .unwrap();

  let buffer_handle = ring_buffer_manager
    .buffers()
    .get("trigger")
    .unwrap()
    .1
    .clone();
  let first_time = time::OffsetDateTime::now_utc();
  let log_bytes = make_test_log_bytes(first_time);
  let mut producer = buffer_handle.new_thread_local_producer().unwrap();
  producer.write(&log_bytes).unwrap();
  buffer_handle.flush();

  std::mem::drop(buffer_handle);
  std::mem::drop(ring_buffer_manager);

  let retention_registry = Arc::new(bd_resilient_kv::RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let (ring_buffer_manager, mut buffer_update_rx) = Manager::new(
    directory.path().to_path_buf(),
    &Collector::default().scope(""),
    &bd_runtime::runtime::ConfigLoader::new(&PathBuf::from(".")),
    retention_registry.clone(),
  );
  tokio::spawn(async move { while buffer_update_rx.recv().await.is_some() {} });

  ring_buffer_manager
    .update_from_config(&config, false)
    .await
    .unwrap();

  let retained = retention_registry.min_retention_timestamp().await.unwrap();
  let first_micros =
    u64::try_from(first_time.unix_timestamp_micros()).expect("timestamp micros fits u64");
  assert_eq!(retained, first_micros);
}

#[tokio::test]
async fn write_failure_stats() {
  let diretory = tempfile::TempDir::with_prefix("ringbuffer").unwrap();

  let collector = Collector::default();
  let (ring_buffer_manager, mut buffer_update_rx) = Manager::new(
    diretory.path().to_owned(),
    &collector.scope(""),
    &bd_runtime::runtime::ConfigLoader::new(&PathBuf::from(".")),
    Arc::new(bd_resilient_kv::RetentionRegistry::new(
      bd_runtime::runtime::IntWatch::new_for_testing(0),
    )),
  );

  // Make sure we're not letting any buffer events sit in the channel, as this extends the
  // lifetime of removed buffers.
  tokio::spawn(async move { while buffer_update_rx.recv().await.is_some() {} });

  let initial_config =
    single_buffer_with_size("trigger", 10000, 1000, buffer_config::Type::TRIGGER);
  ring_buffer_manager
    .update_from_config(&initial_config, false)
    .await
    .unwrap();

  let buffers = ring_buffer_manager.buffers.lock();
  let (_, buffer_handle) = buffers.0.get("trigger").unwrap();

  let mut producer1 = buffer_handle.new_thread_local_producer().unwrap();
  producer1.write(b"data").unwrap();

  // While there is a consumer active, the ring buffer is locked and will prevent further writes.
  let _consumer = buffer_handle.new_consumer().unwrap();
  assert!(producer1.write(b"data").is_err());

  collector.assert_counter_eq(
    1,
    "ring_buffer:record_write",
    labels! {"buffer_id" => "trigger"},
  );
  collector.assert_counter_eq(
    1,
    "ring_buffer:record_write_failure",
    labels! {"buffer_id" => "trigger"},
  );
}

#[tokio::test]
// Verifies that buffer sizes don't change once a buffer has been initialized, even if we get
// a configuration update for the buffers with a new buffer size.
async fn buffer_never_resizes() {
  let buffer_directory = tempfile::TempDir::with_prefix("sdk").unwrap();
  let (ring_buffer_manager, mut buffer_update_rx) = Manager::new(
    buffer_directory.path().to_path_buf(),
    &Collector::default().scope(""),
    &bd_runtime::runtime::ConfigLoader::new(&PathBuf::from("")),
    Arc::new(bd_resilient_kv::RetentionRegistry::new(
      bd_runtime::runtime::IntWatch::new_for_testing(0),
    )),
  );

  // Make sure we're not letting any buffer events sit in the channel, as this extends the
  // lifetime of removed buffers.
  tokio::spawn(async move { while buffer_update_rx.recv().await.is_some() {} });

  let initial_config =
    single_buffer_with_size("buffer", 1234, 123, buffer_config::Type::CONTINUOUS);
  ring_buffer_manager
    .update_from_config(&initial_config, false)
    .await
    .unwrap();

  let buffer_path = buffer_directory.path().join("./buffer");
  assert!(buffer_path.exists());

  assert_eq!(std::fs::metadata(&buffer_path).unwrap().len(), 1234);

  let next_config = single_buffer_with_size("buffer", 2000, 300, buffer_config::Type::CONTINUOUS);
  ring_buffer_manager
    .update_from_config(&next_config, false)
    .await
    .unwrap();

  // After processing the update, the buffer should still be 1234 as we don't resize buffers after
  // they have been initialized.
  assert_eq!(std::fs::metadata(&buffer_path).unwrap().len(), 1234);
}

fn single_buffer_with_size(
  name: &str,
  non_volatile_buffer_size_bytes: u32,
  volatile_buffer_size_bytes: u32,
  buffer_type: buffer_config::Type,
) -> BufferConfigList {
  BufferConfigList {
    buffer_config: vec![BufferConfig {
      name: name.to_string(),
      id: name.to_string(),
      filters: vec![],
      type_: buffer_type.into(),
      buffer_sizes: Some(BufferSizes {
        volatile_buffer_size_bytes,
        non_volatile_buffer_size_bytes,
        ..Default::default()
      })
      .into(),
      ..Default::default()
    }],
    ..Default::default()
  }
}

fn simple_buffer_config(buffers: &[&str]) -> BufferConfigList {
  let mut buffer_config = Vec::new();

  for b in buffers {
    buffer_config.push(BufferConfig {
      name: (*b).to_string(),
      id: (*b).to_string(),
      filters: vec![],
      type_: buffer_config::Type::CONTINUOUS.into(),
      buffer_sizes: Some(BufferSizes {
        non_volatile_buffer_size_bytes: 1000,
        volatile_buffer_size_bytes: 100,
        ..Default::default()
      })
      .into(),
      ..Default::default()
    });
  }

  BufferConfigList {
    buffer_config,
    ..Default::default()
  }
}

fn setup_manager(
  directory: &Path,
  retention_registry: Arc<bd_resilient_kv::RetentionRegistry>,
) -> Arc<Manager> {
  let (ring_buffer_manager, mut buffer_update_rx) = Manager::new(
    directory.to_path_buf(),
    &Collector::default().scope(""),
    &bd_runtime::runtime::ConfigLoader::new(&PathBuf::from(".")),
    retention_registry,
  );
  tokio::spawn(async move { while buffer_update_rx.recv().await.is_some() {} });
  ring_buffer_manager
}
