// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{Stats, with_thread_local_logger_guard};
use crate::app_version::Repository;
use crate::device_command::RegisteredCommandDispatcher;
use crate::labels::{InitialLabels, LabelsState};
use crate::logger::CaptureSession;
use crate::{LoggerHandle, async_log_buffer};
use bd_client_stats_store::Collector;
use bd_event_buffer::{EventBuffer, EventBufferEntry, EventBufferLimits, LoggerControl};
use bd_log_primitives::log_level;
use bd_metadata::labels::Labels;
use bd_proto::protos::logging::payload::LogType;
use bd_session::test::no_timeout;
use bd_test_helpers::session::in_memory_store;
use futures_util::poll;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tempfile::TempDir;
use tokio::pin;
use tokio::sync::watch;
use tokio_test::assert_pending;

fn event_buffer(total_limit_bytes: usize) -> EventBuffer {
  let event_buffer = EventBuffer::new(EventBufferLimits {
    log_limit_bytes: 1024 * 1024,
    total_limit_bytes,
  });
  event_buffer.mark_startup_gate_ready();
  event_buffer
}

fn labels_handle(capacity: usize) -> (LoggerHandle, EventBuffer, TempDir) {
  let buffer = event_buffer(capacity);
  let directory = TempDir::new().unwrap();
  let session_strategy = no_timeout(directory.path()).strategy();
  let store = in_memory_store();
  let handle = LoggerHandle {
    labels: Arc::new(
      LabelsState::new(
        InitialLabels::default(),
        store.clone(),
        Collector::default().scope(""),
        HashMap::new(),
      )
      .unwrap(),
    ),
    tx: async_log_buffer::Sender::from_event_buffer(buffer.clone(), session_strategy.clone()),
    stats: Stats::new(&Collector::default().scope("")),
    session_strategy,
    device: Arc::new(bd_device::Device::new(store.clone())),
    sdk_version: "1.0.0".into(),
    app_version_repo: Repository::new(store),
    opaque_entity_updates: watch::channel(None).0,
    pending_entity_id: Arc::new(parking_lot::Mutex::new(None)),
    sleep_mode_active: watch::channel(false).0,
    is_tracing_active: Arc::new(AtomicBool::new(false)),
    command_dispatcher: RegisteredCommandDispatcher::default(),
  };
  (handle, buffer, directory)
}

#[tokio::test]
async fn labels_prestart_mutations_are_atomic_durable_and_publish_only_actual_changes() {
  let (handle, buffer, _directory) = labels_handle(1024 * 1024);
  let mut wire = handle.labels.updates.subscribe();
  std::thread::scope(|scope| {
    for index in 0 .. 4 {
      let handle = &handle;
      scope.spawn(move || {
        handle
          .set_label(format!("key-{index}"), String::new())
          .unwrap();
      });
    }
  });
  let labels = handle.current_labels();
  assert_eq!(labels.len(), 4);
  assert_eq!(*wire.borrow_and_update(), labels);
  assert_eq!(buffer.next_batch(4).await.entries.len(), 4);
  assert!(
    handle
      .set_label("invalid".to_string(), "x".repeat(257))
      .is_err()
  );
  assert!(
    handle
      .set_label("__bd_env".to_string(), "fake".to_string())
      .is_err()
  );
  assert!(handle.remove_label("__bd_env").is_err());
  handle.replace_labels(labels.clone()).unwrap();
  handle.remove_label("missing").unwrap();
  assert!(!wire.has_changed().unwrap());
  assert_eq!(handle.current_labels(), labels);
  handle.clear_labels().unwrap();
  assert!(handle.current_labels().is_empty());
  assert!(wire.borrow_and_update().is_empty());
  assert!(
    matches!(buffer.next_batch(1).await.entries.as_slice(), [EventBufferEntry::Control(LoggerControl::SetLabels(value))] if value == "{}")
  );
  handle.clear_labels().unwrap();
  assert!(!wire.has_changed().unwrap());
  let mut batch = std::pin::pin!(buffer.next_batch(1));
  assert_pending!(poll!(&mut batch));
}

#[test]
fn rejected_labels_updates_do_not_change_desired_or_wire_state() {
  let (handle, _, _directory) = labels_handle(0);
  let updates = handle.labels.updates.subscribe();
  assert!(
    handle
      .set_label("channel".to_string(), "qa".to_string())
      .is_err()
  );
  assert_eq!(handle.current_labels(), Labels::new());
  assert!(!updates.has_changed().unwrap());
}

#[tokio::test]
async fn thread_local_logger_guard() {
  let event_buffer = event_buffer(1024);
  let sdk_directory = TempDir::new().unwrap();
  let session_strategy = no_timeout(sdk_directory.path()).strategy();
  let sender =
    async_log_buffer::Sender::from_event_buffer(event_buffer.clone(), session_strategy.clone());
  let store = in_memory_store();
  let handle = LoggerHandle {
    labels: Arc::new(
      LabelsState::new(
        InitialLabels::default(),
        store.clone(),
        Collector::default().scope(""),
        HashMap::new(),
      )
      .unwrap(),
    ),
    tx: sender,
    stats: Stats::new(&Collector::default().scope("")),
    session_strategy,
    device: Arc::new(bd_device::Device::new(store.clone())),
    sdk_version: "1.0.0".into(),
    app_version_repo: Repository::new(store),
    opaque_entity_updates: watch::channel(None).0,
    pending_entity_id: Arc::new(parking_lot::Mutex::new(None)),
    sleep_mode_active: watch::channel(false).0,
    is_tracing_active: Arc::new(AtomicBool::new(false)),
    command_dispatcher: RegisteredCommandDispatcher::default(),
  };

  with_thread_local_logger_guard(|| {
    handle.log(
      log_level::INFO,
      LogType::NORMAL,
      "msg".into(),
      [].into(),
      [].into(),
      None,
      &CaptureSession::default(),
    );
  });

  assert!(
    event_buffer
      .next_batch(1)
      .await
      .startup_gate_opened
      .is_some()
  );
  let recv = event_buffer.next_batch(1);
  pin!(recv);
  assert_pending!(poll!(recv));
}

#[tokio::test]
async fn session_id_is_rejected_while_reentrancy_guard_is_held() {
  let sdk_directory = TempDir::new().unwrap();
  let session_strategy = no_timeout(sdk_directory.path()).strategy();
  let sender =
    async_log_buffer::Sender::from_event_buffer(event_buffer(1024), session_strategy.clone());
  let store = in_memory_store();
  let handle = LoggerHandle {
    labels: Arc::new(
      LabelsState::new(
        InitialLabels::default(),
        store.clone(),
        Collector::default().scope(""),
        HashMap::new(),
      )
      .unwrap(),
    ),
    tx: sender,
    stats: Stats::new(&Collector::default().scope("")),
    session_strategy,
    device: Arc::new(bd_device::Device::new(store.clone())),
    sdk_version: "1.0.0".into(),
    app_version_repo: Repository::new(store),
    opaque_entity_updates: watch::channel(None).0,
    pending_entity_id: Arc::new(parking_lot::Mutex::new(None)),
    sleep_mode_active: watch::channel(false).0,
    is_tracing_active: Arc::new(AtomicBool::new(false)),
    command_dispatcher: RegisteredCommandDispatcher::default(),
  };

  let result = with_thread_local_logger_guard(|| handle.session_id());

  assert_eq!(
    "operation not allowed from within a field provider",
    result.unwrap_err().to_string()
  );
}

#[tokio::test]
async fn register_opaque_entity_id_updates_queue_and_watch() {
  let event_buffer = event_buffer(1024 * 1024);
  let sdk_directory = TempDir::new().unwrap();
  let session_strategy = no_timeout(sdk_directory.path()).strategy();
  let sender =
    async_log_buffer::Sender::from_event_buffer(event_buffer.clone(), session_strategy.clone());
  let store = in_memory_store();
  let (opaque_entity_updates_tx, opaque_entity_updates_rx) = watch::channel(None);
  let handle = LoggerHandle {
    labels: Arc::new(
      LabelsState::new(
        InitialLabels::default(),
        store.clone(),
        Collector::default().scope(""),
        HashMap::new(),
      )
      .unwrap(),
    ),
    tx: sender,
    stats: Stats::new(&Collector::default().scope("")),
    session_strategy,
    device: Arc::new(bd_device::Device::new(store.clone())),
    sdk_version: "1.0.0".into(),
    app_version_repo: Repository::new(store.clone()),
    opaque_entity_updates: opaque_entity_updates_tx,
    pending_entity_id: Arc::new(parking_lot::Mutex::new(None)),
    sleep_mode_active: watch::channel(false).0,
    is_tracing_active: Arc::new(AtomicBool::new(false)),
    command_dispatcher: RegisteredCommandDispatcher::default(),
  };

  handle.register_opaque_entity_id(Some("hashed-entity-id"));
  assert!(matches!(
    event_buffer.next_batch(1).await.entries.as_slice(),
    [EventBufferEntry::Control(LoggerControl::SetEntityId(Some(entity_id)))] if entity_id == "hashed-entity-id"
  ));
  assert!(matches!(
    handle.pending_entity_id.lock().clone(),
    Some(super::PendingEntityIdUpdate::Set(entity_id)) if entity_id == "hashed-entity-id"
  ));
  assert_eq!(
    Some("hashed-entity-id".to_string()),
    opaque_entity_updates_rx.borrow().clone()
  );
  assert_eq!(
    Some("hashed-entity-id".to_string()),
    handle.current_opaque_entity_id()
  );

  handle.register_opaque_entity_id(None);
  assert!(matches!(
    event_buffer.next_batch(1).await.entries.as_slice(),
    [EventBufferEntry::Control(LoggerControl::SetEntityId(None))]
  ));
  assert_eq!(
    Some(super::PendingEntityIdUpdate::Clear),
    handle.pending_entity_id.lock().clone()
  );
  assert_eq!(None, opaque_entity_updates_rx.borrow().clone());
  assert_eq!(None, handle.current_opaque_entity_id());
}

#[tokio::test]
async fn register_opaque_entity_id_does_not_update_watch_when_queueing_fails() {
  let sdk_directory = TempDir::new().unwrap();
  let session_strategy = no_timeout(sdk_directory.path()).strategy();
  let sender =
    async_log_buffer::Sender::from_event_buffer(event_buffer(0), session_strategy.clone());
  let store = in_memory_store();
  let (opaque_entity_updates_tx, opaque_entity_updates_rx) = watch::channel(None);
  let handle = LoggerHandle {
    labels: Arc::new(
      LabelsState::new(
        InitialLabels::default(),
        store.clone(),
        Collector::default().scope(""),
        HashMap::new(),
      )
      .unwrap(),
    ),
    tx: sender,
    stats: Stats::new(&Collector::default().scope("")),
    session_strategy,
    device: Arc::new(bd_device::Device::new(store.clone())),
    sdk_version: "1.0.0".into(),
    app_version_repo: Repository::new(store),
    opaque_entity_updates: opaque_entity_updates_tx,
    pending_entity_id: Arc::new(parking_lot::Mutex::new(None)),
    sleep_mode_active: watch::channel(false).0,
    is_tracing_active: Arc::new(AtomicBool::new(false)),
    command_dispatcher: RegisteredCommandDispatcher::default(),
  };

  handle.register_opaque_entity_id(Some("hashed-entity-id"));

  assert!(matches!(
    handle.pending_entity_id.lock().clone(),
    Some(super::PendingEntityIdUpdate::Set(entity_id)) if entity_id == "hashed-entity-id"
  ));
  assert_eq!(None, opaque_entity_updates_rx.borrow().clone());
}
