// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{Stats, with_thread_local_logger_guard};
use crate::app_version::Repository;
use crate::logger::CaptureSession;
use crate::{LoggerHandle, async_log_buffer};
use bd_client_stats_store::Collector;
use bd_event_buffer::{EventBuffer, EventBufferEntry, EventBufferLimits, LoggerControl};
use bd_log_primitives::log_level;
use bd_proto::protos::logging::payload::LogType;
use bd_session::test::no_timeout;
use bd_test_helpers::session::in_memory_store;
use futures_util::poll;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tempfile::TempDir;
use tokio::pin;
use tokio::sync::watch;
use tokio_test::assert_pending;

fn event_buffer(total_limit_bytes: usize) -> EventBuffer {
  EventBuffer::new(EventBufferLimits {
    log_limit_bytes: 1024 * 1024,
    total_limit_bytes,
  })
}

#[tokio::test]
async fn thread_local_logger_guard() {
  let event_buffer = event_buffer(1024);
  let sender = async_log_buffer::Sender::from_event_buffer(event_buffer.clone());

  let sdk_directory = TempDir::new().unwrap();
  let store = in_memory_store();
  let handle = LoggerHandle {
    tx: sender,
    stats: Stats::new(&Collector::default().scope("")),
    session_strategy: no_timeout(sdk_directory.path()).strategy(),
    device: Arc::new(bd_device::Device::new(store.clone())),
    sdk_version: "1.0.0".into(),
    app_version_repo: Repository::new(store),
    opaque_entity_updates: watch::channel(None).0,
    pending_entity_id: Arc::new(parking_lot::Mutex::new(None)),
    sleep_mode_active: watch::channel(false).0,
    is_tracing_active: Arc::new(AtomicBool::new(false)),
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

  let recv = event_buffer.next_batch(1);
  pin!(recv);
  assert_pending!(poll!(recv));
}

#[tokio::test]
async fn session_id_is_rejected_while_reentrancy_guard_is_held() {
  let sender = async_log_buffer::Sender::from_event_buffer(event_buffer(1024));

  let sdk_directory = TempDir::new().unwrap();
  let store = in_memory_store();
  let handle = LoggerHandle {
    tx: sender,
    stats: Stats::new(&Collector::default().scope("")),
    session_strategy: no_timeout(sdk_directory.path()).strategy(),
    device: Arc::new(bd_device::Device::new(store.clone())),
    sdk_version: "1.0.0".into(),
    app_version_repo: Repository::new(store),
    opaque_entity_updates: watch::channel(None).0,
    pending_entity_id: Arc::new(parking_lot::Mutex::new(None)),
    sleep_mode_active: watch::channel(false).0,
    is_tracing_active: Arc::new(AtomicBool::new(false)),
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
  let sender = async_log_buffer::Sender::from_event_buffer(event_buffer.clone());

  let sdk_directory = TempDir::new().unwrap();
  let store = in_memory_store();
  let (opaque_entity_updates_tx, opaque_entity_updates_rx) = watch::channel(None);
  let handle = LoggerHandle {
    tx: sender,
    stats: Stats::new(&Collector::default().scope("")),
    session_strategy: no_timeout(sdk_directory.path()).strategy(),
    device: Arc::new(bd_device::Device::new(store.clone())),
    sdk_version: "1.0.0".into(),
    app_version_repo: Repository::new(store.clone()),
    opaque_entity_updates: opaque_entity_updates_tx,
    pending_entity_id: Arc::new(parking_lot::Mutex::new(None)),
    sleep_mode_active: watch::channel(false).0,
    is_tracing_active: Arc::new(AtomicBool::new(false)),
  };

  handle.register_opaque_entity_id(Some("hashed-entity-id"));
  assert!(matches!(
    event_buffer.next_batch(1).await.as_slice(),
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
    event_buffer.next_batch(1).await.as_slice(),
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
  let sender = async_log_buffer::Sender::from_event_buffer(event_buffer(0));

  let sdk_directory = TempDir::new().unwrap();
  let store = in_memory_store();
  let (opaque_entity_updates_tx, opaque_entity_updates_rx) = watch::channel(None);
  let handle = LoggerHandle {
    tx: sender,
    stats: Stats::new(&Collector::default().scope("")),
    session_strategy: no_timeout(sdk_directory.path()).strategy(),
    device: Arc::new(bd_device::Device::new(store.clone())),
    sdk_version: "1.0.0".into(),
    app_version_repo: Repository::new(store),
    opaque_entity_updates: opaque_entity_updates_tx,
    pending_entity_id: Arc::new(parking_lot::Mutex::new(None)),
    sleep_mode_active: watch::channel(false).0,
    is_tracing_active: Arc::new(AtomicBool::new(false)),
  };

  handle.register_opaque_entity_id(Some("hashed-entity-id"));

  assert!(matches!(
    handle.pending_entity_id.lock().clone(),
    Some(super::PendingEntityIdUpdate::Set(entity_id)) if entity_id == "hashed-entity-id"
  ));
  assert_eq!(None, opaque_entity_updates_rx.borrow().clone());
}
