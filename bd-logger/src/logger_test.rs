// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{Stats, with_thread_local_logger_guard};
use crate::app_version::Repository;
use crate::logger::{Block, CaptureSession};
use crate::{LoggerHandle, async_log_buffer};
use bd_client_stats_store::Collector;
use bd_log_primitives::log_level;
use bd_proto::protos::logging::payload::LogType;
use bd_session::Strategy;
use bd_session::fixed::UUIDCallbacks;
use bd_test_helpers::session::in_memory_store;
use futures_util::poll;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tempfile::TempDir;
use tokio::pin;
use tokio::sync::watch;
use tokio_test::assert_pending;

#[tokio::test]
async fn thread_local_logger_guard() {
  let (log_tx, mut log_rx) = bd_bounded_buffer::channel(100);
  let (state_tx, _state_rx) = bd_bounded_buffer::channel(100);
  let sender = async_log_buffer::Sender::from_parts(log_tx, state_tx);

  let sdk_directory = TempDir::new().unwrap();
  let store = in_memory_store();
  let handle = LoggerHandle {
    tx: sender,
    stats: Stats::new(&Collector::default().scope("")),
    session_strategy: Arc::new(Strategy::fixed(
      sdk_directory.path(),
      Arc::new(UUIDCallbacks),
    )),
    device: Arc::new(bd_device::Device::new(store.clone())),
    sdk_version: "1.0.0".into(),
    app_version_repo: Repository::new(store.clone()),
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
      Block::No,
      &CaptureSession::default(),
    );
  });

  let recv = log_rx.recv();
  pin!(recv);
  assert_pending!(poll!(recv));
}

#[tokio::test]
async fn register_opaque_entity_id_updates_store() {
  let (log_tx, _log_rx) = bd_bounded_buffer::channel(1024 * 1024);
  let (state_tx, mut state_rx) = bd_bounded_buffer::channel(1024 * 1024);
  let sender = async_log_buffer::Sender::from_parts(log_tx, state_tx);

  let sdk_directory = TempDir::new().unwrap();
  let store = in_memory_store();
  let (opaque_entity_updates_tx, opaque_entity_updates_rx) = watch::channel(None);
  let handle = LoggerHandle {
    tx: sender,
    stats: Stats::new(&Collector::default().scope("")),
    session_strategy: Arc::new(Strategy::fixed(
      sdk_directory.path(),
      Arc::new(UUIDCallbacks),
    )),
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
    tokio::time::timeout(std::time::Duration::from_secs(1), state_rx.recv())
      .await
      .unwrap()
      .unwrap()
      .message,
    async_log_buffer::StateUpdateMessage::SetEntityId(Some(entity_id)) if entity_id == "hashed-entity-id"
  ));
  assert!(matches!(
    handle.pending_entity_id.lock().clone(),
    Some(super::PendingEntityIdUpdate::Set(entity_id)) if entity_id == "hashed-entity-id"
  ));
  assert_eq!(
    Some("hashed-entity-id".to_string()),
    opaque_entity_updates_rx.borrow().clone()
  );

  handle.register_opaque_entity_id(None);
  assert!(matches!(
    tokio::time::timeout(std::time::Duration::from_secs(1), state_rx.recv())
      .await
      .unwrap()
      .unwrap()
      .message,
    async_log_buffer::StateUpdateMessage::SetEntityId(None)
  ));
  assert_eq!(
    Some(super::PendingEntityIdUpdate::Clear),
    handle.pending_entity_id.lock().clone()
  );
  assert_eq!(None, opaque_entity_updates_rx.borrow().clone());
}
