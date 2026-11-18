// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

use super::{Stats, with_thread_local_logger_guard};
use crate::app_version::Repository;
use crate::device_command::RegisteredCommandDispatcher;
use crate::logger::CaptureSession;
use crate::{LoggerHandle, async_log_buffer};
use bd_client_stats_store::Collector;
use bd_event_buffer::{
  EventBuffer,
  EventBufferEntry,
  EventBufferLimits,
  EventContext,
  LoggerControl,
  LoggerIngressPayload,
};
use bd_log_primitives::{AnnotatedLogField, LogAttributesOverrides, log_level};
use bd_proto::protos::logging::payload::LogType;
use bd_session::Strategy;
use bd_session::configuration::Callbacks as SessionCallbacks;
use bd_session::test::no_timeout;
use bd_test_helpers::session::in_memory_store;
use bd_time::TestTimeProvider;
use futures_util::poll;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tempfile::TempDir;
use time::{Duration, OffsetDateTime};
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

#[tokio::test]
async fn thread_local_logger_guard() {
  let event_buffer = event_buffer(1024);
  let sdk_directory = TempDir::new().unwrap();
  let session_strategy = no_timeout(sdk_directory.path()).strategy();
  let sender =
    async_log_buffer::Sender::from_event_buffer(event_buffer.clone(), session_strategy.clone());
  let store = in_memory_store();
  let handle = LoggerHandle {
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

fn persisted_event_handle(
  event_buffer: EventBuffer,
  session_strategy: Arc<Strategy>,
) -> LoggerHandle {
  let store = in_memory_store();
  LoggerHandle {
    tx: async_log_buffer::Sender::from_event_buffer(event_buffer, session_strategy.clone()),
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
  }
}

#[tokio::test]
async fn persisted_app_exit_is_admitted_without_creating_a_current_session() {
  let buffer = event_buffer(1024 * 1024);
  let directory = TempDir::new().unwrap();
  let handle = persisted_event_handle(buffer.clone(), no_timeout(directory.path()).strategy());
  let occurred_at = OffsetDateTime::UNIX_EPOCH;
  let fields = [(
    "app_version".into(),
    AnnotatedLogField::new_ootb("original"),
  )]
  .into();
  handle
    .log_persisted_app_exit("original-session".into(), occurred_at, fields)
    .unwrap();
  assert!(handle.current_session_id().is_err());
  let EventBufferEntry::Ingress(event) = buffer.next_batch(1).await.entries.pop().unwrap() else {
    panic!("expected persisted AppExit ingress");
  };
  assert!(matches!(
    event.context,
    EventContext::PreviousProcess { .. }
  ));
  let LoggerIngressPayload::Log(log) = event.payload else {
    panic!("expected log");
  };
  assert_eq!(log.log_type, LogType::LIFECYCLE);
  assert_eq!(log.log_level, log_level::INFO);
  assert_eq!(log.message.as_str(), Some("AppExit"));
  assert_eq!(log.fields["app_version"].value, "original".into());
  assert!(matches!(log.attributes_overrides,
    Some(LogAttributesOverrides::PersistedPreviousRun { session_id, occurred_at: timestamp })
    if session_id.as_ref() == "original-session" && timestamp == occurred_at));
}

#[tokio::test]
async fn persisted_app_exit_reports_full_and_closed_queue_admission_errors() {
  let directory = TempDir::new().unwrap();
  for closed in [false, true] {
    let buffer = event_buffer(if closed { 1024 * 1024 } else { 0 });
    if closed {
      buffer.close();
    }
    let handle = persisted_event_handle(buffer, no_timeout(directory.path()).strategy());
    assert!(
      handle
        .log_persisted_app_exit(
          "original-session".into(),
          OffsetDateTime::UNIX_EPOCH,
          [].into(),
        )
        .is_err()
    );
    assert!(handle.current_session_id().is_err());
  }
}

#[tokio::test]
async fn current_session_id_neither_creates_nor_refreshes_an_inactive_session() {
  #[derive(Default)]
  struct Callbacks(AtomicUsize);
  impl SessionCallbacks for Callbacks {
    fn session_id_changed(&self, _: &str) {
      self.0.fetch_add(1, Ordering::SeqCst);
    }
  }
  let directory = TempDir::new().unwrap();
  let callbacks = Arc::new(Callbacks::default());
  let clock = Arc::new(TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let strategy = Strategy::configuration(
    directory.path(),
    Some("original-session".into()),
    Some(Duration::minutes(1)),
    callbacks.clone(),
    clock.clone(),
  )
  .strategy();
  let handle = persisted_event_handle(event_buffer(1024), strategy);
  assert!(handle.current_session_id().is_err());
  assert_eq!(callbacks.0.load(Ordering::SeqCst), 0);
  let original_session = handle.session_id().unwrap();
  clock.advance(Duration::seconds(50));
  assert_eq!(handle.current_session_id().unwrap(), original_session);
  // Reading above must not refresh activity: the original minute still expires.
  clock.advance(Duration::seconds(20));
  assert_eq!(handle.current_session_id().unwrap(), original_session);
  assert_eq!(callbacks.0.load(Ordering::SeqCst), 1);
  assert_ne!(handle.session_id().unwrap(), original_session);
  assert_eq!(callbacks.0.load(Ordering::SeqCst), 2);
}
