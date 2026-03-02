// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::*;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag as _, IntWatch};
use bd_test_helpers::session::in_memory_store;
use bd_time::{SystemTimeProvider, TestTimeProvider};
use time::OffsetDateTime;
use uuid::Uuid;

/// Creates a persistent `bd_state::Store` backed by the given directory with snapshotting enabled.
/// Inserts a dummy entry so that `rotate_journal` produces a non-empty snapshot file.
async fn make_state_store(
  dir: &std::path::Path,
) -> (
  Arc<bd_state::Store>,
  Arc<bd_state::RetentionRegistry>,
  bd_state::RetentionHandle,
  Arc<TestTimeProvider>,
) {
  let runtime_loader = ConfigLoader::new(dir);
  runtime_loader
    .update_snapshot(bd_proto::protos::client::api::RuntimeUpdate {
      version_nonce: "test".to_string(),
      runtime: Some(bd_proto::protos::client::runtime::Runtime {
        values: std::iter::once((
          bd_runtime::runtime::state::MaxSnapshotCount::path().to_string(),
          bd_proto::protos::client::runtime::runtime::Value {
            type_: Some(bd_proto::protos::client::runtime::runtime::value::Type::UintValue(10)),
            ..Default::default()
          },
        ))
        .collect(),
        ..Default::default()
      })
      .into(),
      ..Default::default()
    })
    .await
    .unwrap();
  let stats = bd_client_stats_store::Collector::default().scope("test");
  let time_provider = Arc::new(TestTimeProvider::new(
    OffsetDateTime::from_unix_timestamp(1).unwrap(),
  ));
  let result = bd_state::Store::persistent(
    dir,
    bd_state::PersistentStoreConfig::default(),
    time_provider.clone(),
    &runtime_loader,
    &stats,
  )
  .await
  .unwrap();
  // Insert a value so journal rotation produces a snapshot file.
  result
    .store
    .insert(
      bd_state::Scope::GlobalState,
      "test_key".to_string(),
      bd_state::string_value("test_value"),
    )
    .await
    .unwrap();
  let retention_handle = result.retention_registry.create_handle().await;
  retention_handle.update_retention_micros(0);
  (
    Arc::new(result.store),
    result.retention_registry,
    retention_handle,
    time_provider,
  )
}

async fn insert_state_change(state_store: &bd_state::Store, key: &str) {
  state_store
    .insert(
      bd_state::Scope::GlobalState,
      key.to_string(),
      bd_state::string_value("test_value"),
    )
    .await
    .unwrap();
}

struct Setup {
  _temp_dir: tempfile::TempDir,
  state_dir: std::path::PathBuf,
  snapshots_dir: std::path::PathBuf,
  store: Arc<bd_key_value::Store>,
  state_store: Arc<bd_state::Store>,
  retention_registry: Arc<bd_state::RetentionRegistry>,
  _state_retention_handle: bd_state::RetentionHandle,
  time_provider: Arc<TestTimeProvider>,
}

impl Setup {
  async fn new() -> Self {
    let temp_dir = tempfile::tempdir().unwrap();
    let state_dir = temp_dir.path().join("state");
    let snapshots_dir = state_dir.join("snapshots");
    let store = in_memory_store();
    let (state_store, retention_registry, state_retention_handle, time_provider) =
      make_state_store(&state_dir).await;
    Self {
      _temp_dir: temp_dir,
      state_dir,
      snapshots_dir,
      store,
      state_store,
      retention_registry,
      _state_retention_handle: state_retention_handle,
      time_provider,
    }
  }

  async fn worker_with_client(
    &self,
    cooldown_micros: u32,
    client: Arc<bd_artifact_upload::MockClient>,
  ) -> StateUploadWorker {
    let stats = bd_client_stats_store::Collector::default().scope("test");
    let (_handle, worker) = StateUploadHandle::new(
      Some(self.state_dir.clone()),
      self.store.clone(),
      Some(self.retention_registry.clone()),
      Some(self.state_store.clone()),
      cooldown_micros,
      self.time_provider.clone(),
      client,
      &stats,
    )
    .await;
    worker
  }

  fn now_micros(&self) -> u64 {
    self
      .time_provider
      .now()
      .unix_timestamp_micros()
      .cast_unsigned()
  }

  async fn create_snapshot_after_state_change(&self, key: &str) -> u64 {
    self.time_provider.advance(time::Duration::seconds(1));
    insert_state_change(&self.state_store, key).await;
    self.create_rotated_snapshot().await
  }

  async fn create_rotated_snapshot(&self) -> u64 {
    let path = self.state_store.rotate_journal().await.unwrap();
    let filename = path.file_name().unwrap().to_str().unwrap();
    bd_resilient_kv::SnapshotFilename::parse(filename)
      .unwrap()
      .timestamp_micros
  }

  fn clear_snapshot_files(&self) {
    if let Ok(entries) = std::fs::read_dir(&self.snapshots_dir) {
      for entry in entries.flatten() {
        std::fs::remove_file(entry.path()).unwrap();
      }
    }
  }

  fn count_snapshot_files(&self) -> usize {
    std::fs::read_dir(&self.snapshots_dir).map_or(0, Iterator::count)
  }
}

#[tokio::test]
async fn no_snapshot_files_skips_upload() {
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");
  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client.expect_enqueue_upload().times(0);
  let (_handle, mut worker) = StateUploadHandle::new(
    None,
    store,
    None,
    None,
    0,
    Arc::new(SystemTimeProvider {}),
    Arc::new(mock_client),
    &stats,
  )
  .await;

  worker.pending_range = Some(PendingRange {
    oldest_micros: 1,
    newest_micros: 100,
  });
  worker.process_pending().await;
  assert!(worker.pending_range.is_none());
}

#[tokio::test]
async fn cooldown_allows_snapshot_after_interval() {
  let setup = Setup::new().await;
  let worker = setup
    .worker_with_client(1, Arc::new(bd_artifact_upload::MockClient::new()))
    .await;

  let batch_ts = setup.now_micros();

  let snapshot1 = worker.create_snapshot_if_needed(batch_ts).await;
  assert!(snapshot1.is_some());

  let file_count_after_first = setup.count_snapshot_files();

  // Advance time past cooldown.
  setup.time_provider.advance(time::Duration::milliseconds(2));

  insert_state_change(&setup.state_store, "test_key_2").await;

  let future_batch_ts = batch_ts + 100;
  let snapshot2 = worker.create_snapshot_if_needed(future_batch_ts).await;
  assert!(snapshot2.is_some());
  assert_eq!(
    setup.count_snapshot_files(),
    file_count_after_first + 1,
    "should create new snapshot after cooldown expires"
  );
}

#[tokio::test]
async fn find_all_snapshots_returns_ordered_snapshots() {
  let setup = Setup::new().await;
  let worker = setup
    .worker_with_client(0, Arc::new(bd_artifact_upload::MockClient::new()))
    .await;

  let old_snapshot_ts = setup.create_rotated_snapshot().await;
  setup.time_provider.advance(time::Duration::milliseconds(1));
  insert_state_change(&setup.state_store, "test_key_2").await;
  let newer_snapshot_ts = setup.create_rotated_snapshot().await;
  // find_all_snapshots returns snapshots sorted oldest first.
  let snapshots = worker.find_all_snapshots();
  assert_eq!(snapshots.len(), 2);
  assert_eq!(
    snapshots[0].timestamp_micros, old_snapshot_ts,
    "oldest snapshot first"
  );
  assert_eq!(snapshots[1].timestamp_micros, newer_snapshot_ts);
}

#[test]
fn pending_range_merge_widens_bounds() {
  let mut pending = PendingRange {
    oldest_micros: 100,
    newest_micros: 200,
  };
  pending.merge(PendingRange {
    oldest_micros: 50,
    newest_micros: 250,
  });
  assert_eq!(pending.oldest_micros, 50);
  assert_eq!(pending.newest_micros, 250);
}

#[tokio::test]
async fn refresh_retention_handle_uses_pending_range_oldest() {
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");
  let retention_registry = Arc::new(bd_state::RetentionRegistry::new(IntWatch::new_for_testing(
    10,
  )));
  let (_handle, mut worker) = StateUploadHandle::new(
    None,
    store,
    Some(retention_registry.clone()),
    None,
    0,
    Arc::new(SystemTimeProvider {}),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

  worker.pending_range = Some(PendingRange {
    oldest_micros: 123,
    newest_micros: 999,
  });
  worker.refresh_retention_handle();
  assert_eq!(
    retention_registry.min_retention_timestamp().await,
    Some(123)
  );

  worker.pending_range = None;
  worker.refresh_retention_handle();
  assert_eq!(retention_registry.min_retention_timestamp().await, None);
}

#[tokio::test]
async fn process_upload_advances_pending_oldest_across_multiple_snapshots() {
  let setup = Setup::new().await;
  let first_snapshot_ts = setup.create_snapshot_after_state_change("test_key_1").await;
  let second_snapshot_ts = setup.create_snapshot_after_state_change("test_key_2").await;

  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client
    .expect_enqueue_upload()
    .times(2)
    .returning(|_, _, _, _, _, _, persisted_tx| {
      if let Some(tx) = persisted_tx {
        let _ = tx.send(Ok(()));
      }
      Ok(Uuid::new_v4())
    });

  let mut worker = setup.worker_with_client(0, Arc::new(mock_client)).await;
  worker.pending_range = Some(PendingRange {
    oldest_micros: first_snapshot_ts,
    newest_micros: second_snapshot_ts,
  });

  let result = worker
    .process_upload(first_snapshot_ts, second_snapshot_ts)
    .await;
  assert_eq!(result, ProcessResult::Progress);
  assert_eq!(
    worker.pending_range.map(|r| r.oldest_micros),
    Some(second_snapshot_ts)
  );
}

#[tokio::test]
async fn older_incoming_range_reexpands_pending_after_progress() {
  let setup = Setup::new().await;
  let first_snapshot_ts = setup.create_snapshot_after_state_change("test_key_1").await;
  let second_snapshot_ts = setup.create_snapshot_after_state_change("test_key_2").await;

  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client
    .expect_enqueue_upload()
    .times(2)
    .returning(|_, _, _, _, _, _, persisted_tx| {
      if let Some(tx) = persisted_tx {
        let _ = tx.send(Ok(()));
      }
      Ok(Uuid::new_v4())
    });

  let stats = bd_client_stats_store::Collector::default().scope("test");
  let (handle, mut worker) = StateUploadHandle::new(
    Some(setup.state_dir.clone()),
    setup.store.clone(),
    Some(setup.retention_registry.clone()),
    Some(setup.state_store.clone()),
    0,
    setup.time_provider.clone(),
    Arc::new(mock_client),
    &stats,
  )
  .await;
  worker.pending_range = Some(PendingRange {
    oldest_micros: first_snapshot_ts,
    newest_micros: second_snapshot_ts,
  });
  let result = worker
    .process_upload(first_snapshot_ts, second_snapshot_ts)
    .await;
  assert_eq!(result, ProcessResult::Progress);
  assert_eq!(
    worker.pending_range.map(|r| r.oldest_micros),
    Some(second_snapshot_ts)
  );

  handle.notify_upload_needed(first_snapshot_ts, second_snapshot_ts);
  worker.drain_pending_accumulator();
  assert_eq!(
    worker.pending_range.map(|r| r.oldest_micros),
    Some(first_snapshot_ts)
  );
}

#[tokio::test]
async fn notify_upload_needed_keeps_range_when_wake_channel_is_full() {
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");
  let (handle, _worker) = StateUploadHandle::new(
    None,
    store,
    None,
    None,
    0,
    Arc::new(SystemTimeProvider {}),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

  handle.wake_tx.try_send(()).unwrap();

  handle.notify_upload_needed(100, 200);
  let pending = handle.pending_accumulator.lock();
  let range = pending.range.unwrap();
  assert_eq!(range.oldest_micros, 100);
  assert_eq!(range.newest_micros, 200);
}

#[tokio::test]
async fn cooldown_defer_keeps_pending_for_retry() {
  let setup = Setup::new().await;
  let mut worker = setup
    .worker_with_client(1000, Arc::new(bd_artifact_upload::MockClient::new()))
    .await;

  // Set recent snapshot creation time to force a cooldown defer path.
  let _created = worker.create_snapshot_if_needed(100).await.unwrap();
  setup.clear_snapshot_files();
  setup.time_provider.advance(time::Duration::milliseconds(1));
  insert_state_change(&setup.state_store, "test_key_2").await;

  worker.pending_range = Some(PendingRange {
    oldest_micros: 1,
    newest_micros: 2_000_000,
  });
  worker.process_pending().await;

  assert!(
    worker.pending_range.is_some(),
    "cooldown defer should keep pending range for retry"
  );
}


#[tokio::test]
async fn enqueue_backpressure_keeps_pending_range() {
  let setup = Setup::new().await;
  let snapshot_ts = setup.create_rotated_snapshot().await;

  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client
    .expect_enqueue_upload()
    .times(1)
    .returning(|_, _, _, _, _, _, _| Err(bd_artifact_upload::EnqueueError::QueueFull));
  let mut worker = setup.worker_with_client(0, Arc::new(mock_client)).await;

  worker.pending_range = Some(PendingRange {
    oldest_micros: 1,
    newest_micros: snapshot_ts,
  });
  worker.process_pending().await;

  assert_eq!(worker.pending_range.map(|r| r.oldest_micros), Some(1));
  assert_eq!(
    worker.pending_range.map(|r| r.newest_micros),
    Some(snapshot_ts)
  );
}

#[tokio::test]
async fn persisted_ack_error_keeps_pending_range() {
  let setup = Setup::new().await;
  let snapshot_ts = setup.create_rotated_snapshot().await;

  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client
    .expect_enqueue_upload()
    .times(1)
    .returning(|_, _, _, _, _, _, persisted_tx| {
      if let Some(tx) = persisted_tx {
        let _ = tx.send(Err(bd_artifact_upload::EnqueueError::Closed));
      }
      Ok(Uuid::new_v4())
    });

  let mut worker = setup.worker_with_client(0, Arc::new(mock_client)).await;

  let result = worker.process_upload(1, snapshot_ts).await;
  assert_eq!(result, ProcessResult::Error);
}

#[tokio::test]
async fn persisted_ack_channel_drop_keeps_pending_range() {
  let setup = Setup::new().await;
  let snapshot_ts = setup.create_rotated_snapshot().await;

  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client
    .expect_enqueue_upload()
    .times(1)
    .returning(|_, _, _, _, _, _, _| Ok(Uuid::new_v4()));

  let mut worker = setup.worker_with_client(0, Arc::new(mock_client)).await;

  let result = worker.process_upload(1, snapshot_ts).await;
  assert_eq!(result, ProcessResult::Error);
}

#[tokio::test]
async fn successful_enqueue_ack_clears_pending() {
  let setup = Setup::new().await;
  let snapshot_ts = setup.create_rotated_snapshot().await;

  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client
    .expect_enqueue_upload()
    .times(1)
    .returning(|_, _, _, _, _, _, persisted_tx| {
      if let Some(tx) = persisted_tx {
        let _ = tx.send(Ok(()));
      }
      Ok(Uuid::new_v4())
    });

  let mut worker = setup.worker_with_client(0, Arc::new(mock_client)).await;

  worker.pending_range = Some(PendingRange {
    oldest_micros: 1,
    newest_micros: snapshot_ts,
  });
  worker.process_pending().await;

  assert!(worker.pending_range.is_none());
}

#[tokio::test]
async fn plan_upload_attempt_skips_when_no_state_changes_or_no_store() {
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");
  let (_handle, mut worker) = StateUploadHandle::new(
    None,
    store,
    None,
    None,
    0,
    Arc::new(SystemTimeProvider {}),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

  // First call with last_change=0: no prior rotation recorded, so we attempt rotation. No state
  // store configured, so create_snapshot_if_needed returns None → Skipped. The attempt records
  // last_change_at_rotation = Some(0).
  let result = worker.plan_upload_attempt(0, 20, 0).await;
  assert!(matches!(result, UploadPreflight::Skipped));

  // Second call with last_change=0: we've already rotated at 0, nothing changed → Skipped.
  let result = worker.plan_upload_attempt(0, 20, 0).await;
  assert!(matches!(result, UploadPreflight::Skipped));

  // Calls with different last_change values: no snapshots found, no state store →
  // create_snapshot_if_needed returns None → Skipped.
  let result = worker.plan_upload_attempt(0, 20, 15).await;
  assert!(matches!(result, UploadPreflight::Skipped));

  let result = worker.plan_upload_attempt(0, 20, 9).await;
  assert!(matches!(result, UploadPreflight::Skipped));
}

#[tokio::test]
async fn skipped_with_no_state_changes_clears_pending() {
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");
  let (_handle, mut worker) = StateUploadHandle::new(
    None,
    store,
    None,
    None,
    0,
    Arc::new(SystemTimeProvider {}),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

  worker.pending_range = Some(PendingRange {
    oldest_micros: 0,
    newest_micros: 100,
  });
  worker.process_pending().await;

  assert!(worker.pending_range.is_none());
}

#[tokio::test]
async fn plan_upload_attempt_returns_ready_for_in_range_snapshots() {
  let setup = Setup::new().await;
  let _first_snapshot_ts = setup.create_snapshot_after_state_change("test_key_1").await;
  let second_snapshot_ts = setup.create_snapshot_after_state_change("test_key_2").await;
  let mut worker = setup
    .worker_with_client(0, Arc::new(bd_artifact_upload::MockClient::new()))
    .await;

  match worker
    .plan_upload_attempt(second_snapshot_ts, second_snapshot_ts, second_snapshot_ts)
    .await
  {
    UploadPreflight::Ready(snapshots) => {
      assert!(!snapshots.is_empty());
      assert_eq!(
        snapshots.last().unwrap().timestamp_micros,
        second_snapshot_ts
      );
    },
    _ => panic!("expected ready preflight"),
  }
}

#[tokio::test]
async fn plan_upload_attempt_filters_snapshots_to_pending_range() {
  let setup = Setup::new().await;
  let first_snapshot_ts = setup.create_snapshot_after_state_change("test_key_1").await;
  let second_snapshot_ts = setup.create_snapshot_after_state_change("test_key_2").await;
  let mut worker = setup
    .worker_with_client(0, Arc::new(bd_artifact_upload::MockClient::new()))
    .await;

  match worker
    .plan_upload_attempt(second_snapshot_ts, second_snapshot_ts, second_snapshot_ts)
    .await
  {
    UploadPreflight::Ready(snapshots) => {
      assert_eq!(snapshots.len(), 1);
      assert_eq!(snapshots[0].timestamp_micros, second_snapshot_ts);
      assert_ne!(snapshots[0].timestamp_micros, first_snapshot_ts);
    },
    _ => panic!("expected ready preflight"),
  }
}

#[tokio::test]
async fn restart_with_zero_last_change_uploads_existing_snapshot() {
  // Simulate pre-restart: create a snapshot and persist a pending range.
  let setup = Setup::new().await;
  let snapshot_ts = setup
    .create_snapshot_after_state_change("pre_restart")
    .await;
  setup.store.set(
    &PENDING_UPLOAD_RANGE_KEY,
    &pending_range_to_proto(PendingRange {
      oldest_micros: snapshot_ts,
      newest_micros: snapshot_ts,
    }),
  );

  // Create a fresh in-memory state store to simulate a restart where last_change_micros is 0.
  // In production this happens when the previous process only wrote to the System scope (not
  // cleared on restart) and ephemeral scopes were already empty.
  let stats = bd_client_stats_store::Collector::default().scope("test");
  let restart_store = Arc::new(bd_state::Store::in_memory(
    setup.time_provider.clone(),
    None,
    &ConfigLoader::new(setup.state_dir.as_path()),
    &stats,
  ));
  assert_eq!(restart_store.last_change_micros(), 0);

  // The worker should find the snapshot on disk and upload it despite last_change being 0.
  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client
    .expect_enqueue_upload()
    .times(1)
    .returning(|_, _, _, _, _, _, persisted_tx| {
      if let Some(tx) = persisted_tx {
        let _ = tx.send(Ok(()));
      }
      Ok(Uuid::new_v4())
    });

  let (handle, worker) = StateUploadHandle::new(
    Some(setup.state_dir.clone()),
    setup.store.clone(),
    Some(setup.retention_registry.clone()),
    Some(restart_store),
    0,
    setup.time_provider.clone(),
    Arc::new(mock_client),
    &stats,
  )
  .await;

  drop(handle);
  worker.run().await;

  // Pending range should be cleared after successful upload.
  assert!(
    setup
      .store
      .get(&PENDING_UPLOAD_RANGE_KEY)
      .and_then(|proto| pending_range_from_proto(&proto))
      .is_none(),
    "pending range should be cleared after restart upload"
  );
}
