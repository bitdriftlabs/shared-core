// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::*;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag as _};
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
  (
    Arc::new(result.store),
    result.retention_registry,
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
  state_store: Arc<bd_state::Store>,
  retention_registry: Arc<bd_state::RetentionRegistry>,
  time_provider: Arc<TestTimeProvider>,
}

impl Setup {
  async fn new() -> Self {
    let temp_dir = tempfile::tempdir().unwrap();
    let state_dir = temp_dir.path().join("state");
    let snapshots_dir = state_dir.join("snapshots");
    let (state_store, retention_registry, time_provider) = make_state_store(&state_dir).await;
    Self {
      _temp_dir: temp_dir,
      state_dir,
      snapshots_dir,
      state_store,
      retention_registry,
      time_provider,
    }
  }

  async fn worker_with_client(
    &self,
    cooldown_micros: u32,
    client: Arc<bd_artifact_upload::MockClient>,
  ) -> StateUploadWorker {
    let store = in_memory_store();
    let stats = bd_client_stats_store::Collector::default().scope("test");
    let (_handle, worker) = StateUploadHandle::new(
      Some(self.state_dir.clone()),
      store,
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
}

#[tokio::test]
async fn uploaded_coverage_prevents_reupload() {
  let setup = Setup::new().await;
  let snapshot_ts = setup.create_rotated_snapshot().await;

  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client.expect_enqueue_upload().times(0);
  let mut worker = setup.worker_with_client(0, Arc::new(mock_client)).await;

  worker.on_state_uploaded(snapshot_ts);
  worker.pending_range = Some(PendingRange {
    oldest_micros: 1,
    newest_micros: snapshot_ts,
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

  let file_count_after_first = count_snapshot_files(&setup.snapshots_dir);

  // Advance time past cooldown.
  setup.time_provider.advance(time::Duration::milliseconds(2));

  insert_state_change(&setup.state_store, "test_key_2").await;

  let future_batch_ts = batch_ts + 100;
  let snapshot2 = worker.create_snapshot_if_needed(future_batch_ts).await;
  assert!(snapshot2.is_some());
  assert_eq!(
    count_snapshot_files(&setup.snapshots_dir),
    file_count_after_first + 1,
    "should create new snapshot after cooldown expires"
  );
}

#[tokio::test]
async fn find_snapshots_in_range_returns_ordered_snapshots() {
  let setup = Setup::new().await;
  let worker = setup
    .worker_with_client(0, Arc::new(bd_artifact_upload::MockClient::new()))
    .await;

  let old_snapshot_ts = setup.create_rotated_snapshot().await;
  setup.time_provider.advance(time::Duration::milliseconds(1));
  insert_state_change(&setup.state_store, "test_key_2").await;
  let newer_snapshot_ts = setup.create_rotated_snapshot().await;
  // find_snapshots_in_range returns both snapshots in the range, sorted oldest first.
  let snapshots = worker.find_snapshots_in_range(0, newer_snapshot_ts + 1_000_000);
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

  for _ in 0 .. UPLOAD_CHANNEL_CAPACITY {
    handle.wake_tx.try_send(()).unwrap();
  }

  handle.notify_upload_needed(100, 200);
  let pending = handle.pending_accumulator.lock();
  let range = pending.range.unwrap();
  assert_eq!(range.oldest_micros, 100);
  assert_eq!(range.newest_micros, 200);
}

#[tokio::test]
async fn cooldown_defer_does_not_advance_watermark() {
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

  assert_eq!(
    worker.state_uploaded_through_micros.load(Ordering::Relaxed),
    0,
    "cooldown defer must not advance uploaded watermark"
  );
  assert!(
    worker.pending_range.is_some(),
    "cooldown defer should keep pending range for retry"
  );
}

fn count_snapshot_files(snapshots_dir: &std::path::Path) -> usize {
  std::fs::read_dir(snapshots_dir).unwrap().count()
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

  assert_eq!(
    worker.state_uploaded_through_micros.load(Ordering::Relaxed),
    0
  );
  assert_eq!(worker.pending_range.map(|r| r.oldest_micros), Some(1));
  assert_eq!(
    worker.pending_range.map(|r| r.newest_micros),
    Some(snapshot_ts)
  );
}

#[tokio::test]
async fn persisted_ack_error_does_not_advance_watermark() {
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

  let worker = setup.worker_with_client(0, Arc::new(mock_client)).await;

  let result = worker.process_upload(1, snapshot_ts).await;
  assert_eq!(result, ProcessResult::Error);
  assert_eq!(
    worker.state_uploaded_through_micros.load(Ordering::Relaxed),
    0
  );
}

#[tokio::test]
async fn persisted_ack_channel_drop_does_not_advance_watermark() {
  let setup = Setup::new().await;
  let snapshot_ts = setup.create_rotated_snapshot().await;

  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client
    .expect_enqueue_upload()
    .times(1)
    .returning(|_, _, _, _, _, _, _| Ok(Uuid::new_v4()));

  let worker = setup.worker_with_client(0, Arc::new(mock_client)).await;

  let result = worker.process_upload(1, snapshot_ts).await;
  assert_eq!(result, ProcessResult::Error);
  assert_eq!(
    worker.state_uploaded_through_micros.load(Ordering::Relaxed),
    0
  );
}

#[tokio::test]
async fn successful_enqueue_ack_advances_watermark_and_clears_pending() {
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

  assert_eq!(
    worker.state_uploaded_through_micros.load(Ordering::Relaxed),
    snapshot_ts
  );
  assert!(worker.pending_range.is_none());
}

#[tokio::test]
async fn plan_upload_attempt_skips_last_change_zero_already_covered_and_no_new_changes() {
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");
  let (_handle, worker) = StateUploadHandle::new(
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

  let result = worker.plan_upload_attempt(10, 20, 0, 0).await;
  assert!(matches!(result, UploadPreflight::Skipped));

  let result = worker.plan_upload_attempt(10, 20, 10, 15).await;
  assert!(matches!(result, UploadPreflight::Skipped));

  let result = worker.plan_upload_attempt(10, 20, 9, 9).await;
  assert!(matches!(result, UploadPreflight::Skipped));
}

#[tokio::test]
async fn skipped_with_incomplete_coverage_keeps_pending() {
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

  assert_eq!(
    worker.state_uploaded_through_micros.load(Ordering::Relaxed),
    0
  );
  assert!(worker.pending_range.is_some());
}

#[tokio::test]
async fn skipped_with_partial_coverage_narrows_pending_range() {
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

  worker
    .state_uploaded_through_micros
    .store(50, Ordering::Relaxed);
  worker.pending_range = Some(PendingRange {
    oldest_micros: 1,
    newest_micros: 100,
  });
  worker.process_pending().await;

  assert_eq!(worker.pending_range.map(|r| r.oldest_micros), Some(51));
  assert_eq!(worker.pending_range.map(|r| r.newest_micros), Some(100));
}

#[tokio::test]
async fn plan_upload_attempt_returns_ready_for_in_range_snapshots() {
  let setup = Setup::new().await;
  let _first_snapshot_ts = setup.create_snapshot_after_state_change("test_key_1").await;
  let second_snapshot_ts = setup.create_snapshot_after_state_change("test_key_2").await;
  let worker = setup
    .worker_with_client(0, Arc::new(bd_artifact_upload::MockClient::new()))
    .await;

  match worker
    .plan_upload_attempt(1, second_snapshot_ts, 0, second_snapshot_ts)
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
