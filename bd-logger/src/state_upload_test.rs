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

/// Creates a persistent `bd_state::Store` backed by the given directory with snapshotting enabled.
/// Inserts a dummy entry so that `rotate_journal` produces a non-empty snapshot file.
async fn make_state_store(
  dir: &std::path::Path,
) -> (Arc<bd_state::Store>, Arc<bd_state::RetentionRegistry>) {
  let runtime_loader = ConfigLoader::new(dir);
  // Enable snapshotting — the default (0) disables it.
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
  let result = bd_state::Store::persistent(
    dir,
    bd_state::PersistentStoreConfig::default(),
    Arc::new(SystemTimeProvider {}),
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
  (Arc::new(result.store), result.retention_registry)
}

#[tokio::test]
async fn no_state_changes() {
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");

  // Verify construction succeeds and notify_upload_needed is non-blocking.
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

  // With no state store, there are no state changes — channel send should succeed without blocking.
  handle.notify_upload_needed(0, 1_000_000);
}

#[tokio::test]
async fn uploaded_coverage_prevents_reupload() {
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

  worker.on_state_uploaded(1_704_067_300_000_000);

  // After uploading through a timestamp, find_snapshots_in_range should return nothing for
  // ranges already covered.
  let snapshots = worker.find_snapshots_in_range(1_704_067_200_000_000, 1_704_067_300_000_000);
  assert!(snapshots.is_empty(), "no snapshots in covered range");
}

#[tokio::test]
async fn cooldown_prevents_rapid_snapshot_creation() {
  let temp_dir = tempfile::tempdir().unwrap();
  let state_dir = temp_dir.path().join("state");
  let snapshots_dir = state_dir.join("snapshots");
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let (state_store, retention_registry) = make_state_store(&state_dir).await;

  let (_handle, worker) = StateUploadHandle::new(
    Some(state_dir),
    store,
    Some(retention_registry),
    Some(state_store),
    1000,
    Arc::new(SystemTimeProvider {}),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

  let batch_ts =
    OffsetDateTime::now_utc().unix_timestamp().cast_unsigned() * 1_000_000 + u64::MAX / 2;

  let snapshot1 = worker.get_or_create_snapshot(batch_ts).await;
  assert!(snapshot1.is_some(), "first snapshot should be created");

  // Count snapshot files created.
  let count_files = || std::fs::read_dir(&snapshots_dir).unwrap().count();
  let file_count_after_first = count_files();
  assert!(file_count_after_first >= 1, "snapshot file should exist");

  let snapshot2 = worker.get_or_create_snapshot(batch_ts + 1000).await;
  assert!(
    snapshot2.is_some(),
    "second call should return existing snapshot"
  );
  assert_eq!(
    count_files(),
    file_count_after_first,
    "should not create new snapshot due to cooldown"
  );
}

#[tokio::test]
async fn cooldown_allows_snapshot_after_interval() {
  let temp_dir = tempfile::tempdir().unwrap();
  let state_dir = temp_dir.path().join("state");
  let snapshots_dir = state_dir.join("snapshots");
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let (state_store, retention_registry) = make_state_store(&state_dir).await;
  let time_provider = Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc()));

  let (_handle, worker) = StateUploadHandle::new(
    Some(state_dir),
    store,
    Some(retention_registry),
    Some(state_store.clone()),
    1,
    time_provider.clone(),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

  let batch_ts = time_provider.now().unix_timestamp().cast_unsigned() * 1_000_000 + u64::MAX / 2;

  let snapshot1 = worker.get_or_create_snapshot(batch_ts).await;
  assert!(snapshot1.is_some());

  let count_files = || std::fs::read_dir(&snapshots_dir).unwrap().count();
  let file_count_after_first = count_files();

  // Advance time past cooldown.
  time_provider.advance(time::Duration::milliseconds(2));

  // Clear the first snapshot so the second call must create a new one.
  for entry in std::fs::read_dir(&snapshots_dir).unwrap() {
    let entry = entry.unwrap();
    std::fs::remove_file(entry.path()).unwrap();
  }

  let future_batch_ts = batch_ts + 10_000_000;
  let snapshot2 = worker.get_or_create_snapshot(future_batch_ts).await;
  assert!(snapshot2.is_some());
  assert_eq!(
    count_files(),
    file_count_after_first,
    "should create new snapshot after cooldown expires"
  );
}

#[tokio::test]
async fn zero_cooldown_allows_immediate_snapshot_creation() {
  let temp_dir = tempfile::tempdir().unwrap();
  let state_dir = temp_dir.path().join("state");
  let snapshots_dir = state_dir.join("snapshots");
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let (state_store, retention_registry) = make_state_store(&state_dir).await;

  let (_handle, worker) = StateUploadHandle::new(
    Some(state_dir),
    store,
    Some(retention_registry),
    Some(state_store.clone()),
    0,
    Arc::new(SystemTimeProvider {}),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

  let base_ts =
    OffsetDateTime::now_utc().unix_timestamp().cast_unsigned() * 1_000_000 + u64::MAX / 2;

  let count_files = || std::fs::read_dir(&snapshots_dir).unwrap().count();
  let mut total_snapshots = 0;

  for i in 0 .. 3 {
    // Clear snapshots so each iteration forces a new creation.
    if let Ok(entries) = std::fs::read_dir(&snapshots_dir) {
      for entry in entries {
        let entry = entry.unwrap();
        std::fs::remove_file(entry.path()).unwrap();
      }
    }

    let snapshot = worker
      .get_or_create_snapshot(base_ts + i * 10_000_000)
      .await;
    assert!(snapshot.is_some());
    total_snapshots += count_files();
  }

  assert!(
    total_snapshots >= 3,
    "all snapshots should be created with zero cooldown"
  );
}

#[tokio::test]
async fn uses_existing_snapshot_from_normal_rotation() {
  let temp_dir = tempfile::tempdir().unwrap();
  let state_dir = temp_dir.path().join("state");
  let snapshots_dir = state_dir.join("snapshots");
  std::fs::create_dir_all(&snapshots_dir).unwrap();
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let existing_timestamp = 1_700_000_000_000_000u64;
  let existing_snapshot = snapshots_dir.join(format!("state.jrn.g0.t{existing_timestamp}.zz"));
  std::fs::write(&existing_snapshot, b"pre-existing snapshot from rotation").unwrap();

  let (_handle, worker) = StateUploadHandle::new(
    Some(state_dir),
    store,
    None,
    None,
    0,
    Arc::new(SystemTimeProvider {}),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

  // find_snapshots_in_range should find the snapshot when it falls in range.
  let snapshots = worker.find_snapshots_in_range(0, existing_timestamp);
  assert_eq!(snapshots.len(), 1);
  assert_eq!(snapshots[0].timestamp_micros, existing_timestamp);
}

#[tokio::test]
async fn creates_on_demand_snapshot_when_none_exists() {
  let temp_dir = tempfile::tempdir().unwrap();
  let state_dir = temp_dir.path().join("state");
  let snapshots_dir = state_dir.join("snapshots");
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let (state_store, retention_registry) = make_state_store(&state_dir).await;

  let (_handle, worker) = StateUploadHandle::new(
    Some(state_dir),
    store,
    Some(retention_registry),
    Some(state_store),
    0,
    Arc::new(SystemTimeProvider {}),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

  let batch_ts =
    OffsetDateTime::now_utc().unix_timestamp().cast_unsigned() * 1_000_000 + u64::MAX / 2;

  let snapshot = worker.get_or_create_snapshot(batch_ts).await;

  assert!(snapshot.is_some());
  assert_eq!(
    std::fs::read_dir(&snapshots_dir).unwrap().count(),
    1,
    "should create new snapshot on-demand when none exists"
  );
}

#[tokio::test]
async fn prefers_existing_snapshot_over_on_demand_creation() {
  let temp_dir = tempfile::tempdir().unwrap();
  let state_dir = temp_dir.path().join("state");
  let snapshots_dir = state_dir.join("snapshots");
  std::fs::create_dir_all(&snapshots_dir).unwrap();
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let old_snapshot_ts = 1_700_000_000_000_000u64;
  let old_snapshot = snapshots_dir.join(format!("state.jrn.g0.t{old_snapshot_ts}.zz"));
  std::fs::write(&old_snapshot, b"old snapshot").unwrap();

  let newer_snapshot_ts = 1_700_001_000_000_000u64;
  let newer_snapshot = snapshots_dir.join(format!("state.jrn.g1.t{newer_snapshot_ts}.zz"));
  std::fs::write(&newer_snapshot, b"newer snapshot").unwrap();

  let (_handle, worker) = StateUploadHandle::new(
    Some(state_dir),
    store,
    None,
    None,
    0,
    Arc::new(SystemTimeProvider {}),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

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

#[test]
fn backpressure_error_detection_matches_queue_full() {
  assert!(StateUploadWorker::is_backpressure_error(
    "upload queue full and all pending uploads are state snapshots"
  ));
  assert!(!StateUploadWorker::is_backpressure_error(
    "failed to open snapshot file"
  ));
}
