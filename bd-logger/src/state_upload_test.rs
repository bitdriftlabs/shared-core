// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::*;
use bd_test_helpers::session::in_memory_store;
use bd_time::{SystemTimeProvider, TestTimeProvider};
use std::sync::atomic::Ordering;
use time::OffsetDateTime;

struct MockSnapshotCreator {
  snapshot_dir: PathBuf,
  call_count: AtomicU64,
}

impl MockSnapshotCreator {
  fn new(snapshot_dir: PathBuf) -> Self {
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    Self {
      snapshot_dir,
      call_count: AtomicU64::new(0),
    }
  }

  fn call_count(&self) -> u64 {
    self.call_count.load(Ordering::Relaxed)
  }
}

#[async_trait::async_trait]
impl SnapshotCreator for MockSnapshotCreator {
  async fn create_snapshot(&self) -> Option<PathBuf> {
    let count = self.call_count.fetch_add(1, Ordering::Relaxed);
    let now = OffsetDateTime::now_utc();
    let timestamp_micros =
      now.unix_timestamp().cast_unsigned() * 1_000_000 + u64::from(now.microsecond());
    let filename = format!("state.jrn.g{count}.t{timestamp_micros}.zz");
    let path = self.snapshot_dir.join(&filename);
    std::fs::write(&path, b"mock snapshot data").unwrap();
    Some(path)
  }
}

#[tokio::test]
async fn correlator_no_state_changes() {
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let (correlator, _worker) = StateLogCorrelator::new(
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

  // With no state changes recorded, the last_state_change_micros should be 0.
  assert_eq!(
    correlator.last_state_change_micros.load(Ordering::Relaxed),
    0,
  );
}

#[tokio::test]
async fn correlator_tracks_state_changes() {
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let (correlator, _worker) = StateLogCorrelator::new(
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

  let timestamp = OffsetDateTime::from_unix_timestamp(1_704_067_200).unwrap();
  correlator.on_state_change(timestamp);

  let last_change = correlator.last_state_change_micros.load(Ordering::Relaxed);
  assert_eq!(last_change, 1_704_067_200_000_000);
}

#[tokio::test]
async fn correlator_uploaded_coverage_prevents_reupload() {
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let (correlator, worker) = StateLogCorrelator::new(
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

  let timestamp = OffsetDateTime::from_unix_timestamp(1_704_067_200).unwrap();
  correlator.on_state_change(timestamp);

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

  let mock_creator = Arc::new(MockSnapshotCreator::new(snapshots_dir));

  let (correlator, worker) = StateLogCorrelator::new(
    Some(state_dir),
    store,
    None,
    Some(mock_creator.clone()),
    1000,
    Arc::new(SystemTimeProvider {}),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

  correlator.on_state_change(OffsetDateTime::now_utc());

  let batch_ts =
    OffsetDateTime::now_utc().unix_timestamp().cast_unsigned() * 1_000_000 + u64::MAX / 2;

  let snapshot1 = worker.get_or_create_snapshot(batch_ts).await;
  assert!(snapshot1.is_some(), "first snapshot should be created");
  assert_eq!(mock_creator.call_count(), 1);

  let snapshot2 = worker.get_or_create_snapshot(batch_ts + 1000).await;
  assert!(
    snapshot2.is_some(),
    "second call should return existing snapshot"
  );
  assert_eq!(
    mock_creator.call_count(),
    1,
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

  let mock_creator = Arc::new(MockSnapshotCreator::new(snapshots_dir.clone()));
  let time_provider = Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc()));

  let (correlator, worker) = StateLogCorrelator::new(
    Some(state_dir),
    store,
    None,
    Some(mock_creator.clone()),
    1,
    time_provider.clone(),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

  correlator.on_state_change(time_provider.now());

  let batch_ts = time_provider.now().unix_timestamp().cast_unsigned() * 1_000_000 + u64::MAX / 2;

  let snapshot1 = worker.get_or_create_snapshot(batch_ts).await;
  assert!(snapshot1.is_some());
  assert_eq!(mock_creator.call_count(), 1);

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
    mock_creator.call_count(),
    2,
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

  let mock_creator = Arc::new(MockSnapshotCreator::new(snapshots_dir.clone()));

  let (correlator, worker) = StateLogCorrelator::new(
    Some(state_dir),
    store,
    None,
    Some(mock_creator.clone()),
    0,
    Arc::new(SystemTimeProvider {}),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

  correlator.on_state_change(OffsetDateTime::now_utc());

  let base_ts =
    OffsetDateTime::now_utc().unix_timestamp().cast_unsigned() * 1_000_000 + u64::MAX / 2;

  for i in 0 .. 3 {
    // Clear snapshots so each iteration forces a new creation.
    for entry in std::fs::read_dir(&snapshots_dir).unwrap() {
      let entry = entry.unwrap();
      std::fs::remove_file(entry.path()).unwrap();
    }

    let snapshot = worker
      .get_or_create_snapshot(base_ts + i * 10_000_000)
      .await;
    assert!(snapshot.is_some());
  }

  assert_eq!(
    mock_creator.call_count(),
    3,
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

  let mock_creator = Arc::new(MockSnapshotCreator::new(snapshots_dir.clone()));

  let existing_timestamp = 1_700_000_000_000_000u64;
  let existing_snapshot = snapshots_dir.join(format!("state.jrn.g0.t{existing_timestamp}.zz"));
  std::fs::write(&existing_snapshot, b"pre-existing snapshot from rotation").unwrap();

  let (_correlator, worker) = StateLogCorrelator::new(
    Some(state_dir),
    store,
    None,
    Some(mock_creator.clone()),
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
  assert_eq!(snapshots[0].generation, 0);
  assert_eq!(
    mock_creator.call_count(),
    0,
    "should not create new snapshot when existing one covers the batch"
  );
}

#[tokio::test]
async fn creates_on_demand_snapshot_when_none_exists() {
  let temp_dir = tempfile::tempdir().unwrap();
  let state_dir = temp_dir.path().join("state");
  let snapshots_dir = state_dir.join("snapshots");
  let store = in_memory_store();
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let mock_creator = Arc::new(MockSnapshotCreator::new(snapshots_dir));

  let (correlator, worker) = StateLogCorrelator::new(
    Some(state_dir),
    store,
    None,
    Some(mock_creator.clone()),
    0,
    Arc::new(SystemTimeProvider {}),
    Arc::new(bd_artifact_upload::MockClient::new()),
    &stats,
  )
  .await;

  correlator.on_state_change(OffsetDateTime::now_utc());

  let batch_ts =
    OffsetDateTime::now_utc().unix_timestamp().cast_unsigned() * 1_000_000 + u64::MAX / 2;

  let snapshot = worker.get_or_create_snapshot(batch_ts).await;

  assert!(snapshot.is_some());
  assert_eq!(
    mock_creator.call_count(),
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

  let mock_creator = Arc::new(MockSnapshotCreator::new(snapshots_dir.clone()));

  let old_snapshot_ts = 1_700_000_000_000_000u64;
  let old_snapshot = snapshots_dir.join(format!("state.jrn.g0.t{old_snapshot_ts}.zz"));
  std::fs::write(&old_snapshot, b"old snapshot").unwrap();

  let newer_snapshot_ts = 1_700_001_000_000_000u64;
  let newer_snapshot = snapshots_dir.join(format!("state.jrn.g1.t{newer_snapshot_ts}.zz"));
  std::fs::write(&newer_snapshot, b"newer snapshot").unwrap();

  let (_correlator, worker) = StateLogCorrelator::new(
    Some(state_dir),
    store,
    None,
    Some(mock_creator.clone()),
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
  assert_eq!(
    mock_creator.call_count(),
    0,
    "should not create snapshot when existing ones are available"
  );
}
