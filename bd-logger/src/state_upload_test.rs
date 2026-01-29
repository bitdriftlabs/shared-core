// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::*;
use bd_client_common::file_system::RealFileSystem;
use bd_resilient_kv::SnapshotFilename;
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

#[test]
fn parse_snapshot_filename_valid() {
  let parsed = SnapshotFilename::parse("state.jrn.g0.t1704067200000000.zz").unwrap();
  assert_eq!(parsed.generation, 0);
  assert_eq!(parsed.timestamp_micros, 1_704_067_200_000_000);

  let parsed = SnapshotFilename::parse("state.jrn.g42.t9999999999999999.zz").unwrap();
  assert_eq!(parsed.generation, 42);
  assert_eq!(parsed.timestamp_micros, 9_999_999_999_999_999);

  let parsed = SnapshotFilename::parse("other.jrn.g5.t123456.zz").unwrap();
  assert_eq!(parsed.generation, 5);
  assert_eq!(parsed.timestamp_micros, 123_456);
}

#[test]
fn parse_snapshot_filename_invalid() {
  assert!(SnapshotFilename::parse("invalid.zz").is_none());
  assert!(SnapshotFilename::parse("state.jrn.g0.zz").is_none());
  assert!(SnapshotFilename::parse("state.jrn.t123.zz").is_none());
  assert!(SnapshotFilename::parse("state.jrn.g0.t123").is_none());
}

#[tokio::test]
async fn correlator_no_state_changes() {
  let temp_dir = tempfile::tempdir().unwrap();
  let file_system = Arc::new(RealFileSystem::new(temp_dir.path().to_path_buf()));
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let correlator = StateLogCorrelator::new(
    None,
    temp_dir.path().to_path_buf(),
    file_system,
    None,
    None,
    0,
    &stats,
  )
  .await;

  assert!(
    correlator
      .should_upload_state(1_000_000, 2_000_000)
      .is_none()
  );
}

#[tokio::test]
async fn correlator_tracks_state_changes() {
  let temp_dir = tempfile::tempdir().unwrap();
  let file_system = Arc::new(RealFileSystem::new(temp_dir.path().to_path_buf()));
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let correlator = StateLogCorrelator::new(
    None,
    temp_dir.path().to_path_buf(),
    file_system,
    None,
    None,
    0,
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
  let temp_dir = tempfile::tempdir().unwrap();
  let file_system = Arc::new(RealFileSystem::new(temp_dir.path().to_path_buf()));
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let correlator = StateLogCorrelator::new(
    None,
    temp_dir.path().to_path_buf(),
    file_system,
    None,
    None,
    0,
    &stats,
  )
  .await;

  let timestamp = OffsetDateTime::from_unix_timestamp(1_704_067_200).unwrap();
  correlator.on_state_change(timestamp);

  correlator.on_state_uploaded(1_704_067_300_000_000).await;

  assert!(
    correlator
      .should_upload_state(1_704_067_100_000_000, 1_704_067_200_000_000)
      .is_none()
  );
}

#[tokio::test]
async fn cooldown_prevents_rapid_snapshot_creation() {
  let temp_dir = tempfile::tempdir().unwrap();
  let state_dir = temp_dir.path().join("state");
  let snapshots_dir = state_dir.join("snapshots");
  let file_system = Arc::new(RealFileSystem::new(temp_dir.path().to_path_buf()));
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let mock_creator = Arc::new(MockSnapshotCreator::new(snapshots_dir));

  let correlator = StateLogCorrelator::new(
    Some(state_dir),
    temp_dir.path().to_path_buf(),
    file_system,
    None,
    Some(mock_creator.clone()),
    1000,
    &stats,
  )
  .await;

  correlator.on_state_change(OffsetDateTime::now_utc());

  let batch_ts =
    OffsetDateTime::now_utc().unix_timestamp().cast_unsigned() * 1_000_000 + u64::MAX / 2;

  let snapshot1 = correlator.get_or_create_snapshot(batch_ts).await;
  assert!(snapshot1.is_some(), "first snapshot should be created");
  assert_eq!(mock_creator.call_count(), 1);

  let snapshot2 = correlator.get_or_create_snapshot(batch_ts + 1000).await;
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
  let file_system = Arc::new(RealFileSystem::new(temp_dir.path().to_path_buf()));
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let mock_creator = Arc::new(MockSnapshotCreator::new(snapshots_dir.clone()));

  let correlator = StateLogCorrelator::new(
    Some(state_dir),
    temp_dir.path().to_path_buf(),
    file_system,
    None,
    Some(mock_creator.clone()),
    1,
    &stats,
  )
  .await;

  correlator.on_state_change(OffsetDateTime::now_utc());

  let batch_ts =
    OffsetDateTime::now_utc().unix_timestamp().cast_unsigned() * 1_000_000 + u64::MAX / 2;

  let snapshot1 = correlator.get_or_create_snapshot(batch_ts).await;
  assert!(snapshot1.is_some());
  assert_eq!(mock_creator.call_count(), 1);

  std::thread::sleep(std::time::Duration::from_millis(2));

  // Clear the first snapshot so the second call must create a new one.
  for entry in std::fs::read_dir(&snapshots_dir).unwrap() {
    let entry = entry.unwrap();
    std::fs::remove_file(entry.path()).unwrap();
  }

  let future_batch_ts = batch_ts + 10_000_000;
  let snapshot2 = correlator.get_or_create_snapshot(future_batch_ts).await;
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
  let file_system = Arc::new(RealFileSystem::new(temp_dir.path().to_path_buf()));
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let mock_creator = Arc::new(MockSnapshotCreator::new(snapshots_dir.clone()));

  let correlator = StateLogCorrelator::new(
    Some(state_dir),
    temp_dir.path().to_path_buf(),
    file_system,
    None,
    Some(mock_creator.clone()),
    0,
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

    let snapshot = correlator
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
  let file_system = Arc::new(RealFileSystem::new(temp_dir.path().to_path_buf()));
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let mock_creator = Arc::new(MockSnapshotCreator::new(snapshots_dir.clone()));

  let existing_timestamp = 1_700_000_000_000_000u64;
  let existing_snapshot = snapshots_dir.join(format!("state.jrn.g0.t{existing_timestamp}.zz"));
  std::fs::write(&existing_snapshot, b"pre-existing snapshot from rotation").unwrap();

  let correlator = StateLogCorrelator::new(
    Some(state_dir),
    temp_dir.path().to_path_buf(),
    file_system,
    None,
    Some(mock_creator.clone()),
    0,
    &stats,
  )
  .await;

  correlator.on_state_change(OffsetDateTime::now_utc());

  let batch_ts = existing_timestamp + 1_000_000;
  let snapshot = correlator.get_or_create_snapshot(batch_ts).await;

  assert!(snapshot.is_some());
  let snapshot_ref = snapshot.unwrap();
  assert_eq!(snapshot_ref.timestamp_micros, existing_timestamp);
  assert_eq!(snapshot_ref.generation, 0);
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
  let file_system = Arc::new(RealFileSystem::new(temp_dir.path().to_path_buf()));
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let mock_creator = Arc::new(MockSnapshotCreator::new(snapshots_dir));

  let correlator = StateLogCorrelator::new(
    Some(state_dir),
    temp_dir.path().to_path_buf(),
    file_system,
    None,
    Some(mock_creator.clone()),
    0,
    &stats,
  )
  .await;

  correlator.on_state_change(OffsetDateTime::now_utc());

  let batch_ts =
    OffsetDateTime::now_utc().unix_timestamp().cast_unsigned() * 1_000_000 + u64::MAX / 2;

  let snapshot = correlator.get_or_create_snapshot(batch_ts).await;

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
  let file_system = Arc::new(RealFileSystem::new(temp_dir.path().to_path_buf()));
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let mock_creator = Arc::new(MockSnapshotCreator::new(snapshots_dir.clone()));

  let old_snapshot_ts = 1_700_000_000_000_000u64;
  let old_snapshot = snapshots_dir.join(format!("state.jrn.g0.t{old_snapshot_ts}.zz"));
  std::fs::write(&old_snapshot, b"old snapshot").unwrap();

  let newer_snapshot_ts = 1_700_001_000_000_000u64;
  let newer_snapshot = snapshots_dir.join(format!("state.jrn.g1.t{newer_snapshot_ts}.zz"));
  std::fs::write(&newer_snapshot, b"newer snapshot").unwrap();

  let correlator = StateLogCorrelator::new(
    Some(state_dir),
    temp_dir.path().to_path_buf(),
    file_system,
    None,
    Some(mock_creator.clone()),
    0,
    &stats,
  )
  .await;

  correlator.on_state_change(OffsetDateTime::now_utc());

  let batch_ts = newer_snapshot_ts + 1_000_000;
  let snapshot = correlator.get_or_create_snapshot(batch_ts).await;

  assert!(snapshot.is_some());
  let snapshot_ref = snapshot.unwrap();
  assert_eq!(
    snapshot_ref.timestamp_micros, newer_snapshot_ts,
    "should select the most recent snapshot before the batch timestamp"
  );
  assert_eq!(snapshot_ref.generation, 1);
  assert_eq!(
    mock_creator.call_count(),
    0,
    "should not create snapshot when existing one is available"
  );
}
