// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::*;
use bd_client_common::file_system::RealFileSystem;
use std::sync::atomic::Ordering;
use time::OffsetDateTime;

#[test]
fn parse_snapshot_filename_valid() {
  let (generation, ts) = parse_snapshot_filename("state.jrn.g0.t1704067200000000.zz").unwrap();
  assert_eq!(generation, 0);
  assert_eq!(ts, 1_704_067_200_000_000);

  let (generation, ts) = parse_snapshot_filename("state.jrn.g42.t9999999999999999.zz").unwrap();
  assert_eq!(generation, 42);
  assert_eq!(ts, 9_999_999_999_999_999);
}

#[test]
fn parse_snapshot_filename_invalid() {
  assert!(parse_snapshot_filename("invalid.zz").is_none());
  assert!(parse_snapshot_filename("state.jrn.g0.zz").is_none());
  assert!(parse_snapshot_filename("state.jrn.t123.zz").is_none());
  assert!(parse_snapshot_filename("state.jrn.g0.t123").is_none());
  assert!(parse_snapshot_filename("other.jrn.g0.t123.zz").is_none());
}

#[tokio::test]
async fn correlator_no_state_changes() {
  let temp_dir = tempfile::tempdir().unwrap();
  let file_system = Arc::new(RealFileSystem::new(temp_dir.path().to_path_buf()));
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let correlator =
    StateLogCorrelator::new(None, temp_dir.path().to_path_buf(), file_system, None, &stats).await;

  // No state changes yet, should not need to upload
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

  let correlator =
    StateLogCorrelator::new(None, temp_dir.path().to_path_buf(), file_system, None, &stats).await;

  // Record a state change
  let timestamp = OffsetDateTime::from_unix_timestamp(1_704_067_200).unwrap();
  correlator.on_state_change(timestamp);

  // The last_state_change should be updated
  let last_change = correlator.last_state_change_micros.load(Ordering::Relaxed);
  assert_eq!(last_change, 1_704_067_200_000_000);
}

#[tokio::test]
async fn correlator_uploaded_coverage_prevents_reupload() {
  let temp_dir = tempfile::tempdir().unwrap();
  let file_system = Arc::new(RealFileSystem::new(temp_dir.path().to_path_buf()));
  let stats = bd_client_stats_store::Collector::default().scope("test");

  let correlator =
    StateLogCorrelator::new(None, temp_dir.path().to_path_buf(), file_system, None, &stats).await;

  // Record a state change
  let timestamp = OffsetDateTime::from_unix_timestamp(1_704_067_200).unwrap();
  correlator.on_state_change(timestamp);

  // Mark state as uploaded through a later timestamp
  correlator.on_state_uploaded(1_704_067_300_000_000).await;

  // Batch with timestamp before uploaded_through should not need upload
  assert!(
    correlator
      .should_upload_state(1_704_067_100_000_000, 1_704_067_200_000_000)
      .is_none()
  );
}
