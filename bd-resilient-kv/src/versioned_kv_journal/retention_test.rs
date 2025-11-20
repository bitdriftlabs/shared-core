// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use super::RetentionRegistry;
use std::sync::Arc;

#[tokio::test]
async fn handle_starts_with_retain_all() {
  let registry = Arc::new(RetentionRegistry::new());
  let handle = registry.create_handle().await;

  assert_eq!(
    handle.get_retention(),
    0,
    "Handle should start with timestamp 0 (retain all)"
  );
}

#[tokio::test]
async fn handle_can_update_retention() {
  let registry = Arc::new(RetentionRegistry::new());
  let handle = registry.create_handle().await;

  let timestamp_micros = 1_000_000_u64; // 1 second since epoch
  handle.update_retention_micros(timestamp_micros);

  assert_eq!(handle.get_retention(), timestamp_micros);
}

#[tokio::test]
async fn registry_returns_none_when_no_handles() {
  let registry = Arc::new(RetentionRegistry::new());

  let min_retention = registry.min_retention_timestamp().await;
  assert_eq!(
    min_retention, None,
    "Should return None when no handles exist"
  );
}

#[tokio::test]
async fn registry_returns_minimum_across_handles() {
  let registry = Arc::new(RetentionRegistry::new());

  let handle1 = registry.create_handle().await;
  let handle2 = registry.create_handle().await;
  let handle3 = registry.create_handle().await;

  // Set different retention timestamps
  let ts1 = 1_000_000_u64;
  let ts2 = 2_000_000_u64;
  let ts3 = 3_000_000_u64;

  handle1.update_retention_micros(ts1);
  handle2.update_retention_micros(ts2);
  handle3.update_retention_micros(ts3);

  let min_retention = registry.min_retention_timestamp().await;

  assert_eq!(
    min_retention,
    Some(ts1),
    "Should return the minimum retention timestamp"
  );
}

#[tokio::test]
async fn registry_handles_zero_retention() {
  let registry = Arc::new(RetentionRegistry::new());

  let _handle1 = registry.create_handle().await;
  let handle2 = registry.create_handle().await;

  // handle1 wants all data (timestamp 0), handle2 wants recent data
  let ts2 = 2_000_000_u64;
  handle2.update_retention_micros(ts2);

  let min_retention = registry.min_retention_timestamp().await;
  assert_eq!(
    min_retention,
    Some(0),
    "Should return 0 when at least one handle wants all data"
  );
}

#[tokio::test]
async fn handle_releases_on_drop() {
  let registry = Arc::new(RetentionRegistry::new());

  {
    let handle = registry.create_handle().await;
    let ts = 1_000_000_u64;
    handle.update_retention_micros(ts);

    let min_retention = registry.min_retention_timestamp().await;
    assert!(
      min_retention.is_some(),
      "Should have a retention requirement"
    );
  }

  // After handle is dropped, give the registry time to clean up
  tokio::time::sleep(std::time::Duration::from_millis(10)).await;

  let min_retention = registry.min_retention_timestamp().await;
  assert_eq!(
    min_retention, None,
    "Should have no retention requirements after handle dropped"
  );
}
