// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{Retry, RetryConfig};
use backoff::ExponentialBackoffBuilder;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn rejects_invalid_budget() {
  assert!(
    Retry::new(RetryConfig {
      budget: Some(0.0),
      max_retries: None,
    })
    .is_err()
  );
  assert!(
    Retry::new(RetryConfig {
      budget: Some(1.1),
      max_retries: None,
    })
    .is_err()
  );
}

#[tokio::test]
async fn stops_at_max_retries() {
  let retry = Retry::new(RetryConfig {
    budget: Some(1.0),
    max_retries: Some(1),
  })
  .unwrap();
  let attempts = AtomicUsize::new(0);
  let notifications = AtomicUsize::new(0);

  let result = retry
    .retry_notify(
      ExponentialBackoffBuilder::new()
        .with_initial_interval(std::time::Duration::ZERO)
        .with_max_interval(std::time::Duration::ZERO)
        .with_max_elapsed_time(None)
        .build(),
      || async {
        attempts.fetch_add(1, Ordering::Relaxed);
        Err::<(), _>(backoff::Error::transient("failed"))
      },
      || {
        notifications.fetch_add(1, Ordering::Relaxed);
      },
    )
    .await;

  assert_eq!(result, Err("failed"));
  assert_eq!(attempts.load(Ordering::Relaxed), 2);
  assert_eq!(notifications.load(Ordering::Relaxed), 1);
}
