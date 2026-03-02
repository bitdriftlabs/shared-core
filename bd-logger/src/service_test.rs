// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{BackoffProvider, RetryPolicy, UploadResult};
use crate::service::UploadRequest;
use assert_matches::assert_matches;
use bd_api::upload::LogBatch;
use bd_client_stats_store::Counter;
use bd_runtime::runtime::FeatureFlag;
use bd_test_helpers::runtime::{ValueKind, make_update};
use bd_time::TimeDurationExt;
use std::task::Poll;
use time::Duration;
use tokio_test::assert_pending;
use tower::retry::Policy;

#[tokio::test(start_paused = true)]
async fn test_retry_backoff() {
  let directory = tempfile::TempDir::with_prefix("backoff_test").unwrap();
  let runtime = bd_runtime::runtime::ConfigLoader::new(directory.path());
  let max_retries = runtime.register_int_watch();
  let retry_limit_exceeded_dropped_logs = Counter::default();
  let retry_limit_exceeded = Counter::default();

  runtime
    .update_snapshot(make_update(
      vec![
        (
          bd_runtime::runtime::retry_backoff::InitialBackoffInterval::path(),
          ValueKind::Int(30_000),
        ),
        (
          bd_runtime::runtime::retry_backoff::MaxBackoffInterval::path(),
          ValueKind::Int(1_800_000),
        ),
        (
          bd_runtime::runtime::retry_backoff::BackoffGrowthFactorBasisPoints::path(),
          ValueKind::Int(1500),
        ),
      ],
      "test".to_string(),
    ))
    .await
    .unwrap();

  let provider = BackoffProvider {
    backoff_policy: bd_api::RuntimeBackoffPolicy::new(&runtime),
  };

  let mut retry = RetryPolicy {
    attempts: 0,
    max_retries,
    backoff: None,
    backoff_provider: provider,
    retry_limit_exceeded,
    retry_limit_exceeded_dropped_logs,
  };

  let mut req = UploadRequest::new(
    LogBatch {
      logs: vec![],
      buffer_id: "buffer".to_string(),
    },
    false,
  );
  let mut retry_task = tokio_test::task::spawn(
    retry
      .retry(&mut req, &mut Ok(UploadResult::Failure))
      .unwrap(),
  );

  assert_pending!(retry_task.poll());

  bd_runtime::runtime::retry_backoff::MaxBackoffInterval::default()
    .sleep()
    .await;

  // After the first retry the interval should be 1.5 the initial.
  assert_matches!(retry_task.poll(), Poll::Ready(()));

  assert_matches!(&retry.backoff, Some(backoff) => {
    assert_eq!(backoff.initial_interval(), Duration::seconds(30));
    assert_eq!(backoff.current_interval(), Duration::seconds(45));
  });

  let mut second_retry = tokio_test::task::spawn(
    retry
      .retry(&mut req, &mut Ok(UploadResult::Failure))
      .unwrap(),
  );

  assert_pending!(second_retry.poll());

  bd_runtime::runtime::retry_backoff::MaxBackoffInterval::default()
    .sleep()
    .await;

  // After the second retry the interval should be 1.5^2 the initial.
  assert_matches!(second_retry.poll(), Poll::Ready(()));
  assert_matches!(retry.backoff, Some(backoff) => {
    assert_eq!(backoff.current_interval(), Duration::milliseconds(67500));
  });
}
