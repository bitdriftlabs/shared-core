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
use bd_runtime::runtime::log_upload::RetryBackoffInitialFlag;
use bd_runtime::runtime::{DurationWatch, FeatureFlag};
use std::task::Poll;
use std::time::Duration;
use tokio_test::assert_pending;
use tower::retry::Policy;

#[tokio::test(start_paused = true)]
async fn test_retry_backoff() {
  let directory = tempfile::TempDir::with_prefix("backoff_test").unwrap();
  let runtime = bd_runtime::runtime::ConfigLoader::new(directory.path());
  let max_retries = runtime.register_watch().unwrap();
  let retry_limit_exceeded_dropped_logs = Counter::default();
  let retry_limit_exceeded = Counter::default();

  let provider = BackoffProvider {
    initial_backoff: DurationWatch::wrap(runtime.register_watch().unwrap()),
    max_backoff: DurationWatch::wrap(runtime.register_watch().unwrap()),
  };

  let mut retry = RetryPolicy {
    attempts: 0,
    max_retries,
    backoff: None,
    backoff_provider: provider,
    retry_limit_exceeded,
    retry_limit_exceeded_dropped_logs,
  };

  let mut req = UploadRequest::new(LogBatch {
    logs: vec![],
    buffer_id: "buffer".to_string(),
  });
  let mut retry_task = tokio_test::task::spawn(
    retry
      .retry(&mut req, &mut Ok(UploadResult::Failure))
      .unwrap(),
  );

  assert_pending!(retry_task.poll());

  tokio::time::sleep(Duration::from_millis(
    bd_runtime::runtime::log_upload::RetryBackoffMaxFlag::default().into(),
  ))
  .await;

  // After the first retry the interval should be 1.5 the initial.
  assert_matches!(retry_task.poll(), Poll::Ready(()));

  assert_matches!(&retry.backoff, Some(backoff) => {
    assert_eq!(backoff.initial_interval,
               Duration::from_millis(RetryBackoffInitialFlag::default().into()));
    assert_eq!(backoff.current_interval, Duration::from_secs(45));
  });

  let mut second_retry = tokio_test::task::spawn(
    retry
      .retry(&mut req, &mut Ok(UploadResult::Failure))
      .unwrap(),
  );

  assert_pending!(second_retry.poll());

  tokio::time::sleep(Duration::from_millis(
    bd_runtime::runtime::log_upload::RetryBackoffMaxFlag::default().into(),
  ))
  .await;

  // After the second retry the interval should be 1.5^2 the initial.
  assert_matches!(second_retry.poll(), Poll::Ready(()));
  assert_matches!(retry.backoff, Some(backoff) => {
      assert_eq!(backoff.current_interval, Duration::from_millis(67500));
  });
}
