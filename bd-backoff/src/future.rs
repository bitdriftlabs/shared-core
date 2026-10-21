// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#[cfg(test)]
#[path = "./future_test.rs"]
mod tests;

use std::future::Future;
use std::time::Duration as StdDuration;
use time::Duration;

/// Retries transient failures until the operation succeeds or the delay source is exhausted.
/// Notification runs only when a retry is scheduled. Dropping the future cancels pending work.
pub async fn retry_notify<ResultType, ErrorType, OperationFuture>(
  next_backoff: impl FnMut() -> Option<Duration>,
  operation: impl FnMut() -> OperationFuture,
  should_retry: impl FnMut(&ErrorType) -> bool,
  notify: impl FnMut(&ErrorType, StdDuration),
) -> Result<ResultType, ErrorType>
where
  OperationFuture: Future<Output = Result<ResultType, ErrorType>>,
{
  retry_notify_with_sleep(
    next_backoff,
    operation,
    should_retry,
    notify,
    |_, delay| async move {
      if !delay.is_zero() {
        tokio::time::sleep(delay).await;
      }
      true
    },
  )
  .await
}

/// Like [`retry_notify`], with a caller-owned wait for cancellation or tracing.
/// Returning false from the wait stops retries and returns the last operation error.
pub async fn retry_notify_with_sleep<ResultType, ErrorType, OperationFuture, SleepFuture>(
  mut next_backoff: impl FnMut() -> Option<Duration>,
  mut operation: impl FnMut() -> OperationFuture,
  mut should_retry: impl FnMut(&ErrorType) -> bool,
  mut notify: impl FnMut(&ErrorType, StdDuration),
  mut sleep: impl FnMut(&ErrorType, StdDuration) -> SleepFuture,
) -> Result<ResultType, ErrorType>
where
  OperationFuture: Future<Output = Result<ResultType, ErrorType>>,
  SleepFuture: Future<Output = bool>,
{
  loop {
    match operation().await {
      Ok(result) => return Ok(result),
      Err(error) => {
        if !should_retry(&error) {
          return Err(error);
        }
        let Some(delay) = next_backoff() else {
          return Err(error);
        };
        let delay = delay.unsigned_abs();
        notify(&error, delay);
        if !sleep(&error, delay).await {
          return Err(error);
        }
      },
    }
  }
}
