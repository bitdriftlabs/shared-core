// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(clippy::unwrap_used)]

use super::{retry_notify, retry_notify_with_sleep};
use std::cell::Cell;
use std::future::{Future, poll_fn, ready};
use std::iter::once;
use std::task::Poll;
use std::time::Duration as StdDuration;
use time::Duration;
use tokio::time::Instant;

#[tokio::test(start_paused = true)]
async fn retries_transient_failures_until_success() {
  let start = Instant::now();
  let mut delays = [Duration::seconds(2), Duration::seconds(3)].into_iter();
  let mut attempts = 0;
  let mut notifications = Vec::new();
  let result = retry_notify(
    || delays.next(),
    || {
      attempts += 1;
      ready(if attempts < 3 { Err(attempts) } else { Ok(42) })
    },
    |_| true,
    |error, delay| notifications.push((*error, delay)),
  )
  .await
  .unwrap();

  assert_eq!(result, 42);
  assert_eq!(attempts, 3);
  assert_eq!(start.elapsed(), StdDuration::from_secs(5));
  assert_eq!(
    notifications,
    vec![
      (1, StdDuration::from_secs(2)),
      (2, StdDuration::from_secs(3))
    ]
  );
}

#[tokio::test(start_paused = true)]
async fn returns_last_failure_when_backoff_exhausts() {
  let start = Instant::now();
  let mut delays = once(Duration::seconds(2));
  let mut attempts = 0;
  let mut notifications = Vec::new();
  let error = retry_notify(
    || delays.next(),
    || {
      attempts += 1;
      ready(Err::<(), _>(attempts))
    },
    |_| true,
    |error, delay| notifications.push((*error, delay)),
  )
  .await
  .unwrap_err();

  assert_eq!(error, 2);
  assert_eq!(attempts, 2);
  assert_eq!(start.elapsed(), StdDuration::from_secs(2));
  assert_eq!(notifications, vec![(1, StdDuration::from_secs(2))]);
}

#[tokio::test(start_paused = true)]
async fn permanent_failure_does_not_consume_delays_or_notify() {
  let start = Instant::now();
  let mut delays_requested = 0;
  let mut attempts = 0;
  let mut notifications = 0;
  let result = retry_notify(
    || {
      delays_requested += 1;
      Some(Duration::seconds(2))
    },
    || {
      attempts += 1;
      ready(Err::<(), _>("permanent failure"))
    },
    |_| false,
    |_, _| notifications += 1,
  )
  .await;

  assert_eq!(result, Err("permanent failure"));
  assert_eq!(attempts, 1);
  assert_eq!(delays_requested, 0);
  assert_eq!(notifications, 0);
  assert_eq!(start.elapsed(), StdDuration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn immediate_success_does_not_consume_delays_or_notify() {
  let mut delays_requested = 0;
  let mut notifications = 0;
  let result = retry_notify(
    || {
      delays_requested += 1;
      Some(Duration::seconds(2))
    },
    || ready(Ok::<_, ()>(42)),
    |()| true,
    |(), _| notifications += 1,
  )
  .await;

  assert_eq!(result, Ok(42));
  assert_eq!(delays_requested, 0);
  assert_eq!(notifications, 0);
}

#[tokio::test(start_paused = true)]
async fn cancelled_wait_returns_last_failure_without_another_attempt() {
  let mut attempts = 0;
  let result = retry_notify_with_sleep(
    || Some(Duration::seconds(2)),
    || {
      attempts += 1;
      ready(Err::<(), _>(attempts))
    },
    |_| true,
    |_, _| {},
    |_, _| ready(false),
  )
  .await;

  assert_eq!(result, Err(1));
  assert_eq!(attempts, 1);
}

#[tokio::test(start_paused = true)]
async fn dropping_sleeping_retry_prevents_another_attempt() {
  let attempts = Cell::new(0);
  let mut retry = Box::pin(retry_notify(
    || Some(Duration::seconds(2)),
    || {
      attempts.set(attempts.get() + 1);
      ready(Err::<(), _>("transient failure"))
    },
    |_| true,
    |_, _| {},
  ));
  poll_fn(|context| {
    assert!(retry.as_mut().poll(context).is_pending());
    Poll::Ready(())
  })
  .await;

  assert_eq!(attempts.get(), 1);
  drop(retry);
  tokio::time::advance(StdDuration::from_secs(10)).await;
  assert_eq!(attempts.get(), 1);
}
