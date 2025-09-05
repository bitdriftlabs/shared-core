// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::RateLimitLayer;
use crate::service::RequestSized;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_time::TimeDurationExt;
use futures_util::future::BoxFuture;
use futures_util::poll;
use std::convert::Infallible;
use time::ext::NumericalDuration;
use tower::Service;

// For test purposes, the size of the request is just the size of the string.
impl RequestSized for &str {
  fn per_request_size(&self) -> u32 {
    self.len().try_into().unwrap()
  }
}

struct TestService {}

impl tower::Service<&str> for TestService {
  type Response = String;

  type Error = Infallible;

  type Future = BoxFuture<'static, std::result::Result<String, Infallible>>;

  fn poll_ready(
    &mut self,
    _cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<std::result::Result<(), Infallible>> {
    std::task::Poll::Ready(Ok(()))
  }

  fn call(&mut self, req: &str) -> Self::Future {
    let s = req.to_string();
    Box::pin(async move { Ok(s) })
  }
}

#[tokio::test(start_paused = true)]
async fn ratelimit() {
  let directory = tempfile::TempDir::with_prefix("ratelimit_test").unwrap();
  let runtime_loader = ConfigLoader::new(directory.path());

  // Set the ratelimit parameters to 10 characters per minute.
  runtime_loader
    .update_snapshot(make_simple_update(vec![
      (
        bd_runtime::runtime::log_upload::RatelimitByteCountPerPeriodFlag::path(),
        ValueKind::Int(10),
      ),
      (
        bd_runtime::runtime::log_upload::RatelimitPeriodFlag::path(),
        ValueKind::Int(1000),
      ),
    ]))
    .await
    .unwrap();

  let rate_limit = RateLimitLayer::new(super::Rate::new(&runtime_loader));
  let mut service = tower::ServiceBuilder::new()
    .layer(rate_limit)
    .service(TestService {});

  // The first request should consume 5/10 of the ratelimit quota.
  let response = service.call("hell1");
  assert_eq!(response.await.unwrap(), "hell1");

  // The second request consumes 10/10 of the quota.
  let response = service.call("hell2");
  assert_eq!(response.await.unwrap(), "hell2");

  // The entire quota has been expended, so calls are now being held.
  let response = service.call("hell3");
  assert!(poll!(response).is_pending());

  // Wait 1s to refill the quota.
  1.seconds().advance().await;

  // The service is now ready for more requests.
  let response = service.call("hell4"); // 10 bytes used
  assert_eq!(response.await.unwrap(), "hell4");
}

#[tokio::test(start_paused = true)]
async fn shared_quota() {
  let directory = tempfile::TempDir::with_prefix("ratelimit-test").unwrap();
  let runtime_loader = ConfigLoader::new(directory.path());

  // Set the ratelimit parameters to 2 characters per second.
  runtime_loader
    .update_snapshot(make_simple_update(vec![
      (
        bd_runtime::runtime::log_upload::RatelimitByteCountPerPeriodFlag::path(),
        ValueKind::Int(2),
      ),
      (
        bd_runtime::runtime::log_upload::RatelimitPeriodFlag::path(),
        ValueKind::Int(1000),
      ),
    ]))
    .await
    .unwrap();

  let rate_limit = RateLimitLayer::new(super::Rate::new(&runtime_loader));
  let mut service1 = tower::ServiceBuilder::new()
    .layer(rate_limit.clone())
    .service(TestService {});
  let mut service2 = tower::ServiceBuilder::new()
    .layer(rate_limit)
    .service(TestService {});

  // Consume one byte from the first service, this should resolve immediately.
  let response = service1.call("x");
  assert_eq!(response.await.unwrap(), "x");

  // Consume one byte from the second service.
  let response = service2.call("x");
  assert_eq!(response.await.unwrap(), "x");

  // Now the quota has been expended, so both services are no longer allowing requests through.
  let future1 = service1.call("x");
  let future2 = service2.call("x");
  assert!(poll!(future1).is_pending());
  assert!(poll!(future2).is_pending());

  // Wait 1s to refill the quota.
  1.seconds().advance().await;

  // Calls can now through again.
  let response = service1.call("x");
  assert_eq!(response.await.unwrap(), "x");

  // Consume one byte from the second service.
  let response = service2.call("x");
  assert_eq!(response.await.unwrap(), "x");
}
