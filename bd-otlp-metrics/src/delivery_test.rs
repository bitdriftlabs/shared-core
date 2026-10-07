// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{
  DeliveryEngine,
  DeliveryObserver,
  HttpRemoteWriteClient,
  HttpRemoteWriteError,
  HttpRetryPolicy,
  MockHttpRemoteWriteClient,
  Retry,
  RetryConfig,
};
use async_trait::async_trait;
use backoff::backoff::{Constant, Zero};
use bytes::Bytes;
use futures::future::pending;
use http::{HeaderMap, StatusCode};
use std::iter::once;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::{Instant, timeout};

#[derive(Default)]
struct TestObserver {
  retries: AtomicUsize,
  sent: AtomicUsize,
}

impl DeliveryObserver for TestObserver {
  fn request_sent(&self, request_size: usize) {
    self.sent.fetch_add(request_size, Ordering::Relaxed);
  }

  fn request_retry(&self) {
    self.retries.fetch_add(1, Ordering::Relaxed);
  }
}

#[tokio::test]
async fn retries_transient_transport_errors() {
  let mut client = MockHttpRemoteWriteClient::new();
  let attempts = Arc::new(AtomicUsize::new(0));
  let cloned_attempts = attempts.clone();
  client
    .expect_send_write_request()
    .times(2)
    .returning(move |_, _| {
      if cloned_attempts.fetch_add(1, Ordering::Relaxed) == 0 {
        Err(HttpRemoteWriteError::Timeout)
      } else {
        Ok(())
      }
    });

  let observer = Arc::new(TestObserver::default());
  let engine = DeliveryEngine::new(
    Arc::new(client),
    Retry::new(RetryConfig {
      budget: Some(1.0),
      max_retries: Some(1),
    })
    .unwrap(),
    Arc::new(|| Box::new(Zero {})),
    observer.clone(),
  );

  engine
    .send(Bytes::from_static(b"request"), None, false)
    .await
    .unwrap();

  assert_eq!(attempts.load(Ordering::Relaxed), 2);
  assert_eq!(observer.retries.load(Ordering::Relaxed), 1);
  assert_eq!(observer.sent.load(Ordering::Relaxed), 14);
}

fn response_error(status: StatusCode, retry_after: Option<&str>) -> HttpRemoteWriteError {
  let mut headers = HeaderMap::new();
  if let Some(value) = retry_after {
    headers.insert(http::header::RETRY_AFTER, value.parse().unwrap());
  }
  HttpRemoteWriteError::Response(status, String::new(), headers)
}

fn otlp_engine(
  client: MockHttpRemoteWriteClient,
  observer: Arc<TestObserver>,
  retry: Arc<Retry>,
) -> DeliveryEngine {
  DeliveryEngine::new(
    Arc::new(client),
    retry,
    Arc::new(|| Box::new(Zero {})),
    observer,
  )
  .with_retry_policy(HttpRetryPolicy::Otlp)
}

fn one_retry() -> Arc<Retry> {
  Retry::new(RetryConfig {
    budget: Some(1.0),
    max_retries: Some(1),
  })
  .unwrap()
}

#[tokio::test(start_paused = true)]
async fn otlp_retries_preserve_bytes_headers_and_honor_retry_after() {
  let start = Instant::now();
  let mut client = MockHttpRemoteWriteClient::new();
  let attempts = Arc::new(AtomicUsize::new(0));
  let observed_attempts = attempts.clone();
  client
    .expect_send_write_request()
    .times(2)
    .returning(move |bytes, headers| {
      assert_eq!(bytes, Bytes::from_static(b"historical-interval"));
      assert_eq!(headers.unwrap()["authorization"], "secret");
      if observed_attempts.fetch_add(1, Ordering::Relaxed) == 0 {
        Err(response_error(StatusCode::TOO_MANY_REQUESTS, Some("3")))
      } else {
        assert_eq!(start.elapsed(), Duration::from_secs(3));
        Ok(())
      }
    });
  let observer = Arc::new(TestObserver::default());
  let engine = otlp_engine(client, observer.clone(), one_retry());
  let headers: HeaderMap = once((http::header::AUTHORIZATION, "secret".parse().unwrap())).collect();
  engine
    .send_with_timeout(
      Bytes::from_static(b"historical-interval"),
      Some(&headers),
      false,
      Duration::from_secs(5),
    )
    .await
    .unwrap();
  assert_eq!(observer.retries.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn otlp_drops_retry_after_outside_delivery_budget() {
  let start = Instant::now();
  let mut client = MockHttpRemoteWriteClient::new();
  client
    .expect_send_write_request()
    .times(1)
    .returning(|_, _| Err(response_error(StatusCode::SERVICE_UNAVAILABLE, Some("30"))));
  let observer = Arc::new(TestObserver::default());
  let engine = otlp_engine(client, observer.clone(), one_retry());
  assert!(matches!(
    engine
      .send_with_timeout(Bytes::new(), None, false, Duration::from_secs(5))
      .await,
    Err(HttpRemoteWriteError::Response(
      StatusCode::SERVICE_UNAVAILABLE,
      ..
    ))
  ));
  assert_eq!(start.elapsed(), Duration::ZERO);
  assert_eq!(observer.retries.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn otlp_terminal_outcomes_and_shutdown_do_not_retry() {
  for shutdown in [false, true] {
    let mut client = MockHttpRemoteWriteClient::new();
    client
      .expect_send_write_request()
      .times(1)
      .returning(move |_, _| {
        if shutdown {
          Err(HttpRemoteWriteError::Timeout)
        } else {
          Err(response_error(StatusCode::INTERNAL_SERVER_ERROR, None))
        }
      });
    let observer = Arc::new(TestObserver::default());
    let engine = otlp_engine(client, observer.clone(), one_retry());
    assert!(engine.send(Bytes::new(), None, shutdown).await.is_err());
    assert_eq!(observer.retries.load(Ordering::Relaxed), 0);
  }
}

#[tokio::test(start_paused = true)]
async fn cancelled_retry_sleep_releases_retry_budget() {
  let retry = Retry::new(RetryConfig {
    budget: Some(0.1),
    max_retries: Some(1),
  })
  .unwrap();
  let mut client = MockHttpRemoteWriteClient::new();
  client
    .expect_send_write_request()
    .times(1)
    .returning(|_, _| Err(HttpRemoteWriteError::Timeout));
  let engine = DeliveryEngine::new(
    Arc::new(client),
    retry.clone(),
    Arc::new(|| Box::new(Constant::new(Duration::from_secs(3)))),
    Arc::new(TestObserver::default()),
  );
  let result = timeout(
    Duration::from_secs(1),
    engine.send(Bytes::new(), None, false),
  )
  .await;
  assert!(result.is_err());
  let mut client = MockHttpRemoteWriteClient::new();
  let attempts = Arc::new(AtomicUsize::new(0));
  client
    .expect_send_write_request()
    .times(2)
    .returning(move |_, _| {
      if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
        Err(HttpRemoteWriteError::Timeout)
      } else {
        Ok(())
      }
    });
  otlp_engine(client, Arc::new(TestObserver::default()), retry)
    .send(Bytes::new(), None, false)
    .await
    .unwrap();
}

#[tokio::test]
async fn otlp_partial_success_and_permanent_errors_are_terminal() {
  for error in [
    HttpRemoteWriteError::PartialSuccess {
      rejected_data_points: 2,
      error_message: "rejected".to_string(),
    },
    HttpRemoteWriteError::PartialSuccess {
      rejected_data_points: 0,
      error_message: "warning only".to_string(),
    },
    HttpRemoteWriteError::Permanent("invalid TLS".to_string()),
  ] {
    let mut client = MockHttpRemoteWriteClient::new();
    client
      .expect_send_write_request()
      .times(1)
      .return_once(move |_, _| Err(error));
    let observer = Arc::new(TestObserver::default());
    assert!(
      otlp_engine(client, observer.clone(), one_retry())
        .send(Bytes::new(), None, false)
        .await
        .is_err()
    );
    assert_eq!(observer.retries.load(Ordering::Relaxed), 0);
  }
}

#[tokio::test(start_paused = true)]
async fn default_policy_retries_500_without_adopting_retry_after() {
  let start = Instant::now();
  let mut client = MockHttpRemoteWriteClient::new();
  client
    .expect_send_write_request()
    .times(1)
    .return_once(|_, _| {
      Err(response_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        Some("30"),
      ))
    });
  client
    .expect_send_write_request()
    .times(1)
    .returning(|_, _| Ok(()));
  let engine = DeliveryEngine::new(
    Arc::new(client),
    one_retry(),
    Arc::new(|| Box::new(Zero {})),
    Arc::new(TestObserver::default()),
  );
  engine
    .send_with_timeout(Bytes::new(), None, false, Duration::from_secs(1))
    .await
    .unwrap();
  assert_eq!(start.elapsed(), Duration::ZERO);
}

//
// PendingClient
//

struct PendingClient;

#[async_trait]
impl HttpRemoteWriteClient for PendingClient {
  async fn send_write_request<'a>(
    &self,
    compressed_write_request: Bytes,
    extra_headers: Option<&'a HeaderMap>,
  ) -> Result<(), HttpRemoteWriteError> {
    assert_eq!(compressed_write_request, Bytes::from_static(b"request"));
    assert!(extra_headers.is_none());
    pending().await
  }
}

#[tokio::test(start_paused = true)]
async fn total_deadline_cancels_an_inflight_attempt() {
  let start = Instant::now();
  let observer = Arc::new(TestObserver::default());
  let engine = DeliveryEngine::new(
    Arc::new(PendingClient),
    one_retry(),
    Arc::new(|| Box::new(Zero {})),
    observer.clone(),
  )
  .with_retry_policy(HttpRetryPolicy::Otlp);
  assert!(matches!(
    engine
      .send_with_timeout(
        Bytes::from_static(b"request"),
        None,
        false,
        Duration::from_secs(1)
      )
      .await,
    Err(HttpRemoteWriteError::Timeout)
  ));
  assert_eq!(start.elapsed(), Duration::from_secs(1));
  assert_eq!(observer.sent.load(Ordering::Relaxed), 7);
  assert_eq!(observer.retries.load(Ordering::Relaxed), 0);
}
