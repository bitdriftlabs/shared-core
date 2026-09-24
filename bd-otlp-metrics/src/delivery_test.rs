// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{
  DeliveryEngine,
  DeliveryObserver,
  HttpRemoteWriteError,
  MockHttpRemoteWriteClient,
  Retry,
  RetryConfig,
};
use backoff::backoff::Zero;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
