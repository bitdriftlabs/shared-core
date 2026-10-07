// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::http::{HttpRemoteWriteClient, HttpRemoteWriteError, HttpRetryPolicy};
use crate::retry::Retry;
use backoff::backoff::Backoff;
use bytes::Bytes;
use http::HeaderMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::time::{Instant, timeout_at};

pub type BackoffFactory = Arc<dyn Fn() -> Box<dyn Backoff + Send> + Send + Sync>;

//
// DeliveryObserver
//

pub trait DeliveryObserver: Send + Sync {
  fn request_sent(&self, request_size: usize);
  fn request_retry(&self);
}

//
// DeliveryEngine
//

pub struct DeliveryEngine {
  backoff: BackoffFactory,
  client: Arc<dyn HttpRemoteWriteClient>,
  observer: Arc<dyn DeliveryObserver>,
  retry: Arc<Retry>,
  policy: HttpRetryPolicy,
}

impl DeliveryEngine {
  pub fn new(
    client: Arc<dyn HttpRemoteWriteClient>,
    retry: Arc<Retry>,
    backoff: BackoffFactory,
    observer: Arc<dyn DeliveryObserver>,
  ) -> Self {
    Self {
      backoff,
      client,
      observer,
      retry,
      policy: HttpRetryPolicy::RemoteWrite,
    }
  }

  #[must_use]
  pub fn with_retry_policy(mut self, policy: HttpRetryPolicy) -> Self {
    self.policy = policy;
    self
  }

  pub async fn send(
    &self,
    compressed_write_request: Bytes,
    extra_headers: Option<&HeaderMap>,
    shutdown_pending: bool,
  ) -> Result<(), HttpRemoteWriteError> {
    self
      .send_until(
        compressed_write_request,
        extra_headers,
        shutdown_pending,
        None,
      )
      .await
  }

  /// Bounds attempts and retry sleeps by one total delivery budget.
  pub async fn send_with_timeout(
    &self,
    compressed_write_request: Bytes,
    extra_headers: Option<&HeaderMap>,
    shutdown_pending: bool,
    timeout: Duration,
  ) -> Result<(), HttpRemoteWriteError> {
    let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
      HttpRemoteWriteError::Permanent("delivery timeout exceeds the clock range".to_string())
    })?;
    timeout_at(
      deadline,
      self.send_until(
        compressed_write_request,
        extra_headers,
        shutdown_pending,
        Some(deadline),
      ),
    )
    .await
    .map_err(|_| HttpRemoteWriteError::Timeout)?
  }

  async fn send_until(
    &self,
    compressed_write_request: Bytes,
    extra_headers: Option<&HeaderMap>,
    shutdown_pending: bool,
    deadline: Option<Instant>,
  ) -> Result<(), HttpRemoteWriteError> {
    self
      .retry
      .retry_notify_until(
        (self.backoff)(),
        || async {
          self.observer.request_sent(compressed_write_request.len());
          match self
            .client
            .send_write_request(compressed_write_request.clone(), extra_headers)
            .await
          {
            Ok(()) => Ok(()),
            Err(error) if self.policy.should_retry(&error) && !shutdown_pending => {
              let retry_after = self.policy.retry_after(&error, SystemTime::now());
              Err(backoff::Error::Transient {
                err: error,
                retry_after,
              })
            },
            Err(error) => Err(backoff::Error::permanent(error)),
          }
        },
        || self.observer.request_retry(),
        deadline,
      )
      .await
  }
}
