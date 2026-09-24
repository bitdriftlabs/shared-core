// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::http::{HttpRemoteWriteClient, HttpRemoteWriteError, should_retry};
use crate::retry::Retry;
use backoff::backoff::Backoff;
use bytes::Bytes;
use http::HeaderMap;
use std::sync::Arc;

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
    }
  }

  pub async fn send(
    &self,
    compressed_write_request: Bytes,
    extra_headers: Option<&HeaderMap>,
    shutdown_pending: bool,
  ) -> Result<(), HttpRemoteWriteError> {
    self
      .retry
      .retry_notify(
        (self.backoff)(),
        || async {
          self.observer.request_sent(compressed_write_request.len());
          match self
            .client
            .send_write_request(compressed_write_request.clone(), extra_headers)
            .await
          {
            Ok(()) => Ok(()),
            Err(error) if should_retry(&error) && !shutdown_pending => {
              Err(backoff::Error::transient(error))
            },
            Err(error) => Err(backoff::Error::permanent(error)),
          }
        },
        || self.observer.request_retry(),
      )
      .await
  }
}
