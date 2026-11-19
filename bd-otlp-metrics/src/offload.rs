// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::http::{HttpRemoteWriteError, should_retry};
use anyhow::Result;
use async_trait::async_trait;
use base64ct::{Base64Unpadded, Encoding};
use bytes::Bytes;
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

//
// SerializedOffloadRequest
//

#[derive(Serialize, Deserialize)]
pub struct SerializedOffloadRequest {
  base64_compressed_write_request: String,
  extra_headers: Option<HashMap<String, String>>,
  num_metrics: u64,
  retry_attempt: u64,
  created_at_unix: i64,
}

//
// OffloadQueue
//

#[mockall::automock]
#[async_trait]
pub trait OffloadQueue: Send + Sync {
  async fn queue_write_request(
    &self,
    serialized_request: SerializedOffloadRequest,
    backoff: Duration,
  ) -> Result<()>;

  async fn receive_write_requests(&self) -> Result<Vec<SerializedOffloadRequest>>;
}

//
// OffloadRetryPolicy
//

#[derive(Clone, Copy)]
pub struct OffloadRetryPolicy {
  pub backoff: Duration,
  pub max_send_attempts: Option<u32>,
  pub window: Duration,
}

pub async fn maybe_queue_for_retry(
  offload_queue: Option<&Arc<dyn OffloadQueue>>,
  policy: OffloadRetryPolicy,
  error: &HttpRemoteWriteError,
  mut serialized: SerializedOffloadRequest,
  now: OffsetDateTime,
) -> Result<bool> {
  let Some(offload_queue) = offload_queue else {
    return Ok(false);
  };

  if !should_retry(error) {
    log::debug!("dropping due to not retriable");
    return Ok(false);
  }

  serialized.inc_retry_attempts();
  if policy
    .max_send_attempts
    .is_some_and(|max_send_attempts| serialized.retry_attempts() > u64::from(max_send_attempts))
  {
    log::debug!("dropping due to max attempts");
    return Ok(false);
  }

  if serialized.retry_attempts() > 1 && now - serialized.created_at() > policy.window {
    log::debug!("dropping due to max window");
    return Ok(false);
  }

  let backoff = policy.backoff * u32::try_from(serialized.retry_attempts()).unwrap();
  offload_queue
    .queue_write_request(serialized, backoff)
    .await?;
  Ok(true)
}

impl SerializedOffloadRequest {
  pub fn new(
    compressed_write_request: &Bytes,
    extra_headers: Option<Arc<HeaderMap>>,
    num_metrics: u64,
    created_at_unix: i64,
  ) -> Self {
    Self {
      base64_compressed_write_request: Base64Unpadded::encode_string(compressed_write_request),
      extra_headers: extra_headers.map(|headers| {
        headers
          .iter()
          .map(|(key, value)| (key.to_string(), value.to_str().unwrap().to_string()))
          .collect()
      }),
      num_metrics,
      retry_attempt: 0,
      created_at_unix,
    }
  }

  #[must_use]
  pub fn compressed_write_request(&self) -> Bytes {
    Base64Unpadded::decode_vec(&self.base64_compressed_write_request)
      .unwrap()
      .into()
  }

  #[must_use]
  pub const fn num_metrics(&self) -> u64 {
    self.num_metrics
  }

  #[must_use]
  pub fn extra_headers(&self) -> Option<Arc<HeaderMap>> {
    self
      .extra_headers
      .as_ref()
      .map(|extra_headers| Arc::new(extra_headers.try_into().unwrap()))
  }

  pub fn inc_retry_attempts(&mut self) {
    self.retry_attempt += 1;
  }

  #[must_use]
  pub const fn retry_attempts(&self) -> u64 {
    self.retry_attempt
  }

  #[must_use]
  pub fn created_at(&self) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(self.created_at_unix).unwrap()
  }
}
