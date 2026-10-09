// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::protos::metrics_service::ExportMetricsServiceResponse;
use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use protobuf::Message;
use std::time::{Duration, SystemTime};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc2822;

//
// HttpRemoteWriteError
//

#[derive(thiserror::Error, Debug)]
pub enum HttpRemoteWriteError {
  #[error("AWS error: {0}")]
  Aws(String),
  #[error("hyper client error: {0}")]
  HyperClient(#[from] hyper_util::client::legacy::Error),
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),
  #[error("response error: {0}: {1}, headers: {2:?}")]
  Response(StatusCode, String, HeaderMap),
  #[error("request timeout")]
  Timeout,
  #[error("permanent delivery error: {0}")]
  Permanent(String),
  #[error("OTLP partial success: {rejected_data_points} rejected data points: {error_message}")]
  PartialSuccess {
    rejected_data_points: u64,
    error_message: String,
  },
}

//
// HttpRetryPolicy
//

#[derive(Clone, Copy, Debug, Default)]
pub enum HttpRetryPolicy {
  #[default]
  RemoteWrite,
  Otlp,
}

impl HttpRetryPolicy {
  #[must_use]
  pub fn should_retry(self, error: &HttpRemoteWriteError) -> bool {
    match (self, error) {
      (_, HttpRemoteWriteError::Permanent(_) | HttpRemoteWriteError::PartialSuccess { .. })
      | (Self::Otlp, HttpRemoteWriteError::Aws(_)) => false,
      (Self::RemoteWrite, error) => should_retry(error),
      (Self::Otlp, HttpRemoteWriteError::Response(status, ..)) => matches!(
        *status,
        StatusCode::TOO_MANY_REQUESTS
          | StatusCode::BAD_GATEWAY
          | StatusCode::SERVICE_UNAVAILABLE
          | StatusCode::GATEWAY_TIMEOUT
      ),
      (Self::Otlp, _) => true,
    }
  }

  #[must_use]
  pub fn retry_after(self, error: &HttpRemoteWriteError, now: SystemTime) -> Option<Duration> {
    if !matches!(self, Self::Otlp) || !self.should_retry(error) {
      return None;
    }
    let HttpRemoteWriteError::Response(_, _, headers) = error else {
      return None;
    };
    let value = headers
      .get(http::header::RETRY_AFTER)?
      .to_str()
      .ok()?
      .trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
      return value.parse::<u64>().ok().map(Duration::from_secs);
    }
    let date: SystemTime = OffsetDateTime::parse(value, &Rfc2822).ok()?.into();
    Some(date.duration_since(now).unwrap_or_default())
  }
}

/// Collector diagnostics are untrusted and may echo authentication credentials.
pub fn decode_otlp_response(body: &[u8]) -> Result<(), HttpRemoteWriteError> {
  let response = ExportMetricsServiceResponse::parse_from_bytes(body)
    .map_err(|_| HttpRemoteWriteError::Permanent("malformed OTLP success response".to_string()))?;
  if let Some(partial) = response.partial_success.into_option() {
    let rejected_data_points = u64::try_from(partial.rejected_data_points).map_err(|_| {
      HttpRemoteWriteError::Permanent("invalid OTLP rejected-point count".to_string())
    })?;
    if rejected_data_points > 0 || !partial.error_message.is_empty() {
      return Err(HttpRemoteWriteError::PartialSuccess {
        rejected_data_points,
        error_message: if partial.error_message.is_empty() {
          String::new()
        } else {
          "collector diagnostic redacted".to_string()
        },
      });
    }
  }
  Ok(())
}

#[allow(clippy::ref_option_ref)]
#[mockall::automock]
#[async_trait]
pub trait HttpRemoteWriteClient: Send + Sync {
  async fn send_write_request<'a>(
    &self,
    compressed_write_request: Bytes,
    extra_headers: Option<&'a HeaderMap>,
  ) -> Result<(), HttpRemoteWriteError>;
}

#[must_use]
pub fn should_retry(error: &HttpRemoteWriteError) -> bool {
  match error {
    HttpRemoteWriteError::Permanent(_) | HttpRemoteWriteError::PartialSuccess { .. } => false,
    HttpRemoteWriteError::Response(status, ..) => {
      status.is_server_error() || *status == StatusCode::TOO_MANY_REQUESTS
    },
    _ => true,
  }
}
