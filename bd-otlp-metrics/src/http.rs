// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, StatusCode};

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
    HttpRemoteWriteError::Response(status, ..) => {
      status.is_server_error() || *status == StatusCode::TOO_MANY_REQUESTS
    },
    _ => true,
  }
}
