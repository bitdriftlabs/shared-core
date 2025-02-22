// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{CONTENT_TYPE_CONNECT_STREAMING, CONTENT_TYPE_GRPC};
use http::header::CONTENT_TYPE;
use http_body::Body;
use tower_http::compression::predicate::SizeAbove;
use tower_http::compression::{CompressionLayer, Predicate};

//
// ConnectSafeCompressionLayer
//

#[derive(Clone)]
pub struct ConnectSafeCompressionLayer {}

impl Predicate for ConnectSafeCompressionLayer {
  fn should_compress<B>(&self, response: &http::Response<B>) -> bool
  where
    B: Body,
  {
    // Custom predicate that keeps default min size and skips both gRPC and connect streaming.
    SizeAbove::default().should_compress(response)
      && response.headers().get(CONTENT_TYPE).is_none_or(|v| {
        v != CONTENT_TYPE_GRPC && v != CONTENT_TYPE_CONNECT_STREAMING
      })
  }
}

impl ConnectSafeCompressionLayer {
  #[must_use]
  pub fn new() -> CompressionLayer<Self> {
    CompressionLayer::new().compress_when(Self {})
  }
}

//
// Compression
//

// What compression type to use for gRPC requests.
pub enum Compression {
  // No compression.
  None,
  // Spec compliant gRPC compression.
  GRpc(bd_grpc_codec::Compression),
  // Snappy raw compression. This is meant for unary requests only as it does not use the snappy
  // frame format and persist state for the duration of the stream. We can add this later if
  // needed. This is not compliant with the gRPC spec.
  Snappy,
}
