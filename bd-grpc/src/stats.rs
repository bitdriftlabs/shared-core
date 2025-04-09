// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::service::ServiceMethod;
use bd_server_stats::stats::{CounterWrapper, Scope};
use bd_stats_common::DynCounter;
use prometheus::{IntCounter, IntCounterVec};
use protobuf::MessageFull;
use std::collections::HashMap;

//
// EndpointStats
//

pub struct EndpointStats {
  rpc: IntCounterVec,
  scope: Scope,
}
#[derive(Clone)]
pub struct ResolvedEndpointStats {
  pub success: IntCounter,
  pub failure: IntCounter,
}

impl EndpointStats {
  #[must_use]
  pub fn new(scope: Scope) -> Self {
    let rpc = scope.counter_vec("rpc", &["service", "endpoint", "result"]);

    Self { rpc, scope }
  }

  #[must_use]
  pub fn resolve<OutgoingType: MessageFull, IncomingType: MessageFull>(
    &self,
    service: &ServiceMethod<OutgoingType, IncomingType>,
  ) -> ResolvedEndpointStats {
    ResolvedEndpointStats {
      success: self.rpc.with_label_values(&[
        service.service_name().replace('.', "_").as_str(),
        service.method_name(),
        "success",
      ]),
      failure: self.rpc.with_label_values(&[
        service.service_name().replace('.', "_").as_str(),
        service.method_name(),
        "failure",
      ]),
    }
  }

  #[must_use]
  pub fn resolve_streaming<OutgoingType: MessageFull, IncomingType: MessageFull>(
    &self,
    service: &ServiceMethod<OutgoingType, IncomingType>,
  ) -> StreamStats {
    let labels: HashMap<String, String> = [
      (
        "service".to_string(),
        service.service_name().replace('.', "_"),
      ),
      ("endpoint".to_string(), service.method_name().to_string()),
    ]
    .into();

    let stream_initiations_total = self
      .scope
      .counter_with_labels("stream_initiations_total", labels.clone());

    let rpc = self.resolve(service);

    let tx_messages_total = CounterWrapper::make_dyn(
      self
        .scope
        .counter_with_labels("stream_tx_messages_total", labels.clone()),
    );
    let tx_bytes_total = CounterWrapper::make_dyn(
      self
        .scope
        .counter_with_labels("bandwidth_tx_bytes_total", labels.clone()),
    );
    let tx_bytes_uncompressed_total = CounterWrapper::make_dyn(
      self
        .scope
        .counter_with_labels("bandwidth_tx_bytes_uncompressed_total", labels),
    );

    StreamStats {
      stream_initiations_total,
      rpc,

      tx_messages_total,

      tx_bytes_total,
      tx_bytes_uncompressed_total,
    }
  }
}

//
// BandwidthStatsSummary
//

pub struct BandwidthStatsSummary {
  pub rx: u64,
  pub rx_decompressed: u64,
  pub tx: u64,
  pub tx_uncompressed: u64,
}

//
// StreamStats
//

/// gRPC streaming request stats.
#[derive(Clone)]
pub struct StreamStats {
  // The number of initiated streaming requests.
  pub(crate) stream_initiations_total: IntCounter,

  pub(crate) rpc: ResolvedEndpointStats,

  // The number of messages sent across a stream that was opened in response to a streaming
  // request.
  pub(crate) tx_messages_total: DynCounter,

  pub(crate) tx_bytes_total: DynCounter,
  pub(crate) tx_bytes_uncompressed_total: DynCounter,
}
