// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_server_stats::stats::{CounterWrapper, Scope};
use bd_stats_common::DynCounter;
use prometheus::IntCounter;

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
#[derive(Clone, Debug)]
pub struct StreamStats {
  // The number of initiated streaming requests.
  pub(crate) stream_initiations_total: IntCounter,
  // The number of successfully completed streaming requests. These streams completed cleanly,
  // without errors.
  pub(crate) stream_completion_successes_total: IntCounter,
  // The number of streaming requests completed due to an error.
  pub(crate) stream_completion_failures_total: IntCounter,

  // The number of messages sent across a stream that was opened in response to a streaming
  // request.
  pub(crate) tx_messages_total: DynCounter,

  pub(crate) tx_bytes_total: DynCounter,
  pub(crate) tx_bytes_uncompressed_total: DynCounter,
}

impl StreamStats {
  #[must_use]
  pub fn new(scope: &Scope, stream_name: &str) -> Self {
    let scope = scope.scope(stream_name);

    let stream_initiations_total = scope.counter("stream_initiations_total");

    let stream_completions_total = scope.counter_vec("stream_completions_total", &["result"]);
    let stream_completion_successes_total = stream_completions_total
      .get_metric_with_label_values(&["success"])
      .unwrap();
    let stream_completion_failures_total = stream_completions_total
      .get_metric_with_label_values(&["failure"])
      .unwrap();

    let tx_messages_total = CounterWrapper::make_dyn(scope.counter("stream_tx_messages_total"));
    let tx_bytes_total = CounterWrapper::make_dyn(scope.counter("bandwidth_tx_bytes_total"));
    let tx_bytes_uncompressed_total =
      CounterWrapper::make_dyn(scope.counter("bandwidth_tx_bytes_uncompressed_total"));

    Self {
      stream_initiations_total,
      stream_completion_successes_total,
      stream_completion_failures_total,

      tx_messages_total,

      tx_bytes_total,
      tx_bytes_uncompressed_total,
    }
  }
}
