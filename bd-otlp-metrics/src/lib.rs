// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./metric_test.rs"]
mod tests;

#[cfg(test)]
#[path = "./retry_test.rs"]
mod retry_test;

#[cfg(test)]
#[path = "./http_test.rs"]
mod http_test;

#[cfg(test)]
#[path = "./offload_test.rs"]
mod offload_test;

#[cfg(test)]
#[path = "./delivery_test.rs"]
mod delivery_test;

pub mod delivery;
pub mod http;
pub mod metric;
pub mod offload;
pub mod otlp;
#[allow(
  clippy::nursery,
  clippy::pedantic,
  clippy::style,
  renamed_and_removed_lints
)]
pub mod protos;
pub mod retry;

pub use delivery::{BackoffFactory, DeliveryEngine, DeliveryObserver};
pub use http::{
  HttpRemoteWriteClient,
  HttpRemoteWriteError,
  MockHttpRemoteWriteClient,
  should_retry,
};
pub use metric::{
  CounterType,
  HistogramBucket,
  HistogramData,
  Metric,
  MetricId,
  MetricType,
  MetricValue,
  ParseError,
  SummaryBucket,
  SummaryData,
  TagValue,
  default_timestamp,
  prom_stale_marker,
  unwrap_timestamp,
};
pub use offload::{
  MockOffloadQueue,
  OffloadQueue,
  OffloadRetryPolicy,
  SerializedOffloadRequest,
  maybe_queue_for_retry,
};
pub use otlp::{OtlpCompression, deserialize_otlp_metrics_request, encode_otlp_metrics};
pub use retry::{Retry, RetryConfig};
