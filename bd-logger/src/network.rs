// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./network_test.rs"]
mod network_test;
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  LogFieldKey,
  LogInterceptor,
  LogLevel,
  LogMessage,
  LogType,
  StringOrBytes,
};
use bd_network_quality::{NetworkQuality, NetworkQualityProvider};
use itertools::Itertools;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;
use time::ext::NumericalDuration;

/// Adds a given value to a specified field of a given sample.
macro_rules! sample_add {
  ($sample:expr, $field_name:ident, $value:expr) => {
    if let Some(unwrapped_value) = $value {
      $sample.$field_name += unwrapped_value;
    }
  };
}

/// Takes values of the specified fields of passed samples, adds them to each other, and assign the
/// result to the first passed sample.
macro_rules! accumulate_samples {
  ($lhs:expr, $rhs:expr, $field_name:ident) => {
    $lhs.$field_name += $rhs.$field_name
  };
}

//
// HTTPTrafficDataUsageTracker
//

/// Responsible for emitting logs that enable the SDK to track the number of bytes downloaded and
/// uploaded due to HTTP requests performed by the app, measured on a per-minute basis.
pub(crate) struct HTTPTrafficDataUsageTracker {
  container: parking_lot::Mutex<MetricsContainer>,
  network_quality_provider: Arc<dyn NetworkQualityProvider>,
}

impl HTTPTrafficDataUsageTracker {
  pub(crate) fn new(
    time_provider: Arc<dyn TimeProvider>,
    network_quality_provider: Arc<dyn NetworkQualityProvider>,
  ) -> Self {
    Self {
      container: parking_lot::Mutex::new(MetricsContainer::new(time_provider)),
      network_quality_provider,
    }
  }
}

impl HTTPTrafficDataUsageTracker {
  fn process_http_response_log(&self, fields: &AnnotatedLogFields) {
    // If we get a HTTP response with a status code, we assume the network is online. This is done
    // to smooth out race conditions where the tracker in the API mux is in backoff waiting to
    // attempt to reconnect before indicating online. It's possible that after setting to online
    // here it will be set to Unknown at the beginning of the API mux loop, but that's OK for
    // right now.
    //
    // Additionally, there are many edge cases where this may not work. For example, some library
    // that turns network failures into HTTP status codes, or a bound hot spot that ends up
    // returning bad content as the internet is not actually connected. We will need to revisit
    // this in the future.
    if get_int_field_value(fields, "_status_code").is_some() {
      self
        .network_quality_provider
        .set_network_quality(NetworkQuality::Online);
    }

    let mut samples = self.container.lock();

    let Some(sample) = samples.last_mut() else {
      return;
    };

    sample_add!(
      sample,
      request_body_bytes_count,
      get_int_field_value(fields, "_request_body_bytes_sent_count")
    );
    sample_add!(
      sample,
      request_headers_bytes_count,
      get_int_field_value(fields, "_request_headers_bytes_count")
    );
    sample_add!(
      sample,
      response_body_bytes_count,
      get_int_field_value(fields, "_response_body_bytes_received_count")
    );
    sample_add!(
      sample,
      response_headers_bytes_count,
      get_int_field_value(fields, "_response_headers_bytes_count")
    );
  }

  fn process_resource_utilization_log(&self, fields: &mut AnnotatedLogFields) {
    let mut guard = self.container.lock();

    let Some(sample) = guard.get_summary_sample() else {
      return;
    };

    fields.extend(
      [
        create_int_field(
          "_request_bytes_per_min_count".into(),
          sample.request_bytes_count(),
        ),
        create_int_field(
          "_request_body_bytes_per_min_count".into(),
          sample.request_body_bytes_count,
        ),
        create_int_field(
          "_request_headers_bytes_per_min_count".into(),
          sample.request_headers_bytes_count,
        ),
        create_int_field(
          "_response_bytes_per_min_count".into(),
          sample.response_bytes_count(),
        ),
        create_int_field(
          "_response_body_bytes_per_min_count".into(),
          sample.response_body_bytes_count,
        ),
        create_int_field(
          "_response_headers_bytes_per_min_count".into(),
          sample.response_headers_bytes_count,
        ),
      ]
      .into_iter()
      .collect_vec(),
    );
  }
}

impl LogInterceptor for HTTPTrafficDataUsageTracker {
  fn process(
    &self,
    _log_level: LogLevel,
    log_type: LogType,
    msg: &LogMessage,
    fields: &mut AnnotatedLogFields,
    _matching_fields: &mut AnnotatedLogFields,
  ) {
    let LogMessage::String(msg) = msg else { return };

    if log_type == LogType::Span && msg == "HTTPResponse" {
      self.process_http_response_log(fields);
    } else if log_type == LogType::Resource && msg.is_empty() {
      self.process_resource_utilization_log(fields);
    }
  }
}

//
// MetricsContainer
//

#[derive(Clone)]
struct MetricsContainer {
  /// Each sample aggregates download/upload data for `interval` amount of time.
  /// When the SDK is initialized, the container starts with no samples and allows the creation of
  /// a new sample every `interval` duration of time, up until there are "1 minute / interval"
  /// samples. After that, the container drops the oldest sample each time it creates a new one.
  /// Samples are ordered from the oldest to the newest.
  samples: VecDeque<MetricsSample>,
  time_provider: Arc<dyn TimeProvider>,
}

impl MetricsContainer {
  fn new(time_provider: Arc<dyn TimeProvider>) -> Self {
    Self {
      samples: [MetricsSample::new(time_provider.now())].into(),
      time_provider,
    }
  }

  fn last_mut(&mut self) -> Option<&mut MetricsSample> {
    self.samples.back_mut()
  }

  fn get_summary_sample(&mut self) -> Option<MetricsSample> {
    let now = self.time_provider.now();
    let samples_count = self.samples.len();
    loop {
      if self
        .samples
        .front()
        .is_some_and(|sample| now.duration_since(sample.started_at) > 1.minutes())
      {
        self.samples.pop_front();
      } else {
        break;
      }
    }

    match self.samples.len().cmp(&samples_count) {
      // We removed some samples so some of them were older than 60s so we have enough data to
      // report 60s worth of data.
      Ordering::Less => {
        let mut result = MetricsSample::new(now);
        for sample in &self.samples {
          accumulate_samples!(result, sample, request_body_bytes_count);
          accumulate_samples!(result, sample, request_headers_bytes_count);
          accumulate_samples!(result, sample, response_body_bytes_count);
          accumulate_samples!(result, sample, response_headers_bytes_count);
        }

        self.samples.push_back(MetricsSample::new(now));

        Some(result)
      },
      Ordering::Greater => {
        debug_assert!(false, "We should never remove more samples than we have");
        None
      },
      Ordering::Equal => {
        self.samples.push_back(MetricsSample::new(now));
        None
      },
    }
  }
}

//
// MetricsSample
//

#[allow(clippy::struct_field_names)]
#[derive(Clone, Debug)]
pub(crate) struct MetricsSample {
  started_at: std::time::Instant,

  request_body_bytes_count: u64,
  request_headers_bytes_count: u64,

  response_body_bytes_count: u64,
  response_headers_bytes_count: u64,
}

impl MetricsSample {
  const fn new(now: std::time::Instant) -> Self {
    Self {
      started_at: now,

      request_body_bytes_count: 0,
      request_headers_bytes_count: 0,

      response_body_bytes_count: 0,
      response_headers_bytes_count: 0,
    }
  }
}

impl MetricsSample {
  const fn request_bytes_count(&self) -> u64 {
    self.request_body_bytes_count + self.request_headers_bytes_count
  }

  const fn response_bytes_count(&self) -> u64 {
    self.response_body_bytes_count + self.response_headers_bytes_count
  }
}

/// Retrieves an integer value of a field with the specified key from the provided list of the
/// fields.
fn get_int_field_value(fields: &AnnotatedLogFields, field_key: &str) -> Option<u64> {
  let value = fields.get(field_key)?;
  let string_value = match &value.value {
    StringOrBytes::String(value) => value,
    StringOrBytes::SharedString(value) => value,
    StringOrBytes::Bytes(_) => return None,
  };

  string_value.parse::<u64>().ok()
}

/// Creates a string field using a provided key and integer value.
fn create_int_field(key: LogFieldKey, value: u64) -> (LogFieldKey, AnnotatedLogField) {
  (key, AnnotatedLogField::new_ootb(value.to_string()))
}

//
// TimeProvider
//

pub(crate) trait TimeProvider: Send + Sync {
  fn now(&self) -> std::time::Instant;
}

//
// SystemTimeProvider
//

pub(crate) struct SystemTimeProvider;

impl TimeProvider for SystemTimeProvider {
  fn now(&self) -> std::time::Instant {
    std::time::Instant::now()
  }
}

//
// NetworkQualityInterceptor
//

pub struct NetworkQualityInterceptor {
  network_quality_provider: Arc<dyn NetworkQualityProvider>,
}

impl NetworkQualityInterceptor {
  pub fn new(network_quality_provider: Arc<dyn NetworkQualityProvider>) -> Self {
    Self {
      network_quality_provider,
    }
  }
}

impl LogInterceptor for NetworkQualityInterceptor {
  fn process(
    &self,
    _log_level: LogLevel,
    log_type: LogType,
    _msg: &LogMessage,
    fields: &mut AnnotatedLogFields,
    _matching_fields: &mut AnnotatedLogFields,
  ) {
    if log_type == LogType::Resource
      || log_type == LogType::Replay
      || log_type == LogType::InternalSDK
    {
      return;
    }

    // Currently we only attach the field attribute if we think we are offline. In the future when
    // we have a more complex definition of network quality we can revisit this.
    let network_quality = self.network_quality_provider.get_network_quality();
    if network_quality != NetworkQuality::Offline {
      return;
    }

    fields.insert(
      "_network_quality".into(),
      AnnotatedLogField::new_ootb("offline"),
    );
  }
}
