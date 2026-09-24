// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bytes::Bytes;
use std::fmt::Display;
use std::hash::Hash;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use time::OffsetDateTime;

const STALE_MARKER_BITS: u64 = 0x7ff0_0000_0000_0002;
const MAX_SECONDS_TIMESTAMP: u64 = 100_000_000_000;

//
// CounterType
//

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CounterType {
  Delta,
  Absolute,
}

//
// MetricType
//

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum MetricType {
  Counter(CounterType),
  DeltaGauge,
  DirectGauge,
  Gauge,
  Histogram,
  Summary,
  Timer,
  BulkTimer,
}

impl MetricType {
  pub const fn from_statsd(metric_type: &[u8]) -> Result<Self, ParseError> {
    match metric_type {
      b"c" => Ok(Self::Counter(CounterType::Delta)),
      b"k" | b"G" => Ok(Self::DirectGauge),
      b"g" => Ok(Self::Gauge),
      b"h" | b"ms" => Ok(Self::Timer),
      _ => Err(ParseError::InvalidType),
    }
  }

  #[must_use]
  pub fn to_statsd(&self) -> &'static [u8] {
    match self {
      Self::Counter(_) => b"c",
      Self::DeltaGauge | Self::Gauge => b"g",
      Self::DirectGauge => b"G",
      Self::Histogram | Self::Summary | Self::BulkTimer => unreachable!(),
      Self::Timer => b"ms",
    }
  }
}

//
// TagValue
//

#[derive(PartialOrd, Eq, Ord, Debug, Clone, PartialEq, Hash)]
pub struct TagValue {
  pub tag: Bytes,
  pub value: Bytes,
}

impl Display for TagValue {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{}={}",
      String::from_utf8_lossy(&self.tag),
      String::from_utf8_lossy(&self.value)
    )
  }
}

//
// MetricId
//

#[derive(Clone, Debug, Eq, PartialOrd, PartialEq)]
pub struct MetricId {
  name: Bytes,
  mtype: Option<MetricType>,
  tags: Vec<TagValue>,
}

#[derive(Error, Debug, Eq, PartialEq)]
pub enum ParseError {
  #[error("generic parse error")]
  Generic,
  #[error("invalid parsed value")]
  InvalidValue,
  #[error("invalid sample rate")]
  InvalidSampleRate,
  #[error("invalid type")]
  InvalidType,
  #[error("invalid tag")]
  InvalidTag,
  #[error("overall invalid line - no structural elements found in parsing")]
  InvalidLine,
  #[error("invalid protocol")]
  InvalidProtocol,
  #[error("prometheus remote write error: {0}")]
  PromRemoteWrite(String),
  #[error("more than one sample rate field found")]
  RepeatedSampleRate,
  #[error("more than one set of tags found")]
  RepeatedTags,
  #[error("name, tag name, or tag value length too large")]
  TooLarge,
  #[error("unsupported extension field")]
  UnsupportedExtensionField,
  #[error("cannot change protocol for unparsable metric sample")]
  UnparsableMetricChangeProtocol,
  #[error("invalid timestamp")]
  InvalidTimestamp,
}

fn tags_sorted(tags: &[TagValue]) -> bool {
  let mut cloned_tags = tags.to_vec();
  cloned_tags.sort_unstable();
  tags == cloned_tags
}

impl MetricId {
  pub fn new(
    name: Bytes,
    mtype: Option<MetricType>,
    mut tags: Vec<TagValue>,
    already_sorted: bool,
  ) -> Result<Self, ParseError> {
    if name.len() > u16::MAX as usize
      || tags
        .iter()
        .any(|tag| tag.tag.len() > u16::MAX as usize || tag.value.len() > u16::MAX as usize)
    {
      return Err(ParseError::TooLarge);
    }

    if already_sorted {
      debug_assert!(tags_sorted(&tags));
    } else {
      tags.sort_unstable();
    }
    Ok(Self { name, mtype, tags })
  }

  pub const fn mtype(&self) -> Option<MetricType> {
    self.mtype
  }

  pub fn set_mtype(&mut self, mtype: MetricType) {
    self.mtype = Some(mtype);
  }

  pub fn set_name(&mut self, name: Bytes) {
    self.name = name;
  }

  pub const fn name(&self) -> &Bytes {
    &self.name
  }

  pub fn tags(&self) -> &[TagValue] {
    &self.tags
  }

  pub fn tags_mut(&mut self) -> &mut Vec<TagValue> {
    &mut self.tags
  }

  pub fn tag(&self, tag_name: &str) -> Option<&TagValue> {
    self
      .tags
      .binary_search_by(|tag| tag.tag.as_ref().cmp(tag_name.as_bytes()))
      .ok()
      .map(|index| &self.tags[index])
  }

  pub fn into_parts(self) -> (Bytes, Option<MetricType>, Vec<TagValue>) {
    (self.name, self.mtype, self.tags)
  }
}

impl Hash for MetricId {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.name.hash(state);
    self.mtype.hash(state);
    for tag in &self.tags {
      tag.tag.hash(state);
      tag.value.hash(state);
    }
  }
}

impl Display for MetricId {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let name = String::from_utf8_lossy(self.name.as_ref());
    write!(f, "{name}(")?;
    for tag in &self.tags {
      write!(f, "[{tag}]")?;
    }
    write!(f, ")")
  }
}

//
// HistogramData
//

#[derive(Clone, Debug, Default)]
pub struct HistogramBucket {
  pub le: f64,
  pub count: f64,
}

#[derive(Clone, Debug, Default)]
pub struct HistogramData {
  pub buckets: Vec<HistogramBucket>,
  pub sample_count: f64,
  pub sample_sum: f64,
}

impl PartialEq for HistogramData {
  fn eq(&self, other: &Self) -> bool {
    self.buckets.len() == other.buckets.len()
      && self
        .buckets
        .iter()
        .zip(other.buckets.iter())
        .all(|(lhs, rhs)| f64_or_stale_marker_eq(lhs.count, rhs.count) && lhs.le == rhs.le)
      && f64_or_stale_marker_eq(self.sample_count, other.sample_count)
      && f64_or_stale_marker_eq(self.sample_sum, other.sample_sum)
  }
}

//
// SummaryData
//

#[derive(Clone, Debug, Default)]
pub struct SummaryBucket {
  pub quantile: f64,
  pub value: f64,
}

#[derive(Clone, Debug, Default)]
pub struct SummaryData {
  pub quantiles: Vec<SummaryBucket>,
  pub sample_count: f64,
  pub sample_sum: f64,
}

impl PartialEq for SummaryData {
  fn eq(&self, other: &Self) -> bool {
    self.quantiles.len() == other.quantiles.len()
      && self
        .quantiles
        .iter()
        .zip(other.quantiles.iter())
        .all(|(lhs, rhs)| {
          f64_or_stale_marker_eq(lhs.value, rhs.value) && lhs.quantile == rhs.quantile
        })
      && f64_or_stale_marker_eq(self.sample_count, other.sample_count)
      && f64_or_stale_marker_eq(self.sample_sum, other.sample_sum)
  }
}

//
// MetricValue
//

#[derive(Clone, Debug)]
pub enum MetricValue {
  Simple(f64),
  Histogram(HistogramData),
  Summary(SummaryData),
  BulkTimer(Vec<f64>),
}

impl PartialEq for MetricValue {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Self::Simple(lhs), Self::Simple(rhs)) => f64_or_stale_marker_eq(*lhs, *rhs),
      (Self::Histogram(lhs), Self::Histogram(rhs)) => lhs == rhs,
      (Self::Summary(lhs), Self::Summary(rhs)) => lhs == rhs,
      (Self::BulkTimer(lhs), Self::BulkTimer(rhs)) => lhs == rhs,
      _ => false,
    }
  }
}

impl MetricValue {
  #[must_use]
  pub fn to_simple(&self) -> f64 {
    match self {
      Self::Simple(value) => *value,
      Self::Histogram(_) | Self::Summary(_) | Self::BulkTimer(_) => unreachable!(),
    }
  }

  #[must_use]
  pub fn maybe_simple(&self) -> Option<f64> {
    if let Self::Simple(value) = self {
      Some(*value)
    } else {
      None
    }
  }

  #[must_use]
  pub fn to_histogram(&self) -> &HistogramData {
    match self {
      Self::Histogram(histogram) => histogram,
      Self::Simple(_) | Self::Summary(_) | Self::BulkTimer(_) => unreachable!(),
    }
  }

  #[must_use]
  pub fn to_summary(&self) -> &SummaryData {
    match self {
      Self::Summary(summary) => summary,
      Self::Simple(_) | Self::Histogram(_) | Self::BulkTimer(_) => unreachable!(),
    }
  }

  #[must_use]
  pub fn to_bulk_timer(&self) -> &[f64] {
    match self {
      Self::BulkTimer(timers) => timers,
      Self::Simple(_) | Self::Histogram(_) | Self::Summary(_) => unreachable!(),
    }
  }

  #[must_use]
  pub fn into_histogram(self) -> HistogramData {
    match self {
      Self::Histogram(histogram) => histogram,
      Self::Simple(_) | Self::Summary(_) | Self::BulkTimer(_) => unreachable!(),
    }
  }

  #[must_use]
  pub fn into_summary(self) -> SummaryData {
    match self {
      Self::Summary(summary) => summary,
      Self::Simple(_) | Self::Histogram(_) | Self::BulkTimer(_) => unreachable!(),
    }
  }

  #[must_use]
  pub fn into_bulk_timer(self) -> Vec<f64> {
    match self {
      Self::BulkTimer(timers) => timers,
      Self::Simple(_) | Self::Histogram(_) | Self::Summary(_) => unreachable!(),
    }
  }
}

//
// Metric
//

#[derive(Clone, Debug, PartialEq)]
pub struct Metric {
  id: MetricId,
  pub sample_rate: Option<f64>,
  pub timestamp: u64,
  pub value: MetricValue,
}

impl Metric {
  pub const fn new(
    id: MetricId,
    sample_rate: Option<f64>,
    timestamp: u64,
    value: MetricValue,
  ) -> Self {
    Self {
      id,
      sample_rate,
      timestamp,
      value,
    }
  }

  pub fn into_parts(self) -> (MetricId, Option<f64>, u64, MetricValue) {
    (self.id, self.sample_rate, self.timestamp, self.value)
  }

  pub const fn get_id(&self) -> &MetricId {
    &self.id
  }

  pub fn get_id_mut(&mut self) -> &mut MetricId {
    &mut self.id
  }

  pub fn set_id(&mut self, id: MetricId) {
    self.id = id;
  }

  pub fn to_datetime(&self) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp(i64::try_from(self.timestamp).unwrap()).ok()
  }
}

impl Display for Metric {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let value = match &self.value {
      MetricValue::Simple(value) => value.to_string(),
      MetricValue::Histogram(_) => "histogram".to_string(),
      MetricValue::Summary(_) => "summary".to_string(),
      MetricValue::BulkTimer(_) => "bulk_timer".to_string(),
    };
    write!(
      f,
      "{}[VALUE={value}][TIMESTAMP={}]",
      self.id, self.timestamp
    )
  }
}

#[must_use]
pub const fn prom_stale_marker() -> f64 {
  f64::from_bits(STALE_MARKER_BITS)
}

#[must_use]
pub fn default_timestamp() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|duration| duration.as_secs())
    .unwrap()
}

#[must_use]
pub fn unwrap_timestamp(timestamp: Option<u64>) -> u64 {
  timestamp.map_or_else(default_timestamp, |value| {
    if value > MAX_SECONDS_TIMESTAMP {
      value / 1000
    } else {
      value
    }
  })
}

fn f64_or_stale_marker_eq(lhs: f64, rhs: f64) -> bool {
  lhs == rhs || (lhs.to_bits() == STALE_MARKER_BITS && rhs.to_bits() == STALE_MARKER_BITS)
}
