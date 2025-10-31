// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_log_matcher::matcher::Tree;
use bd_log_primitives::tiny_set::{TinyMap, TinySet};
use bd_log_primitives::{FieldsRef, LogLevel, LogMessage, LogType};
use bd_proto::protos::config::v1::config::BufferConfigList;
use std::borrow::Cow;

// A single buffer filter, containing the matchers used to determine if logs should be written to
// the specific buffer.
#[derive(Debug)]
struct BufferFilter {
  // The name of the buffer to write to.
  buffer_id: String,

  // Each buffer can have a number of match criteria, each with their own ID. While we don't use
  // it right now the proto calls out a future use case. Only one of the matchers needs to match in
  // order to write the log to this buffer.
  matchers: Vec<(String, Tree)>,
}

// Used to determine which buffers a specific log line should be written to.
#[derive(Debug)]
pub struct BufferSelector {
  buffer_filters: Vec<BufferFilter>,
}

impl BufferSelector {
  pub fn new(config: &BufferConfigList) -> anyhow::Result<Self> {
    let mut buffer_filters = Vec::new();

    for buffer_config in &config.buffer_config {
      let mut matchers = Vec::new();
      for filter in &buffer_config.filters {
        if let Some(filter_matcher) = &filter.filter.as_ref() {
          matchers.push((filter.id.clone(), Tree::new_legacy(filter_matcher)?));
        }
      }

      buffer_filters.push(BufferFilter {
        buffer_id: buffer_config.id.clone(),
        matchers,
      });
    }

    Ok(Self { buffer_filters })
  }

  // Evaluates a log line against the buffer matchers. Returns the list of the name of the buffers
  // this log should be written to.
  #[must_use]
  pub fn buffers(
    &self,
    log_type: LogType,
    log_level: LogLevel,
    message: &LogMessage,
    fields: FieldsRef<'_>,
  ) -> TinySet<Cow<'_, str>> {
    let mut buffers = TinySet::default();
    for buffer in &self.buffer_filters {
      for (_id, matcher) in &buffer.matchers {
        if matcher.do_match(
          log_level,
          log_type,
          message,
          fields,
          // TODO(snowp): If we ever support using the new matcher format for buffer selectors
          // we'll want to plumb through feature flags here. For now there is no way to specify a
          // feature flag matcher so there is no point.
          None,
          &TinyMap::default(),
        ) {
          buffers.insert(Cow::Borrowed(buffer.buffer_id.as_str()));

          // No reason to match further.
          // TODO(snowp): If we ever want to report on how often the different filters match we'll
          // maybe want to keep matching.
          break;
        }
      }
    }

    buffers
  }
}
