// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_log_matcher::matcher::{MatchContext, Tree};
use bd_log_primitives::tiny_set::{TinyMap, TinySet};
use bd_log_primitives::{FieldsRef, LogLevel, LogMessage};
#[cfg(test)]
use bd_proto::protos::config::v1::config::BufferConfigList;
use bd_proto::protos::logging::payload::LogType;
#[cfg(test)]
use bd_proto_util::serialization::inline::ProtoDeserialize;
use bd_proto_util::serialization::inline::views::config::BufferConfigList as BufferConfigView;
#[cfg(test)]
use protobuf::Message;
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
  pub fn from_view(config: &BufferConfigView<'_>) -> anyhow::Result<Self> {
    let buffer_filters = config
      .buffer_config()?
      .iter()
      .map(|buffer| {
        let mut matchers = Vec::new();
        for filter in buffer.filters()? {
          if let Some(matcher) = filter.filter()? {
            matchers.push((filter.id()?.to_owned(), Tree::from_legacy_view(&matcher)?));
          }
        }
        Ok(BufferFilter {
          buffer_id: buffer.id()?.to_owned(),
          matchers,
        })
      })
      .collect::<anyhow::Result<_>>()?;
    Ok(Self { buffer_filters })
  }

  #[cfg(test)]
  pub fn new(config: &BufferConfigList) -> anyhow::Result<Self> {
    let bytes = config.write_to_bytes()?;
    Self::from_view(&BufferConfigView::from_proto_bytes(&bytes)?)
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
    state: &dyn bd_state::StateReader,
  ) -> TinySet<Cow<'_, str>> {
    let mut buffers = TinySet::default();
    for buffer in &self.buffer_filters {
      for (_id, matcher) in &buffer.matchers {
        if matcher.do_match(
          log_level,
          log_type,
          message,
          fields,
          state,
          &TinyMap::default(),
          0,
          MatchContext::default(),
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
