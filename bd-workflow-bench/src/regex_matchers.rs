// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./regex_matchers_test.rs"]
mod tests;

use crate::corpus::CorpusReader;
use anyhow::anyhow;
use bd_log_matcher::matcher::{MatchContext, Tree};
use bd_log_primitives::tiny_set::TinyMap;
use bd_log_primitives::{FieldsRef, Log};
use bd_proto::protos::log_matcher::log_matcher::LogMatcher;
use bd_proto::protos::log_matcher::log_matcher::log_matcher::base_log_matcher;
use bd_proto::protos::value_matcher::value_matcher::{Operator, StringValueMatch};
use bd_state::InMemoryStateReader;
use serde_json::Value;
use std::collections::BTreeSet;
use std::hint::black_box;
use std::path::Path;

//
// RegexMatcherCorpus
//

/// Every distinct regex message or field matcher in a workflow configuration, with the corpus logs
/// to evaluate them against. This isolates regex evaluation from the rest of the workflow engine.
pub struct RegexMatcherCorpus {
  patterns: Vec<String>,
  matchers: Vec<Tree>,
  logs: Vec<Log>,
}

impl RegexMatcherCorpus {
  /// Loads the regex matchers from `config_path` and the logs from `logs_path`. Logs whose message
  /// is longer than `max_message_bytes`, when given, are left out.
  pub fn load(
    config_path: &Path,
    logs_path: &Path,
    max_message_bytes: Option<usize>,
  ) -> anyhow::Result<Self> {
    let config: Value = serde_json::from_str(&crate::fixtures::read_to_string(config_path)?)?;
    let mut distinct = BTreeSet::new();
    collect_regex_matchers(&config, false, &mut distinct)?;

    let mut patterns = Vec::new();
    let mut matchers = Vec::new();
    for json in distinct {
      let proto: LogMatcher = protobuf_json_mapping::parse_from_str(&json)?;
      let Some(pattern) = regex_pattern(&proto) else {
        continue;
      };
      // The workflow engine rejects a configuration whose regex does not compile, so neither
      // evaluates it.
      if let Ok(matcher) = Tree::new(&proto) {
        patterns.push(pattern.to_owned());
        matchers.push(matcher);
      }
    }

    let mut reader = CorpusReader::open(logs_path)?;
    let mut logs = Vec::new();
    while let Some(source) = reader.next_log()? {
      let message_bytes = message_len(&source.log);
      if max_message_bytes.is_none_or(|max| message_bytes <= max) {
        logs.push(source.log);
      }
    }
    if logs.is_empty() {
      return Err(anyhow!("no logs to evaluate"));
    }

    Ok(Self {
      patterns,
      matchers,
      logs,
    })
  }

  /// The pattern of each matcher, in evaluation order.
  #[must_use]
  pub fn patterns(&self) -> &[String] {
    &self.patterns
  }

  #[must_use]
  pub fn log_count(&self) -> usize {
    self.logs.len()
  }

  #[must_use]
  pub fn message_bytes(&self) -> usize {
    self.logs.iter().map(message_len).sum()
  }

  /// Evaluates every matcher against every log and returns the number of matches.
  #[must_use]
  pub fn evaluate_all(&self) -> usize {
    let state = InMemoryStateReader::new();
    let extracted_fields = TinyMap::default();
    let mut matches = 0;
    for log in &self.logs {
      for matcher in &self.matchers {
        if matcher.do_match(
          log.log_level,
          log.log_type,
          black_box(&log.message),
          FieldsRef::new(&log.fields, &log.matching_fields),
          &state,
          &extracted_fields,
          0,
          MatchContext::default(),
        ) {
          matches += 1;
        }
      }
    }
    matches
  }
}

/// Collects each workflow log matcher whose base matcher is a regex message or field match, as
/// compact JSON so identical matchers from different workflows are evaluated once. Matchers outside
/// the workflow configurations, such as buffer selectors, use a different schema and are skipped.
fn collect_regex_matchers(
  value: &Value,
  in_workflows: bool,
  distinct: &mut BTreeSet<String>,
) -> anyhow::Result<()> {
  match value {
    Value::Object(object) => {
      if in_workflows && object.contains_key("baseMatcher") {
        let json = serde_json::to_string(value)?;
        // Skip anything that is not a log matcher this build understands.
        if protobuf_json_mapping::parse_from_str::<LogMatcher>(&json)
          .is_ok_and(|proto| regex_pattern(&proto).is_some())
        {
          distinct.insert(json);
        }
      }
      for (key, child) in object {
        let in_workflows =
          in_workflows || key == "workflowsConfiguration" || key == "debugWorkflows";
        collect_regex_matchers(child, in_workflows, distinct)?;
      }
    },
    Value::Array(items) => {
      for item in items {
        collect_regex_matchers(item, in_workflows, distinct)?;
      }
    },
    _ => {},
  }
  Ok(())
}

fn message_len(log: &Log) -> usize {
  log
    .message
    .as_str()
    .map(str::len)
    .or_else(|| log.message.as_bytes().map(<[u8]>::len))
    .unwrap_or_default()
}

fn regex_pattern(matcher: &LogMatcher) -> Option<&str> {
  let string_match: &StringValueMatch = match matcher.base_matcher().match_type.as_ref()? {
    base_log_matcher::Match_type::MessageMatch(message_match) => &message_match.string_value_match,
    base_log_matcher::Match_type::TagMatch(tag_match) => match tag_match.value_match.as_ref()? {
      base_log_matcher::tag_match::Value_match::StringValueMatch(string_match) => string_match,
      _ => return None,
    },
    _ => return None,
  };
  (string_match.operator.enum_value_or_default() == Operator::OPERATOR_REGEX)
    .then(|| string_match.match_value())
}
