// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::matcher::Tree;
use bd_feature_flags::test::TestFeatureFlags;
use bd_log_primitives::tiny_set::TinyMap;
use bd_log_primitives::{FieldsRef, LogMessage, StringOrBytes, TypedLogLevel};
use bd_proto::protos::log_matcher::log_matcher::LogMatcher;
use bd_proto::protos::logging::payload::LogType;
use std::collections::HashMap;

/// Test helper that provides a more convenient way to test log matchers.
pub struct TestMatcher {
  tree: Tree,
}

impl TestMatcher {
  pub fn new(matcher: &LogMatcher) -> anyhow::Result<Self> {
    Ok(Self {
      tree: Tree::new(matcher)?,
    })
  }

  /// A simplified version of `match_log` that takes a simpler set of parameters.
  pub fn match_log_with_feature_flags<'a>(
    &self,
    log_level: TypedLogLevel,
    log_type: LogType,
    message: impl Into<LogMessage>,
    fields: impl Into<HashMap<&'a str, &'a str>>,
    feature_flags: &bd_feature_flags::FeatureFlags,
  ) -> bool {
    let fields = fields
      .into()
      .into_iter()
      .map(|(k, v)| (k.to_string().into(), StringOrBytes::from(v)))
      .collect();
    let matching_fields = [].into();
    self.tree.do_match(
      log_level.as_u32(),
      log_type,
      &message.into(),
      FieldsRef::new(&fields, &matching_fields),
      Some(feature_flags),
      &TinyMap::default(),
    )
  }

  pub fn match_log<'a>(
    &self,
    log_level: TypedLogLevel,
    log_type: LogType,
    message: impl Into<LogMessage>,
    fields: impl Into<HashMap<&'a str, &'a str>>,
  ) -> bool {
    self.match_log_with_feature_flags(
      log_level,
      log_type,
      message,
      fields,
      &TestFeatureFlags::default(),
    )
  }
}
