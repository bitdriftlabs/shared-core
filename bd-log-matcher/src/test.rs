use crate::matcher::Tree;
use bd_log_primitives::{LogMessage, LogType, StringOrBytes, TypedLogLevel};
use bd_matcher::FieldProvider;
use bd_proto::protos::log_matcher::log_matcher::LogMatcher;
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
  pub fn match_log<'a>(
    &self,
    log_level: TypedLogLevel,
    log_type: LogType,
    message: impl Into<LogMessage>,
    fields: impl Into<HashMap<&'a str, &'a str>>,
  ) -> bool {
    self.tree.do_match(
      log_level.as_u32(),
      log_type,
      &message.into(),
      &TestFields::new(fields.into()),
      None,
    )
  }
}

struct TestFields(HashMap<String, StringOrBytes<String, Vec<u8>>>);

impl TestFields {
  pub fn new(fields: HashMap<&str, &str>) -> Self {
    let fields = fields
      .into_iter()
      .map(|(k, v)| (k.to_string(), StringOrBytes::from(v)))
      .collect();
    Self(fields)
  }
}

impl FieldProvider for TestFields {
  fn field_value(&self, key: &str) -> Option<std::borrow::Cow<'_, str>> {
    self
      .0
      .get(key)
      .and_then(|v| v.as_str().map(std::borrow::Cow::Borrowed))
  }
}
