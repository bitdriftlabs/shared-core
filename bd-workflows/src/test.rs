// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Test code only.
#![allow(clippy::unwrap_used)]

use crate::config::Config;
use bd_test_helpers::workflow::WorkflowBuilder;
use std::collections::BTreeMap;
use time::OffsetDateTime;

pub trait MakeConfig {
  fn make_config(self) -> Config;
}

impl MakeConfig for WorkflowBuilder {
  fn make_config(self) -> Config {
    Config::new(self.build()).unwrap()
  }
}

pub struct TestLog {
  pub message: String,
  pub occurred_at: OffsetDateTime,
  pub now: OffsetDateTime,
  pub tags: BTreeMap<String, String>,
  pub session: Option<String>,
}

impl TestLog {
  #[must_use]
  pub fn new(message: &str) -> Self {
    let now = OffsetDateTime::now_utc();
    Self {
      message: message.to_string(),
      occurred_at: now,
      now,
      tags: BTreeMap::new(),
      session: None,
    }
  }

  #[must_use]
  pub fn with_occurred_at(mut self, occurred_at: OffsetDateTime) -> Self {
    self.occurred_at = occurred_at;
    self
  }

  #[must_use]
  pub fn with_tags(mut self, tags: BTreeMap<String, String>) -> Self {
    self.tags = tags;
    self
  }

  #[must_use]
  pub fn with_session(mut self, session: &str) -> Self {
    self.session = Some(session.to_string());
    self
  }

  #[must_use]
  pub fn with_now(mut self, now: OffsetDateTime) -> Self {
    self.now = now;
    self
  }
}
