// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::collections::BTreeMap;
use time::OffsetDateTime;

pub struct TestLog {
  pub message: String,
  pub occurred_at: OffsetDateTime,
  pub now: OffsetDateTime,
  pub tags: BTreeMap<String, String>,
  pub session: Option<String>,
}

impl TestLog {
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

  pub fn with_occurred_at(mut self, occurred_at: OffsetDateTime) -> Self {
    self.occurred_at = occurred_at;
    self
  }

  pub fn with_tags(mut self, tags: BTreeMap<String, String>) -> Self {
    self.tags = tags;
    self
  }

  pub fn with_session(mut self, session: &str) -> Self {
    self.session = Some(session.to_string());
    self
  }

  pub fn with_now(mut self, now: OffsetDateTime) -> Self {
    self.now = now;
    self
  }
}
