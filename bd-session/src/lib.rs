// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod activity_based;
pub mod fixed;

use bd_client_stats_store::{Collector, Counter};
pub use bd_key_value::Store;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

//
// Strategy
//

enum Inner {
  Fixed(fixed::Strategy),
  ActivityBased(activity_based::Strategy),
}

pub struct Strategy {
  inner: Inner,
  new_session: Option<Counter>,
}

impl Strategy {
  pub fn new_fixed(fixed: fixed::Strategy) -> Self {
    Self {
      inner: Inner::Fixed(fixed),
      new_session: None,
    }
  }

  pub fn new_activity_based(activity_based: activity_based::Strategy) -> Self {
    Self {
      inner: Inner::ActivityBased(activity_based),
      new_session: None,
    }
  }

  pub fn initialize_stats(&mut self, collector: &Collector) {
    self.new_session = Some(collector.scope("session").counter("new"));
  }

  pub fn increment_new_session(&self) {
    debug_assert!(
      self.new_session.is_some(),
      "Cannot increment new session counter when it is not initialized"
    );

    if let Some(counter) = &self.new_session {
      counter.inc();
    }
  }

  pub fn session_id(&self) -> String {
    let session_id = match &self.inner {
      Inner::Fixed(strategy) => strategy.session_id(),
      Inner::ActivityBased(strategy) => strategy.session_id(),
    };

    match session_id {
      SessionId::New(id) => {
        self.increment_new_session();
        id
      },
      SessionId::Existing(id) => id,
    }
  }

  pub fn start_new_session(&self) {
    let new_session_id = match &self.inner {
      Inner::Fixed(strategy) => strategy.start_new_session(),
      Inner::ActivityBased(strategy) => Ok(strategy.start_new_session()),
    };

    self.increment_new_session();

    match new_session_id {
      Ok(new_session_id) => log::info!("bitdrift Capture started new session: {new_session_id:?}"),
      Err(e) => log::error!("bitdrift Capture failed to start new session: {e:?}"),
    }
  }

  pub fn flush(&self) {
    if let Inner::ActivityBased(strategy) = &self.inner {
      strategy.flush();
    }
  }

  /// The last active session ID from the previous SDK run.
  pub fn previous_process_session_id(&self) -> Option<String> {
    match &self.inner {
      Inner::Fixed(strategy) => strategy.previous_process_session_id(),
      Inner::ActivityBased(strategy) => strategy.previous_process_session_id(),
    }
  }

  /// Pretty name of the strategy.
  pub const fn type_name(&self) -> &'static str {
    match &self.inner {
      Inner::Fixed(_) => "fixed",
      Inner::ActivityBased(_) => "activity_based",
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SessionId {
  New(String),
  Existing(String),
}

impl SessionId {
  #[cfg(test)]
  pub fn into_inner(self) -> String {
    match self {
      Self::New(id) | Self::Existing(id) => id,
    }
  }
}
