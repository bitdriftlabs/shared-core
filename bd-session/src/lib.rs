// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod activity_based;
pub mod fixed;

pub use bd_key_value::Store;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

//
// Strategy
//

pub enum Strategy {
  Fixed(fixed::Strategy),
  ActivityBased(activity_based::Strategy),
}

impl Strategy {
  pub fn session_id(&self) -> String {
    match self {
      Self::Fixed(strategy) => strategy.session_id(),
      Self::ActivityBased(strategy) => strategy.session_id(),
    }
  }

  pub fn start_new_session(&self) {
    let new_session_id = match self {
      Self::Fixed(strategy) => strategy.start_new_session(),
      Self::ActivityBased(strategy) => Ok(strategy.start_new_session()),
    };

    match new_session_id {
      Ok(new_session_id) => log::info!("bitdrift Capture started new session: {new_session_id:?}"),
      Err(e) => log::error!("bitdrift Capture failed to start new session: {e:?}"),
    }
  }

  pub fn flush(&self) {
    if let Self::ActivityBased(strategy) = self {
      strategy.flush();
    }
  }

  /// The last active session ID from the previous SDK run.
  pub fn previous_process_session_id(&self) -> Option<String> {
    match self {
      Self::Fixed(strategy) => strategy.previous_process_session_id(),
      Self::ActivityBased(strategy) => strategy.previous_process_session_id(),
    }
  }

  /// Pretty name of the strategy.
  pub const fn type_name(&self) -> &'static str {
    match self {
      Self::Fixed(_) => "fixed",
      Self::ActivityBased(_) => "activity_based",
    }
  }
}
