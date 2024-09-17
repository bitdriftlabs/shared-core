// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./listener_test.rs"]
mod listener_test;

use bd_runtime::runtime::platform_events::ListenerEnabledFlag;
use bd_runtime::runtime::{BoolWatch, ConfigLoader};
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
use std::sync::Arc;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}


//
// ListenerTarget
//

pub trait ListenerTarget {
  fn start(&self);
  fn stop(&self);
}

//
// Listener
//

/// Responsible for starting and stopping the passed events listener target based on
/// a runtime-controlled flag.
pub struct Listener {
  target: Box<dyn ListenerTarget + Send + Sync>,

  // Whether the events listener has seen at least one update to the value of the `is_enabled`
  // flag.
  has_seen_is_enabled_flag_update: bool,

  is_enabled_flag: BoolWatch<ListenerEnabledFlag>,
}

impl Listener {
  pub fn new(
    target: Box<dyn ListenerTarget + Send + Sync>,
    runtime_loader: &Arc<ConfigLoader>,
  ) -> Self {
    let is_enabled_flag: BoolWatch<ListenerEnabledFlag> = runtime_loader.register_watch().unwrap();

    Self {
      target,
      has_seen_is_enabled_flag_update: false,
      is_enabled_flag,
    }
  }

  pub async fn run(&mut self) {
    let shutdown_trigger = ComponentShutdownTrigger::default();
    self
      .run_with_shutdown(shutdown_trigger.make_shutdown())
      .await;
  }

  pub async fn run_with_shutdown(&mut self, mut shutdown: ComponentShutdown) {
    let local_shutdown = shutdown.cancelled();
    tokio::pin!(local_shutdown);

    loop {
      tokio::select! {
        _ = self.is_enabled_flag.changed() => {
          let new_is_enabled = self.is_enabled_flag.read();
          if new_is_enabled {
            log::debug!("events listener start");
            self.target.start();
          } else if self.has_seen_is_enabled_flag_update {
            // Stop only if event listener was previously started.
            log::debug!("events listener stop");
            self.target.stop();
          }

          self.has_seen_is_enabled_flag_update = true;
        }
        () = &mut local_shutdown => {
          break;
        }
      }
    }
  }
}
