// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

#[cfg(test)]
#[path = "./listener_test.rs"]
mod listener_test;

use bd_runtime::runtime::platform_events::ListenerEnabledFlag;
use bd_runtime::runtime::{BoolWatch, ConfigLoader};
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
use std::sync::Arc;

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn test_global_init() {
  bd_test_helpers_core::test_global_init();
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

  // Tracks whether the target is currently running so runtime updates only transition it when
  // needed.
  target_is_active: bool,

  is_enabled_flag: BoolWatch<ListenerEnabledFlag>,
}

impl Listener {
  pub fn new(
    target: Box<dyn ListenerTarget + Send + Sync>,
    runtime_loader: &Arc<ConfigLoader>,
  ) -> Self {
    let is_enabled_flag = ListenerEnabledFlag::register(runtime_loader);

    Self {
      target,
      target_is_active: false,
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
    // The listener's default is enabled, so start it even when no runtime update has arrived.
    if *self.is_enabled_flag.read_mark_update() {
      log::debug!("events listener start");
      self.target.start();
      self.target_is_active = true;
    }

    let local_shutdown = shutdown.cancelled();
    tokio::pin!(local_shutdown);

    loop {
      tokio::select! {
        _ = self.is_enabled_flag.changed() => {
          let new_is_enabled = *self.is_enabled_flag.read();
          if new_is_enabled && !self.target_is_active {
            log::debug!("events listener start");
            self.target.start();
            self.target_is_active = true;
          } else if !new_is_enabled && self.target_is_active {
            log::debug!("events listener stop");
            self.target.stop();
            self.target_is_active = false;
          }
        }
        () = &mut local_shutdown => {
          break;
        }
      }
    }
  }
}
