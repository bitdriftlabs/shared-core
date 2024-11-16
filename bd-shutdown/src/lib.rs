// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::sync::Arc;
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;

// The real graceful shutdown function that listens for SIGTERM or SIGINT.
#[cfg(unix)]
pub async fn real_graceful_shutdown() {
  let mut sigterm_stream = signal(SignalKind::terminate()).unwrap();
  let mut sigint_stream = signal(SignalKind::interrupt()).unwrap();
  tokio::select! {
    _ = sigterm_stream.recv() => {},
    _ = sigint_stream.recv() => {},
  }

  log::info!("received SIGTERM or SIGINT");
}

#[cfg(windows)]
pub async fn real_graceful_shutdown() {
  // Windows doesn't have signals, so we just wait for a key press.
  tokio::signal::ctrl_c().await.unwrap();
  log::info!("received CTRL-C");
}

//
// ComponentStatus
//

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ComponentStatus {
  Running,
  PendingShutdown,
}

//
// ComponentShutdownTriggerHandle
//

/// This is a non-owning handle to the shutdown trigger. It will not block shutdown but will allow
/// creating blocking shutdowns. This allows the intention to block on shutdown to be passed around
/// without actually blocking on shutdown which helps avoid confusing circular references that
/// block shutdown.
#[derive(Clone, Debug)]
pub struct ComponentShutdownTriggerHandle {
  status_tx: Arc<watch::Sender<ComponentStatus>>,
}

impl ComponentShutdownTriggerHandle {
  #[must_use]
  pub fn make_shutdown(&self) -> ComponentShutdown {
    ComponentShutdown {
      status_rx: self.status_tx.subscribe(),
    }
  }
}

//
// ComponentShutdownTrigger
//

// This is used to initiate shutdown for a component.
#[derive(Debug)]
pub struct ComponentShutdownTrigger {
  status_tx: Arc<watch::Sender<ComponentStatus>>,
}

impl Default for ComponentShutdownTrigger {
  fn default() -> Self {
    let (status_tx, _) = watch::channel(ComponentStatus::Running);
    Self {
      status_tx: Arc::new(status_tx),
    }
  }
}

impl ComponentShutdownTrigger {
  #[must_use]
  pub fn make_handle(&self) -> ComponentShutdownTriggerHandle {
    ComponentShutdownTriggerHandle {
      status_tx: self.status_tx.clone(),
    }
  }

  #[must_use]
  pub fn make_shutdown(&self) -> ComponentShutdown {
    ComponentShutdown {
      status_rx: self.status_tx.subscribe(),
    }
  }

  // Shutdown and wait for all components to have dropped. Used in async context.
  pub async fn shutdown(self) {
    self
      .status_tx
      .send_replace(ComponentStatus::PendingShutdown);
    self.status_tx.closed().await;
  }

  // Shutdown and wait for all components to have been dropped. Used in sync context.
  pub fn shutdown_blocking(self) {
    self
      .status_tx
      .send_replace(ComponentStatus::PendingShutdown);
    // For the blocking case, this spin loop is unfortunate, but it allows us to just use a watch
    // for the entire mechanism, since we can use closed() to know when all receivers have dropped.
    // If this becomes an issue we can figure out a way to improve this later.
    while !self.status_tx.is_closed() {
      std::thread::sleep(std::time::Duration::from_millis(100));
    }
  }
}

//
// ComponentShutdown
//

/// A struct to manage a component's shutdown process.
///
/// This includes a receiver to check the status of the component and wait for shutdown. The sender
/// knows everything is shutdown when this drops.
#[derive(Clone, Debug)]
pub struct ComponentShutdown {
  status_rx: watch::Receiver<ComponentStatus>,
}

impl ComponentShutdown {
  /// Returns when the component has been cancelled.
  pub async fn cancelled(&mut self) {
    if *self.status_rx.borrow_and_update() == ComponentStatus::PendingShutdown {
      return;
    }
    let _ignored = self.status_rx.changed().await;
    // TODO(mattklein123): This debug assert fails in at least one test. There shouldn't be any
    // places where the trigger is dropping before the shutdown so we should figure out what is
    // going on here to see if it's a real bug.
    // debug_assert_eq!(*self.status_rx.borrow(), ComponentStatus::PendingShutdown);
  }

  /// Returns the status of the component.
  #[must_use]
  pub fn component_status(&self) -> ComponentStatus {
    *self.status_rx.borrow()
  }
}
