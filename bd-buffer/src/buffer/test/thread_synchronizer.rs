// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use parking_lot::{Condvar, Mutex};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default)]
struct EntryLockedData {
  wait_on: bool,
  at_barrier: bool,
  signaled: bool,
}

#[derive(Default)]
struct Entry {
  at_barrier_condition: Condvar,
  signal_condition: Condvar,
  locked_data: Mutex<EntryLockedData>,
}

#[derive(Default)]
pub struct ThreadSynchronizer {
  data: Mutex<HashMap<String, Arc<Entry>>>,
}

impl ThreadSynchronizer {
  fn entry(&self, name: &str) -> Arc<Entry> {
    self
      .data
      .lock()
      .entry(name.to_string())
      .or_default()
      .clone()
  }

  // This is the only API that should generally be called from production code. It introduces a
  // "sync point" that test code can then use to force blocking, thread barriers, etc. The
  // sync_point() will do nothing unless it has been registered to block via wait_on().
  pub fn sync_point(&self, name: &str) {
    let entry = self.entry(name);
    let mut locked_data = entry.locked_data.lock();

    // See if we are ignoring waits. If so, just return.
    if !locked_data.wait_on {
      log::debug!("sync point {}: ignoring", name);
      return;
    }
    locked_data.wait_on = false;

    // See if we are already signaled. If so, just clear signaled and return.
    if locked_data.signaled {
      log::debug!("sync point {}: already signaled", name);
      locked_data.signaled = false;
      return;
    }

    // Now signal any barrier waiters.
    locked_data.at_barrier = true;
    entry.at_barrier_condition.notify_one();

    // Now wait to be signaled.
    log::debug!("blocking on sync point {}", name);
    entry
      .signal_condition
      .wait_while(&mut locked_data, |locked_data| !locked_data.signaled);
    log::debug!("done blocking for sync point {}", name);

    // Clear the barrier and signaled before unlocking and returning.
    assert!(locked_data.at_barrier);
    locked_data.at_barrier = false;
    assert!(locked_data.signaled);
    locked_data.signaled = false;
  }

  // The next time the sync point registered with name is invoked via sync_point(), the calling code
  // will block until signaled. Note that this is a one-shot operation and the sync point's wait
  // status will be cleared.
  pub fn wait_on(&self, name: &str) {
    let entry = self.entry(name);
    let mut locked_data = entry.locked_data.lock();
    log::debug!("waiting on next {}", name);
    assert!(!locked_data.wait_on);
    locked_data.wait_on = true;
  }

  // This call will block until the next time the sync point registered with name is invoked. The
  // name must have been previously registered for blocking via wait_on(). The typical test pattern
  // is to have a thread arrive at a sync point, block, and then release a test thread which
  // continues test execution, eventually calling signal() to release the other thread.
  pub fn barrier_on(&self, name: &str) {
    let entry = self.entry(name);
    let mut locked_data = entry.locked_data.lock();
    log::debug!("barrier on {}", name);
    entry
      .at_barrier_condition
      .wait_while(&mut locked_data, |locked_data| !locked_data.at_barrier);
    log::debug!("barrier complete {}", name);
  }

  // Signal an event such that a thread that is blocked within sync_point() will now proceed.
  pub fn signal(&self, name: &str) {
    let entry = self.entry(name);
    let mut locked_data = entry.locked_data.lock();
    assert!(!locked_data.signaled);
    log::debug!("signaling {}", name);
    locked_data.signaled = true;
    entry.signal_condition.notify_one();
  }
}
