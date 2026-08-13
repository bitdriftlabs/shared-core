// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::Strategy;

// External integration tests still need the previous best-effort start-new-session behavior so
// they can exercise queueing and re-entrancy flows without relying on a production-only wrapper.
#[allow(clippy::unused_async)] // Preserve this helper's external asynchronous test API.
pub async fn start_new_session(strategy: &Strategy) {
  match strategy.start_new_session_sync() {
    Ok(()) => {},
    Err(e) => {
      log::error!("bitdrift Capture failed to start new session: {e:?}");
      return;
    },
  }

  log::info!("bitdrift Capture started new session");
}
