// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::Strategy;

// External integration tests still need the previous best-effort start-new-session behavior so
// they can exercise queueing and re-entrancy flows without relying on a production-only wrapper.
pub async fn start_new_session(strategy: &Strategy) {
  let prepared = match strategy.prepare_start_new_session() {
    Ok(prepared) => prepared,
    Err(e) => {
      log::error!("bitdrift Capture failed to start new session: {e:?}");
      return;
    },
  };

  let session_id = prepared.current_session_id().to_string();
  let callback = strategy.persist_prepared(prepared).await;
  strategy.run_prepared_callback(callback);
  log::info!("bitdrift Capture started new session: {session_id:?}");
}
