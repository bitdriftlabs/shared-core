// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{PersistenceWorker, Strategy, StrategyWithWorker, configuration};
use std::path::Path;
use std::sync::Arc;

/// Creates a no-timeout session configuration for integration tests.
pub fn no_timeout(sdk_directory: impl AsRef<Path>) -> StrategyWithWorker {
  Strategy::configuration(
    sdk_directory,
    None,
    None,
    Arc::new(configuration::NoopCallbacks),
    Arc::new(bd_time::SystemTimeProvider),
  )
}

/// Runs a test-only worker around an explicit persistence barrier.
pub async fn flush(strategy: Arc<Strategy>, worker: PersistenceWorker) {
  let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
  let flusher = tokio::spawn(worker.run(
    async move {
      let _ignored = shutdown_rx.await;
    },
    || {},
  ));

  strategy.flush().await;
  let _ignored = shutdown_tx.send(());
  let _ignored = flusher.await;
}
