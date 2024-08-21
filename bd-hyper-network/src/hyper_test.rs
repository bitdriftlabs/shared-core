// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::HyperNetwork;
use assert_matches::assert_matches;
use bd_api::PlatformNetworkManager;
use bd_shutdown::ComponentShutdownTrigger;
use std::collections::HashMap;


#[tokio::test]
async fn connect_failure() {
  let shutdown = ComponentShutdownTrigger::default();
  let (network, handle) = HyperNetwork::new("http://localhost:1234", shutdown.make_shutdown());

  tokio::spawn(network.start());

  let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);
  handle
    .start_stream(event_tx, &(), &HashMap::new())
    .await
    .unwrap();

  assert_matches!(
    event_rx.recv().await,
    Some(bd_api::StreamEvent::StreamClosed(_))
  );
}
