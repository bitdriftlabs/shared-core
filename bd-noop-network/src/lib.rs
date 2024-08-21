// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_api::{PlatformNetworkManager, PlatformNetworkStream, StreamEventSender};
use std::collections::HashMap;

/// A noop implementation of the platform network that connects to nothing. This can be used to
/// initialize the logger without it attempting to connect to the backend.
pub struct NoopNetwork;

#[async_trait::async_trait]
impl<T> PlatformNetworkManager<T> for NoopNetwork {
  async fn start_stream(
    &self,
    _event_tx: StreamEventSender,
    _runtime: &T,
    _headers: &HashMap<&str, &str>,
  ) -> anyhow::Result<Box<dyn PlatformNetworkStream>> {
    Ok(Box::new(Self {}))
  }
}

#[async_trait::async_trait]
impl PlatformNetworkStream for NoopNetwork {
  async fn send_data(&mut self, _data: &[u8]) -> anyhow::Result<()> {
    Ok(())
  }
}
