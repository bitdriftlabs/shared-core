// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::start_server;
use std::net::TcpListener;
use std::time::{Duration, Instant};

#[test]
fn dropping_server_releases_listener() {
  let server = start_server(false, None);
  let port = server.port;

  drop(server);

  let deadline = Instant::now() + Duration::from_secs(2);
  loop {
    match TcpListener::bind(("127.0.0.1", port)) {
      Ok(listener) => {
        drop(listener);
        return;
      },
      Err(error) if Instant::now() < deadline => {
        log::debug!("waiting for test server listener on port {port} to close: {error}");
        std::thread::sleep(Duration::from_millis(10));
      },
      Err(error) => panic!("listener on port {port} was not released after server drop: {error}"),
    }
  }
}
