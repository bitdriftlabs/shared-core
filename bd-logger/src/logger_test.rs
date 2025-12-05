// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{Stats, with_thread_local_logger_guard};
use crate::app_version::Repository;
use crate::logger::{Block, CaptureSession};
use crate::{LoggerHandle, async_log_buffer};
use bd_client_stats_store::Collector;
use bd_log_primitives::log_level;
use bd_proto::protos::logging::payload::LogType;
use bd_session::Strategy;
use bd_session::fixed::{self, UUIDCallbacks};
use bd_test_helpers::session::in_memory_store;
use futures_util::poll;
use std::sync::Arc;
use tokio::pin;
use tokio::sync::watch;
use tokio_test::assert_pending;

#[tokio::test]
async fn thread_local_logger_guard() {
  let (log_tx, mut log_rx) = bd_bounded_buffer::channel(1, 100);
  let (state_tx, _state_rx) = bd_bounded_buffer::channel(1, 100);
  let sender = async_log_buffer::Sender::from_parts(log_tx, state_tx);

  let store = in_memory_store();
  let handle = LoggerHandle {
    tx: sender,
    stats: Stats::new(&Collector::default().scope("")),
    session_strategy: Arc::new(Strategy::Fixed(fixed::Strategy::new(
      store.clone(),
      Arc::new(UUIDCallbacks),
    ))),
    device: Arc::new(bd_device::Device::new(store.clone())),
    sdk_version: "1.0.0".into(),
    app_version_repo: Repository::new(store),
    sleep_mode_active: watch::channel(false).0,
  };

  with_thread_local_logger_guard(|| {
    handle.log(
      log_level::INFO,
      LogType::NORMAL,
      "msg".into(),
      [].into(),
      [].into(),
      None,
      Block::No,
      &CaptureSession::default(),
    );
  });

  let recv = log_rx.recv();
  pin!(recv);
  assert_pending!(poll!(recv));
}
