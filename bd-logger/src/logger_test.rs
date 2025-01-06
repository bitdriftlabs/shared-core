// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{with_thread_local_logger_guard, Stats};
use crate::app_version::Repository;
use crate::{bounded_buffer, LoggerHandle};
use bd_client_stats_store::Collector;
use bd_key_value::Store;
use bd_log_primitives::log_level;
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_session::fixed::{self, UUIDCallbacks};
use bd_session::Strategy;
use bd_test_helpers::session::InMemoryStorage;
use futures_util::poll;
use std::sync::Arc;
use tokio::pin;
use tokio_test::assert_pending;

#[tokio::test]
async fn thread_local_logger_guard() {
  let (tx, mut rx) = bounded_buffer::channel(1, 100);

  let store = Arc::new(Store::new(Box::<InMemoryStorage>::default()));
  let handle = LoggerHandle {
    tx,
    stats: Stats::new(&Collector::default().scope("")),
    session_strategy: Arc::new(Strategy::Fixed(fixed::Strategy::new(
      store.clone(),
      Arc::new(UUIDCallbacks),
    ))),
    device: Arc::new(bd_device::Device::new(store.clone())),
    sdk_version: "1.0.0".into(),
    app_version_repo: Repository::new(store),
  };

  with_thread_local_logger_guard(|| {
    handle.log(
      log_level::INFO,
      LogType::Normal,
      "msg".into(),
      vec![],
      vec![],
      None,
      false,
    );
  });

  let recv = rx.recv();
  pin!(recv);
  assert_pending!(poll!(recv));
}
