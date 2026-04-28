// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::active_tokio_runtime_flavor;
use tokio::runtime::{Builder, RuntimeFlavor};

#[test]
fn detects_current_thread_runtime_flavor() {
  let runtime = Builder::new_current_thread().enable_all().build().unwrap();

  let flavor = runtime.block_on(async { active_tokio_runtime_flavor().unwrap() });

  assert_eq!(RuntimeFlavor::CurrentThread, flavor);
}

#[test]
fn detects_multi_thread_runtime_flavor() {
  let runtime = Builder::new_multi_thread()
    .worker_threads(2)
    .enable_all()
    .build()
    .unwrap();

  let flavor = runtime.block_on(async { active_tokio_runtime_flavor().unwrap() });

  assert_eq!(RuntimeFlavor::MultiThread, flavor);
}

#[test]
fn rejects_missing_tokio_runtime() {
  assert!(active_tokio_runtime_flavor().is_err());
}
