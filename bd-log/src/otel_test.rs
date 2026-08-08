// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

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
