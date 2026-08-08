// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::warn_every;
use bd_time::TimeDurationExt;
use time::ext::NumericalDuration;

fn test_warn() {
  warn_every!(1.seconds(), "{}", "function");
}

fn test_warn_captured_arg() {
  let err = "boom";
  warn_every!(1.seconds(), "hello: {err}");
}

fn test_warn_runtime_string() {
  let message = "runtime string";
  warn_every!(1.seconds(), "{message}");
}

#[tokio::test(start_paused = true)]
async fn rate_limit_log() {
  // These should both warn as they are different logs.
  warn_every!(1.seconds(), "{}", "hello");
  warn_every!(1.seconds(), "{}", "world");

  // This should warn and then debug as it's a single log.
  test_warn();
  test_warn();
  test_warn_captured_arg();
  test_warn_runtime_string();

  // Should output another debug.
  500.milliseconds().sleep().await;
  test_warn();

  // Should output another warn.
  501.milliseconds().sleep().await;
  test_warn();
}
