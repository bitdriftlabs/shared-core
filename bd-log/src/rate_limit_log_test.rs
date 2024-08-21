// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::warn_every;
use bd_time::TimeDurationExt;
use time::ext::NumericalDuration;

fn test_warn() {
  warn_every!(1.seconds(), "{}", "function");
}

#[tokio::test(start_paused = true)]
async fn rate_limit_log() {
  // These should both warn as they are different logs.
  warn_every!(1.seconds(), "{}", "hello");
  warn_every!(1.seconds(), "{}", "world");

  // This should warn and then debug as it's a single log.
  test_warn();
  test_warn();

  // Should output another debug.
  500.milliseconds().sleep().await;
  test_warn();

  // Should output another warn.
  501.milliseconds().sleep().await;
  test_warn();
}
