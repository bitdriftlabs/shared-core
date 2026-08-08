// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use time::Duration;

pub trait FiniteBackoff {
  fn reset(&mut self) {}
  fn next_backoff(&mut self) -> Option<Duration>;
}

pub trait InfiniteBackoff {
  fn reset(&mut self) {}
  fn next_backoff(&mut self) -> Duration;
}
