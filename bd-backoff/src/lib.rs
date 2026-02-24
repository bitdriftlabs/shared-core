// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

pub mod backoff;
pub mod clock;
pub mod default;
pub mod exponential;

pub use crate::backoff::{FiniteBackoff, InfiniteBackoff};
pub use crate::clock::{Clock, SystemClock};
pub use crate::exponential::{
  ExponentialBackoffBuilder,
  ExponentialBackoffFinite,
  ExponentialBackoffInfinite,
  Finite,
  Infinite,
};

pub type ExponentialBackoff = exponential::ExponentialBackoffInfinite<SystemClock>;
