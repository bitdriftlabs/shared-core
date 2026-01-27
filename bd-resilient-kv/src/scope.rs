// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use strum::FromRepr;

/// A state scope that determines the namespace used for storing state. This avoids key collisions
/// and provides logical separation of different types of state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, FromRepr)]
#[repr(u8)]
pub enum Scope {
  FeatureFlagExposure = 1,
  GlobalState         = 2,
  System              = 3,
}

impl Scope {
  /// Convert to a u8 for wire format encoding.
  #[must_use]
  pub const fn to_u8(self) -> u8 {
    self as u8
  }
}
