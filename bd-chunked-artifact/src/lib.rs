// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

// Hard protocol ceiling; SDK runtime policy may set a lower admission limit.
pub const MAX_LOGICAL_BYTES: u32 = 256 * 1024 * 1024;
