// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::PlatformMutexGuard;
use static_assertions::assert_not_impl_any;

// The iOS implementation must unlock on the thread that acquired the unfair lock.
assert_not_impl_any!(PlatformMutexGuard<'static, ()>: Send);
