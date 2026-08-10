// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

pub mod feature_flags;

use bd_panic::PanicType;

pub fn test_global_init() {
  bd_panic::default(PanicType::ForceAbort);
  bd_log::SwapLogger::initialize();
}
