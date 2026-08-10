// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Lightweight, rate-limited logging helpers.

pub mod rate_limit_log;

pub use rate_limit_log::WarnTracker;
