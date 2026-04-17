// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(not(feature = "with-source-info"))]
pub mod log_matcher;
#[cfg(feature = "with-source-info")]
#[path = "with_source/log_matcher.rs"]
pub mod log_matcher;

use super::state::{matcher, scope};
use super::value_matcher::value_matcher;
use bd_pgv::generated::protos::validate;
