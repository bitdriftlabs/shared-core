// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

pub mod log_matcher;

use super::state::{matcher, scope};
use super::value_matcher::value_matcher;
use bd_pgv::generated::protos::validate;
