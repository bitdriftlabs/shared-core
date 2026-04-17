// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(not(feature = "with-source-info"))]
pub mod api;
#[cfg(feature = "with-source-info")]
#[path = "with_source/api.rs"]
pub mod api;

#[cfg(not(feature = "with-source-info"))]
pub mod artifact;
#[cfg(feature = "with-source-info")]
#[path = "with_source/artifact.rs"]
pub mod artifact;

#[cfg(not(feature = "with-source-info"))]
pub mod feature_flag;
#[cfg(feature = "with-source-info")]
#[path = "with_source/feature_flag.rs"]
pub mod feature_flag;

#[cfg(not(feature = "with-source-info"))]
pub mod key_value;
#[cfg(feature = "with-source-info")]
#[path = "with_source/key_value.rs"]
pub mod key_value;

#[cfg(not(feature = "with-source-info"))]
pub mod matcher;
#[cfg(feature = "with-source-info")]
#[path = "with_source/matcher.rs"]
pub mod matcher;

#[cfg(not(feature = "with-source-info"))]
pub mod metric;
#[cfg(feature = "with-source-info")]
#[path = "with_source/metric.rs"]
pub mod metric;

#[cfg(not(feature = "with-source-info"))]
pub mod runtime;
#[cfg(feature = "with-source-info")]
#[path = "with_source/runtime.rs"]
pub mod runtime;

use super::bdtail::bdtail_config;
use super::config::v1::config;
use super::filter::filter;
use super::logging::payload;
use super::workflow::workflow;
use bd_pgv::generated::protos::validate;
