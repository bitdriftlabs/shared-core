// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

source_info_gated_mod! {
  api => "with_source/api.rs",
  artifact => "with_source/artifact.rs",
  feature_flag => "with_source/feature_flag.rs",
  key_value => "with_source/key_value.rs",
  matcher => "with_source/matcher.rs",
  metric => "with_source/metric.rs",
  runtime => "with_source/runtime.rs",
}

use super::bdtail::bdtail_config;
use super::config::v1::config;
use super::filter::filter;
use super::logging::payload;
use super::workflow::workflow;
use bd_pgv::generated::protos::validate;
