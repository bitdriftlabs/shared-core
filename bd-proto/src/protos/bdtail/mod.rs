// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

source_info_gated_mod! {
  bdtail_api => "with_source/bdtail_api.rs",
  bdtail_config => "with_source/bdtail_config.rs",
}

use super::log_matcher::log_matcher;
use super::logging::payload;
use bd_pgv::generated::protos::validate;
