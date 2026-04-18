// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod admin;
pub mod api;
pub mod api_ootb_fields;
pub mod chart_id;
pub mod chart_metadata;
pub mod common;
pub mod crash;
pub mod dashboards;
pub mod debug_files;
pub mod explorations;
pub mod hydration;
pub mod info;
pub mod issues;
pub mod logs;
pub mod platform;
pub mod time_series;
pub mod timeline;
pub mod workflow;
pub mod workflow_metadata;

// Re-export external proto dependencies referenced by generated code.
pub use super::logging::payload;
pub use super::workflow::save_field;
pub use bd_pgv::generated::protos::validate;
