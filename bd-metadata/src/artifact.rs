// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Shared metadata conventions for client artifacts.

/// Content type for raw Perfetto traces uploaded by commands, workflows and crash profiling.
pub const PERFETTO_CONTENT_TYPE: &str = "application/vnd.bitdrift.perfetto-trace";
