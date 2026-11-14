// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Shared metadata conventions for client artifacts.

/// Hard logical-file ceiling; admission policy may set a lower limit.
pub const MAX_ARTIFACT_LOGICAL_BYTES: u32 = 256 * 1024 * 1024;
pub const ARTIFACT_CHUNK_BYTES: u32 = 1024 * 1024;
pub const MAX_ARTIFACT_CHUNKS: u32 = MAX_ARTIFACT_LOGICAL_BYTES / ARTIFACT_CHUNK_BYTES;
pub const MAX_ARTIFACT_MANIFEST_BYTES: u32 = 64 * 1024;
pub const MAX_ARTIFACT_ORIGIN_BYTES: usize = 8 * 1024;

/// Content type for raw Perfetto traces uploaded by commands, workflows and crash profiling.
pub const PERFETTO_CONTENT_TYPE: &str = "application/vnd.bitdrift.perfetto-trace";
