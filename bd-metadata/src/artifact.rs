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
pub const MAX_ARTIFACT_ENCODED_BYTES: u32 = MAX_ARTIFACT_LOGICAL_BYTES + ARTIFACT_CHUNK_BYTES;
pub const MAX_ARTIFACT_CHUNKS: u32 = MAX_ARTIFACT_ENCODED_BYTES / ARTIFACT_CHUNK_BYTES;
pub const MAX_ARTIFACT_MANIFEST_BYTES: u32 = 64 * 1024;
pub const MAX_ARTIFACT_ORIGIN_BYTES: usize = 8 * 1024;
/// Maximum serialized metadata (`state_metadata` + `feature_flags`) attached to a final chunk.
/// Leaves room for a full 1 MiB encoded chunk and gRPC framing inside the 2 MiB request cap.
pub const MAX_ARTIFACT_ATTACHED_METADATA_BYTES: u32 = 960 * 1024;
/// Identity/policy `state_metadata` keys sent on `Begin` (full scrubbed metadata travels on the
/// final chunk and is persisted in the canonical completed artifact envelope).
pub const ARTIFACT_BEGIN_METADATA_KEYS: &[&str] = &[
  "app_id",
  "app_version",
  "device_id",
  "platform",
  "_app_version_code",
  "_build_number",
];

/// Maximum encoded size reserved and accepted for one artifact's continuous zlib stream.
///
/// This is zlib's `compressBound()` formula: the shifts divide by 4096, 16384 and 33554432.
/// Those input-dependent allowances and the fixed 13 bytes cover worst-case DEFLATE block
/// overhead plus stream framing, even when compression expands incompressible input. Reserving
/// only the logical size plus a header/trailer would underestimate that growth.
///
/// The bound assumes the encoder's default window/memory settings and no intermediate codec
/// flushes. Draining its output into fragment files does not flush the codec. Changing those
/// settings or adding flushes requires revisiting this bound on both the SDK and server.
///
/// Keep this calculation constant and free of codec dependencies: the SDK uses it before
/// compression for disk admission, and the server uses it to validate manifest geometry. Callers
/// enforce the protocol's 256 MiB logical ceiling; arithmetic cannot overflow within that domain.
#[must_use]
pub const fn artifact_encoded_bound(logical_bytes: u64) -> u64 {
  if logical_bytes == 0 {
    // Empty command files use RAW with no fragments, not an encoded empty zlib stream.
    0
  } else {
    logical_bytes + (logical_bytes >> 12) + (logical_bytes >> 14) + (logical_bytes >> 25) + 13
  }
}

/// Content type for raw Perfetto traces uploaded by commands, workflows and crash profiling.
pub const PERFETTO_CONTENT_TYPE: &str = "application/vnd.bitdrift.perfetto-trace";
