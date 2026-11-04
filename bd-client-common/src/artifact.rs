// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Shared conventions for client artifact uploads.

/// Hard protocol ceiling; SDK runtime policy may set a lower admission limit.
pub const MAX_CHUNKED_ARTIFACT_LOGICAL_BYTES: u32 = 256 * 1024 * 1024;

/// Artifact type ID used for crash reports uploaded by the client.
pub const CLIENT_REPORT_ARTIFACT_TYPE_ID: &str = "client_report";

/// Artifact type ID used for Android ANR/OOM profiles linked to crash reports.
pub const CRASH_PROFILE_ARTIFACT_TYPE_ID: &str = "crash_profile";

/// Maximum number of parent report IDs a single crash-profile upload may name.
pub const CRASH_PROFILE_MAX_REPORT_IDS: usize = 16;

/// Artifact type ID used for uploaded versioned state snapshots.
pub const STATE_SNAPSHOT_ARTIFACT_TYPE_ID: &str = "state_snapshot";
