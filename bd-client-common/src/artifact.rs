// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Shared conventions for client artifact uploads.

/// Artifact type ID used for crash reports uploaded by the client.
pub const CLIENT_REPORT_ARTIFACT_TYPE_ID: &str = "client_report";

/// Artifact type ID used for uploaded versioned state snapshots.
pub const STATE_SNAPSHOT_ARTIFACT_TYPE_ID: &str = "state_snapshot";
