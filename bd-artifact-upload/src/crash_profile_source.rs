// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto::protos::client::artifact::CrashProfileSource;
use protobuf::Message as _;
use uuid::Uuid;

/// Encodes this process's source facts, with a fresh run UUID. `wall_ns`/`elapsed_ns` must be a
/// paired REALTIME/BOOTTIME sample.
#[must_use]
pub fn encode_source(
  pid: u32,
  boot_count: u64,
  process_start_elapsed_ns: u64,
  wall_ns: i64,
  elapsed_ns: u64,
) -> Vec<u8> {
  CrashProfileSource {
    run_id: Uuid::new_v4().as_bytes().to_vec(),
    pid,
    boot_count,
    process_start_elapsed_ns,
    wall_ns,
    elapsed_ns,
    ..Default::default()
  }
  .write_to_bytes()
  .unwrap_or_default()
}
