// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

#[cfg(target_os = "linux")]
mod linux_only;

#[cfg(target_os = "linux")]
fn main() {
  use crate::linux_only::matcher::benches;
  use gungraun::main;

  main!(library_benchmark_groups = benches);
  main();
}

#[cfg(not(target_os = "linux"))]
fn main() {
  println!("workflow benchmarks are only available on Linux");
}
