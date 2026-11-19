// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(target_os = "linux")]
mod linux_only;
// Each benchmark binary uses a different part of the shared scenarios.
#[cfg(target_os = "linux")]
#[allow(dead_code)]
mod scenarios;

#[cfg(target_os = "linux")]
fn main() {
  use crate::linux_only::interning::benches;
  use gungraun::main;

  main!(library_benchmark_groups = benches);
  main();
}

#[cfg(not(target_os = "linux"))]
fn main() {
  println!("field interning benchmarks are only available on Linux");
}
