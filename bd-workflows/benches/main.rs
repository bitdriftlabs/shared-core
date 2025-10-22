// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

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
