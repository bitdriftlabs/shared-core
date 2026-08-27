// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::scenarios::{
  AdmissionSetup,
  ingress_and_admit,
  ingress_and_insertion_setup,
  insertion_setup,
  insertion_with_capacity_setup,
  multi_lane_eviction_setup,
  multiple_victims_setup,
  single_victim_setup,
};
use bd_event_buffer::EventBuffer;
use gungraun::{
  Callgrind,
  EntryPoint,
  LibraryBenchmarkConfig,
  library_benchmark,
  library_benchmark_group,
};
use std::hint::black_box;

#[library_benchmark(
  config = LibraryBenchmarkConfig::default()
    .tool(
      Callgrind::with_args(["--instr-atstart=no", "--dump-instr=yes"])
        .entry_point(EntryPoint::None)
    )
)]
#[bench::insert_empty(insertion_setup())]
#[bench::insert_with_capacity(insertion_with_capacity_setup())]
#[bench::single_victim(single_victim_setup())]
#[bench::multiple_victims(multiple_victims_setup())]
#[bench::multi_lane_eviction(multi_lane_eviction_setup())]
fn bench_admission(setup: AdmissionSetup) {
  gungraun::client_requests::callgrind::start_instrumentation();
  let outcome = setup.admit();
  gungraun::client_requests::callgrind::stop_instrumentation();
  black_box(outcome);
}

// Includes creation of the entry so a future boxed variant accounts for its extra allocation.
#[library_benchmark(
  config = LibraryBenchmarkConfig::default()
    .tool(
      Callgrind::with_args(["--instr-atstart=no", "--dump-instr=yes"])
        .entry_point(EntryPoint::None)
    )
)]
#[bench::log_1_kib(ingress_and_insertion_setup(1024))]
fn bench_ingress_and_admission(buffer: EventBuffer) {
  gungraun::client_requests::callgrind::start_instrumentation();
  let outcome = ingress_and_admit(&buffer, 1024);
  gungraun::client_requests::callgrind::stop_instrumentation();
  black_box(outcome);
}

library_benchmark_group!(
  name = benches;
  benchmarks = bench_admission, bench_ingress_and_admission
);
