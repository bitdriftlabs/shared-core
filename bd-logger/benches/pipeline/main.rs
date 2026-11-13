// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Compiled everywhere so it is checked on every platform; only Linux runs it.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod scenarios;

#[cfg(target_os = "linux")]
mod linux_only {
  // `library_benchmark` wraps each benchmark in a function that always returns `Some`.
  #![allow(clippy::unnecessary_wraps)]

  use crate::scenarios::{FieldSource, PipelineSetup, Scenario};
  use gungraun::client_requests::callgrind::toggle_collect;
  use gungraun::{
    Callgrind,
    EntryPoint,
    LibraryBenchmarkConfig,
    library_benchmark,
    library_benchmark_group,
  };

  // Each benchmark measures one stage of every log, so collection is toggled thousands of times.
  // Toggling collection keeps Callgrind's translated code and simulated cache; starting and
  // stopping instrumentation would discard the code each time and make every log start cold,
  // which multiplies the run time about tenfold and inflates the cache misses.

  // Caller-thread admission of every corpus log: provider fields, session, and queueing. The
  // event loop processes each log outside the measurement.
  #[library_benchmark(
    config = LibraryBenchmarkConfig::default()
      .tool(
        Callgrind::with_args(["--collect-atstart=no", "--dump-instr=yes"])
          .entry_point(EntryPoint::None)
      )
  )]
  #[bench::provider_fields(PipelineSetup::new(Scenario::new(FieldSource::Provider, false)))]
  #[bench::persistent_fields(PipelineSetup::new(Scenario::new(FieldSource::Persistent, false)))]
  fn bench_admit(mut setup: PipelineSetup) {
    for index in 0 .. setup.len() {
      setup.prepare(index);
      toggle_collect();
      setup.admit(index);
      toggle_collect();
      setup.process();
    }
  }

  // Event-loop processing of every corpus log: dequeue, normalization, scrub rules and filters,
  // buffer selection, workflows, interning, and buffer writes. Admission is not measured. Where
  // platform fields come from changes processing by about 1%, so only admission measures the
  // provider.
  #[library_benchmark(
    config = LibraryBenchmarkConfig::default()
      .tool(
        Callgrind::with_args(["--collect-atstart=no", "--dump-instr=yes"])
          .entry_point(EntryPoint::None)
      )
  )]
  #[bench::persistent_fields(PipelineSetup::new(Scenario::new(FieldSource::Persistent, false)))]
  #[bench::persistent_fields_interned(PipelineSetup::new(Scenario::new(
    FieldSource::Persistent,
    true
  )))]
  #[bench::persistent_fields_small_messages(PipelineSetup::new(
    Scenario::new(FieldSource::Persistent, false).skipping_large_messages()
  ))]
  fn bench_process(mut setup: PipelineSetup) {
    for index in 0 .. setup.len() {
      setup.prepare(index);
      setup.admit(index);
      toggle_collect();
      setup.process();
      toggle_collect();
    }
  }

  library_benchmark_group!(
    name = benches;
    benchmarks = bench_admit, bench_process
  );
}

#[cfg(target_os = "linux")]
fn main() {
  use crate::linux_only::benches;
  use gungraun::main;

  main!(library_benchmark_groups = benches);
  main();
}

#[cfg(not(target_os = "linux"))]
fn main() {
  println!("logger pipeline benchmarks are only available on Linux");
}
