// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Callgrind instruction counts for evaluating every regex matcher in the large corpus's workflow
//! configuration against its logs. `all` uses every log; `typical` leaves out logs whose message
//! is over 10 KiB, since a few very large messages hold most of the corpus's message bytes.

#[cfg(target_os = "linux")]
mod linux_only {
  use bd_workflow_bench::{RegexMatcherCorpus, default_fixture_paths};
  use gungraun::{
    Callgrind,
    EntryPoint,
    LibraryBenchmarkConfig,
    library_benchmark,
    library_benchmark_group,
  };
  use std::hint::black_box;

  fn load(max_message_bytes: Option<usize>) -> RegexMatcherCorpus {
    let fixture = default_fixture_paths()
      .unwrap()
      .into_iter()
      .find(|fixture| fixture.name == "large")
      .unwrap();
    RegexMatcherCorpus::load(&fixture.config_path, &fixture.logs_path, max_message_bytes).unwrap()
  }

  #[library_benchmark(
    config = LibraryBenchmarkConfig::default()
      // The fixture paths are resolved from the crate directory.
      .pass_through_env("CARGO_MANIFEST_DIR")
      .tool(Callgrind::with_args(["--instr-atstart=no", "--dump-instr=yes"])
        .entry_point(EntryPoint::None)
      ))]
  #[bench::all(None)]
  #[bench::typical(Some(10 * 1024))]
  fn regex_matchers(max_message_bytes: Option<usize>) {
    let corpus = load(max_message_bytes);
    // Compile any lazily built regex state outside the measurement.
    black_box(corpus.evaluate_all());

    gungraun::client_requests::callgrind::start_instrumentation();
    black_box(corpus.evaluate_all());
    gungraun::client_requests::callgrind::stop_instrumentation();
  }

  library_benchmark_group!(
    name = benches;
    benchmarks = regex_matchers
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
  println!("regex matcher Callgrind benchmarks are only available on Linux");
}
