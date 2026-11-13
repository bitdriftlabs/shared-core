// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// `library_benchmark` wraps each benchmark in a function that always returns `Some`.
#![allow(clippy::unnecessary_wraps)]

use crate::scenarios::{DecideSetup, InternSetup, Mode, ResolveSetup, Scenario, WriteSetup};
use bd_log_primitives::Log;
use gungraun::client_requests::callgrind::{start_instrumentation, stop_instrumentation};
use gungraun::{
  Callgrind,
  EntryPoint,
  LibraryBenchmarkConfig,
  library_benchmark,
  library_benchmark_group,
};
use std::hint::black_box;

// The whole per-log write: `intern` (when on), `set_interned`, then `compute_size` and
// `serialize_to_bytes` for each buffer. Building the log and the interner, and dropping them, are
// not measured.
#[library_benchmark(
  config = LibraryBenchmarkConfig::default()
    .tool(
      Callgrind::with_args(["--instr-atstart=no", "--dump-instr=yes"])
        .entry_point(EntryPoint::None)
    )
)]
#[bench::off_uncompressed_1_buffer(WriteSetup::with_log(Scenario::new(Mode::Off, false, 1)))]
#[bench::unchanged_uncompressed_1_buffer(WriteSetup::with_log(Scenario::new(
  Mode::Unchanged,
  false,
  1
)))]
#[bench::cleared_uncompressed_1_buffer(WriteSetup::with_log(Scenario::new(
  Mode::Cleared,
  false,
  1
)))]
#[bench::off_compressed_1_buffer(WriteSetup::with_log(Scenario::new(Mode::Off, true, 1)))]
#[bench::unchanged_compressed_1_buffer(WriteSetup::with_log(Scenario::new(
  Mode::Unchanged,
  true,
  1
)))]
#[bench::cleared_compressed_1_buffer(WriteSetup::with_log(Scenario::new(Mode::Cleared, true, 1)))]
#[bench::off_uncompressed_3_buffers(WriteSetup::with_log(Scenario::new(Mode::Off, false, 3)))]
#[bench::unchanged_uncompressed_3_buffers(WriteSetup::with_log(Scenario::new(
  Mode::Unchanged,
  false,
  3
)))]
fn bench_write((mut setup, log): (WriteSetup, Log)) {
  start_instrumentation();
  let log = setup.write(log);
  stop_instrumentation();
  black_box(log);
}

// `FieldInterner::intern` alone, against a version the interner already wrote.
#[library_benchmark(
  config = LibraryBenchmarkConfig::default()
    .tool(
      Callgrind::with_args(["--instr-atstart=no", "--dump-instr=yes"])
        .entry_point(EntryPoint::None)
    )
)]
#[bench::unchanged(InternSetup::new(Mode::Unchanged))]
#[bench::cleared(InternSetup::new(Mode::Cleared))]
fn bench_intern(mut setup: InternSetup) {
  start_instrumentation();
  let interned = setup.intern();
  stop_instrumentation();
  black_box(interned);
}

// The leave-out decision alone, which `intern` makes once per log and encoding reuses.
#[library_benchmark(
  config = LibraryBenchmarkConfig::default()
    .tool(
      Callgrind::with_args(["--instr-atstart=no", "--dump-instr=yes"])
        .entry_point(EntryPoint::None)
    )
)]
#[bench::decide(DecideSetup::unchanged())]
fn bench_decide(setup: DecideSetup) {
  start_instrumentation();
  let interned = setup.decide();
  stop_instrumentation();
  black_box(interned);
}

// `intern` with volatile keys that stay inline and are not yet due for re-admission.
#[library_benchmark(
  config = LibraryBenchmarkConfig::default()
    .tool(
      Callgrind::with_args(["--instr-atstart=no", "--dump-instr=yes"])
        .entry_point(EntryPoint::None)
    )
)]
#[bench::volatile20(InternSetup::with_volatile(20))]
fn bench_intern_volatile(mut setup: InternSetup) {
  start_instrumentation();
  let interned = setup.intern();
  stop_instrumentation();
  black_box(interned);
}

// `RESOLUTIONS` uploader resolutions, of one version repeatedly or of consecutive versions.
#[library_benchmark(
  config = LibraryBenchmarkConfig::default()
    .tool(
      Callgrind::with_args(["--instr-atstart=no", "--dump-instr=yes"])
        .entry_point(EntryPoint::None)
    )
)]
#[bench::same(ResolveSetup::same())]
#[bench::consecutive(ResolveSetup::consecutive())]
fn bench_resolve(setup: ResolveSetup) {
  start_instrumentation();
  let resolutions = setup.resolve();
  stop_instrumentation();
  black_box(resolutions);
}

library_benchmark_group!(
  name = benches;
  benchmarks = bench_write, bench_intern, bench_decide, bench_intern_volatile, bench_resolve
);
