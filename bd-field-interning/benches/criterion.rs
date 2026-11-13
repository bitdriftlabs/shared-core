// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Each benchmark binary uses a different part of the shared scenarios.
#[allow(dead_code)]
mod scenarios;

use crate::scenarios::{DecideSetup, InternSetup, Mode, Scenario, WriteSetup};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn write(criterion: &mut Criterion) {
  let mut group = criterion.benchmark_group("field_interning_write");
  for scenario in Scenario::CASES {
    let mut setup = WriteSetup::new(scenario);
    let log = setup.log_builder();
    group.bench_function(scenario.name(), |bench| {
      bench.iter_batched(&log, |log| setup.write(log), BatchSize::SmallInput);
    });
  }
  group.finish();
}

fn intern(criterion: &mut Criterion) {
  let mut group = criterion.benchmark_group("field_interning_intern");
  for mode in [Mode::Unchanged, Mode::Cleared] {
    let mut setup = InternSetup::new(mode);
    group.bench_function(mode.name(), |bench| {
      bench.iter(|| black_box(setup.intern()));
    });
  }
  group.finish();
}

fn decide(criterion: &mut Criterion) {
  let setup = DecideSetup::unchanged();
  criterion.bench_function("field_interning_decide", |bench| {
    bench.iter(|| black_box(setup.decide()));
  });
}

criterion_group!(benches, write, intern, decide);
criterion_main!(benches);
