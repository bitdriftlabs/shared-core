// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

mod scenarios;

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
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn admission(criterion: &mut Criterion) {
  let mut group = criterion.benchmark_group("event_buffer_admission");
  for (name, setup) in [
    ("insert_empty", insertion_setup as fn() -> AdmissionSetup),
    ("insert_with_capacity", insertion_with_capacity_setup),
    ("single_victim", single_victim_setup),
    ("multiple_victims", multiple_victims_setup),
    ("multi_lane_eviction", multi_lane_eviction_setup),
  ] {
    group.bench_function(name, |bench| {
      bench.iter_batched_ref(
        setup,
        |setup| black_box(setup.admit()),
        BatchSize::SmallInput,
      );
    });
  }
  group.finish();
}

fn ingress_and_admission(criterion: &mut Criterion) {
  let mut group = criterion.benchmark_group("event_buffer_ingress_and_admission");
  group.bench_function("log_1_kib", |bench| {
    bench.iter_batched_ref(
      ingress_and_insertion_setup,
      |buffer| black_box(ingress_and_admit(buffer)),
      BatchSize::SmallInput,
    );
  });
  group.finish();
}

criterion_group!(benches, admission, ingress_and_admission);
criterion_main!(benches);
