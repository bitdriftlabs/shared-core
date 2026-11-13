// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Wall time for evaluating every regex matcher in the large corpus's workflow configuration
//! against its logs. See `regex_matchers.rs` for the `all` and `typical` log sets.

use bd_workflow_bench::{RegexMatcherCorpus, default_fixture_paths};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

fn regex_matchers(criterion: &mut Criterion) {
  let fixture = default_fixture_paths()
    .unwrap()
    .into_iter()
    .find(|fixture| fixture.name == "large")
    .unwrap();
  let mut group = criterion.benchmark_group("regex_matchers");
  for (name, max_message_bytes) in [("all", None), ("typical", Some(10 * 1024))] {
    let corpus =
      RegexMatcherCorpus::load(&fixture.config_path, &fixture.logs_path, max_message_bytes)
        .unwrap();
    group.throughput(Throughput::Elements(corpus.log_count().try_into().unwrap()));
    group.bench_function(name, |bench| {
      bench.iter(|| black_box(corpus.evaluate_all()));
    });
  }
  group.finish();
}

criterion_group!(benches, regex_matchers);
criterion_main!(benches);
