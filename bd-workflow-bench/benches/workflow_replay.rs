// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_workflow_bench::{BenchmarkCorpus, fixture_paths};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::path::PathBuf;
use tokio::runtime::Builder;

const CONFIG_ENV: &str = "BD_WORKFLOW_BENCH_CONFIG";
const LOGS_ENV: &str = "BD_WORKFLOW_BENCH_LOGS";

fn workflow_replay(criterion: &mut Criterion) {
  let (config_path, logs_path, corpus_name) = corpus_paths();
  let corpus = BenchmarkCorpus::load(&config_path, &logs_path).unwrap_or_else(|error| {
    panic!(
      "failed to load Criterion corpus config={} logs={}: {error}",
      config_path.display(),
      logs_path.display()
    )
  });
  let runtime = Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap_or_else(|error| panic!("failed to create benchmark runtime: {error}"));
  let mut group = criterion.benchmark_group("workflow_replay");
  group.throughput(Throughput::Elements(
    corpus
      .log_count()
      .try_into()
      .unwrap_or_else(|error| panic!("corpus log count does not fit in u64: {error}")),
  ));
  group.bench_function(corpus_name, |bench| {
    bench.iter_batched_ref(
      || runtime.block_on(corpus.new_replay()),
      |replay| corpus.replay_all(replay),
      BatchSize::PerIteration,
    );
  });
  group.finish();
}

fn corpus_paths() -> (PathBuf, PathBuf, &'static str) {
  match (std::env::var_os(CONFIG_ENV), std::env::var_os(LOGS_ENV)) {
    (None, None) => {
      let (config, logs) = fixture_paths();
      (config, logs, "fixed_corpus")
    },
    (Some(config), Some(logs)) => (
      PathBuf::from(config),
      PathBuf::from(logs),
      "provided_corpus",
    ),
    _ => panic!("{CONFIG_ENV} and {LOGS_ENV} must be set together"),
  }
}

criterion_group!(benches, workflow_replay);
criterion_main!(benches);
