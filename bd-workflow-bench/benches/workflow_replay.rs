// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_workflow_bench::{BenchmarkCorpus, FixturePaths, default_fixture_paths};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::path::PathBuf;
use tokio::runtime::Builder;

const CONFIG_ENV: &str = "BD_WORKFLOW_BENCH_CONFIG";
const LOGS_ENV: &str = "BD_WORKFLOW_BENCH_LOGS";

fn workflow_replay(criterion: &mut Criterion) {
  let runtime = Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap_or_else(|error| panic!("failed to create benchmark runtime: {error}"));
  for fixture in corpus_paths() {
    benchmark_corpus(criterion, &runtime, fixture);
  }
}

fn benchmark_corpus(
  criterion: &mut Criterion,
  runtime: &tokio::runtime::Runtime,
  fixture: FixturePaths,
) {
  let corpus =
    BenchmarkCorpus::load(&fixture.config_path, &fixture.logs_path).unwrap_or_else(|error| {
      panic!(
        "failed to load Criterion corpus {} config={} logs={}: {error}",
        fixture.name,
        fixture.config_path.display(),
        fixture.logs_path.display()
      )
    });
  let mut group = criterion.benchmark_group("workflow_replay");
  group.throughput(Throughput::Elements(
    corpus
      .log_count()
      .try_into()
      .unwrap_or_else(|error| panic!("corpus log count does not fit in u64: {error}")),
  ));
  group.bench_function(fixture.name, |bench| {
    bench.iter_batched_ref(
      || runtime.block_on(corpus.new_replay()),
      |replay| corpus.replay_all(replay),
      BatchSize::PerIteration,
    );
  });
  group.finish();
}

fn corpus_paths() -> Vec<FixturePaths> {
  match (std::env::var_os(CONFIG_ENV), std::env::var_os(LOGS_ENV)) {
    (None, None) => default_fixture_paths()
      .unwrap_or_else(|error| panic!("failed to resolve default Criterion corpora: {error}")),
    (Some(config), Some(logs)) => vec![FixturePaths {
      name: "provided".to_owned(),
      config_path: PathBuf::from(config),
      logs_path: PathBuf::from(logs),
    }],
    _ => panic!("{CONFIG_ENV} and {LOGS_ENV} must be set together"),
  }
}

criterion_group!(benches, workflow_replay);
criterion_main!(benches);
