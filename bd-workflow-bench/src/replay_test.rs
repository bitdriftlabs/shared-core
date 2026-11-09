// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use crate::{BenchmarkCorpus, default_fixture_paths};

#[tokio::test]
async fn checked_in_fixtures_replay_through_the_engine() {
  let fixtures = default_fixture_paths().unwrap();
  assert_eq!(2, fixtures.len());

  let small = fixtures.first().unwrap();
  let corpus = BenchmarkCorpus::load(&small.config_path, &small.logs_path).unwrap();

  assert_eq!(2, corpus.loaded_workflow_count());
  assert_eq!(12, corpus.log_count());

  let mut replay = corpus.new_replay().await;
  corpus.replay_all(&mut replay);

  let large = fixtures.last().unwrap();
  let corpus = BenchmarkCorpus::load(&large.config_path, &large.logs_path).unwrap();
  assert_eq!(127, corpus.loaded_workflow_count());
  assert_eq!(7794, corpus.log_count());

  let mut replay = corpus.new_replay().await;
  corpus.replay_all(&mut replay);
}
