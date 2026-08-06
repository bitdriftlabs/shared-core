// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.

#![allow(clippy::unwrap_used)]

use crate::{BenchmarkCorpus, fixture_paths};

#[tokio::test]
async fn checked_in_fixture_replays_through_the_engine() {
  let (config, logs) = fixture_paths();
  let corpus = BenchmarkCorpus::load(&config, &logs).unwrap();

  assert_eq!(2, corpus.loaded_workflow_count());
  assert_eq!(12, corpus.log_count());

  let mut replay = corpus.new_replay().await;
  corpus.replay_all(&mut replay);
}
