// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{Config, Configuration};
use anyhow::anyhow;
use bd_client_stats_store::Collector;
use bd_proto::protos::bdtail::bdtail_config::BdTailConfigurations;
use bd_proto::protos::client::api::configuration_update::{StateOfTheWorld, Update_type};
use bd_proto::protos::client::api::ConfigurationUpdate;
use bd_proto::protos::config::v1::config::BufferConfigList;
use bd_proto::protos::insight::insight::InsightsConfiguration;
use bd_proto::protos::workflow::workflow::WorkflowsConfiguration;
use pretty_assertions::assert_eq;
use std::path::Path;
use tempfile::TempDir;
use tokio::sync::mpsc::channel;

struct TestUpdate {
  configuration_tx: tokio::sync::mpsc::Sender<Configuration>,
}

#[async_trait::async_trait]
impl super::ApplyConfig for TestUpdate {
  async fn apply_configuration(&mut self, configuration: Configuration) -> anyhow::Result<()> {
    self
      .configuration_tx
      .send(configuration)
      .await
      .map_err(|_| anyhow!("test"))
  }
}

fn configuration_update() -> ConfigurationUpdate {
  ConfigurationUpdate {
    version_nonce: "hello".to_string(),
    update_type: Some(Update_type::StateOfTheWorld(StateOfTheWorld {
      buffer_config_list: Some(BufferConfigList::default()).into(),
      ..Default::default()
    })),
    ..Default::default()
  }
}

#[tokio::test]
async fn process_and_load() {
  let stats = Collector::default();
  let (configuration_tx, mut configuration_rx) = channel(2);
  let update = TestUpdate { configuration_tx };
  let directory = TempDir::with_prefix("sdk").unwrap();
  let mut config = Config::new(directory.path(), update, &stats.scope("")).unwrap();

  config
    .process_configuration_update(&configuration_update())
    .await
    .unwrap();

  assert_eq!(
    (
      BufferConfigList::default(),
      WorkflowsConfiguration::default(),
      InsightsConfiguration::default(),
      BdTailConfigurations::default()
    ),
    configuration_rx.recv().await.unwrap()
  );

  config.try_load_persisted_config_helper().await.unwrap();

  configuration_rx.recv().await;
}

#[tokio::test]
async fn load_bad_file() {
  let (configuration_tx, _configuration_rx) = channel(2);
  let update = TestUpdate { configuration_tx };
  let stats = Collector::default();
  let config = Config::new(Path::new("doesntexist"), update, &stats.scope(""));
  assert_eq!(
    "an invalid sdk directory was provided: \"doesntexist\"",
    config.err().unwrap().to_string(),
  );
}

#[tokio::test]
async fn cache_write_fails() {
  let (configuration_tx, mut configuration_rx) = channel(2);
  let update = TestUpdate { configuration_tx };
  let directory = TempDir::with_prefix("sdk").unwrap();
  let stats = Collector::default();
  let mut config = Config::new(directory.path(), update, &stats.scope("")).unwrap();

  // Create the config file, then make it read only. This should make the attempt to cache fail,
  // which should result in this being reported as an error.
  let config_file = directory.path().join("config.pb");
  std::fs::write(&config_file, []).unwrap();
  let mut perms = std::fs::metadata(&config_file).unwrap().permissions();
  perms.set_readonly(true);
  std::fs::set_permissions(&config_file, perms).unwrap();

  let config_received = tokio::spawn(async move {
    configuration_rx.recv().await;
  });

  let nack = config
    .process_configuration_update(&configuration_update())
    .await;

  // The config was valid, so we don't expect a nack.
  assert!(nack.is_ok());

  // Make sure that we still announce the configuration update.
  config_received.await.unwrap();
}

#[tokio::test]
async fn load_malformed_file() {
  let (configuration_tx, _configuration_rx) = channel(2);
  let update = TestUpdate { configuration_tx };
  let stats = Collector::default();
  let mut config = Config::new(Path::new("."), update, &stats.scope("")).unwrap();

  let config_file = Path::new("./config.pb");

  std::fs::write(config_file, "not a proto").unwrap();

  let load_result = config.try_load_persisted_config_helper().await;
  assert_eq!(load_result.err().unwrap().to_string(), "Incorrect tag");

  // Validate that we remove the file if it fails decoding.
  assert!(!config_file.exists());
}
