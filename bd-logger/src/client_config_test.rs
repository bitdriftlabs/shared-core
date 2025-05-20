// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{Config, Configuration};
use anyhow::anyhow;
use bd_client_common::{ConfigurationUpdate as _, HANDSHAKE_FLAG_CONFIG_UP_TO_DATE};
use bd_proto::protos::client::api::configuration_update::{StateOfTheWorld, Update_type};
use bd_proto::protos::client::api::ConfigurationUpdate;
use bd_proto::protos::config::v1::config::BufferConfigList;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::sync::mpsc::channel;

struct TestUpdate {
  configuration_tx: tokio::sync::mpsc::Sender<Configuration>,
}

#[async_trait::async_trait]
impl super::ApplyConfig for TestUpdate {
  async fn apply_configuration(&self, configuration: Configuration) -> anyhow::Result<()> {
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
  let (configuration_tx, mut configuration_rx) = channel(2);
  let update = TestUpdate { configuration_tx };
  let directory = TempDir::with_prefix("sdk").unwrap();
  let config = Config::new(directory.path(), update);

  config
    .process_configuration_update(&configuration_update())
    .await
    .unwrap();

  assert_eq!(
    Configuration::default(),
    configuration_rx.recv().await.unwrap()
  );

  config.try_load_persisted_config().await;
  configuration_rx.recv().await;

  // With no safety marking, we should have a retry count of 1.
  assert_eq!(
    std::fs::read(directory.path().join("config").join("retry_count")).unwrap(),
    &[1]
  );

  // Getting a handshake without runtime being up to date should not do anything.
  config.on_handshake_complete(0).await;
  assert_eq!(
    std::fs::read(directory.path().join("config").join("retry_count")).unwrap(),
    &[1]
  );

  // Getting a handshake with runtime being up to date should mark the config as safe.
  config
    .on_handshake_complete(HANDSHAKE_FLAG_CONFIG_UP_TO_DATE)
    .await;
  assert_eq!(
    std::fs::read(directory.path().join("config").join("retry_count")).unwrap(),
    &[0]
  );
}

#[tokio::test]
async fn cache_write_fails() {
  let (configuration_tx, mut configuration_rx) = channel(2);
  let update = TestUpdate { configuration_tx };
  let directory = TempDir::with_prefix("sdk").unwrap();
  let config = Config::new(directory.path(), update);

  // Create the config file, then make it read only. This should make the attempt to cache fail,
  // which should result in this being reported as an error.
  let config_file = directory.path().join("config").join("protobuf.pb");
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
  let directory = TempDir::with_prefix("sdk").unwrap();
  let config = Config::new(directory.path(), update);

  let config_file = directory.path().join("config").join("protobuf.pb");
  std::fs::write(&config_file, "not a proto").unwrap();

  config.try_load_persisted_config_helper().await;

  // Validate that we remove the file if it fails decoding.
  assert!(!config_file.exists());
}
