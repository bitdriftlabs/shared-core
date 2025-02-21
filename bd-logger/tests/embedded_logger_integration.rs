// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use assert_matches::assert_matches;
use bd_api::DataUpload;
use bd_api::upload::Tracked;
use bd_key_value::Store;
use bd_logger::{AnnotatedLogField, InitParams, LogType, Logger, MetadataProvider, log_level};
use bd_proto::protos::client::api::{OpaqueRequest, RuntimeUpdate};
use bd_proto::protos::client::runtime::Runtime;
use bd_proto::protos::client::runtime::runtime::{Value, value};
use bd_proto::protos::config::v1::config::BufferConfigList;
use bd_proto::protos::config::v1::config::buffer_config::Type;
use bd_runtime::runtime::FeatureFlag;
use bd_session::fixed::UUIDCallbacks;
use bd_session::{Strategy, fixed};
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::config_helper::{
  configuration_update,
  default_buffer_config,
  make_buffer_matcher_matching_everything,
};
use bd_test_helpers::metadata::EmptyMetadata;
use bd_test_helpers::resource_utilization::EmptyTarget;
use bd_test_helpers::runtime::make_update;
use bd_test_helpers::session::InMemoryStorage;
use bd_test_helpers::test_api_server::StreamAction;
use protobuf::well_known_types::any::Any;
use std::sync::Arc;

#[ctor::ctor]
fn global_init() {
  bd_test_helpers::test_global_init();
}

// TODO(snowp): Shared test fixture.
struct TestMetadataProvider;

impl MetadataProvider for TestMetadataProvider {
  fn timestamp(&self) -> anyhow::Result<time::OffsetDateTime> {
    Ok(time::OffsetDateTime::now_utc())
  }

  fn fields(&self) -> anyhow::Result<Vec<AnnotatedLogField>> {
    Ok(Vec::new())
  }
}

struct Setup {
  _sdk_directory: tempfile::TempDir,
  upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
  server: Box<bd_test_helpers::test_api_server::ServerHandle>,
  logger: Logger,
  shutdown: ComponentShutdownTrigger,
}

impl Setup {
  async fn new() -> Self {
    let server = bd_test_helpers::test_api_server::start_server(false, None);

    let shutdown = ComponentShutdownTrigger::default();
    let sdk_directory = tempfile::TempDir::with_prefix("embedded_logger").unwrap();
    let (network, handle) = bd_hyper_network::HyperNetwork::new(
      &format!("http://localhost:{}", server.port),
      shutdown.make_shutdown(),
    );

    tokio::task::spawn(network.start());

    let store = Arc::new(Store::new(Box::<InMemoryStorage>::default()));
    let device = Arc::new(bd_device::Device::new(store.clone()));

    let (logger, upload_tx, future) = bd_logger::LoggerBuilder::new(InitParams {
      sdk_directory: sdk_directory.path().to_owned(),
      network: Box::new(handle),
      session_strategy: Arc::new(Strategy::Fixed(fixed::Strategy::new(
        store.clone(),
        Arc::new(UUIDCallbacks),
      ))),
      metadata_provider: Arc::new(TestMetadataProvider),
      store,
      resource_utilization_target: Box::new(EmptyTarget),
      session_replay_target: Box::new(bd_test_helpers::session_replay::NoOpTarget),
      events_listener_target: Box::new(bd_test_helpers::events::NoOpListenerTarget),
      device,
      static_metadata: Arc::new(EmptyMetadata),
      api_key: "apikey".to_string(),
    })
    .with_shutdown_handle(shutdown.make_handle())
    .build()
    .unwrap();

    tokio::task::spawn(future);

    Self {
      _sdk_directory: sdk_directory,
      upload_tx,
      server,
      logger,
      shutdown,
    }
  }
}

#[tokio::test]
async fn opaque_uploads() {
  let mut setup = Setup::new().await;

  let (tracked, response) = Tracked::new(
    "123".to_string(),
    OpaqueRequest {
      uuid: "123".to_string(),
      request: Some(Any::default()).into(),
      ..Default::default()
    },
  );

  let tracked_request = tracked.payload.clone();

  setup
    .upload_tx
    .send(DataUpload::OpaqueRequest(tracked))
    .await
    .unwrap();

  let uploaded_payload = setup.server.next_opaque_upload().await.unwrap();

  assert_eq!(uploaded_payload, tracked_request);

  assert!(response.await.unwrap().success);

  setup.shutdown.shutdown().await;
}

#[tokio::test]
async fn runtime_update() {
  let mut setup = Setup::new().await;

  let stream_id = setup.server.next_initialized_stream().await.unwrap();

  setup
    .server
    .stream_action(
      stream_id,
      StreamAction::SendRuntime(RuntimeUpdate {
        runtime: Some(Runtime {
          values: [(
            "test".to_string(),
            Value {
              type_: Some(value::Type::StringValue("test".to_string())),
              ..Default::default()
            },
          )]
          .into(),
          ..Default::default()
        })
        .into(),
        ..Default::default()
      }),
    )
    .await;

  setup.server.next_runtime_ack(stream_id).await;

  assert_eq!(
    setup
      .logger
      .runtime_snapshot()
      .get_string("test", "default"),
    "test"
  );

  setup.shutdown.shutdown().await;
}


#[tokio::test]
async fn configuration_update_with_log_uploads() {
  let mut setup = Setup::new().await;

  let stream_id = setup.server.next_initialized_stream().await.unwrap();

  setup
    .server
    .stream_action(
      stream_id,
      StreamAction::SendRuntime(make_update(
        vec![(
          bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
          bd_test_helpers::runtime::ValueKind::Int(1),
        )],
        "version".to_string(),
      )),
    )
    .await;

  setup
    .server
    .stream_action(
      stream_id,
      StreamAction::SendConfiguration(configuration_update(
        "test",
        bd_proto::protos::client::api::configuration_update::StateOfTheWorld {
          buffer_config_list: Some(BufferConfigList {
            buffer_config: vec![default_buffer_config(
              Type::CONTINUOUS,
              make_buffer_matcher_matching_everything().into(),
            )],
            ..Default::default()
          })
          .into(),
          ..Default::default()
        },
      )),
    )
    .await;

  setup.server.next_configuration_ack(stream_id).await;

  setup.logger.new_logger_handle().log(
    log_level::DEBUG,
    LogType::Normal,
    "test".into(),
    vec![],
    vec![],
    None,
    false,
  );

  assert_matches!(setup.server.next_log_upload().await, Some(log_upload) => {
      assert_eq!(log_upload.logs.len(), 1);
  });

  setup.shutdown.shutdown().await;
}
