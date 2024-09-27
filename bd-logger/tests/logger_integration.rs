// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
mod tests {
  use assert_matches::assert_matches;
  use bd_api::Platform;
  use bd_client_common::error::UnexpectedErrorHandler;
  use bd_key_value::Store;
  use bd_log_metadata::LogFieldKind;
  use bd_logger::{
    log_level,
    AnnotatedLogField,
    AnnotatedLogFields,
    AppVersionExtra,
    InitParams,
    LogAttributesOverridesPreviousRunSessionID,
    LogField,
    LogLevel,
    LogMessage,
    LogType,
    Logger,
    StringOrBytes,
  };
  use bd_noop_network::NoopNetwork;
  use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::root_as_log;
  use bd_proto::protos::bdtail::bdtail_config::{BdTailConfigurations, BdTailStream};
  use bd_proto::protos::client::api::configuration_update::StateOfTheWorld;
  use bd_proto::protos::client::api::configuration_update_ack::Nack;
  use bd_proto::protos::client::api::ConfigurationUpdate;
  use bd_proto::protos::config::v1::config::buffer_config::Type;
  use bd_proto::protos::config::v1::config::BufferConfigList;
  use bd_proto::protos::filter::filter::{Filter, FiltersConfiguration};
  use bd_runtime::runtime::FeatureFlag;
  use bd_session::fixed::{State, UUIDCallbacks};
  use bd_session::{fixed, Strategy};
  use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
  use bd_stats_common::labels;
  use bd_test_helpers::config_helper::{
    self,
    configuration_update,
    default_buffer_config,
    invalid_configuration,
    make_buffer_matcher_matching_everything,
    make_buffer_matcher_matching_everything_except_internal_logs,
    make_buffer_matcher_matching_resource_logs,
    make_configuration_update_with_workflow_flushing_buffer_on_anything,
    make_workflow_config_flushing_buffer,
    match_message,
    BufferConfigBuilder,
  };
  use bd_test_helpers::metadata::EmptyMetadata;
  use bd_test_helpers::metadata_provider::LogMetadata;
  use bd_test_helpers::resource_utilization::EmptyTarget;
  use bd_test_helpers::runtime::{make_update, ValueKind};
  use bd_test_helpers::session::InMemoryStorage;
  use bd_test_helpers::stats::StatsRequestHelper;
  use bd_test_helpers::test_api_server::{ExpectedStreamEvent, StreamAction};
  use bd_test_helpers::workflow::macros::{
    action,
    declare_transition,
    insight,
    insights,
    log_matches,
    rule,
    state,
    workflow_proto,
    workflows_configuration,
  };
  use bd_test_helpers::{field_value, metric_tag, metric_value, set_field, RecordingErrorReporter};
  use std::ops::Add;
  use std::sync::Arc;
  use std::time::Instant;
  use tempfile::TempDir;
  use time::ext::{NumericalDuration, NumericalStdDuration};

  #[ctor::ctor]
  fn global_init() {
    bd_test_helpers::test_global_init();
  }

  struct Setup {
    logger: Logger,
    logger_handle: bd_logger::LoggerHandle,
    sdk_directory: Arc<TempDir>,
    server: Box<bd_test_helpers::test_api_server::ServerHandle>,
    current_api_stream: i32,
    store: Arc<Store>,
    _shutdown: ComponentShutdownTrigger,
  }

  impl Setup {
    fn new() -> Self {
      Self::new_with_directory(
        Arc::new(TempDir::with_prefix("sdk").unwrap()),
        LogMetadata {
          timestamp: time::OffsetDateTime::now_utc(),
          fields: Vec::new(),
        },
      )
    }

    fn new_with_metadata(metadata_provider: LogMetadata) -> Self {
      Self::new_with_directory(
        Arc::new(TempDir::with_prefix("sdk").unwrap()),
        metadata_provider,
      )
    }

    fn new_with_directory(sdk_directory: Arc<TempDir>, metadata_provider: LogMetadata) -> Self {
      let mut server = bd_test_helpers::test_api_server::start_server(false, None);
      let shutdown = ComponentShutdownTrigger::default();

      let store = Arc::new(Store::new(Box::<InMemoryStorage>::default()));
      let device = Arc::new(bd_device::Device::new(store.clone()));

      let logger = bd_logger::LoggerBuilder::new(InitParams {
        sdk_directory: sdk_directory.path().into(),
        api_key: "foo-api-key".to_string(),
        session_strategy: Arc::new(Strategy::Fixed(fixed::Strategy::new(
          store.clone(),
          Arc::new(UUIDCallbacks),
        ))),
        metadata_provider: Arc::new(metadata_provider),
        resource_utilization_target: Box::new(EmptyTarget),
        events_listener_target: Box::new(bd_test_helpers::events::NoOpListenerTarget),
        device,
        store: store.clone(),
        network: Box::new(Self::run_network(server.port, shutdown.make_shutdown())),
        platform: Platform::Other("integration-test", "test"),
        static_metadata: Arc::new(EmptyMetadata),
      })
      .with_mobile_features(true)
      .with_internal_logger(true)
      .build_dedicated_thread()
      .unwrap()
      .0;

      let logger_handle = logger.new_logger_handle();
      let current_api_stream = Self::do_stream_setup(&mut server);
      Self {
        logger,
        logger_handle,
        sdk_directory,
        server,
        current_api_stream,
        store,
        _shutdown: shutdown,
      }
    }

    fn run_network(port: u16, shutdown: ComponentShutdown) -> bd_hyper_network::Handle {
      bd_hyper_network::HyperNetwork::run_on_thread(&format!("http://localhost:{port}"), shutdown)
    }

    fn do_stream_setup(server: &mut bd_test_helpers::test_api_server::ServerHandle) -> i32 {
      let current_api_stream = server.blocking_next_stream().unwrap();
      assert!(server.await_event_with_timeout(
        current_api_stream,
        ExpectedStreamEvent::Handshake(None),
        1.seconds(),
      ));

      server.blocking_stream_action(
        current_api_stream,
        StreamAction::SendRuntime(make_update(
          Self::get_default_runtime_values(),
          "base".to_string(),
        )),
      );

      let (_, response) = server.blocking_next_runtime_ack();
      assert!(response.nack.is_none());

      current_api_stream
    }

    fn get_default_runtime_values() -> Vec<(&'static str, ValueKind)> {
      vec![
        (
          bd_runtime::runtime::filters::FilterChainEnabledFlag::path(),
          ValueKind::Bool(true),
        ),
        (
          bd_runtime::runtime::platform_events::ListenerEnabledFlag::path(),
          ValueKind::Bool(true),
        ),
        (
          bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
          bd_test_helpers::runtime::ValueKind::Int(10),
        ),
        (
          "upload_ratelimit.bytes_count",
          bd_test_helpers::runtime::ValueKind::Int(100_000),
        ),
        (
          bd_runtime::runtime::workflows::WorkflowsEnabledFlag::path(),
          ValueKind::Bool(true),
        ),
        (
          bd_runtime::runtime::workflows::WorkflowsInsightsEnabledFlag::path(),
          ValueKind::Bool(true),
        ),
        (
          bd_runtime::runtime::resource_utilization::ResourceUtilizationEnabledFlag::path(),
          ValueKind::Bool(false),
        ),
      ]
    }

    fn log(
      &self,
      level: LogLevel,
      log_type: LogType,
      message: LogMessage,
      fields: AnnotatedLogFields,
      matching_fields: AnnotatedLogFields,
      attributes_overrides: Option<LogAttributesOverridesPreviousRunSessionID>,
    ) {
      self.logger_handle.log(
        level,
        log_type,
        message,
        fields,
        matching_fields,
        attributes_overrides,
        false,
      );
    }

    fn blocking_log(
      &self,
      level: LogLevel,
      log_type: LogType,
      message: LogMessage,
      fields: AnnotatedLogFields,
      matching_fields: AnnotatedLogFields,
    ) {
      self.logger_handle.log(
        level,
        log_type,
        message,
        fields,
        matching_fields,
        None,
        true,
      );
    }

    fn send_configuration_update(&mut self, config: ConfigurationUpdate) -> Option<Nack> {
      self.server.blocking_stream_action(
        self.current_api_stream,
        StreamAction::SendConfiguration(config),
      );
      let (stream_id, mut ack) = self.server.blocking_next_configuration_ack();
      assert_eq!(stream_id, self.current_api_stream);

      ack.nack.take()
    }

    fn send_runtime_update(
      &self,
      workflows_enabled: bool,
      workflows_insights_enabled: bool,
      immediate_stats_upload_enabled: bool,
    ) {
      let mut values = Self::get_default_runtime_values();

      if immediate_stats_upload_enabled {
        values.append(&mut vec![
          (
            bd_runtime::runtime::stats::DirectStatFlushIntervalFlag::path(),
            ValueKind::Int(100),
          ),
          (
            bd_runtime::runtime::stats::UploadStatFlushIntervalFlag::path(),
            ValueKind::Int(100),
          ),
        ]);
      }

      values.push((
        bd_runtime::runtime::workflows::WorkflowsEnabledFlag::path(),
        ValueKind::Bool(workflows_enabled),
      ));

      values.push((
        bd_runtime::runtime::workflows::WorkflowsInsightsEnabledFlag::path(),
        ValueKind::Bool(workflows_insights_enabled),
      ));

      self.server.blocking_stream_action(
        self.current_api_stream,
        StreamAction::SendRuntime(make_update(values, "version".to_string())),
      );
    }

    fn aggregated_stats_file_path(&self) -> std::path::PathBuf {
      self.sdk_directory.path().join("aggregated_stats.pb")
    }

    fn workflows_state_file_path(&self) -> std::path::PathBuf {
      self
        .sdk_directory
        .path()
        .join("workflows_state_snapshot.3.bin")
    }
  }

  impl Drop for Setup {
    fn drop(&mut self) {
      // Perform blocking shutdown to ensure that the ring buffers are released by the time Drop
      // completes.
      self.logger.shutdown(true);
    }
  }

  #[test]
  fn attributes_accessors() {
    let setup = Setup::new();

    assert_eq!(36, setup.logger_handle.session_id().len());
    assert_eq!(36, setup.logger_handle.device_id().len());
  }

  #[test]
  fn logger_api() {
    // Test basic handshaking which is handled by Setup.
    let mut setup = Setup::new();
    let nack = setup.send_configuration_update(
      make_configuration_update_with_workflow_flushing_buffer_on_anything(
        "default",
        Type::CONTINUOUS,
      ),
    );

    assert!(nack.is_none());

    // Verify that the buffer is written to the right location.
    assert!(setup.sdk_directory.path().join("buffers/default").exists());
  }

  #[test]
  fn log_upload() {
    let mut setup = Setup::new();
    setup.send_configuration_update(
      make_configuration_update_with_workflow_flushing_buffer_on_anything(
        "default",
        Type::CONTINUOUS,
      ),
    );

    // TODO(snowp): Either figure out how to use test time or make the
    // intervals configurable so we can avoid having to log a full batch.
    for _ in 0 .. 10 {
      setup.log(
        log_level::DEBUG,
        LogType::Normal,
        "some log".into(),
        vec![],
        vec![],
        None,
      );
    }

    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "default");
      uuid::Uuid::parse_str(log_upload.upload_uuid.as_str()).unwrap();
      assert_eq!(log_upload.logs.len(), 10);

      assert_eq!(expected_message(&log_upload.logs[0]), "some log");
    });
  }

  #[test]
  fn log_upload_attributes_override() {
    let time_first = time::OffsetDateTime::now_utc();
    let mut setup = Setup::new_with_metadata(LogMetadata {
      timestamp: time_first,
      fields: vec![],
    });

    setup.send_configuration_update(
      make_configuration_update_with_workflow_flushing_buffer_on_anything(
        "default",
        Type::CONTINUOUS,
      ),
    );

    let time_second = time::OffsetDateTime::now_utc();

    let error_reporter = Arc::new(RecordingErrorReporter::default());
    UnexpectedErrorHandler::set_reporter(error_reporter.clone());

    setup.store.set(
      &fixed::STATE_KEY,
      &State {
        session_id: "foo_overridden".to_string(),
      },
    );

    let current_session_id = setup.logger.new_logger_handle().session_id();

    // This log should end up being emitted with an overridden session ID and timestamp.
    setup.log(
      log_level::DEBUG,
      LogType::Normal,
      "log with overridden attributes".into(),
      vec![],
      vec![],
      Some(LogAttributesOverridesPreviousRunSessionID {
        expected_previous_process_session_id: "foo_overridden".to_string(),
        occurred_at: time_second,
      }),
    );

    // This log should end up being dropped.
    setup.log(
      log_level::DEBUG,
      LogType::Normal,
      "log with overridden attributes".into(),
      vec![],
      vec![],
      Some(LogAttributesOverridesPreviousRunSessionID {
        expected_previous_process_session_id: "bar_overridden".to_string(),
        occurred_at: time_second,
      }),
    );

    for _ in 0 .. 8 {
      setup.log(
        log_level::DEBUG,
        LogType::Normal,
        "some log".into(),
        vec![],
        vec![],
        None,
      );
    }

    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "default");
      uuid::Uuid::parse_str(log_upload.upload_uuid.as_str()).unwrap();
      assert_eq!(log_upload.logs.len(), 10);

      // Confirm both session ID and timestamp are overridden.
      let first_uploaded_log = &log_upload.logs[0];
      assert_eq!(expected_session_id(first_uploaded_log), "foo_overridden");
      assert_eq!(expected_timestamp(first_uploaded_log), time_second);
      assert_eq!(expected_field_value(first_uploaded_log, "_logged_at"), time_first.to_string());
      assert_eq!(expected_message(first_uploaded_log), "log with overridden attributes");

      // Confirm that second log was dropped and error was emitted.
      let second_uploaded_log = &log_upload.logs[1];
      assert_eq!(expected_session_id(second_uploaded_log), current_session_id);
      assert_eq!(expected_field_value(second_uploaded_log, "_override_session_id"), "bar_overridden");

      assert!(error_reporter.error().is_some());
    });
  }

  #[test]
  fn api_bandwidth_counters() {
    let mut setup = Setup::new();

    setup.server.blocking_stream_action(
      setup.current_api_stream,
      StreamAction::SendRuntime(make_update(
        vec![
          (
            bd_runtime::runtime::stats::DirectStatFlushIntervalFlag::path(),
            ValueKind::Int(1),
          ),
          (
            bd_runtime::runtime::stats::UploadStatFlushIntervalFlag::path(),
            ValueKind::Int(1),
          ),
          (
            bd_runtime::runtime::resource_utilization::ResourceUtilizationEnabledFlag::path(),
            ValueKind::Bool(false),
          ),
        ],
        "version".to_string(),
      )),
    );

    // Verify that we emit counters for how much data we transmit/receive.
    assert_matches!(setup.server.next_stat_upload(), Some(upload) => {
        let upload = StatsRequestHelper::new(upload);

        // If these numbers end up being too variable we do something more generic.
        assert_eq!(upload.get_counter("api:bandwidth_tx_uncompressed", labels! {}), Some(120));
        assert!(upload.get_counter("api:bandwidth_tx", labels! {}).unwrap() > 100);
        assert_eq!(upload.get_counter("api:bandwidth_rx", labels! {}), Some(375));
        assert_eq!(upload.get_counter("api:bandwidth_rx_decompressed", labels! {}), Some(360));
        assert_eq!(upload.get_counter("api:stream_total", labels! {}), Some(1));
    });
  }

  #[test]
  fn buffer_selection_update() {
    let mut setup = Setup::new();

    setup.send_configuration_update(configuration_update(
      "",
      StateOfTheWorld {
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
    ));

    for _ in 0 .. 10 {
      setup.log(
        log_level::DEBUG,
        LogType::Normal,
        "something".into(),
        vec![],
        vec![],
        None,
      );
    }

    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.logs.len(), 10);
    });

    // Now update the configuration to drop all logs.
    setup.send_configuration_update(configuration_update(
      "update",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![default_buffer_config(Type::CONTINUOUS, None)],
          ..Default::default()
        })
        .into(),
        ..Default::default()
      },
    ));

    for _ in 0 .. 10 {
      setup.log(
        log_level::DEBUG,
        LogType::Normal,
        "something".into(),
        vec![],
        vec![],
        None,
      );
    }

    assert_matches!(setup.server.blocking_next_log_upload(), None);
  }

  #[test]
  fn bad_config() {
    let mut setup = Setup::new();

    let maybe_nack = setup.send_configuration_update(invalid_configuration());
    assert_matches!(maybe_nack, Some(nack) => {
      assert_eq!(nack.error_details,
        "An invalid match configuration was received: missing oneof");
      assert_eq!(nack.version_nonce, "");
    });
  }

  #[test]
  fn configuration_caching() {
    let directory = Arc::new(tempfile::TempDir::with_prefix("sdk").unwrap());

    // Initialize the logger once, sending it a configuration that will upload all logs.
    {
      let mut setup = Setup::new_with_directory(
        directory.clone(),
        LogMetadata {
          timestamp: time::OffsetDateTime::now_utc(),
          fields: Vec::new(),
        },
      );

      setup.send_configuration_update(configuration_update(
        "",
        StateOfTheWorld {
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
      ));
    }

    // After shutting down the previous logger, create a new one with the same buffer directory.
    // It should reuse the previous configuration and upload logs without receiving a configuration
    // update.
    let mut setup = Setup::new_with_directory(
      directory,
      LogMetadata {
        timestamp: time::OffsetDateTime::now_utc(),
        fields: Vec::new(),
      },
    );

    // Update the batch size to 1 so we don't have to log a whole bunch of logs to trigger the
    // update.
    setup.server.blocking_stream_action(
      setup.current_api_stream,
      StreamAction::SendRuntime(make_update(
        vec![(
          bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
          bd_test_helpers::runtime::ValueKind::Int(1),
        )],
        "base".to_string(),
      )),
    );

    setup.log(
      log_level::DEBUG,
      LogType::Normal,
      "foo".into(),
      vec![],
      vec![],
      None,
    );

    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "default");
      assert_eq!(log_upload.logs.len(), 1);
    });
  }

  #[test]
  fn trigger_buffers_not_uploaded() {
    let mut setup = Setup::new();

    let maybe_nack = setup.send_configuration_update(configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![default_buffer_config(
            Type::TRIGGER,
            make_buffer_matcher_matching_everything().into(),
          )],
          ..Default::default()
        })
        .into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    for _ in 0 .. 10 {
      setup.log(
        log_level::DEBUG,
        LogType::Normal,
        "something".into(),
        vec![],
        vec![],
        None,
      );
    }

    assert_matches!(setup.server.blocking_next_log_upload(), None);
  }

  #[test]
  fn blocking_log() {
    let mut setup = Setup::new();

    setup.send_runtime_update(true, true, false);

    // Send down a configuration with a single 'default' buffer and a workflow that matches on 'foo'
    // log message.
    // After the log 'foo' is emitted the workflow should trigger a flush buffer action. In response
    // to this action th engine will persist workflows state to disk.
    let maybe_nack = setup.send_configuration_update(config_helper::configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![config_helper::default_buffer_config(
            Type::TRIGGER,
            make_buffer_matcher_matching_everything().into(),
          )],
          ..Default::default()
        })
        .into(),
        workflows_configuration: Some(make_workflow_config_flushing_buffer(
          "default",
          log_matches!(message == "foo"),
        ))
        .into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    setup.blocking_log(
      log_level::DEBUG,
      LogType::Normal,
      "foo".into(),
      vec![],
      vec![],
    );

    // Confim that workflows state is persisted to disk after the processing of log completes.
    assert!(setup.workflows_state_file_path().exists());
    assert!(setup.aggregated_stats_file_path().exists());
  }

  #[test]
  fn flush_state() {
    let mut setup = Setup::new();

    // Send down a configuration with a single 'default' buffer.
    let maybe_nack = setup.send_configuration_update(config_helper::configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![config_helper::default_buffer_config(
            Type::TRIGGER,
            make_buffer_matcher_matching_everything().into(),
          )],
          ..Default::default()
        })
        .into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    setup.logger_handle.flush_state(false);

    // File should not exist immediately after flush_state call.
    assert!(!setup.aggregated_stats_file_path().exists());

    // Wait a bit for the event loop to be able to process `flush_state` request.
    std::thread::sleep(1.std_seconds());

    assert!(setup.aggregated_stats_file_path().exists());
  }

  #[test]
  fn blocking_flush_state() {
    let mut setup = Setup::new();

    // Send down a configuration with a single 'default' buffer.
    let maybe_nack = setup.send_configuration_update(config_helper::configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![config_helper::default_buffer_config(
            Type::TRIGGER,
            make_buffer_matcher_matching_everything().into(),
          )],
          ..Default::default()
        })
        .into(),
        workflows_configuration: Some(make_workflow_config_flushing_buffer(
          "default",
          log_matches!(message == "foo"),
        ))
        .into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    setup.log(
      log_level::DEBUG,
      LogType::Normal,
      "foo".into(),
      vec![],
      vec![],
      None,
    );

    setup.logger_handle.flush_state(true);

    assert!(setup.workflows_state_file_path().exists());
    assert!(setup.aggregated_stats_file_path().exists());
  }

  #[test]
  fn flush_state_uninitialized() {
    let setup = Setup::new();

    setup.logger_handle.flush_state(false);

    // File should not exist immediately after flush_state call.
    assert!(!setup.aggregated_stats_file_path().exists());

    // Wait a bit for the event loop to be able to process `flush_state` request.
    std::thread::sleep(1.std_seconds());

    assert!(setup.aggregated_stats_file_path().exists());
  }

  #[test]
  fn blocking_flush_state_uninitialized() {
    let setup = Setup::new();

    setup.logger_handle.flush_state(true);

    assert!(!setup.workflows_state_file_path().exists());
    assert!(setup.aggregated_stats_file_path().exists());
  }

  #[test]
  fn log_tailing() {
    let mut setup = Setup::new();

    let maybe_nack = setup.send_configuration_update(config_helper::configuration_update(
      "",
      StateOfTheWorld {
        bdtail_configuration: Some(BdTailConfigurations {
          active_streams: vec![BdTailStream {
            stream_id: "all".into(),
            matcher: None.into(),
            ..Default::default()
          }],
          ..Default::default()
        })
        .into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    setup.log(
      log_level::DEBUG,
      LogType::Normal,
      "something".into(),
      vec![],
      vec![],
      None,
    );

    // Logs are immediately uploaded with "streamed" as the buffer id.
    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "streamed");
      assert_eq!(log_upload.logs.len(), 1);
      assert_eq!("something", expected_message(&log_upload.logs[0]));
      assert_eq!(vec!["all"], expected_stream_ids(&log_upload.logs[0]));
    });

    let maybe_nack = setup.send_configuration_update(config_helper::configuration_update(
      "",
      StateOfTheWorld {
        bdtail_configuration: Some(BdTailConfigurations {
          active_streams: vec![
            BdTailStream {
              stream_id: "all".into(),
              matcher: None.into(),
              ..Default::default()
            },
            BdTailStream {
              stream_id: "some".into(),
              matcher: Some(log_matches!(message == "something")).into(),
              ..Default::default()
            },
          ],
          ..Default::default()
        })
        .into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    setup.log(
      log_level::DEBUG,
      LogType::Normal,
      "something".into(),
      vec![],
      vec![],
      None,
    );

    // When multiple streams match the same log the log is uploaded once with multiple tagged stream
    // IDs.
    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "streamed");
      assert_eq!(log_upload.logs.len(), 1);
      assert_eq!("something", expected_message(&log_upload.logs[0]));
      assert_eq!(vec!["all", "some"], expected_stream_ids(&log_upload.logs[0]));
    });
  }

  #[test]
  fn workflow_flush_buffers_action_uploads_buffer() {
    let mut setup = Setup::new();

    setup.send_runtime_update(true, true, false);

    // Send down a configuration with a single buffer ('default')
    // which accepts all logs and a single workflow which matches for logs
    // with the 'fire workflow action!' message in order to flush all buffers.
    let maybe_nack = setup.send_configuration_update(config_helper::configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![config_helper::default_buffer_config(
            Type::TRIGGER,
            make_buffer_matcher_matching_everything().into(),
          )],
          ..Default::default()
        })
        .into(),
        workflows_configuration: Some(make_workflow_config_flushing_buffer(
          "default",
          log_matches!(message == "fire workflow action!"),
        ))
        .into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    for _ in 0 .. 9 {
      setup.log(
        log_level::DEBUG,
        LogType::Normal,
        "something".into(),
        vec![],
        vec![],
        None,
      );
    }

    setup.log(
      log_level::DEBUG,
      LogType::Normal,
      "fire workflow action!".into(),
      vec![],
      vec![],
      None,
    );

    // Since there are 10 logs in the buffer at this point, we should now see an upload containing
    // 10 logs.
    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "default");
      assert_eq!(log_upload.logs.len(), 10);
      assert_eq!(vec!["flush_action_id"], expected_workflow_action_ids(&log_upload.logs[9]));
    });
  }

  #[test]
  fn workflow_flush_buffers_action_emits_synthetic_log_and_uploads_buffer_and_starts_streaming() {
    let mut a = state!("A");
    let b = state!("B");
    let c = state!("C");
    declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message == "fire flush trigger buffer and start streaming action!"));
      do action!(
        flush_buffers &["trigger_buffer_id"];
        continue_streaming_to vec!["default"];
        logs_count 10;
        id "flush_with_streaming_action_id"
      ),
      action!(
        emit_counter "insight_action_id";
        value metric_value!(1)
      )
    );
    declare_transition!(
      &mut a => &c;
      when rule!(log_matches!(message == "fire flush trigger buffer action!"));
      do action!(
        flush_buffers &["trigger_buffer_id"];
        id "flush_with_streaming_action_id"
      )
    );

    let mut setup = Setup::new_with_metadata(LogMetadata {
      timestamp: time::OffsetDateTime::now_utc(),
      fields: vec![
        AnnotatedLogField {
          field: LogField {
            key: "k1".into(),
            value: StringOrBytes::String("provider_value_1".into()),
          },
          kind: LogFieldKind::Custom,
        },
        AnnotatedLogField {
          field: LogField {
            key: "k2".into(),
            value: StringOrBytes::String("provider_value_2".into()),
          },
          kind: LogFieldKind::Ootb,
        },
      ],
    });

    // Send down a configuration with a single buffer ('default')
    // which does not accept `InternalSDK` logs and a single workflow
    // which matches for logs with the 'fire workflow action!' message
    // in order to flush all buffers.
    let maybe_nack = setup.send_configuration_update(config_helper::configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![
            default_buffer_config(
              Type::CONTINUOUS,
              make_buffer_matcher_matching_resource_logs().into(),
            ),
            BufferConfigBuilder {
              name: "trigger_buffer_id",
              buffer_type: Type::TRIGGER,
              filter: make_buffer_matcher_matching_everything_except_internal_logs().into(),
              non_volatile_size: 100_000,
              volatile_size: 10_000,
            }
            .build(),
          ],
          ..Default::default()
        })
        .into(),
        workflows_configuration: Some(workflows_configuration!(vec![
          workflow_proto!("workflow"; exclusive with a, b, c)
        ]))
        .into(),
        insights_configuration: Some(insights!(
          insight!("insight_1"),
          insight!("insight_2"),
          // "insight_3" doesn't exist on emitted logs, should be ignored.
          insight!("insight_3")
        ))
        .into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    // Enable immediate stats upload.
    setup.send_runtime_update(true, true, true);

    for _ in 0 .. 9 {
      setup.log(
        log_level::DEBUG,
        LogType::InternalSDK,
        "something".into(),
        vec![],
        vec![],
        None,
      );
    }

    setup.log(
      log_level::DEBUG,
      LogType::Normal,
      "fire flush trigger buffer and start streaming action!".into(),
      vec![
        LogField {
          key: "k3".into(),
          value: StringOrBytes::String("value_3".into()),
        },
        LogField {
          key: "insight_1".to_string(),
          value: StringOrBytes::String("value_1".to_string()),
        },
        LogField {
          key: "insight_2".to_string(),
          value: StringOrBytes::String("value_2".to_string()),
        },
      ]
      .into_iter()
      .map(|field| AnnotatedLogField {
        field,
        kind: LogFieldKind::Ootb,
      })
      .collect(),
      vec![],
      None,
    );

    // Confirm that we emit an insight for a triggered workflow action.
    let stat_upload = StatsRequestHelper::new(setup.server.next_stat_upload().unwrap());
    assert_eq!(
      stat_upload.get_counter(
        "workflows_dyn:action",
        labels! {
          "_id" => "insight_action_id",
          "_insights" => "true",
          "insight_1" => "value_1",
          "insight_2" => "value_2"
        }
      ),
      Some(1),
    );

    // Out of 10 emitted logs none of them was stored in a configured
    // "default" buffer. Since the last of these logs triggered
    // flush buffers action a synthetic log resembling
    // the log that triggered the action was added to the "trigger_buffer_id" buffer.
    // We verify that this synthetic log is uploaded as part of flushing
    // the "trigger_buffer_id" buffer.
    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "trigger_buffer_id");
      assert_eq!(log_upload.logs.len(), 1);

      assert_eq!(vec!["flush_with_streaming_action_id"], expected_workflow_action_ids(&log_upload.logs[0]));

      assert_eq!("provider_value_1", expected_field_value(&log_upload.logs[0], "k1"));
      assert_eq!("provider_value_2", expected_field_value(&log_upload.logs[0], "k2"));
      assert_eq!("value_3", expected_field_value(&log_upload.logs[0], "k3"));

      assert_eq!(LogType::Normal, log_type(&log_upload.logs[0]));
    });

    // Emit 20 logs that should go to a "trigger_buffer_id" but due to the streaming
    // activated by the flush buffer action above the first 10 logs ends up being redirected
    // to the `default` continuous buffer instead.
    for _ in 0 .. 19 {
      setup.log(
        log_level::DEBUG,
        LogType::Normal,
        "message that should be streamed".into(),
        vec![],
        vec![],
        None,
      );
    }

    // Confirm that the first ten out of nineteenth emitted logs ended up in `default` continuous
    // buffer.
    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid, "default");
      assert_eq!(log_upload.logs.len(), 10);
    });

    // Trigger the upload of a trigger "trigger_buffer_id" buffer.
    setup.log(
      log_level::DEBUG,
      LogType::Normal,
      "fire flush trigger buffer action!".into(),
      vec![],
      vec![],
      None,
    );

    // Confirm that the second ten out of nineteenth emitted logs ended up in `trigger_buffer_id`
    // continuous buffer. Assert for nine + the trigger log so ten logs in total.
    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid, "trigger_buffer_id");
      assert_eq!(log_upload.logs.len(), 10);

      assert_eq!("fire flush trigger buffer action!", expected_message(log_upload.logs.last().unwrap()));
    });
  }

  #[test]
  fn workflow_emit_metric_action_emits_metric() {
    let mut setup = Setup::new();

    setup.send_runtime_update(true, true, true);

    let mut a = state!("A");
    let b = state!("B");

    declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message == "fire workflow action!"));
      do action!(
        emit_counter "foo_id";
        value metric_value!(123);
        tags {
          metric_tag!(fix "fixed_key" => "fixed_value"),
          metric_tag!(extract "extraction_key_from" => "extraction_key_to")
        }
      )
    );

    // Send down a configuration with a single buffer ('default') buffer
    // and our workflow that matches on "fire workflow action!" log.
    let maybe_nack = setup.send_configuration_update(config_helper::configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![config_helper::default_buffer_config(
            Type::TRIGGER,
            make_buffer_matcher_matching_everything().into(),
          )],
          ..Default::default()
        })
        .into(),
        workflows_configuration: Some(workflows_configuration!(vec![
          workflow_proto!("workflow_1"; exclusive with a, b),
          workflow_proto!("workflow_2"; exclusive with a, b),
        ]))
        .into(),
        insights_configuration: Some(insights!(
          insight!("insight_1"),
          insight!("insight_2"),
          // "insight_3" doesn't exist on emitted logs, should be ignored.
          insight!("insight_3")
        ))
        .into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    setup.blocking_log(
      log_level::DEBUG,
      LogType::Normal,
      "fire workflow action!".into(),
      vec![
        LogField {
          key: "extraction_key_from".into(),
          value: StringOrBytes::String("extracted_value".into()),
        },
        LogField {
          key: "insight_1".to_string(),
          value: StringOrBytes::String("value_1".to_string()),
        },
        LogField {
          key: "insight_2".to_string(),
          value: StringOrBytes::String("value_2".to_string()),
        },
      ]
      .into_iter()
      .map(|field| AnnotatedLogField {
        field,
        kind: LogFieldKind::Ootb,
      })
      .collect(),
      vec![],
    );

    let stat_upload = StatsRequestHelper::new(setup.server.next_stat_upload().unwrap());
    assert_eq!(
      stat_upload.get_counter(
        "workflows_dyn:action",
        labels! {
          "_id" => "foo_id",
          "_insights" => "true",
          "fixed_key" => "fixed_value",
          "extraction_key_to" => "extracted_value",
          "insight_1" => "value_1",
          "insight_2" => "value_2",
        }
      ),
      // There are 2 emit metric actions that increment a counter with `_id=foo_id` by 123
      // but since both of them have the same action ID we dedup them and perform action only once.
      Some(123),
    );
  }

  #[test]
  fn workflow_emit_metric_action_triggers_runtime_limits() {
    let mut setup = Setup::new();

    setup.server.blocking_stream_action(
      setup.current_api_stream,
      StreamAction::SendRuntime(make_update(
        vec![
          (
            bd_runtime::runtime::stats::MaxDynamicCountersFlag::path(),
            ValueKind::Int(1),
          ),
          (
            bd_runtime::runtime::workflows::WorkflowsEnabledFlag::path(),
            ValueKind::Bool(true),
          ),
        ],
        "stats cap".to_string(),
      )),
    );

    let mut a = state!("a");
    let b = state!("b");
    let c = state!("c");

    declare_transition!(
      &mut a => &b;
      when rule!(
        log_matches!(message == "first log")
      );
      do action!(
        emit_counter "foo";
        value metric_value!(1);
        tags {
          metric_tag!(fix "fixed_key" => "fixed_value")
        }
      )
    );

    declare_transition!(
      &mut a => &c;
      when rule!(
        log_matches!(message == "second log")
      );
      do action!(emit_counter "bar"; value metric_value!(1))
    );

    let workflow = workflow_proto!("1"; exclusive with a, b, c);

    let maybe_nack = setup.send_configuration_update(configuration_update(
      "",
      StateOfTheWorld {
        workflows_configuration: Some(workflows_configuration!(vec![workflow])).into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    setup.blocking_log(
      log_level::DEBUG,
      LogType::Normal,
      "first log".into(),
      vec![],
      vec![],
    );

    setup.blocking_log(
      log_level::DEBUG,
      LogType::Normal,
      "second log".into(),
      vec![],
      vec![],
    );

    setup.send_runtime_update(false, false, true);

    let stat_upload = StatsRequestHelper::new(setup.server.next_stat_upload().unwrap());

    assert_eq!(
      stat_upload.get_counter(
        "workflows_dyn:action",
        labels!(
          "_id" => "foo",
          "fixed_key" => "fixed_value",
        )
      ),
      Some(1)
    );

    assert_eq!(
      stat_upload.get_counter("workflows_dyn:action", labels! { "_id" => "bar" }),
      None
    );

    assert_eq!(
      stat_upload.get_counter("stats:dynamic_stats_overflow", labels!()),
      Some(1)
    );
  }

  #[test]
  fn transforms_emitted_logs_according_to_filters() {
    let mut setup = Setup::new();

    setup.send_runtime_update(true, true, false);

    // Send down a configuration:
    //  * with a single buffer ('default') which accepts all logs
    //  * a single workflow which flushes all buffers when it sees a log with field "foo" equal to
    //    'fire workflow action!'
    //  * a filter that adds a field "foo" with value 'fire workflow action!'
    let maybe_nack = setup.send_configuration_update(config_helper::configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![config_helper::default_buffer_config(
            Type::TRIGGER,
            make_buffer_matcher_matching_everything().into(),
          )],
          ..Default::default()
        })
        .into(),
        workflows_configuration: Some(make_workflow_config_flushing_buffer(
          "default",
          log_matches!(tag("foo") == "fire workflow action!"),
        ))
        .into(),
        filters_configuration: Some(FiltersConfiguration {
          filters: vec![Filter {
            matcher: Some(log_matches!(message == "message")).into(),
            transforms: vec![set_field!(
              captured("foo") = field_value!("fire workflow action!")
            )],
            ..Default::default()
          }],
          ..Default::default()
        })
        .into(),
        ..Default::default()
      },
    ));

    assert!(maybe_nack.is_none());

    for _ in 0 .. 9 {
      setup.log(
        log_level::DEBUG,
        LogType::Normal,
        "message".into(),
        vec![],
        vec![],
        None,
      );
    }

    setup.log(
      log_level::DEBUG,
      LogType::Normal,
      "yet another message!".into(),
      vec![AnnotatedLogField {
        field: LogField {
          key: "foo".into(),
          value: "fire workflow action!".into(),
        },
        kind: LogFieldKind::Custom,
      }],
      vec![],
      None,
    );

    // Since there are 10 logs in the buffer at this point, we should now see an upload containing
    // 10 logs.
    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "default");
      assert_eq!(log_upload.logs.len(), 10);
      assert_eq!("fire workflow action!", expected_field_value(&log_upload.logs[9], "foo"));
      assert_eq!(vec!["flush_action_id"], expected_workflow_action_ids(&log_upload.logs[9]));
    });
  }

  #[test]
  fn remote_buffer_upload() {
    let mut setup = Setup::new();

    // Send down a configurarion with a trigger buffer ('default') which accepts all logs with no
    // local listeners that would cause it to trigger.
    let maybe_nack = setup.send_configuration_update(configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![BufferConfigBuilder {
            name: "default",
            buffer_type: Type::TRIGGER,
            filter: make_buffer_matcher_matching_everything_except_internal_logs().into(),
            non_volatile_size: 100_000,
            volatile_size: 10_000,
          }
          .build()],
          ..Default::default()
        })
        .into(),
        ..Default::default()
      },
    ));

    assert!(maybe_nack.is_none());

    // Do some logging.
    for _ in 0 .. 10 {
      setup.log(
        log_level::DEBUG,
        LogType::Normal,
        "test".into(),
        vec![],
        vec![],
        None,
      );
    }

    // No logs should be uploaded at this point.
    assert_matches!(setup.server.blocking_next_log_upload(), None);

    // Trigger a remote upload of the `default` buffer.
    setup.server.blocking_stream_action(
      setup.current_api_stream,
      StreamAction::FlushBuffers(vec!["default".to_string()]),
    );

    // We receive a log upload without intent negotiation.
    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
        assert_eq!(log_upload.logs.len(), 10);
    });
  }

  #[test]
  fn continuous_and_trigger_buffer() {
    let mut setup = Setup::new();

    // Send down a configuration with a trigger buffer ('trigger')
    // which accepts all logs and a single workflow which matches for logs
    // with the 'fire!' message in order to flush the default buffer.
    // Also send down a continuous buffer which matches a smaller subset of the logs.
    let maybe_nack = setup.send_configuration_update(configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![
            default_buffer_config(
              Type::CONTINUOUS,
              make_buffer_matcher_matching_everything_except_internal_logs().into(),
            ),
            BufferConfigBuilder {
              name: "trigger",
              buffer_type: Type::TRIGGER,
              filter: make_buffer_matcher_matching_everything_except_internal_logs().into(),
              non_volatile_size: 100_000,
              volatile_size: 10_000,
            }
            .build(),
          ],
          ..Default::default()
        })
        .into(),
        workflows_configuration: Some(make_workflow_config_flushing_buffer(
          "trigger",
          log_matches!(message == "fire!"),
        ))
        .into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    for _ in 0 .. 10 {
      setup.log(
        log_level::DEBUG,
        LogType::Normal,
        "test".into(),
        vec![],
        vec![],
        None,
      );
    }

    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "default");
      assert_eq!(log_upload.logs.len(), 10);
      assert_eq!(expected_message(&log_upload.logs[0]), "test");
    });

    setup.log(
      log_level::DEBUG,
      LogType::Normal,
      "fire!".into(),
      vec![],
      vec![],
      None,
    );

    // After writing this log we expect to see two uploads:
    // * from the trigger upload uploading
    // * from the continuous buffer uploading the single trigger line.

    assert_matches!(setup.server.next_log_intent(), Some(_intent));
    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "trigger");
      assert_eq!(log_upload.logs.len(), 10);
      assert_eq!(expected_message(&log_upload.logs[0]), "test");
    });

    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "trigger");
      assert_eq!(log_upload.logs.len(), 1);
      assert_eq!(expected_message(&log_upload.logs[0]), "fire!");
    });

    // Write an additional 9 logs to trigger an immediate continuous log batch upload, to avoid
    // waiting for the batch deadline.
    // TODO(snowp): Configurable batch timeouts or test time to better manage this kinda stuff.
    for _ in 0 .. 9 {
      setup.log(
        log_level::DEBUG,
        LogType::Normal,
        "test".into(),
        vec![],
        vec![],
        None,
      );
    }

    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "default");
      assert_eq!(log_upload.logs.len(), 10);
      assert_eq!(expected_message(&log_upload.logs[0]), "fire!");
      assert_eq!(expected_message(&log_upload.logs[1]), "test");
    });
  }

  #[test]
  fn matching_on_but_not_capturing_matching_fields() {
    let mut setup = Setup::new();

    // Send down a configuration with a trigger buffer ('trigger') which accepts all logs and a
    // single workflow which matches for logs with the 'fire!' message in order to flush the
    // default buffer.
    let maybe_nack = setup.send_configuration_update(configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![BufferConfigBuilder {
            name: "trigger",
            buffer_type: Type::TRIGGER,
            filter: make_buffer_matcher_matching_everything_except_internal_logs().into(),
            non_volatile_size: 100_000,
            volatile_size: 10_000,
          }
          .build()],
          ..Default::default()
        })
        .into(),
        workflows_configuration: Some(make_workflow_config_flushing_buffer(
          "trigger",
          log_matches!(tag("_phantom_key") == "_phantom_value"),
        ))
        .into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    for _ in 0 .. 9 {
      setup.log(
        log_level::DEBUG,
        LogType::Normal,
        "test".into(),
        vec![],
        vec![],
        None,
      );
    }

    setup.log(
      log_level::DEBUG,
      LogType::InternalSDK,
      "fire!".into(),
      vec![
        AnnotatedLogField {
          field: LogField {
            key: "_should_be_dropped_starting_with_underscore_key".into(),
            value: StringOrBytes::String("should be dropped value".into()),
          },
          kind: LogFieldKind::Custom,
        },
        AnnotatedLogField {
          field: LogField {
            key: "_key".into(),
            value: StringOrBytes::String(
              "_should_be_overridden_due_to_conflict_with_ootb_field".into(),
            ),
          },
          kind: LogFieldKind::Custom,
        },
        AnnotatedLogField {
          field: LogField {
            key: "_key".into(),
            value: StringOrBytes::String("_value".into()),
          },
          kind: LogFieldKind::Ootb,
        },
        AnnotatedLogField {
          field: LogField {
            key: "key".into(),
            value: StringOrBytes::String("value".into()),
          },
          kind: LogFieldKind::Custom,
        },
      ],
      vec![AnnotatedLogField {
        field: LogField {
          key: "_phantom_key".into(),
          value: StringOrBytes::String("_phantom_value".into()),
        },
        kind: LogFieldKind::Ootb,
      }],
      None,
    );

    // After writing this log we expect to see two uploads:
    //  1. from the trigger upload uploading
    //  2. from the continuous buffer uploading the single trigger line.
    assert_matches!(setup.server.next_log_intent(), Some(_intent));
    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "trigger");
      assert_eq!(log_upload.logs.len(), 10);

      let log = &log_upload.logs[9];
      assert_eq!(expected_message(log), "fire!");
      assert_eq!(expected_field_value(log, "key"), "value");
      assert_eq!(expected_field_value(log, "_key"), "_value");
      assert!(!exists_field_value(log, "_should_be_dropped_starting_with_underscore_key"));
      assert!(!exists_field_value(log, "_phantom_key"));
    });
  }

  #[test]
  fn log_app_update() {
    let mut setup = Setup::new();

    setup.send_configuration_update(configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![default_buffer_config(
            Type::CONTINUOUS,
            make_buffer_matcher_matching_everything_except_internal_logs().into(),
          )],
          ..Default::default()
        })
        .into(),
        ..Default::default()
      },
    ));

    setup.logger_handle.log_app_update(
      "1".to_string(),
      AppVersionExtra::BuildNumber("2".to_string()),
      Some(123),
      vec![],
      time::Duration::seconds(1),
    );
    setup.logger_handle.log_app_update(
      "2".to_string(),
      AppVersionExtra::BuildNumber("3".to_string()),
      Some(123),
      vec![],
      time::Duration::seconds(1),
    );

    for _ in 0 .. 9 {
      setup.log(
        log_level::DEBUG,
        LogType::Normal,
        "test".into(),
        vec![],
        vec![],
        None,
      );
    }

    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "default");
      assert_eq!(log_upload.logs.len(), 10);

      let app_update_log = &log_upload.logs[0];
      assert_eq!("AppUpdated", expected_message(app_update_log));
      assert_eq!("1", expected_field_value(app_update_log, "_previous_app_version"));
      assert_eq!("2", expected_field_value(app_update_log, "_previous_build_number"));

      let test_log = &log_upload.logs[1];
      assert_eq!("test", expected_message(test_log));
    });
  }

  #[test]
  fn continuous_buffer_resume_with_full_buffer() {
    let mut setup = Setup::new();

    // Start by making a simple buffer configuration with a single continuous buffer with a
    // relatively small capacity.
    let maybe_nack = setup.send_configuration_update(configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![BufferConfigBuilder {
            name: "continuous",
            buffer_type: Type::CONTINUOUS,
            filter: make_buffer_matcher_matching_everything_except_internal_logs().into(),
            non_volatile_size: 240,
            volatile_size: 200,
          }
          .build()],
          ..Default::default()
        })
        .into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    // Log a single log to the continuous buffer.
    setup.blocking_log(
      log_level::DEBUG,
      LogType::Normal,
      "test".into(),
      vec![],
      vec![],
    );

    // Shut down the logger. The bufer should now be "full" and only have a single log in place.

    let dir = setup.sdk_directory.clone();
    std::mem::drop(setup);

    // Restart the logger + server from the previous directory. This should resume us from a full
    // buffer.
    let mut setup = Setup::new_with_directory(
      dir,
      LogMetadata {
        timestamp: time::OffsetDateTime::now_utc(),
        fields: Vec::new(),
      },
    );

    setup.server.blocking_stream_action(
      setup.current_api_stream,
      StreamAction::SendRuntime(make_update(
        vec![(
          bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
          bd_test_helpers::runtime::ValueKind::Int(1),
        )],
        "base".to_string(),
      )),
    );

    // Despite being full on startup the logger is able to free up space by immediately starting
    // uploading, allowing us to capture both the new and old log.
    //
    // Note that as the first log is being uploaded, the second log is stuck in the volatile buffer
    // due to the continuous buffer blocking writes into concurrent reads (and the logs pending
    // uploads are still being read). Because of this we must first complete an upload for the
    // log taking up space in the non-volatile buffer before the second log can be written to
    // disk and be considered for an upload. In prod this would fix itself due to upload deadlines
    // for pending batches, but in test we rely on setting a low batch size to avoid the extra
    // wait.

    setup.log(
      log_level::DEBUG,
      LogType::Normal,
      "after startup".into(),
      vec![],
      vec![],
      None,
    );
    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "continuous");
      assert_eq!(log_upload.logs.len(), 1);
      assert_eq!(expected_message(&log_upload.logs[0]), "test");
    });

    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!(log_upload.buffer_uuid.as_str(), "continuous");
      assert_eq!(log_upload.logs.len(), 1);
      assert_eq!(expected_message(&log_upload.logs[0]), "after startup");
    });
  }

  #[test]
  fn runtime_update() {
    let mut setup = Setup::new();

    setup.server.blocking_stream_action(
      setup.current_api_stream,
      StreamAction::SendRuntime(make_update(
        vec![("test", ValueKind::Bool(true))],
        "something".to_string(),
      )),
    );

    let (_, update) = setup.server.blocking_next_runtime_ack();
    assert_eq!(update.last_applied_version_nonce, "something");
    assert!(update.nack.is_none());

    let snapshot = setup.logger.runtime_snapshot();
    assert!(snapshot.get_bool("test", false));
  }

  // Verifies that stat uploading works by checking that we record stats for the number of error
  // logs written (amongst other stats).
  #[test]
  fn stats_upload() {
    let mut setup = Setup::new();

    // Note that we need to send a configuration update due to how we propagate the counter for
    // error logs. As we add better suppport for log tagging this can probably be improved.
    let maybe_nack = setup.send_configuration_update(configuration_update(
      "",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![default_buffer_config(
            Type::TRIGGER,
            make_buffer_matcher_matching_everything().into(),
          )],
          ..Default::default()
        })
        .into(),
        ..Default::default()
      },
    ));
    assert!(maybe_nack.is_none());

    // Log error and info logs twice, other levels once.
    for level in &[
      log_level::ERROR,
      log_level::INFO,
      log_level::ERROR,
      log_level::INFO,
      log_level::TRACE,
      log_level::DEBUG,
      log_level::WARNING,
    ] {
      setup.log(*level, LogType::Normal, "log".into(), vec![], vec![], None);
    }

    let stats = setup.logger.stats();

    // Create an unused stat that will not be incremented.
    let _counter = stats.scope("test").counter("unused");

    // Create one stat that is incremented.
    stats.scope("test").counter("used").inc();

    setup.send_runtime_update(false, false, true);

    let stat_upload = StatsRequestHelper::new(setup.server.next_stat_upload().unwrap());
    assert_eq!(
      stat_upload.get_counter("logger:logs_received", labels! {"log_level" => "trace"}),
      Some(1),
    );
    assert_eq!(
      stat_upload.get_counter("logger:logs_received", labels! {"log_level" => "debug"}),
      Some(1),
    );
    assert_eq!(
      stat_upload.get_counter("logger:logs_received", labels! {"log_level" => "info"}),
      Some(2),
    );
    assert_eq!(
      stat_upload.get_counter("logger:logs_received", labels! {"log_level" => "warn"}),
      Some(1),
    );
    assert_eq!(
      stat_upload.get_counter("logger:logs_received", labels! {"log_level" => "error"}),
      Some(2),
    );
    assert_eq!(stat_upload.get_counter("test:used", labels! {}), Some(1));
    assert_eq!(stat_upload.get_counter("test:unused", labels! {}), None);
  }

  // Verifies end to end processing of binary messages and fields, ensuring that the binary data
  // is preserverd all the way to the test server.
  #[test]
  fn binary_message_and_fields() {
    let mut setup = Setup::new();

    setup.server.blocking_stream_action(
      setup.current_api_stream,
      StreamAction::SendRuntime(make_update(
        vec![
          (
            bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
            ValueKind::Int(1),
          ),
          (
            bd_runtime::runtime::resource_utilization::ResourceUtilizationEnabledFlag::path(),
            ValueKind::Bool(false),
          ),
        ],
        "version".to_string(),
      )),
    );

    setup.send_configuration_update(
      make_configuration_update_with_workflow_flushing_buffer_on_anything(
        "default",
        Type::CONTINUOUS,
      ),
    );

    setup.log(
      log_level::DEBUG,
      LogType::Normal,
      LogMessage::Bytes(vec![1, 2, 3]),
      vec![
        LogField {
          key: "str".into(),
          value: StringOrBytes::String("str-data".to_string()),
        },
        LogField {
          key: "binary".into(),
          value: StringOrBytes::Bytes(vec![0, 0, 0]),
        },
      ]
      .into_iter()
      .map(|field| AnnotatedLogField {
        field,
        kind: LogFieldKind::Ootb,
      })
      .collect(),
      vec![],
      None,
    );
    assert_matches!(setup.server.blocking_next_log_upload(), Some(log_upload) => {
      assert_eq!([1, 2, 3], expected_binary_message(&log_upload.logs[0]));
      assert_eq!("str-data", expected_field_value(&log_upload.logs[0], "str"));
      assert_eq!([0, 0, 0], expected_binary_field_value(&log_upload.logs[0], "binary"));
    });
  }

  #[test]
  fn logs_before_cache_load() {
    let directory = Arc::new(tempfile::TempDir::with_prefix("sdk").unwrap());
    let mut server = bd_test_helpers::test_api_server::start_server(false, None);

    let shutdown = ComponentShutdownTrigger::default();
    let network = Box::new(Setup::run_network(server.port, shutdown.make_shutdown()));

    let store = Arc::new(Store::new(Box::<InMemoryStorage>::default()));
    let device = Arc::new(bd_device::Device::new(store.clone()));

    let logger = bd_logger::LoggerBuilder::new(InitParams {
      sdk_directory: directory.path().into(),
      api_key: "foo-api-key".to_string(),
      session_strategy: Arc::new(Strategy::Fixed(fixed::Strategy::new(
        store.clone(),
        Arc::new(UUIDCallbacks),
      ))),
      metadata_provider: Arc::new(LogMetadata {
        timestamp: time::OffsetDateTime::now_utc(),
        fields: Vec::new(),
      }),
      store,
      resource_utilization_target: Box::new(EmptyTarget),
      events_listener_target: Box::new(bd_test_helpers::events::NoOpListenerTarget),
      device,
      network,
      platform: Platform::Other("integration-test", "test"),
      static_metadata: Arc::new(EmptyMetadata),
    })
    .with_mobile_features(true)
    .build_dedicated_thread()
    .unwrap()
    .0;

    let handle = logger.new_logger_handle();

    // Write logs *before* configuration arrives.

    // These first ten logs should be continuously uploaded.
    for i in 0 .. 9 {
      let msg = i.to_string();

      handle.log(
        log_level::ERROR,
        LogType::Normal,
        msg.as_str().into(),
        vec![],
        vec![],
        None,
        false,
      );
    }

    // This log should trigger a trigger buffer upload.
    handle.log(
      log_level::DEBUG,
      LogType::Normal,
      "trigger".into(),
      vec![],
      vec![],
      None,
      false,
    );

    let active_stream_id = Setup::do_stream_setup(&mut server);

    server.blocking_stream_action(
      active_stream_id,
      StreamAction::SendConfiguration(configuration_update(
        "test",
        StateOfTheWorld {
          buffer_config_list: Some(BufferConfigList {
            buffer_config: vec![
              default_buffer_config(
                Type::CONTINUOUS,
                make_buffer_matcher_matching_everything().into(),
              ),
              BufferConfigBuilder {
                name: "trigger",
                buffer_type: Type::TRIGGER,
                non_volatile_size: 100_000,
                volatile_size: 10_000,
                filter: match_message("trigger").into(),
              }
              .build(),
            ],
            ..Default::default()
          })
          .into(),
          workflows_configuration: Some(make_workflow_config_flushing_buffer(
            "trigger",
            log_matches!(message == "trigger"),
          ))
          .into(),
          ..Default::default()
        },
      )),
    );

    server.blocking_next_configuration_ack();

    // At this point we should see both a trigger upload and a continuous upload.
    let mut verify_upload = || {
      assert_matches!(server.blocking_next_log_upload(), Some(log_upload) => {
          match log_upload.buffer_uuid.as_str() {
              "trigger" => {
                  assert_eq!(log_upload.logs.len(), 1);
              }
              "default" => {
              assert_eq!(log_upload.logs.len(), 10);
              for i in 0..9 {
                  assert_eq!(expected_message(&log_upload.logs[i]), i.to_string());
              }
              }
              buffer => panic!("unknown buffer {buffer}"),
          }

          log_upload
      })
    };

    verify_upload();
    verify_upload();
  }

  #[test]
  fn runtime_caching() {
    let directory = {
      let mut setup = Setup::new();

      setup.server.blocking_stream_action(
        setup.current_api_stream,
        StreamAction::SendRuntime(make_update(
          vec![(
            bd_runtime::runtime::log_upload::BatchSizeFlag::path(),
            ValueKind::Int(1),
          )],
          "version".to_string(),
        )),
      );

      setup.server.blocking_next_runtime_ack();

      setup.sdk_directory.clone()
    };

    let retry_file = directory.path().join("runtime").join("retry_count");
    assert!(directory.path().join("runtime").join("update.pb").exists());
    assert!(retry_file.exists());
    assert_eq!(std::fs::read(&retry_file).unwrap(), b"0");

    let network = Box::new(NoopNetwork);

    // Start up a new logger that won't be able to connect to the server.
    {
      let store = Arc::new(Store::new(Box::<InMemoryStorage>::default()));
      let device = Arc::new(bd_device::Device::new(store.clone()));

      let logger = bd_logger::LoggerBuilder::new(InitParams {
        api_key: "foo-api-key".to_string(),
        network,
        platform: Platform::Other("integration-test", "test"),
        session_strategy: Arc::new(Strategy::Fixed(fixed::Strategy::new(
          store.clone(),
          Arc::new(UUIDCallbacks),
        ))),
        static_metadata: Arc::new(EmptyMetadata),
        store,
        resource_utilization_target: Box::new(EmptyTarget),
        events_listener_target: Box::new(bd_test_helpers::events::NoOpListenerTarget),
        sdk_directory: directory.path().into(),
        metadata_provider: Arc::new(LogMetadata {
          timestamp: time::OffsetDateTime::now_utc(),
          fields: Vec::new(),
        }),
        device,
      })
      .with_mobile_features(true)
      .build_dedicated_thread()
      .unwrap()
      .0;

      // The runtime configuration should use the cached value. As we load the cached config from
      // the event loop thread, there is a slight delay before we pick up on this cached value.

      let deadline = Instant::now().add(500.milliseconds());

      let mut deadline_elapsed = true;
      while Instant::now() < deadline {
        if 1
          == logger
            .runtime_snapshot()
            .get_integer(bd_runtime::runtime::log_upload::BatchSizeFlag::path(), 0)
        {
          deadline_elapsed = false;
          break;
        }
      }

      assert!(!deadline_elapsed);
    }

    assert_eq!(std::fs::read(&retry_file).unwrap(), b"1");

    // Now start another logger with the same directory, this time going through the standard
    // handshake initialization.
    let _setup = Setup::new_with_directory(
      directory,
      LogMetadata {
        timestamp: time::OffsetDateTime::now_utc(),
        fields: Vec::new(),
      },
    );

    // At this point the retry count should have been reset since we were able to verify that we
    // can connect to the backend with this runtime configuration.
    assert_eq!(std::fs::read(&retry_file).unwrap(), b"0");
  }

  fn exists_field_value(log: &[u8], key: &str) -> bool {
    let log = root_as_log(log).unwrap();

    log.fields().unwrap().iter().any(|field| field.key() == key)
  }

  // Extracts out the value of an expected field from a flatbuffer encoded log.
  fn expected_field_value<'a>(log: &'a [u8], key: &str) -> &'a str {
    let log = root_as_log(log).unwrap();

    let field_entry = log
      .fields()
      .unwrap()
      .iter()
      .find(|field| field.key() == key)
      .unwrap();

    field_entry.value_as_string_data().unwrap().data()
  }

  fn expected_stream_ids(log: &[u8]) -> Vec<String> {
    let log = root_as_log(log).unwrap();

    log
      .stream_ids()
      .unwrap()
      .iter()
      .map(ToString::to_string)
      .collect()
  }

  fn log_type(log: &[u8]) -> LogType {
    root_as_log(log).unwrap().log_type()
  }

  fn expected_binary_field_value<'a>(log: &'a [u8], key: &str) -> &'a [u8] {
    let log = root_as_log(log).unwrap();

    let field_entry = log
      .fields()
      .unwrap()
      .iter()
      .find(|field| field.key() == key)
      .unwrap();

    field_entry.value_as_binary_data().unwrap().data().bytes()
  }

  // Extracts out the message from a flatbuffer encoded log.
  fn expected_message(log: &[u8]) -> &str {
    let log = root_as_log(log).unwrap();

    log.message_as_string_data().unwrap().data()
  }

  fn expected_binary_message(log: &[u8]) -> &[u8] {
    let log = root_as_log(log).unwrap();

    log.message_as_binary_data().unwrap().data().bytes()
  }

  fn expected_session_id(log: &[u8]) -> &str {
    let log = root_as_log(log).unwrap();

    log.session_id().unwrap()
  }

  fn expected_timestamp(log: &[u8]) -> time::OffsetDateTime {
    let log = root_as_log(log).unwrap();
    let timestamp = log.timestamp().unwrap();

    #[allow(clippy::cast_lossless)]
    time::OffsetDateTime::from_unix_timestamp_nanos(
      (timestamp.seconds() as i128) * 1_000_000_000 + (timestamp.nanos() as i128),
    )
    .unwrap()
  }

  fn expected_workflow_action_ids(log: &[u8]) -> Vec<String> {
    let log = root_as_log(log).unwrap();

    log
      .workflow_action_ids()
      .map(|workflow_action_ids| {
        workflow_action_ids
          .iter()
          .map(ToString::to_string)
          .collect()
      })
      .unwrap_or_default()
  }
}
