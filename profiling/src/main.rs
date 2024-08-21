// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

mod paths;

use crate::paths::PATHS;
use bd_client_stats::{DynamicStats, Stats};
use bd_client_stats_store::{Collector, Scope};
use bd_log_primitives::{log_level, FieldsRef, LogField, LogLevel, LogMessage, LogRef};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_proto::protos::client::api::RuntimeUpdate;
use bd_proto::protos::workflow::workflow;
use bd_proto::protos::workflow::workflow::workflow::execution::{
  ExecutionExclusive,
  Execution_type,
};
use bd_proto::protos::workflow::workflow::workflow::{Execution, State};
use bd_proto::protos::workflow::workflow::Workflow;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_shutdown::ComponentShutdownTrigger;
use bd_stats_common::labels;
use bd_test_helpers::workflow::macros::{
  action,
  declare_transition,
  insight,
  insights,
  log_matches,
  metric_tag,
  metric_value,
  rule,
  state,
  workflows_configuration,
};
use bd_time::TimeDurationExt;
use bd_workflows::config::WorkflowsConfiguration;
use bd_workflows::engine::{WorkflowsEngine, WorkflowsEngineConfig};
use flate2::bufread::ZlibEncoder;
use flate2::Compression;
use rand::Rng;
use sha2::Digest;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use time::ext::NumericalDuration;
use time::OffsetDateTime;

struct WorkflowConfigurationsInit {
  workflow_states: Vec<Vec<State>>,
}

impl WorkflowConfigurationsInit {
  const fn new() -> Self {
    Self {
      workflow_states: Vec::new(),
    }
  }

  fn push(&mut self, states: Vec<State>) {
    self.workflow_states.push(states);
  }

  fn configs(self) -> Vec<Workflow> {
    self
      .workflow_states
      .into_iter()
      .enumerate()
      .map(|(index, states)| workflow::Workflow {
        id: index.to_string(),
        states,
        execution: Some(Execution {
          execution_type: Some(Execution_type::ExecutionExclusive(
            ExecutionExclusive::default(),
          )),
          ..Default::default()
        })
        .into(),
        limit_matched_logs_count: None.into(),
        limit_duration: None.into(),
        ..Default::default()
      })
      .collect()
  }
}

struct AnnotatedWorkflowsEngine {
  engine: WorkflowsEngine,
}

impl AnnotatedWorkflowsEngine {
  fn new(
    directory: &Path,
    runtime_loader: &Arc<ConfigLoader>,
    scope: &Scope,
    dynamic_stats: Arc<DynamicStats>,
  ) -> Self {
    let (data_tx, _data_rx) = tokio::sync::mpsc::channel(1);

    let (mut engine, _) =
      WorkflowsEngine::new(scope, directory, runtime_loader, data_tx, dynamic_stats);

    let mut workflow_configurations = WorkflowConfigurationsInit::new();
    Self::create_general_health_workflows(&mut workflow_configurations);
    Self::create_networking_workflows(&mut workflow_configurations);

    engine.start(WorkflowsEngineConfig::new(
      WorkflowsConfiguration::new(
        &workflows_configuration!(workflow_configurations.configs()),
        &insights!(
          insight!("app_id"),
          insight!("app_version"),
          insight!("os"),
          insight!("os_version"),
          insight!("foreground"),
          insight!("model"),
          insight!("radio_type"),
          insight!("network_type"),
          insight!("_locale")
        ),
      ),
      BTreeSet::default(),
      BTreeSet::default(),
    ));

    Self { engine }
  }

  fn process_log(
    &mut self,
    log_level: LogLevel,
    message: &str,
    extra_fields: BTreeMap<&str, &str>,
  ) {
    let mut fields = Self::get_default_fields();

    for (key, value) in extra_fields {
      fields.push(LogField {
        key: key.to_string(),
        value: value.into(),
      });
    }

    self.engine.process_log(
      &LogRef {
        log_type: LogType::Normal,
        log_level,
        message: &LogMessage::String(message.to_string()),
        fields: &FieldsRef::new(&fields, &vec![]),
        session_id: "1231231231312312312312",
        occurred_at: OffsetDateTime::now_utc(),
      },
      &BTreeSet::new(),
    );
  }

  fn get_default_fields() -> Vec<LogField> {
    let mut fields = vec![
      LogField {
        key: "app_id".to_string(),
        value: "io.bitdrift.app.great_app".into(),
      },
      LogField {
        key: "app_id".to_string(),
        value: "io.bitdrift.app.great_app".into(),
      },
      LogField {
        key: "app_version".to_string(),
        value: "1.0.0".into(),
      },
      LogField {
        key: "os".to_string(),
        value: "android".into(),
      },
      LogField {
        key: "os_version".to_string(),
        value: "10".into(),
      },
      LogField {
        key: "model".to_string(),
        value: "Pixel 4".into(),
      },
      LogField {
        key: "radio_type".to_string(),
        value: "CTRadioAccessTechnologyGPRS".into(),
      },
      LogField {
        key: "network_type".to_string(),
        value: "WIFI".into(),
      },
      LogField {
        key: "_locale".to_string(),
        value: "en_US".into(),
      },
    ];

    let mut rng = rand::thread_rng();
    let generated = rng.gen::<u32>() % 100;

    let foreground = if generated < 75 { "true" } else { "false" };
    fields.push(LogField {
      key: "foreground".to_string(),
      value: foreground.into(),
    });

    fields
  }

  fn create_networking_workflows(workflow_configurations: &mut WorkflowConfigurationsInit) {
    let mut a = state!("A");
    let b = state!("B");
    declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message == "HTTPRequest"));
      do action!(emit_counter &Self::generate_action_id(); value metric_value!(1))
    );
    workflow_configurations.push(vec![a, b]);

    let mut a = state!("A");
    let b = state!("B");
    declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message == "HTTPResponse"));
      do action!(
        emit_counter &Self::generate_action_id();
        value metric_value!(1);
        tags {
          metric_tag!(extract "result" => "result"),
          metric_tag!(extract "status_code" => "status_code")
        }
      )
    );
    workflow_configurations.push(vec![a, b]);

    let mut a = state!("A");
    let b = state!("B");
    declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message == "HTTPResponse"));
      do action!(
        emit_counter &Self::generate_action_id();
        value metric_value!(1);
        tags {
          metric_tag!(extract "result" => "result")
        }
      )
    );
    workflow_configurations.push(vec![a, b]);

    let mut a = state!("A");
    let b = state!("B");
    declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message == "HTTPResponse"));
      do action!(
        emit_counter &Self::generate_action_id();
        value metric_value!(1);
        tags {
          metric_tag!(extract "result" => "result"),
          metric_tag!(extract "status_code" => "status_code")
        }
      )
    );
    workflow_configurations.push(vec![a, b]);

    let mut a = state!("A");
    let b = state!("B");
    declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message == "HTTPResponse"));
      do action!(
        emit_counter &Self::generate_action_id();
        value metric_value!(extract "body_bytes_sent_count");
        tags {
          metric_tag!(extract "result" => "result"),
          metric_tag!(extract "status_code" => "status_code")
        }
      )
    );
    workflow_configurations.push(vec![a, b]);

    let mut a = state!("A");
    let b = state!("B");
    declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message == "HTTPResponse"));
      do action!(
        emit_counter &Self::generate_action_id();
        value metric_value!(extract "body_bytes_sent_count");
        tags {
          metric_tag!(extract "path" => "path")
        }
      )
    );
    workflow_configurations.push(vec![a, b]);

    let mut a = state!("A");
    let b = state!("B");
    declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message == "HTTPResponse"));
      do action!(
        emit_counter &Self::generate_action_id();
        value metric_value!(extract "body_bytes_received_count");
        tags {
          metric_tag!(extract "path" => "path")
        }
      )
    );
    workflow_configurations.push(vec![a, b]);

    let mut a = state!("A");
    let b = state!("B");
    declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message == "HTTPResponse"));
      do action!(
        emit_counter &Self::generate_action_id();
        value metric_value!(extract "duration_ms");
        tags {
          metric_tag!(extract "path" => "path")
        }
      )
    );
    workflow_configurations.push(vec![a, b]);
  }

  fn create_general_health_workflows(workflow_configurations: &mut WorkflowConfigurationsInit) {
    let mut a = state!("A");
    let b = state!("B");
    declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message == "SceneDidActivate"));
      do action!(emit_counter &Self::generate_action_id(); value metric_value!(1))
    );
    workflow_configurations.push(vec![a, b]);

    let mut a = state!("A");
    let b = state!("B");
    declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message ~= ".*"));
      do action!(
        emit_counter &Self::generate_action_id();
        value metric_value!(1);
        tags { metric_tag!(extract "log_level" => "log_level") }
      )
    );
    workflow_configurations.push(vec![a, b]);
  }

  fn generate_action_id() -> String {
    let mut rng = rand::thread_rng();
    let generated: u32 = rng.gen();

    let mut hasher = sha2::Sha256::new();
    sha2::Digest::update(&mut hasher, generated.to_be_bytes());

    base64_url::encode(&hasher.finalize())
  }
}

struct Setup {
  directory: Arc<tempdir::TempDir>,
  runtime_loader: Arc<ConfigLoader>,
  dynamic_stats: Arc<DynamicStats>,
  stats: Arc<Stats>,
}

impl Setup {
  fn new() -> Self {
    let directory = Arc::new(tempdir::TempDir::new("stats-benches-").unwrap());
    let runtime_loader = ConfigLoader::new(directory.path());

    let dynamic_stats = Arc::new(DynamicStats::new(
      &Collector::default().scope("test"),
      &runtime_loader,
    ));
    let stats = Stats::new(Collector::default(), dynamic_stats.clone());

    Self {
      directory,
      runtime_loader,
      dynamic_stats,
      stats,
    }
  }

  #[allow(dead_code)]
  fn create_engine(&self) -> AnnotatedWorkflowsEngine {
    AnnotatedWorkflowsEngine::new(
      self.directory.path(),
      &self.runtime_loader,
      &self.stats.scope(""),
      self.dynamic_stats.clone(),
    )
  }

  async fn run_stats_flush_handler(&self) {
    let shutdown_trigger = ComponentShutdownTrigger::default();
    let (data_tx, _data_rx) = tokio::sync::mpsc::channel(1);

    let flush_handles = self
      .stats
      .flush_handle(
        &self.runtime_loader,
        shutdown_trigger.make_shutdown(),
        self.directory.path(),
        data_tx,
      )
      .unwrap();

    let flush_handle = tokio::spawn(async move {
      flush_handles.flusher.periodic_flush().await.unwrap();
    });

    1.seconds().sleep().await;

    // Need to drop to allow for the shutdown to complete.
    drop(flush_handles.uploader);

    shutdown_trigger.shutdown().await;

    flush_handle.await.unwrap();
  }

  fn aggregated_stats_file_path(&self) -> PathBuf {
    self.directory.path().join("aggregated_stats.pb")
  }
}


fn run_profiling<T: Fn(&mut AnnotatedWorkflowsEngine) + std::marker::Send + 'static>(
  setup: Setup,
  f: T,
) {
  std::thread::spawn(move || {
    tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .unwrap()
      .block_on(async {
        setup.runtime_loader.update_snapshot(&RuntimeUpdate {
          version_nonce: "123".to_string(),
          runtime: Some(bd_test_helpers::runtime::make_proto(vec![
            (
              bd_runtime::runtime::stats::DirectStatFlushIntervalFlag::path(),
              bd_test_helpers::runtime::ValueKind::Int(900),
            ),
            (
              bd_runtime::runtime::workflows::WorkflowsInsightsEnabledFlag::path(),
              bd_test_helpers::runtime::ValueKind::Bool(true),
            ),
          ]))
          .into(),
          ..Default::default()
        });

        // Given the runtime configuration update time to propagate.
        1.seconds().sleep().await;

        let mut engine = setup.create_engine();
        f(&mut engine);

        // Let the stats flush to disk to run.
        setup.run_stats_flush_handler().await;

        // Query the size of the file on a disk.
        let file_path = setup.aggregated_stats_file_path();
        let metadata = fs::metadata(file_path.clone()).unwrap();

        // Compress the file too check its size of the data after the compression.
        let file = File::open(file_path).unwrap();
        let buffer = BufReader::new(file);
        // `5` is the compression level we use for compressing traffic in prod.
        let mut encoder = ZlibEncoder::new(buffer, Compression::new(5));
        let mut buffer = Vec::new();
        encoder.read_to_end(&mut buffer).unwrap();

        log::info!(
          "++ The size of the `aggregated_stats.pb` file {:?} bytes (after compression {:?} \
           bytes).",
          metadata.size(),
          buffer.len(),
        );
      });
  })
  .join()
  .unwrap();
}

fn run_network_requests_profiling() {
  log::info!("++++ Running network requests profiling:");
  log::info!(
    "Each profiling session simulates performing a given number of network requests.
    Each of the simulated request/response pairs uses one of the paths from the pool of 200 \
     predefined paths."
  );

  run_network_requests_profiling_with_request_count(20);
  run_network_requests_profiling_with_request_count(50);
  run_network_requests_profiling_with_request_count(100);
  run_network_requests_profiling_with_request_count(200);
  run_network_requests_profiling_with_request_count(500);
  run_network_requests_profiling_with_request_count(1_000);
}

fn run_network_requests_profiling_with_request_count(network_request_count: u32) {
  log::info!("+++ {:?} network requests:", network_request_count);
  run_profiling(Setup::new(), move |engine| {
    engine.process_log(log_level::INFO, "SceneDidActivate", labels! {});

    for _ in 0 .. network_request_count {
      let mut rng = rand::thread_rng();

      #[allow(clippy::cast_possible_truncation)]
      let path_index: u32 = rng.gen::<u32>() % (PATHS.len() as u32);

      engine.process_log(
        log_level::DEBUG,
        "HTTPRequest",
        labels! { "path" => PATHS[path_index as usize] },
      );

      let duration_ms = (rng.gen::<u32>() % 5_000).to_string();
      let body_bytes_sent = (rng.gen::<u32>() % 10_000).to_string();
      let body_bytes_received = (rng.gen::<u32>() % 10_000).to_string();

      // 85% success rate.
      let is_success = rng.gen::<u32>() % 100 < 85;
      let result = if is_success { "success" } else { "failure" };
      let status_code = if is_success { "200" } else { "500" };

      engine.process_log(
        log_level::DEBUG,
        "HTTPResponse",
        labels! {
          "body_bytes_sent_count" => body_bytes_sent.as_str(),
          "body_bytes_received_count" => body_bytes_received.as_str(),
          "path" => PATHS[path_index as usize],
          "result" => result,
          "status_code" => status_code,
          "duration_ms" => duration_ms.as_str(),
        },
      );
    }
  });
}

#[tokio::main]
pub async fn main() {
  bd_log::SwapLogger::initialize();

  run_network_requests_profiling();
}
