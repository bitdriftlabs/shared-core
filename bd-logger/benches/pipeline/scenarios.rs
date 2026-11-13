// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Replays the anonymized `large` workflow-bench corpus through the logger's per-log path, with
//! the corpus's own configuration: its buffers and their filters, its filter chain and its
//! workflows.
//!
//! Exported logs carry every field the SDK attached, so the scenario splits them the way the SDK
//! produced them. A field on every log is a platform field: SDK-defined names (reserved names and
//! names starting with `_`) are OOTB fields and the rest are custom fields the app set once. Every
//! other field belongs to the log. `FieldSource` chooses whether platform fields come from the
//! field provider on each log or are persistent fields updated only when they change, which is
//! what field interning applies to.

use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  DataValue,
  LogFieldValue,
  LogFields,
  LogLevel,
  LogLine,
  log_level,
};
use bd_logger::MetadataProvider;
use bd_logger::benchmark::{PipelineBenchmark, PlatformParts};
use bd_proto::protos::client::api::ApiResponse;
use bd_proto::protos::client::api::api_response::Response_type;
use bd_proto::protos::client::api::configuration_update::StateOfTheWorld;
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag as _, field_interning};
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_time::TestTimeProvider;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::io::Read as _;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use tempfile::TempDir;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// SDK-defined field names that do not start with `_`.
const RESERVED_OOTB_FIELDS: &[&str] = &[
  "app_id",
  "app_version",
  "carrier",
  "foreground",
  "model",
  "network_type",
  "os",
  "os_version",
  "radio_type",
];

#[derive(Clone, Copy, Debug)]
pub enum FieldSource {
  /// The field provider returns every platform field on every log.
  Provider,
  /// Platform fields are persistent fields, updated when they change.
  Persistent,
}

/// Messages above this size are a small share of the corpus's logs but most of its bytes.
pub const LARGE_MESSAGE_BYTES: usize = 10 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct Scenario {
  pub fields: FieldSource,
  pub interning: bool,
  /// Skips logs whose message is larger than `LARGE_MESSAGE_BYTES`, to show the cost of a typical
  /// log apart from the few large ones.
  pub skip_large_messages: bool,
}

impl Scenario {
  #[must_use]
  pub const fn new(fields: FieldSource, interning: bool) -> Self {
    Self {
      fields,
      interning,
      skip_large_messages: false,
    }
  }

  #[must_use]
  pub const fn skipping_large_messages(mut self) -> Self {
    self.skip_large_messages = true;
    self
  }
}

//
// Corpus
//

struct CorpusLog {
  occurred_at: OffsetDateTime,
  log_level: LogLevel,
  log_type: LogType,
  message: String,
  fields: LogFields,
}

struct Corpus {
  state_of_the_world: StateOfTheWorld,
  logs: Vec<CorpusLog>,
  platform_keys: HashSet<String>,
}

fn fixture(name: &str) -> String {
  let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    .join("../bd-workflow-bench/fixtures/large")
    .join(name);
  let mut contents = String::new();
  zstd::Decoder::new(std::fs::File::open(&path).unwrap())
    .unwrap()
    .read_to_string(&mut contents)
    .unwrap();
  contents
}

fn string_field(value: &serde_json::Value) -> LogFieldValue {
  let string = value
    .get("string_data")
    .and_then(serde_json::Value::as_str)
    .expect("the corpus has only string fields");
  DataValue::String(string.to_string())
}

static CORPUS: LazyLock<Corpus> = LazyLock::new(|| {
  let response: ApiResponse = protobuf_json_mapping::parse_from_str(&fixture("config.json.zst"))
    .expect("valid configuration");
  let Some(Response_type::ConfigurationUpdate(mut update)) = response.response_type else {
    panic!("the corpus configuration is a configuration update");
  };
  let state_of_the_world = update.take_state_of_the_world();

  let logs: Vec<CorpusLog> = fixture("logs.ndjson.zst")
    .lines()
    .filter(|line| !line.trim().is_empty())
    .map(|line| {
      let record: serde_json::Value = serde_json::from_str(line).unwrap();
      let fields = record["fields"]["fields"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone().into(), string_field(value)))
        .collect();
      CorpusLog {
        occurred_at: OffsetDateTime::parse(record["timestamp"].as_str().unwrap(), &Rfc3339)
          .unwrap(),
        log_level: match record["log_level"].as_str().unwrap() {
          "TRACE" => log_level::TRACE,
          "DEBUG" => log_level::DEBUG,
          "INFO" => log_level::INFO,
          "WARN" | "WARNING" => log_level::WARNING,
          "ERROR" => log_level::ERROR,
          level => panic!("unknown log level {level}"),
        },
        log_type: match record["log_type"].as_str().unwrap_or("NORMAL") {
          "NORMAL" => LogType::NORMAL,
          "LIFECYCLE" => LogType::LIFECYCLE,
          "DEVICE" => LogType::DEVICE,
          "UX" => LogType::UX,
          "SPAN" => LogType::SPAN,
          log_type => panic!("unknown log type {log_type}"),
        },
        message: record["message"].as_str().unwrap().to_string(),
        fields,
      }
    })
    .collect();

  let mut counts: HashMap<&str, usize> = HashMap::new();
  for log in &logs {
    for key in log.fields.keys() {
      *counts.entry(key.as_ref()).or_default() += 1;
    }
  }
  let platform_keys = counts
    .into_iter()
    .filter(|(_, count)| *count == logs.len())
    .map(|(key, _)| key.to_string())
    .collect();

  Corpus {
    state_of_the_world,
    logs,
    platform_keys,
  }
});

fn is_ootb(key: &str) -> bool {
  key.starts_with('_') || RESERVED_OOTB_FIELDS.contains(&key)
}

//
// Provider
//

/// A field provider that returns the platform fields of the log being admitted.
struct Provider {
  current: Mutex<(OffsetDateTime, LogFields, LogFields)>,
}

impl MetadataProvider for Provider {
  fn timestamp(&self) -> anyhow::Result<OffsetDateTime> {
    Ok(self.current.lock().0)
  }

  fn fields(&self) -> anyhow::Result<(LogFields, LogFields)> {
    let current = self.current.lock();
    Ok((current.2.clone(), current.1.clone()))
  }
}

//
// Step
//

/// One corpus log, prepared for admission.
struct Step {
  occurred_at: OffsetDateTime,
  /// Platform fields the provider returns, OOTB then custom.
  provider: (LogFields, LogFields),
  /// Persistent fields that changed since the previous log.
  updates: Vec<(String, LogFieldValue, bool)>,
  log: Option<LogLine>,
}

fn steps(scenario: Scenario) -> Vec<Step> {
  let corpus = &*CORPUS;
  let fields = scenario.fields;
  let mut persistent: HashMap<&str, &LogFieldValue> = HashMap::new();
  corpus
    .logs
    .iter()
    .filter(|log| !scenario.skip_large_messages || log.message.len() <= LARGE_MESSAGE_BYTES)
    .map(|log| {
      let mut provider = (LogFields::default(), LogFields::default());
      let mut updates = Vec::new();
      let mut log_fields = AnnotatedLogFields::default();
      for (key, value) in &log.fields {
        let ootb = is_ootb(key);
        if corpus.platform_keys.contains(key.as_ref()) {
          match fields {
            FieldSource::Provider => {
              let target = if ootb {
                &mut provider.0
              } else {
                &mut provider.1
              };
              target.insert(key.clone(), value.clone());
            },
            FieldSource::Persistent => {
              if persistent.insert(key.as_ref(), value) != Some(value) {
                updates.push((key.to_string(), value.clone(), ootb));
              }
            },
          }
        } else {
          let field = if ootb {
            AnnotatedLogField::new_ootb(value.clone())
          } else {
            AnnotatedLogField::new_custom(value.clone())
          };
          log_fields.insert(key.clone(), field);
        }
      }
      Step {
        occurred_at: log.occurred_at,
        provider,
        updates,
        log: Some(LogLine {
          log_level: log.log_level,
          log_type: log.log_type,
          message: log.message.as_str().into(),
          fields: log_fields,
          matching_fields: AnnotatedLogFields::default(),
          command_attachment: None,
          attributes_overrides: None,
          capture_session: None,
        }),
      }
    })
    .collect()
}

//
// PipelineSetup
//

/// A logger pipeline configured from the corpus, and the corpus logs to replay through it.
pub struct PipelineSetup {
  // Declared first so it is dropped before the directory it writes to.
  pipeline: PipelineBenchmark,
  provider: Arc<Provider>,
  time: Arc<TestTimeProvider>,
  steps: Vec<Step>,
  _directory: TempDir,
}

impl PipelineSetup {
  #[must_use]
  pub fn new(scenario: Scenario) -> Self {
    let corpus = &*CORPUS;
    let directory = TempDir::new().unwrap();
    let runtime_loader = ConfigLoader::new(directory.path());
    tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .unwrap()
      .block_on(runtime_loader.update_snapshot(make_simple_update(vec![(
        field_interning::Enabled::path(),
        ValueKind::Bool(scenario.interning),
      )])))
      .unwrap();

    let provider = Arc::new(Provider {
      current: Mutex::new((
        corpus.logs[0].occurred_at,
        LogFields::default(),
        LogFields::default(),
      )),
    });
    let time = Arc::new(TestTimeProvider::new(corpus.logs[0].occurred_at));
    let pipeline = PipelineBenchmark::new(
      directory.path(),
      &runtime_loader,
      corpus.state_of_the_world.clone(),
      PlatformParts {
        metadata_provider: provider.clone(),
        session_strategy: bd_session::test::no_timeout(directory.path()).strategy(),
        store: bd_test_helpers::session::in_memory_store(),
        time_provider: time.clone(),
        resource_utilization_target: Box::new(bd_test_helpers::resource_utilization::EmptyTarget),
        session_replay_target: Box::new(bd_test_helpers::session_replay::NoOpTarget),
        events_listener_target: Box::new(bd_test_helpers::events::NoOpListenerTarget),
      },
    )
    .unwrap();

    Self {
      pipeline,
      provider,
      time,
      steps: steps(scenario),
      _directory: directory,
    }
  }

  #[must_use]
  pub fn len(&self) -> usize {
    self.steps.len()
  }

  /// Applies the persistent-field updates and provider fields for log `index`.
  pub fn prepare(&mut self, index: usize) {
    let step = &mut self.steps[index];
    for (key, value, ootb) in step.updates.drain(..) {
      if ootb {
        self.pipeline.update_ootb_field(&key, value).unwrap();
      } else {
        self.pipeline.add_field(&key, value).unwrap();
      }
    }
    self.time.set_time(step.occurred_at);
    *self.provider.current.lock() = (
      step.occurred_at,
      std::mem::take(&mut step.provider.0),
      std::mem::take(&mut step.provider.1),
    );
  }

  /// Admits log `index` on the caller's thread.
  pub fn admit(&mut self, index: usize) {
    let log = self.steps[index].log.take().unwrap();
    self.pipeline.admit(log).unwrap();
  }

  /// Processes the admitted log on the event loop.
  pub fn process(&mut self) {
    self.pipeline.process_next().unwrap();
  }
}
