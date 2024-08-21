// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::client_config::TailConfigurations;
use crate::memory_bound::MemorySized;
use crate::pre_config_buffer::{self, PreConfigBuffer};
use anyhow::anyhow;
use bd_api::{DataUpload, TriggerUpload};
use bd_buffer::BuffersWithAck;
use bd_client_stats::{DynamicStats, FlushTrigger};
use bd_client_stats_store::{Counter, Scope};
use bd_log_primitives::{log_level, LogLevel};
use bd_matcher::buffer_selector::BufferSelector;
use bd_runtime::runtime::ConfigLoader;
use bd_stats_common::labels;
use bd_workflows::actions_flush_buffers::BuffersToFlush;
use bd_workflows::config::WorkflowsConfiguration;
use bd_workflows::engine::{WorkflowsEngine, WorkflowsEngineConfig};
use flatbuffers::FlatBufferBuilder;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender};

//
// LoggingState
//

/// The logging state used by the `AsyncLogBuffer` to encapsulate objects
/// that are needed to process incoming logs.
#[derive(Debug)]
pub enum LoggingState<T: MemorySized + Debug> {
  /// The initial state that each `AsyncLogBuffer` starts in. While in this state
  /// the buffer takes incoming logs, populates them with extra information using
  /// its metadata provider and puts them on hold for further processing inside of
  /// a `PreConfigBuffer`. The final processing of logs is postponed until after the
  /// buffer moves to `Initialized` state.
  ///
  /// The buffer stays in `Uninitialized` state until it gets a configuration update.
  /// Configuration updates come from either a local cache (disk) or a Bitdrift control plane.
  /// While loading from a local cache is extremely fast (measured in milliseconds),
  /// the cached version of the configuration is not always available and in these
  /// cases the `AsyncLogBuffer` waits for the configuration to be fetched
  /// from the Bitdrift control plane (can potentially take seconds or even minutes).
  Uninitialized(UninitializedLoggingContext<T>),
  /// The state that `AsyncLogBuffer` moves to as soon as it receives any configuration
  /// update.
  /// While in this state the `AsyncLogBuffer` takes incoming logs, populates them with
  /// extra information its metadata provider and sends them for their final processing.
  /// The first thing that the buffer does when it moves to this state is a replay of all
  /// logs stored inside of its `PreConfigBuffer`. All replayed logs are sent for their final
  /// processing to now initialized parts of the logs processing pipeline such as workflows engine
  /// or various ring buffers.
  Initialized(InitializedLoggingContext),
}

impl<T: MemorySized + Debug> LoggingState<T> {
  pub(crate) const fn flush_buffers_trigger(&self) -> &Sender<BuffersWithAck> {
    match self {
      Self::Uninitialized(context) => &context.flush_buffers_tx,
      Self::Initialized(context) => &context.flush_buffers_tx,
    }
  }

  pub(crate) const fn flush_stats_trigger(&self) -> &Option<FlushTrigger> {
    match self {
      Self::Uninitialized(context) => &context.flush_stats_trigger,
      Self::Initialized(context) => &context.flush_stats_trigger,
    }
  }

  pub(crate) fn workflows_engine(&mut self) -> Option<&mut WorkflowsEngine> {
    match self {
      Self::Uninitialized(_) => None,
      Self::Initialized(context) => context.workflows_engine.as_mut(),
    }
  }
}

//
// UninitializedLoggingContext
//
pub struct UninitializedLoggingContext<T: MemorySized + Debug> {
  pub(crate) pre_config_log_buffer: PreConfigBuffer<T>,

  data_upload_tx: Sender<DataUpload>,
  trigger_upload_tx: Sender<TriggerUpload>,
  flush_buffers_tx: Sender<BuffersWithAck>,
  flush_stats_trigger: Option<FlushTrigger>,

  sdk_directory: PathBuf,
  pub(crate) stats: UninitializedLoggingContextStats,
  runtime: Arc<ConfigLoader>,
}

// Skip `stats` and `runtime` fields that does not implement `std::fmt::Debug`.
impl<T: MemorySized + Debug> Debug for UninitializedLoggingContext<T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("UninitializedLoggingContext")
      .field("pre_config_log_buffer", &self.pre_config_log_buffer)
      .field("trigger_upload_tx", &self.trigger_upload_tx)
      .field("flush_buffers_tx", &self.flush_buffers_tx)
      .field("flush_stats_trigger", &self.flush_stats_trigger)
      .field("sdk_directory", &self.sdk_directory)
      .finish_non_exhaustive()
  }
}

impl<T: MemorySized + Debug> UninitializedLoggingContext<T> {
  pub(crate) fn new(
    sdk_directory: &Path,
    runtime: &Arc<ConfigLoader>,
    stats: Scope,
    dynamic_stats: Arc<DynamicStats>,
    trigger_upload_tx: Sender<TriggerUpload>,
    data_upload_tx: Sender<DataUpload>,
    flush_buffers_tx: Sender<BuffersWithAck>,
    flush_stats_trigger: Option<FlushTrigger>,
    max_count: usize,
    max_size: usize,
  ) -> Self {
    Self {
      pre_config_log_buffer: PreConfigBuffer::new(max_count, max_size),
      data_upload_tx,
      trigger_upload_tx,
      flush_buffers_tx,
      flush_stats_trigger,
      sdk_directory: sdk_directory.to_owned(),
      stats: UninitializedLoggingContextStats::new(stats, dynamic_stats),
      runtime: runtime.clone(),
    }
  }

  pub(crate) async fn updated(
    self,
    config: ConfigUpdate,
    workflows_enabled: bool,
  ) -> (InitializedLoggingContext, PreConfigBuffer<T>) {
    let (workflows_engine, buffers_to_flush_rx) = if workflows_enabled {
      let (mut workflows_engine, flush_buffers_tx) = WorkflowsEngine::new(
        &self.stats.root_scope,
        &self.sdk_directory,
        &self.runtime,
        self.data_upload_tx.clone(),
        self.stats.dynamic_stats.clone(),
      );

      workflows_engine.start(WorkflowsEngineConfig::new(
        config.workflows_configuration,
        config.buffer_producers.trigger_buffer_ids.clone(),
        config.buffer_producers.continuous_buffer_ids.clone(),
      ));

      (Some(workflows_engine), Some(flush_buffers_tx))
    } else {
      (None, None)
    };

    let context = InitializedLoggingContext::new(
      config.buffer_producers,
      config.buffer_selector,
      config.tail_configs,
      self.data_upload_tx,
      self.flush_buffers_tx.clone(),
      self.flush_stats_trigger,
      workflows_engine,
      self.trigger_upload_tx.clone(),
      buffers_to_flush_rx,
      &self.runtime,
      &self.sdk_directory,
      &self.stats,
    );

    (context, self.pre_config_log_buffer)
  }
}

//
// InitializedLoggingContext
//
pub struct InitializedLoggingContext {
  pub(crate) buffer_producers: BufferProducers,
  pub(crate) buffer_selector: BufferSelector,
  pub(crate) data_upload_tx: Sender<DataUpload>,
  pub(crate) flush_buffers_tx: Sender<BuffersWithAck>,
  pub(crate) flush_stats_trigger: Option<FlushTrigger>,

  pub(crate) workflows_engine: Option<WorkflowsEngine>,

  pub(crate) tail_configs: TailConfigurations,

  pub(crate) trigger_upload_tx: Sender<TriggerUpload>,
  pub(crate) buffers_to_flush_rx: Option<Receiver<BuffersToFlush>>,

  runtime: Arc<ConfigLoader>,
  sdk_directory: PathBuf,
  pub(crate) stats: InitializedLoggingContextStats,
}

// Skip `buffer_producers`, `trigger_matcher`, `runtime`, and `stats` fields that don't implement
// `std::fmt::Debug`.
impl Debug for InitializedLoggingContext {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("InitializedLoggingContext")
      .field("workflows_engine", &self.workflows_engine)
      .field("buffer_selector", &self.buffer_selector)
      .field("flush_buffers_tx", &self.flush_buffers_tx)
      .field("flush_stats_trigger", &self.flush_stats_trigger)
      .field("trigger_upload_tx", &self.trigger_upload_tx)
      .field("sdk_directory", &self.sdk_directory)
      .finish_non_exhaustive()
  }
}

impl InitializedLoggingContext {
  fn new(
    buffer_producers: BufferProducers,
    buffer_selector: BufferSelector,
    tail_configs: TailConfigurations,
    data_upload_tx: Sender<DataUpload>,
    flush_buffers_tx: Sender<BuffersWithAck>,
    flush_stats_trigger: Option<FlushTrigger>,
    workflows_engine: Option<WorkflowsEngine>,
    trigger_upload_tx: Sender<TriggerUpload>,
    buffers_to_flush_rx: Option<Receiver<BuffersToFlush>>,
    runtime: &Arc<ConfigLoader>,
    sdk_directory: &Path,
    stats: &UninitializedLoggingContextStats,
  ) -> Self {
    Self {
      buffer_producers,
      buffer_selector,
      data_upload_tx,
      flush_buffers_tx,
      flush_stats_trigger,
      workflows_engine,
      tail_configs,
      trigger_upload_tx,
      buffers_to_flush_rx,
      runtime: runtime.clone(),
      sdk_directory: sdk_directory.to_owned(),
      stats: InitializedLoggingContextStats::new(stats),
    }
  }

  pub(crate) async fn update(&mut self, config: ConfigUpdate, workflows_enabled: bool) {
    self.buffer_selector = config.buffer_selector;
    self.buffer_producers = config.buffer_producers;
    self.tail_configs = config.tail_configs;

    if workflows_enabled {
      let workflows_engine_config = WorkflowsEngineConfig::new(
        config.workflows_configuration,
        self.buffer_producers.trigger_buffer_ids.clone(),
        self.buffer_producers.continuous_buffer_ids.clone(),
      );

      match &mut self.workflows_engine {
        Some(workflows_engine) => workflows_engine.update(workflows_engine_config),
        None => {
          let (mut engine, buffers_to_flush_rx) = WorkflowsEngine::new(
            &self.stats.root_scope,
            &self.sdk_directory,
            &self.runtime,
            self.data_upload_tx.clone(),
            self.stats.dynamic_stats.clone(),
          );
          engine.start(workflows_engine_config);
          self.workflows_engine = Some(engine);
          self.buffers_to_flush_rx = Some(buffers_to_flush_rx);
        },
      }
    } else {
      self.workflows_engine = None;
    }
  }
}

//
// UninitializedLoggingContextStats
//

pub(crate) struct UninitializedLoggingContextStats {
  pub(crate) pre_config_log_buffer: pre_config_buffer::PushCounters,
  scope: Scope,
  root_scope: Scope,
  dynamic_stats: Arc<DynamicStats>,
}

impl UninitializedLoggingContextStats {
  fn new(root_scope: Scope, dynamic_stats: Arc<DynamicStats>) -> Self {
    let stats_scope = root_scope.scope("logger");
    let pre_config_buffer_scope = stats_scope.scope("pre_config_log_buffer");

    Self {
      pre_config_log_buffer: pre_config_buffer::PushCounters::new(&pre_config_buffer_scope),
      scope: stats_scope,
      root_scope,
      dynamic_stats,
    }
  }
}

//
// InitializedLoggingContextStats
//
pub(crate) struct InitializedLoggingContextStats {
  pub(crate) log_level_counters: LogLevelCounters,
  pub(crate) streamed_logs: Counter,
  pub(crate) trigger_upload_stats: TriggerUploadStats,
  pub(crate) root_scope: Scope,
  pub(crate) dynamic_stats: Arc<DynamicStats>,
}

impl InitializedLoggingContextStats {
  fn new(stats: &UninitializedLoggingContextStats) -> Self {
    Self {
      log_level_counters: LogLevelCounters::new(&stats.scope),
      streamed_logs: stats.scope.counter("streamed_logs"),
      trigger_upload_stats: TriggerUploadStats::new(&stats.scope),
      root_scope: stats.root_scope.clone(),
      dynamic_stats: stats.dynamic_stats.clone(),
    }
  }
}

//
// TriggerUploadCounters
//

pub(crate) struct TriggerUploadStats {
  send_err_full: Counter,
  send_err_closed: Counter,
}

impl TriggerUploadStats {
  fn new(scope: &Scope) -> Self {
    Self {
      send_err_full: scope
        .counter_with_labels("send_trigger_upload", labels!("result" => "failure_full")),
      send_err_closed: scope
        .counter_with_labels("send_trigger_upload", labels!("result" => "failure_closed")),
    }
  }

  pub(crate) fn record(&self, error: &TrySendError<TriggerUpload>) {
    match error {
      TrySendError::Full(_) => {
        self.send_err_full.inc();
      },
      TrySendError::Closed(_) => {
        self.send_err_closed.inc();
      },
    }
  }
}

#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)]
pub struct LogLevelCounters {
  // We don't need dynamic labels here so we just maintain explicit counters.
  trace_counter: Counter,
  debug_counter: Counter,
  info_counter: Counter,
  warn_counter: Counter,
  error_counter: Counter,
}

impl LogLevelCounters {
  fn new(scope: &Scope) -> Self {
    Self {
      trace_counter: scope.counter_with_labels("logs_received", labels!("log_level" => "trace")),
      debug_counter: scope.counter_with_labels("logs_received", labels!("log_level" => "debug")),
      info_counter: scope.counter_with_labels("logs_received", labels!("log_level" => "info")),
      warn_counter: scope.counter_with_labels("logs_received", labels!("log_level" => "warn")),
      error_counter: scope.counter_with_labels("logs_received", labels!("log_level" => "error")),
    }
  }

  pub(crate) fn record(&self, level: LogLevel) {
    match level {
      log_level::ERROR => self.error_counter.inc(),
      log_level::WARNING => self.warn_counter.inc(),
      log_level::INFO => self.info_counter.inc(),
      log_level::DEBUG => self.debug_counter.inc(),
      log_level::TRACE => self.trace_counter.inc(),
      _ => {},
    }
  }
}

pub(crate) struct ConfigUpdate {
  pub(crate) buffer_producers: BufferProducers,
  pub(crate) buffer_selector: BufferSelector,
  pub(crate) workflows_configuration: WorkflowsConfiguration,
  pub(crate) tail_configs: TailConfigurations,
}

pub(crate) struct BufferProducers {
  pub(crate) buffers: HashMap<String, bd_buffer::Producer>,
  pub(crate) builder: FlatBufferBuilder<'static>,
  pub(crate) continuous_buffer_ids: BTreeSet<Cow<'static, str>>,
  pub(crate) trigger_buffer_ids: BTreeSet<Cow<'static, str>>,
}

impl BufferProducers {
  pub(crate) fn new(buffer_manager: &Arc<bd_buffer::Manager>) -> anyhow::Result<Self> {
    // TODO(snowp): Consider making this update logic more granular if the perf here becomes an
    // issue (e.g. only update things that changed).
    let buffers = buffer_manager
      .buffers()
      .iter()
      .map(|(id, buffer)| Ok((id.clone(), buffer.1.new_thread_local_producer()?)))
      .collect::<anyhow::Result<_>>()?;

    let mut continuous_buffer_ids = BTreeSet::new();
    let mut trigger_buffer_ids = BTreeSet::new();

    for (buffer_id, (buffer_type, _)) in buffer_manager.buffers() {
      match buffer_type {
        bd_proto::protos::config::v1::config::buffer_config::Type::CONTINUOUS => {
          continuous_buffer_ids.insert(buffer_id.clone().into());
        },
        bd_proto::protos::config::v1::config::buffer_config::Type::TRIGGER => {
          trigger_buffer_ids.insert(buffer_id.clone().into());
        },
      }
    }

    Ok(Self {
      buffers,
      builder: FlatBufferBuilder::new(),
      continuous_buffer_ids,
      trigger_buffer_ids,
    })
  }

  pub fn producer<'a>(
    buffers: &'a mut HashMap<String, bd_buffer::Producer>,
    buffer_id: &str,
  ) -> anyhow::Result<&'a mut bd_buffer::Producer> {
    buffers
      .get_mut(buffer_id)
      .ok_or_else(|| anyhow!("attempted to interact with invalid buffer: {buffer_id:?}"))
  }
}
