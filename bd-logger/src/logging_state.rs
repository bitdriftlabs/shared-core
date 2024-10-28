// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::client_config::TailConfigurations;
use crate::log_replay::ProcessingPipeline;
use crate::memory_bound::MemorySized;
use crate::pre_config_buffer::{self, PreConfigBuffer};
use anyhow::anyhow;
use bd_api::{DataUpload, TriggerUpload};
use bd_buffer::BuffersWithAck;
use bd_client_stats::{DynamicStats, FlushTrigger};
use bd_client_stats_store::{Counter, Scope};
use bd_log_filter::FilterChain;
use bd_log_primitives::{log_level, LogLevel};
use bd_matcher::buffer_selector::BufferSelector;
use bd_runtime::runtime::ConfigLoader;
use bd_session_replay::CaptureScreenshotHandler;
use bd_stats_common::labels;
use bd_workflows::config::WorkflowsConfiguration;
use bd_workflows::engine::WorkflowsEngine;
use flatbuffers::FlatBufferBuilder;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;

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
      Self::Initialized(context) => &context.processing_pipeline.flush_buffers_tx,
    }
  }

  pub(crate) const fn flush_stats_trigger(&self) -> &Option<FlushTrigger> {
    match self {
      Self::Uninitialized(context) => &context.flush_stats_trigger,
      Self::Initialized(context) => &context.processing_pipeline.flush_stats_trigger,
    }
  }

  pub(crate) fn workflows_engine(&mut self) -> Option<&mut WorkflowsEngine> {
    match self {
      Self::Uninitialized(_) => None,
      Self::Initialized(context) => context.processing_pipeline.workflows_engine.as_mut(),
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
    capture_screenshot_handler: CaptureScreenshotHandler,
  ) -> (InitializedLoggingContext, PreConfigBuffer<T>) {
    let processing_pipeline = ProcessingPipeline::new(
      self.data_upload_tx,
      self.flush_buffers_tx,
      self.flush_stats_trigger,
      self.trigger_upload_tx,
      capture_screenshot_handler,
      config,
      self.sdk_directory.clone(),
      self.runtime,
      InitializedLoggingContextStats::new(&self.stats),
    );

    let context = InitializedLoggingContext::new(processing_pipeline);

    (context, self.pre_config_log_buffer)
  }
}

//
// InitializedLoggingContext
//
pub struct InitializedLoggingContext {
  pub(crate) processing_pipeline: ProcessingPipeline,
}

// Skip `buffer_producers`, `trigger_matcher`, `runtime`, and `stats` fields that don't implement
// `std::fmt::Debug`.
impl Debug for InitializedLoggingContext {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("InitializedLoggingContext")
      .finish_non_exhaustive()
  }
}

impl InitializedLoggingContext {
  const fn new(processing_pipeline: ProcessingPipeline) -> Self {
    Self {
      processing_pipeline,
    }
  }

  pub(crate) async fn update(&mut self, config: ConfigUpdate) {
    self.processing_pipeline.update(config);
  }

  #[cfg(test)]
  pub(crate) const fn workflows_engine(&self) -> Option<&WorkflowsEngine> {
    self.processing_pipeline.workflows_engine.as_ref()
  }
}

//
// UninitializedLoggingContextStats
//

pub(crate) struct UninitializedLoggingContextStats {
  pub(crate) pre_config_log_buffer: pre_config_buffer::PushCounters,
  pub(crate) scope: Scope,
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
  pub(crate) filter_chain: FilterChain,
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
