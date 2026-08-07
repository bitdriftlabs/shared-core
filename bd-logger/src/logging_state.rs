// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::buffer_selector::BufferSelector;
use crate::client_config::TailConfigurations;
use crate::consumer::RemoteFlushStreamingRequest;
use crate::init_buffer::{InitBuffer, InitBufferStats, Prioritizable};
use crate::log_replay::{LogReplay, ProcessingPipeline};
use crate::logger::with_thread_local_logger_guard;
use crate::metadata::MetadataCollector;
use anyhow::anyhow;
use bd_api::{DataUpload, TriggerUpload};
use bd_buffer::BuffersWithAck;
use bd_client_stats::{FlushTrigger, Stats};
use bd_client_stats_store::{Counter, Histogram, Scope};
use bd_crash_handler::global_state;
use bd_error_reporter::reporter::handle_unexpected;
use bd_log_filter::FilterChain;
use bd_log_primitives::size::MemorySized;
use bd_log_primitives::tiny_set::TinySet;
use bd_proto::protos::logging::payload::LogType;
use bd_resilient_kv::Scope as StateScope;
use bd_runtime::runtime::ConfigLoader;
use bd_session_replay::CaptureScreenshotHandler;
use bd_stats_common::{Counter as _, labels};
use bd_workflows::config::WorkflowsConfiguration;
use bd_workflows::engine::{ProcessLocalPendingFlushState, WorkflowsEngine};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use time::OffsetDateTime;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender};

//
// LoggingState
//

/// The logging state used by the `AsyncLogBuffer` to encapsulate objects
/// that are needed to process incoming logs.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum LoggingState<T: MemorySized + Prioritizable + Debug> {
  /// The initial state that each `AsyncLogBuffer` starts in. While in this state
  /// the buffer takes incoming logs, populates them with extra information using
  /// its metadata provider and puts them on hold for further processing inside of
  /// an `InitBuffer`. The final processing of logs is postponed until after the
  /// buffer moves to `Initialized` state.
  ///
  /// The buffer stays in this state until it receives a configuration update from the local
  /// cache or Bitdrift control plane. While it waits, the `InitBuffer` retains a bounded amount
  /// of startup work. Cached configuration normally arrives within milliseconds, but when it is
  /// unavailable the control-plane update can take seconds or minutes.
  Uninitialized(UninitializedLoggingContext<T>),
  /// The state that `AsyncLogBuffer` moves to as soon as it receives any configuration
  /// update.
  /// While in this state the `AsyncLogBuffer` takes incoming logs, populates them with
  /// extra information its metadata provider and sends them for their final processing.
  /// Startup work buffered in the `Uninitialized` state is replayed through the initialized
  /// pipeline after the runtime-configured `InitBuffer` replay delay. The crash handler can send
  /// a crash-pending hint before or during that delay; the separately configured crash-pending
  /// delay extends the replay deadline once. This gives prior-run crash logs time to arrive and
  /// replay ahead of current-session startup activity, so persisted workflows process the crash
  /// report first.
  Initialized(InitializedLoggingContext),
}

impl<T: MemorySized + Prioritizable + Debug> LoggingState<T> {
  pub(crate) const fn flush_buffers_trigger(&self) -> &Sender<BuffersWithAck> {
    match self {
      Self::Uninitialized(context) => &context.flush_buffers_tx,
      Self::Initialized(context) => &context.processing_pipeline.flush_buffers_tx,
    }
  }

  pub(crate) const fn flush_stats_trigger(&self) -> &FlushTrigger {
    match self {
      Self::Uninitialized(context) => &context.flush_stats_trigger,
      Self::Initialized(context) => &context.processing_pipeline.flush_stats_trigger,
    }
  }

  pub(crate) const fn workflows_engine(
    &mut self,
  ) -> Option<&mut WorkflowsEngine<Counter, Histogram>> {
    match self {
      Self::Uninitialized(_) => None,
      Self::Initialized(context) => Some(&mut context.processing_pipeline.workflows_engine),
    }
  }
}

//
// UninitializedLoggingContext
//

pub struct UninitializedLoggingContext<T: MemorySized + Prioritizable + Debug> {
  pub(crate) init_buffer: InitBuffer<T>,

  data_upload_tx: Sender<DataUpload>,
  trigger_upload_tx: Sender<TriggerUpload>,
  remote_flush_streaming_rx: Receiver<RemoteFlushStreamingRequest>,
  flush_buffers_tx: Sender<BuffersWithAck>,
  flush_stats_trigger: FlushTrigger,

  sdk_directory: PathBuf,
  pub(crate) stats: UninitializedLoggingContextStats,
  runtime: Arc<ConfigLoader>,
  is_tracing_active: Arc<AtomicBool>,
  process_local_pending_flush_state: Arc<ProcessLocalPendingFlushState>,
}

// Skip `stats` and `runtime` fields that does not implement `std::fmt::Debug`.
impl<T: MemorySized + Prioritizable + Debug> Debug for UninitializedLoggingContext<T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("UninitializedLoggingContext")
      .field("init_buffer", &self.init_buffer)
      .field("trigger_upload_tx", &self.trigger_upload_tx)
      .field("flush_buffers_tx", &self.flush_buffers_tx)
      .field("flush_stats_trigger", &self.flush_stats_trigger)
      .field("sdk_directory", &self.sdk_directory)
      .finish_non_exhaustive()
  }
}

impl<T: MemorySized + Prioritizable + Debug> UninitializedLoggingContext<T> {
  pub(crate) fn new(
    sdk_directory: &Path,
    runtime: &Arc<ConfigLoader>,
    scope: Scope,
    stats: Arc<Stats>,
    trigger_upload_tx: Sender<TriggerUpload>,
    remote_flush_streaming_rx: Receiver<RemoteFlushStreamingRequest>,
    data_upload_tx: Sender<DataUpload>,
    flush_buffers_tx: Sender<BuffersWithAck>,
    flush_stats_trigger: FlushTrigger,
    max_size: usize,
    is_tracing_active: Arc<AtomicBool>,
    process_local_pending_flush_state: Arc<ProcessLocalPendingFlushState>,
  ) -> Self {
    Self {
      init_buffer: InitBuffer::new(max_size),
      data_upload_tx,
      trigger_upload_tx,
      remote_flush_streaming_rx,
      flush_buffers_tx,
      flush_stats_trigger,
      sdk_directory: sdk_directory.to_owned(),
      stats: UninitializedLoggingContextStats::new(scope, stats),
      runtime: runtime.clone(),
      is_tracing_active,
      process_local_pending_flush_state,
    }
  }

  pub(crate) async fn updated(
    self,
    config: ConfigUpdate,
    capture_screenshot_handler: CaptureScreenshotHandler,
  ) -> (InitializedLoggingContext, InitBuffer<T>, InitBufferStats) {
    let processing_pipeline = ProcessingPipeline::new(
      self.data_upload_tx,
      self.flush_buffers_tx,
      self.flush_stats_trigger,
      self.trigger_upload_tx,
      self.remote_flush_streaming_rx,
      capture_screenshot_handler,
      config,
      &self.sdk_directory,
      &self.runtime,
      InitializedLoggingContextStats::new(&self.stats),
      self.is_tracing_active,
      self.process_local_pending_flush_state,
    )
    .await;

    let context = InitializedLoggingContext::new(processing_pipeline);

    (context, self.init_buffer, self.stats.init_buffer)
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

  pub(crate) fn update(&mut self, config: ConfigUpdate) {
    self.processing_pipeline.update(config);
  }

  /// Handles state insertion and replays the state change through workflows with global metadata.
  /// This method collects global metadata fields (like device info, app version, etc.) and passes
  /// them to the workflow engine, enabling state changes to match against and extract from global
  /// fields just like logs do.
  pub(crate) async fn handle_state_insert<R: LogReplay>(
    &mut self,
    state_store: &bd_state::Store,
    metadata_collector: &MetadataCollector,
    global_state_tracker: &mut global_state::Tracker,
    replayer: &mut R,
    scope: StateScope,
    key: String,
    value: String,
    now: OffsetDateTime,
    session_id: &str,
  ) {
    match state_store
      .insert(scope, key, bd_state::string_value(value))
      .await
    {
      Ok(Some(state_change)) => {
        // Collect global metadata fields for state changes, similar to logs.
        // We pass empty annotated fields since state changes don't have log-specific fields,
        // but we want to capture global metadata (like device info, app version, etc.)
        let metadata_result = with_thread_local_logger_guard(|| {
          metadata_collector.normalized_metadata_with_extra_fields(
            [].into(), // empty log fields
            [].into(), // empty matching fields
            LogType::INTERNAL_SDK,
            global_state_tracker,
          )
        });

        match metadata_result {
          Ok(metadata) => {
            replayer
              .replay_state_change(
                state_change,
                &mut self.processing_pipeline,
                state_store,
                now,
                session_id,
                &metadata.fields,
                &metadata.matching_fields,
              )
              .await;
          },
          Err(e) => {
            log::debug!("failed to collect metadata for state change, using empty fields: {e}");
            // Fall back to empty fields if metadata collection fails
            replayer
              .replay_state_change(
                state_change,
                &mut self.processing_pipeline,
                state_store,
                now,
                session_id,
                &[].into(),
                &[].into(),
              )
              .await;
          },
        }
      },
      Ok(None) => {},
      Err(e) => {
        handle_unexpected::<(), anyhow::Error>(Err(e), "async log buffer: failed to update state");
      },
    }
  }
}

//
// UninitializedLoggingContextStats
//

pub struct UninitializedLoggingContextStats {
  pub(crate) init_buffer: InitBufferStats,
  pub(crate) scope: Scope,
  root_scope: Scope,
  stats: Arc<Stats>,
}

impl UninitializedLoggingContextStats {
  fn new(root_scope: Scope, stats: Arc<Stats>) -> Self {
    let stats_scope = root_scope.scope("logger");

    Self {
      init_buffer: InitBufferStats::new(&stats_scope),
      scope: stats_scope,
      root_scope,
      stats,
    }
  }
}

//
// InitializedLoggingContextStats
//
pub struct InitializedLoggingContextStats {
  pub(crate) logs_received: Counter,
  pub(crate) streamed_logs: Counter,
  pub(crate) trigger_upload_stats: TriggerUploadStats,
  pub(crate) root_scope: Scope,
  pub(crate) stats: Arc<Stats>,
}

impl InitializedLoggingContextStats {
  fn new(stats: &UninitializedLoggingContextStats) -> Self {
    Self {
      logs_received: stats.scope.counter("logs_received"),
      streamed_logs: stats.scope.counter("streamed_logs"),
      trigger_upload_stats: TriggerUploadStats::new(&stats.scope),
      root_scope: stats.root_scope.clone(),
      stats: stats.stats.clone(),
    }
  }
}

//
// TriggerUploadCounters
//

pub struct TriggerUploadStats {
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

pub struct ConfigUpdate {
  pub(crate) buffer_producers: BufferProducers,
  pub(crate) buffer_selector: BufferSelector,
  pub(crate) workflows_configuration: WorkflowsConfiguration,
  pub(crate) tail_configs: TailConfigurations,
  pub(crate) filter_chain: FilterChain,
  pub(crate) from_cache: bool,
}

pub struct BufferProducers {
  pub(crate) buffers: HashMap<String, bd_buffer::Producer>,
  pub(crate) continuous_buffer_ids: TinySet<Cow<'static, str>>,
  pub(crate) trigger_buffer_ids: TinySet<Cow<'static, str>>,
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

    let mut continuous_buffer_ids = TinySet::default();
    let mut trigger_buffer_ids = TinySet::default();

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
