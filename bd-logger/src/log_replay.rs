// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::buffer_selector::BufferSelector;
use crate::client_config::TailConfigurations;
use crate::logging_state::{BufferProducers, ConfigUpdate, InitializedLoggingContextStats};
use crate::write_log_to_buffer;
use bd_api::{DataUpload, TriggerUpload};
use bd_buffer::BuffersWithAck;
use bd_client_stats::{FlushTrigger, FlushTriggerRequest};
use bd_log_filter::FilterChain;
use bd_log_metadata::LogFields;
use bd_log_primitives::tiny_set::TinySet;
use bd_log_primitives::{FieldsRef, Log, LogEncodingHelper, LogMessage, LossyIntToU32, log_level};
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::log_upload::MinLogCompressionSize;
use bd_runtime::runtime::{ConfigLoader, IntWatch};
use bd_session_replay::CaptureScreenshotHandler;
use bd_workflows::actions_flush_buffers::BuffersToFlush;
use bd_workflows::config::FlushBufferId;
use bd_workflows::engine::{WorkflowsEngine, WorkflowsEngineConfig};
use bd_workflows::workflow::{WorkflowDebugStateMap, WorkflowEvent};
use itertools::Itertools;
use protobuf::CodedOutputStream;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::path::Path;
use time::OffsetDateTime;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Default)]
pub struct LogReplayResult {
  pub logs_to_inject: Vec<Log>,
  pub workflow_debug_state: Vec<(String, WorkflowDebugStateMap)>,
  pub engine_has_debug_workflows: bool,
}

//
// LogReplay
//

// An abstraction for logger replay. It's a layer of indirection that's supposed to make testing
// easier.
#[async_trait::async_trait]
pub trait LogReplay {
  // Replays logs for further processing.
  // In production code, this method acts as an indirection layer that
  // takes in a log and a processing pipeline, using the pipeline to process the log.
  // In tests, this method can be used to capture replayed logs for confirmation that they look as
  // expected.
  async fn replay_log(
    &mut self,
    log: Log,
    block: bool,
    pipeline: &mut ProcessingPipeline,
    state: &bd_state::Store,
    now: OffsetDateTime,
  ) -> anyhow::Result<LogReplayResult>;

  // Replays state changes for further processing.
  async fn replay_state_change(
    &mut self,
    state_change: bd_state::StateChange,
    pipeline: &mut ProcessingPipeline,
    state: &bd_state::Store,
    now: OffsetDateTime,
    session_id: &str,
    fields: &LogFields,
    matching_fields: &LogFields,
  ) -> LogReplayResult;
}

//
// LoggerReplay
//

pub struct LoggerReplay;

#[async_trait::async_trait]
impl LogReplay for LoggerReplay {
  async fn replay_log(
    &mut self,
    log: Log,
    block: bool,
    pipeline: &mut ProcessingPipeline,
    state_store: &bd_state::Store,
    now: OffsetDateTime,
  ) -> anyhow::Result<LogReplayResult> {
    pipeline.process_log(log, state_store, block, now).await
  }

  async fn replay_state_change(
    &mut self,
    state_change: bd_state::StateChange,
    pipeline: &mut ProcessingPipeline,
    state: &bd_state::Store,
    now: OffsetDateTime,
    session_id: &str,
    fields: &LogFields,
    matching_fields: &LogFields,
  ) -> LogReplayResult {
    pipeline
      .process_state_change(
        state_change,
        state,
        now,
        session_id,
        fields,
        matching_fields,
      )
      .await
  }
}

//
// ProcessingPipeline
//

// Maintains the machinery required to process incoming logs.
// Processing a log involves (not a full list):
//  * modifying it according to active filter chain.
//  * processing it using workflows engine.
//  * potentially streaming it according to bd tail configuration.
//  * writing it to buffers according to active buffer selector(s).
pub struct ProcessingPipeline {
  buffer_producers: BufferProducers,
  buffer_selector: BufferSelector,
  pub(crate) workflows_engine: WorkflowsEngine,
  tail_configs: TailConfigurations,
  filter_chain: FilterChain,
  min_log_compression_size: IntWatch<MinLogCompressionSize>,

  pub(crate) flush_buffers_tx: Sender<BuffersWithAck>,
  pub(crate) flush_stats_trigger: FlushTrigger,

  // The channel used to signal to the buffer consumer that it should flush the buffers.
  trigger_upload_tx: Sender<TriggerUpload>,
  // The channel used to receive a signal from the workflows engine that it should flush the
  // buffers.
  buffers_to_flush_rx: Receiver<BuffersToFlush>,
  capture_screenshot_handler: CaptureScreenshotHandler,

  stats: InitializedLoggingContextStats,
}

impl ProcessingPipeline {
  pub(crate) async fn new(
    data_upload_tx: Sender<DataUpload>,
    flush_buffers_tx: Sender<BuffersWithAck>,
    flush_stats_trigger: FlushTrigger,
    trigger_upload_tx: Sender<TriggerUpload>,
    capture_screenshot_handler: CaptureScreenshotHandler,

    config: ConfigUpdate,

    sdk_directory: &Path,
    runtime: &ConfigLoader,
    stats: InitializedLoggingContextStats,
  ) -> Self {
    let (workflows_engine, buffers_to_flush_rx) = {
      let (mut workflows_engine, flush_buffers_tx) = WorkflowsEngine::new(
        &stats.root_scope,
        sdk_directory,
        runtime,
        data_upload_tx,
        stats.stats.clone(),
      );

      workflows_engine
        .start(
          WorkflowsEngineConfig::new(
            config.workflows_configuration,
            config.buffer_producers.trigger_buffer_ids.clone(),
            config.buffer_producers.continuous_buffer_ids.clone(),
          ),
          config.from_cache,
        )
        .await;

      (workflows_engine, flush_buffers_tx)
    };

    Self {
      buffer_producers: config.buffer_producers,
      buffer_selector: config.buffer_selector,
      workflows_engine,
      tail_configs: config.tail_configs,
      filter_chain: config.filter_chain,
      min_log_compression_size: runtime.register_int_watch(),

      flush_buffers_tx,
      flush_stats_trigger,

      trigger_upload_tx,
      buffers_to_flush_rx,

      capture_screenshot_handler,

      stats,
    }
  }

  pub(crate) fn update(&mut self, config: ConfigUpdate) {
    self.buffer_selector = config.buffer_selector;
    self.buffer_producers = config.buffer_producers;
    self.tail_configs = config.tail_configs;
    self.filter_chain = config.filter_chain;

    let workflows_engine_config = WorkflowsEngineConfig::new(
      config.workflows_configuration,
      self.buffer_producers.trigger_buffer_ids.clone(),
      self.buffer_producers.continuous_buffer_ids.clone(),
    );

    self.workflows_engine.update(workflows_engine_config);
  }

  async fn process_log(
    &mut self,
    mut log: Log,
    state: &bd_state::Store,
    block: bool,
    now: OffsetDateTime,
  ) -> anyhow::Result<LogReplayResult> {
    self.stats.logs_received.inc();

    let state_reader = state.read().await;
    // TODO(Augustyniak): Add a histogram for the time it takes to process a log.
    self.filter_chain.process(&mut log, &state_reader);
    let mut log = LogEncodingHelper::new(log, (*self.min_log_compression_size.read()).into());

    let flush_stats_trigger = self.flush_stats_trigger.clone();

    match self.tail_configs.maybe_stream_log(&mut log, &state_reader) {
      Ok(streamed) => {
        if streamed {
          self.stats.streamed_logs.inc();
        }
      },
      Err(e) => {
        log::debug!("failed to stream log: {e:?}");
      },
    }

    let mut matching_buffers = self.buffer_selector.buffers(
      log.log.log_type,
      log.log.log_level,
      &log.log.message,
      FieldsRef::new(&log.log.fields, &log.log.matching_fields),
      &state_reader,
    );

    let mut result = self.workflows_engine.process_event(
      WorkflowEvent::Log(&log.log),
      &matching_buffers,
      &state_reader,
      now,
    );
    let log_replay_result = LogReplayResult {
      logs_to_inject: std::mem::take(&mut result.logs_to_inject)
        .into_values()
        .collect(),
      workflow_debug_state: result.workflow_debug_state,
      engine_has_debug_workflows: result.has_debug_workflows,
    };

    log::debug!(
      "processed {:?} log, destination buffer(s): {:?}, capture session {:?}",
      log.log.message.as_str().unwrap_or("[DATA]"),
      result.log_destination_buffer_ids,
      log.log.capture_session
    );

    Self::handle_common_pre_buffer_write(
      &self.capture_screenshot_handler,
      &result.triggered_flush_buffers_action_ids,
      result.capture_screenshot,
    );

    Self::write_to_buffers(
      &mut self.buffer_producers,
      &result.log_destination_buffer_ids,
      &mut log,
      &result
        .triggered_flush_buffers_action_ids
        .iter()
        .filter_map(|id| match id.as_ref() {
          bd_workflows::config::FlushBufferId::WorkflowActionId(workflow) => Some(workflow),
          bd_workflows::config::FlushBufferId::ExplicitSessionCapture(_) => None,
        })
        .map(std::convert::AsRef::as_ref)
        .collect_vec(),
    )?;

    if let Some(extra_matching_buffer) = Self::process_flush_buffers_actions(
      &result.triggered_flush_buffers_action_ids,
      &mut self.buffer_producers,
      &result.triggered_flushes_buffer_ids,
      &result.log_destination_buffer_ids,
      &log.log.message,
      &log.log.fields,
      &log.log.session_id,
      log.log.occurred_at,
    ) {
      // We emitted a synthetic log. Add the buffer it was written to to the list of matching
      // buffers.
      matching_buffers.insert(extra_matching_buffer.into());
    }

    // Force the persistence of workflows state to disk if log is blocking.
    self.workflows_engine.maybe_persist(block).await;

    if block {
      Self::finish_blocking_log_processing(
        &self.flush_buffers_tx,
        flush_stats_trigger,
        matching_buffers,
      )
      .await?;
    }

    Ok(log_replay_result)
  }

  fn handle_common_pre_buffer_write(
    capture_screenshot_handler: &CaptureScreenshotHandler,
    triggered_flush_buffers_action_ids: &BTreeSet<Cow<'_, FlushBufferId>>,
    capture_screenshot: bool,
  ) {
    if !triggered_flush_buffers_action_ids.is_empty() {
      log::debug!("triggered flush buffer action IDs: {triggered_flush_buffers_action_ids:?}");
    }

    if capture_screenshot {
      capture_screenshot_handler.capture_screenshot();
    }
  }

  pub(crate) async fn process_state_change(
    &mut self,
    state_change: bd_state::StateChange,
    state: &bd_state::Store,
    now: OffsetDateTime,
    session_id: &str,
    fields: &LogFields,
    matching_fields: &LogFields,
  ) -> LogReplayResult {
    let state_reader = state.read().await;

    let empty_set = TinySet::default();
    let mut result = self.workflows_engine.process_event(
      bd_workflows::workflow::WorkflowEvent::StateChange(
        &state_change,
        FieldsRef::new(fields, matching_fields),
      ),
      &empty_set,
      &state_reader,
      now,
    );

    let log_replay_result = LogReplayResult {
      logs_to_inject: std::mem::take(&mut result.logs_to_inject)
        .into_values()
        .collect(),
      workflow_debug_state: result.workflow_debug_state,
      engine_has_debug_workflows: result.has_debug_workflows,
    };

    log::debug!("processed {state_change:?} state change");

    Self::handle_common_pre_buffer_write(
      &self.capture_screenshot_handler,
      &result.triggered_flush_buffers_action_ids,
      result.capture_screenshot,
    );

    // In order to work with session capture we need there to be a log that can be emitted with the
    // action ID for the flush action. Since state changes typically don't have associated logs, we
    // need to rely on the mechanism to synthesize a log here. Now we pass the collected fields
    // (global metadata) so they can be included in the synthetic log.
    Self::process_flush_buffers_actions(
      &result.triggered_flush_buffers_action_ids,
      &mut self.buffer_producers,
      &result.triggered_flushes_buffer_ids,
      &result.log_destination_buffer_ids,
      &"State Change".into(),
      fields,
      session_id,
      state_change.timestamp,
    );

    // State changes are never blocking, so we pass false for force_state_persistence.
    self.workflows_engine.maybe_persist(false).await;

    log_replay_result
  }

  async fn finish_blocking_log_processing(
    flush_buffers_tx: &tokio::sync::mpsc::Sender<BuffersWithAck>,
    flush_stats_trigger: FlushTrigger,
    matching_buffers: TinySet<Cow<'_, str>>,
  ) -> anyhow::Result<()> {
    // The processing of a blocking log is about to complete.
    // We make an arbitrary decision to start with the flushing of log buffers to disk first and
    // move on to flushing stats to disk next.

    log::debug!("blocking log: sending signal to flush buffers after log");

    let flush_stats_trigger = flush_stats_trigger.clone();

    if matching_buffers.is_empty() {
      // Return early to avoid unnecessary tokio messages.
      log::debug!(
        "blocking log: log processed but no buffers matched, returning without waiting for \
         buffers flush"
      );
    } else {
      let (tx, rx) = bd_completion::Sender::new();
      // call the sender to flush the buffers using the tx that was created along with the logger
      let buffers_to_flush = BuffersWithAck::new(
        matching_buffers.iter().map(ToString::to_string).collect(),
        Some(tx),
      );

      let result = flush_buffers_tx.send(buffers_to_flush).await;
      if let Err(e) = result {
        anyhow::bail!("blocking log: failed to send signal to flush buffer(s): {e:?}");
      }

      if let Err(e) = rx.recv().await {
        anyhow::bail!("blocking log: failed to receive buffer(s) flush completion signal: {e:?}");
      }
    }

    // If the log is blocking, we need to flush the stats to disk.
    // TODO(mattklein123): Figure out if we need blocking logs and explicit flushing. It would be
    // better to remove this complexity if we could do it all as part of the explicit flush call
    // which also uploads stats and does other things. For now in this case we just do what we did
    // before which is write to disk only.
    log::debug!("blocking log: sending signal to flush stats to disk");
    let (sender, receiver) = bd_completion::Sender::new();
    if let Err(e) = flush_stats_trigger
      .flush(FlushTriggerRequest {
        do_upload: false,
        completion_tx: Some(sender),
      })
      .await
    {
      anyhow::bail!("blocking log: failed to send signal to flush stats: {e:?}");
    }

    receiver.recv().await.map_err(|e| {
      anyhow::anyhow!("failed to await receiving flush stats trigger completion: {e:?}")
    })
  }

  fn write_to_buffers(
    buffers: &mut BufferProducers,
    matching_buffers: &TinySet<Cow<'_, str>>,
    log: &mut LogEncodingHelper,
    action_ids: &[&str],
  ) -> anyhow::Result<()> {
    if matching_buffers.is_empty() {
      return Ok(());
    }

    for buffer in matching_buffers.iter() {
      // TODO(snowp): For both logger and buffer lookup we end up doing a map lookup, which
      // seems less than ideal in the logging path. Look into ways to optimize this,
      // possibly via vector indices instead of string keys.
      let producer = BufferProducers::producer(&mut buffers.buffers, buffer)?;
      write_log_to_buffer(producer, log, action_ids, &[])?;
    }

    Ok(())
  }

  /// Processes flush buffer actions that were triggered by the log being processed.
  /// If the log that triggered the flush was not written to any of the buffers that are
  /// about to be flushed, a synthetic log resembling the original log is created and added
  /// to one of the buffers scheduled for flushing.
  /// Returns the ID of the buffer to which the synthetic log was added, if any.
  fn process_flush_buffers_actions(
    triggered_flush_buffers_action_ids: &BTreeSet<Cow<'_, FlushBufferId>>,
    buffers: &mut BufferProducers,
    triggered_flushes_buffer_ids: &TinySet<Cow<'_, str>>,
    written_to_buffers: &TinySet<Cow<'_, str>>,
    log_message: &LogMessage,
    log_fields: &LogFields,
    session_id: &str,
    occurred_at: OffsetDateTime,
  ) -> Option<String> {
    if triggered_flush_buffers_action_ids.is_empty() {
      return None;
    }

    // Indicates whether the log was written to any of the continuous buffers. Continuous buffers
    // are periodically uploaded, so if the log was written to a continuous buffer, we assume
    // that it will eventually be uploaded to the remote.
    let written_to_continuous_buffer =
      !written_to_buffers.is_disjoint(&buffers.continuous_buffer_ids);

    // Indicates whether the log was written to any of the trigger buffers. Trigger buffers are
    // uploaded only if there is a workflow action that instructs the system to flush them.
    // Therefore, we assume that the log ends up being uploaded only if it is directed to one of the
    // trigger buffers that is about to be flushed.
    let written_to_flushed_trigger_buffer =
      !written_to_buffers.is_disjoint(&buffers.trigger_buffer_ids);

    let is_log_about_to_be_uploaded =
      written_to_continuous_buffer || written_to_flushed_trigger_buffer;

    // The log that triggered a flush of the buffer(s) has not been written to any of the buffers,
    // so it's not going to be flushed. To work around that, create a synthetic log that
    // resembles the original log and add it to one of the buffers scheduled to be uploaded.
    if !is_log_about_to_be_uploaded
      // Select a 'random' buffer from the 'list' of buffers scheduled for flushing.
      && let Some(arbitrary_buffer_id_to_flush) = triggered_flushes_buffer_ids
        .iter()
        .next()
        .map(std::string::ToString::to_string)
    {
      log::debug!(
        "adding synthetic log \"{log_message:?}\" to \"{arbitrary_buffer_id_to_flush}\" buffer; \
         flush buffer action IDs {triggered_flush_buffers_action_ids:?}"
      );

      if let Ok(buffer_producer) =
        BufferProducers::producer(&mut buffers.buffers, arbitrary_buffer_id_to_flush.as_str())
        && let Err(e) = (|| {
          let action_ids = triggered_flush_buffers_action_ids
            .iter()
            .filter_map(|id| match id.as_ref() {
              bd_workflows::config::FlushBufferId::WorkflowActionId(workflow) => Some(workflow),
              bd_workflows::config::FlushBufferId::ExplicitSessionCapture(_) => None,
            })
            .map(std::convert::AsRef::as_ref)
            .collect_vec();
          let mut cached_encoding_data = None;
          let size = LogEncodingHelper::serialize_proto_size_inner(
            log_level::DEBUG,
            log_message,
            log_fields,
            session_id,
            occurred_at,
            LogType::INTERNAL_SDK,
            &action_ids,
            &[],
            &mut cached_encoding_data,
            u64::MAX,
          )?;
          let reserved = buffer_producer.reserve(size.to_u32_lossy(), true)?;
          let mut os = CodedOutputStream::bytes(reserved);
          LogEncodingHelper::serialize_proto_to_stream_inner(
            log_level::DEBUG,
            log_message,
            log_fields,
            session_id,
            occurred_at,
            LogType::INTERNAL_SDK,
            &action_ids,
            &[],
            &mut os,
            cached_encoding_data.as_ref(),
          )?;
          drop(os);
          buffer_producer.commit()?;
          Ok::<_, anyhow::Error>(())
        })()
      {
        log::debug!("failed to write synthetic log to buffer: {e}");
      }

      return Some(arbitrary_buffer_id_to_flush);
    }

    None
  }

  pub(crate) async fn run(&mut self) {
    tokio::select! {
      () = self.workflows_engine.run()  => {}
      Some(buffers_to_flush) =  self.buffers_to_flush_rx.recv() => {
        log::debug!("received flush buffers action signal, buffer IDs to flush: \"{:?}\"", buffers_to_flush.buffer_ids);

        let trigger_upload = TriggerUpload::new(
          buffers_to_flush.buffer_ids
            .iter()
            .map(std::string::ToString::to_string)
            .collect(),
            buffers_to_flush.response_tx,
          );

        let result = self.trigger_upload_tx.try_send(trigger_upload);
        match result {
          Ok(()) => {
            log::debug!("triggered flush buffers action with buffer IDs: \"{:?}\"", buffers_to_flush.buffer_ids);
          },
          Err(e) => {
            log::debug!("failed to send trigger flush: {e}");
            self.stats.trigger_upload_stats.record(&e);
          }
        }
      },
    }
  }
}
