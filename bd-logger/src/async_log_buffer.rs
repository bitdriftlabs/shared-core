// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./async_log_buffer_test.rs"]
mod async_log_buffer_test;

use crate::logger::with_thread_local_logger_guard;
use crate::logging_state::{
  ConfigUpdate,
  InitializedLoggingContext,
  LoggingState,
  UninitializedLoggingContext,
};
use crate::memory_bound::{channel, MemorySized, Receiver, Sender, TrySendError};
use crate::metadata::MetadataCollector;
use crate::pre_config_buffer::PreConfigBuffer;
use crate::thread_local::write_log_with_logging_context;
use crate::{internal_report, network};
use bd_api::TriggerUpload;
use bd_buffer::BuffersWithAck;
use bd_client_common::error::{handle_unexpected, handle_unexpected_error_with_details};
use bd_log_metadata::{AnnotatedLogFields, MetadataProvider};
use bd_log_primitives::{
  FieldsRef,
  LogField,
  LogFieldValue,
  LogFields,
  LogLevel,
  LogMessage,
  LogRef,
  StringOrBytes,
};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_runtime::runtime::workflows::WorkflowsEnabledFlag;
use bd_runtime::runtime::{ConfigLoader, Watch};
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger, ComponentShutdownTriggerHandle};
use std::mem::size_of_val;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum AsyncLogBufferMessage {
  EmitLog(LogLine),
  AddLogField(LogField),
  RemoveLogField(String),
  FlushState(Option<bd_completion::Sender<()>>),
}

impl MemorySized for AsyncLogBufferMessage {
  fn size(&self) -> usize {
    size_of_val(self)
      + match self {
        Self::EmitLog(log) => log.size(),
        Self::AddLogField(field) => field.size(),
        Self::RemoveLogField(field_name) => field_name.len(),
        Self::FlushState(sender) => size_of_val(sender),
      }
  }
}

//
// LogLine
//

/// A copy of an incoming log line, used to allow for offloading the
/// processing of the incoming logs to an async run loop.
///
/// The log does not have a `group`, `timestamp` and all of the `fields` yet.
/// These are populated only after the log is dequeued for processing on the
/// run loop and the execution of the program calls into `metadata_provider`
/// to retrieve the aforementioned properties and merge them into the final log
/// before passing them for further processing.
#[derive(Debug)]
pub struct LogLine {
  // Remember to update the implementation
  // of the `MemorySized` trait every
  // time the struct is modified!!!
  pub log_level: LogLevel,
  pub log_type: LogType,
  pub message: LogMessage,
  pub fields: AnnotatedLogFields,
  pub matching_fields: AnnotatedLogFields,
  pub attributes_overrides: Option<LogAttributesOverridesPreviousRunSessionID>,
  /// Used to send a message when the log is processed. In this context, 'processed' means that the
  /// log was written to either a pre-config buffer or one of the final ring buffers used by the
  /// SDK. Neither one of those guarantees that the log is written to a disk.
  pub log_processing_completed_tx: Option<oneshot::Sender<()>>,
}

//
// LogAttributesOverridesPreviousRunSessionID
//

#[derive(Debug)]
pub struct LogAttributesOverridesPreviousRunSessionID {
  /// The hint that tells the SDK what the expected previous session ID was. The SDK uses it to
  /// verify whether the passed information matches its internal session ID tracking and drops
  /// logs whose hints are invalid.
  pub expected_previous_process_session_id: String,
  pub occurred_at: OffsetDateTime,
}

impl MemorySized for LogLine {
  fn size(&self) -> usize {
    // Add a constant number of bytes (48) to account for the size of `log_processing_completed_tx`.
    // We do not use `size_of_val` or `size_of` to do that as it reports different size of the
    // the field when ran on a server and locally on a laptop. The number was captured by
    // calling `size_of_val(log_processing_completed_tx)` on an M2 Macbook.
    //
    // Add a constant number of bytes (24) to account for field alignments etc. that we do not
    // account for when not using `size_of_val(self)`.
    size_of_val(&self.log_level)
      + size_of_val(&self.log_type)
      + self.message.size()
      + self.fields.size()
      + self.matching_fields.size()
      + size_of_val(&self.attributes_overrides)
      + 48
      + 24
  }
}

//
// AnnotatedLogLine
//

/// A copy of an incoming log line.
///
/// Contrary to `LogLine` it contains group, timestamp
/// and all of the fields that are supposed to be logged with a given log.
#[derive(Debug)]
pub struct AnnotatedLogLine {
  // Remember to update the implementation
  // of the `MemorySized` trait every
  // time the struct is modified!!!
  pub log_level: LogLevel,
  pub log_type: LogType,
  pub message: StringOrBytes<String, Vec<u8>>,
  pub fields: LogFields,
  pub matching_fields: LogFields,
  pub session_id: String,
  pub occurred_at: time::OffsetDateTime,
}

impl MemorySized for AnnotatedLogLine {
  fn size(&self) -> usize {
    // The size cannot be computed by just calling a `size_of_val(self)` in here
    // as that does not account for various heap allocations.
    size_of_val(self)
      + self.message.size()
      + self.fields.size()
      + self.matching_fields.size()
      + self.session_id.len()
  }
}

//
// LogInterceptor
//

pub(crate) trait LogInterceptor: Send + Sync {
  fn process(
    &self,
    log_level: LogLevel,
    log_type: LogType,
    msg: &LogMessage,
    fields: &mut AnnotatedLogFields,
  );
}


//
// AsyncLogBuffer
//

// Orchestrates buffering of incoming logs and offloading their processing to
// a run loop in an async way.
pub struct AsyncLogBuffer<L: LogReplay> {
  communication_rx: Receiver<AsyncLogBufferMessage>,
  config_update_rx: mpsc::Receiver<ConfigUpdate>,
  shutdown_trigger_handle: ComponentShutdownTriggerHandle,

  session_strategy: Arc<bd_session::Strategy>,
  metadata_collector: MetadataCollector,
  resource_utilization_reporter: bd_resource_utilization::Reporter,
  events_listener: bd_events::Listener,

  interceptors: Vec<Arc<dyn LogInterceptor>>,

  logging_state: LoggingState<AnnotatedLogLine>,
  workflows_enabled_flag: Watch<bool, WorkflowsEnabledFlag>,
}

impl<L: LogReplay + Send + 'static> AsyncLogBuffer<L> {
  pub(crate) fn new(
    uninitialized_logging_context: UninitializedLoggingContext<AnnotatedLogLine>,
    replayer: L,
    session_strategy: Arc<bd_session::Strategy>,
    metadata_provider: Arc<dyn MetadataProvider + Send + Sync>,
    resource_utilization_target: Box<dyn bd_resource_utilization::Target + Send + Sync>,
    events_listener_target: Box<dyn bd_events::ListenerTarget + Send + Sync>,
    config_update_rx: mpsc::Receiver<ConfigUpdate>,
    shutdown_trigger_handle: ComponentShutdownTriggerHandle,
    runtime_loader: &Arc<ConfigLoader>,
  ) -> (Self, Sender<AsyncLogBufferMessage>) {
    let (async_log_buffer_communication_tx, async_log_buffer_communication_rx) = channel(
      uninitialized_logging_context
        .pre_config_log_buffer
        .max_count(),
      uninitialized_logging_context
        .pre_config_log_buffer
        .max_size(),
    );

    let internal_periodic_fields_reporter =
      Arc::new(internal_report::Reporter::new(runtime_loader));
    let bandwidth_usage_tracker = Arc::new(network::HTTPTrafficDataUsageTracker::new());

    (
      Self {
        communication_rx: async_log_buffer_communication_rx,
        config_update_rx,
        shutdown_trigger_handle,

        session_strategy,
        metadata_collector: MetadataCollector::new(metadata_provider),
        resource_utilization_reporter: bd_resource_utilization::Reporter::new(
          resource_utilization_target,
          runtime_loader,
        ),
        events_listener: bd_events::Listener::new(events_listener_target, runtime_loader),

        interceptors: vec![internal_periodic_fields_reporter, bandwidth_usage_tracker],

        // The size of the pre-config buffer matches the size of the enclosing
        // async log buffer.
        logging_state: LoggingState::Uninitialized(uninitialized_logging_context),
        workflows_enabled_flag: runtime_loader.register_watch().unwrap(),

        replayer,
      },
      async_log_buffer_communication_tx,
    )
  }

  pub fn enqueue_log(
    tx: &Sender<AsyncLogBufferMessage>,
    log_level: LogLevel,
    log_type: LogType,
    message: LogMessage,
    fields: AnnotatedLogFields,
    matching_fields: AnnotatedLogFields,
    attributes_overrides: Option<LogAttributesOverridesPreviousRunSessionID>,
    blocking: bool,
  ) -> Result<(), TrySendError<AsyncLogBufferMessage>> {
    let (log_processing_completed_tx_option, log_processing_completed_rx_option) = if blocking {
      // Create a (sender, receiver) pair only if the caller wants to wait on
      // on the log being pushed through the whole log processing pipeline.
      let (tx, rx) = tokio::sync::oneshot::channel::<()>();
      (Some(tx), Some(rx))
    } else {
      (None, None)
    };

    let log = LogLine {
      log_level,
      log_type,
      message,
      fields,
      matching_fields,
      attributes_overrides,
      log_processing_completed_tx: log_processing_completed_tx_option,
    };

    if let Err(e) = tx.try_send(AsyncLogBufferMessage::EmitLog(log)) {
      log::debug!("enqueue_log: sending to channel failed: {e:?}");

      if let TrySendError::Closed(_) = &e {
        handle_unexpected::<(), anyhow::Error>(
          Err(anyhow::anyhow!("channel closed")),
          "async log buffer: channel is closed",
        );
      }

      // Return early from here. There is no point in continuing the execution
      // of the method and waiting for the log processing to complete if we
      // failed to send the log in here. In fact, waiting in such case would lead
      // to infinite waiting.
      return Err(e);
    }

    // Wait for log processing to be completed only if passed `blocking`
    // argument is equal to `true` and we created a relevant one shot Tokio channel.
    if let Some(rx) = log_processing_completed_rx_option {
      match rx.blocking_recv() {
        Ok(()) => {
          log::debug!("enqueue_log: log processing completion received");
        },
        Err(e) => {
          log::debug!(
            "enqueue_log: received an error when waiting for log processing completion: {e}"
          );
        },
      }
    }

    // Report success even if the `blocking == true` part of the
    // implementation above failed.
    Ok(())
  }

  pub fn add_log_field(
    tx: &Sender<AsyncLogBufferMessage>,
    field: LogField,
  ) -> Result<(), TrySendError<AsyncLogBufferMessage>> {
    tx.try_send(AsyncLogBufferMessage::AddLogField(field))
  }

  pub fn remove_log_field(
    tx: &Sender<AsyncLogBufferMessage>,
    field_name: &str,
  ) -> Result<(), TrySendError<AsyncLogBufferMessage>> {
    tx.try_send(AsyncLogBufferMessage::RemoveLogField(
      field_name.to_string(),
    ))
  }

  pub fn flush_state(
    tx: &Sender<AsyncLogBufferMessage>,
    blocking: bool,
  ) -> Result<(), TrySendError<AsyncLogBufferMessage>> {
    let (completion_tx, completion_rx) = if blocking {
      let (tx, rx) = bd_completion::Sender::new();
      (Some(tx), Some(rx))
    } else {
      (None, None)
    };

    tx.try_send(AsyncLogBufferMessage::FlushState(completion_tx))?;

    // Wait for the processing to be completed only if passed `blocking` argument is equal to
    // `true`.
    if let Some(completion_rx) = completion_rx {
      match &completion_rx.blocking_recv() {
        Ok(()) => {
          log::debug!("flush state: completion received");
        },
        Err(e) => {
          log::debug!("flush state: received an error when waiting for completion: {e}");
        },
      }
    }

    Ok(())
  }

  async fn process_log(&mut self, log: LogLine) -> anyhow::Result<()> {
    // Prevent re-entrancy when we are evaluating the log metadata.
    let result = with_thread_local_logger_guard(|| {
      self
        .metadata_collector
        .normalized_metadata_with_extra_fields(log.fields, log.matching_fields, log.log_type)
    });

    match result {
      Ok(log_metadata) => {
        let (session_id, timestamp, extra_fields) =
          if let Some(overrides) = log.attributes_overrides {
            if Some(overrides.expected_previous_process_session_id.clone())
              == self.session_strategy.previous_process_session_id()
            {
              // Session ID override hint provided and matches our expectations. Emit log with
              // overrides applied.
              (
                overrides.expected_previous_process_session_id,
                overrides.occurred_at,
                Some(vec![LogField {
                  key: "_logged_at".to_string(),
                  value: LogFieldValue::String(log_metadata.timestamp.to_string()),
                }]),
              )
            } else {
              // Session ID override hint provided but doesn't match our expectations. Drop log.
              let session_id = self.session_strategy.session_id();

              handle_unexpected_error_with_details(
                anyhow::Error::msg(
                  "failed to override log attributes, provided override attributes do not match \
                   expectations",
                ),
                &format!(
                  "original_session_id {session_id:?}, override attribute session ID {:?} \
                   original timestamp {:?}, override timestamp {:?}",
                  overrides.expected_previous_process_session_id,
                  log_metadata.timestamp,
                  overrides.occurred_at
                ),
                || None,
              );

              return Ok(());
            }
          } else {
            (
              self.session_strategy.session_id(),
              log_metadata.timestamp,
              None,
            )
          };

        let annotated_log = AnnotatedLogLine {
          log_level: log.log_level,
          log_type: log.log_type,
          message: log.message,
          fields: if let Some(extra_fields) = extra_fields {
            log_metadata
              .fields
              .iter()
              .chain(extra_fields.iter())
              .cloned()
              .collect()
          } else {
            log_metadata.fields
          },
          matching_fields: log_metadata.matching_fields,
          occurred_at: timestamp,
          session_id,
        };

        match &mut self.logging_state {
          LoggingState::Uninitialized(uninitialized_logging_context) => {
            let result = uninitialized_logging_context
              .pre_config_log_buffer
              .push(annotated_log);

            uninitialized_logging_context
              .stats
              .pre_config_log_buffer
              .record(&result);
            if let Err(e) = result {
              log::debug!("failed to push log to a pre-config buffer: {e}");
            }

            if let Some(tx) = log.log_processing_completed_tx {
              if let Err(e) = tx.send(()) {
                log::debug!("failed to send log processing completion message: {e:?}");
              }
            }

            return Ok(());
          },
          LoggingState::Initialized(initialized_logging_context) => {
            if let Err(e) = self
              .replayer
              .replay_log(
                annotated_log,
                log.log_processing_completed_tx,
                initialized_logging_context,
              )
              .await
            {
              log::debug!("failed to replay async log buffer log: {e}");
            }
          },
        }

        Ok(())
      },
      Err(e) => {
        // TODO(Augustyniak): Consider logging as error so that SDK customers can see these
        // errors which are mostly emitted as the result of calls into platform-provided metadata
        // provider.
        log::debug!("failed to process a log inside of thread_local_logging section: {e}");
        Ok(())
      },
    }
  }

  async fn update(
    mut self,
    config: ConfigUpdate,
    workflows_enabled: bool,
  ) -> (Self, Option<PreConfigBuffer<AnnotatedLogLine>>) {
    let (initialized_logging_context, maybe_pre_config_log_buffer) = match self.logging_state {
      LoggingState::Uninitialized(uninitialized_logging_context) => {
        let (initialized_logging_context, pre_config_log_buffer) = uninitialized_logging_context
          .updated(config, workflows_enabled)
          .await;
        (initialized_logging_context, Some(pre_config_log_buffer))
      },
      LoggingState::Initialized(mut initialized_logging_context) => {
        initialized_logging_context
          .update(config, workflows_enabled)
          .await;
        (initialized_logging_context, None)
      },
    };

    self.logging_state = LoggingState::Initialized(initialized_logging_context);

    (self, maybe_pre_config_log_buffer)
  }

  async fn maybe_replay_pre_config_buffer_logs(
    &mut self,
    pre_config_log_buffer: PreConfigBuffer<AnnotatedLogLine>,
  ) {
    let LoggingState::Initialized(initialized_logging_context) = &mut self.logging_state else {
      return;
    };

    for log_line in pre_config_log_buffer.pop_all() {
      if let Err(e) = self
        .replayer
        .replay_log(log_line, None, initialized_logging_context)
        .await
      {
        log::debug!("failed to reply pre-config log buffer logs: {e}");
      }
    }
  }

  fn workflows_enabled(&self) -> bool {
    self.workflows_enabled_flag.read()
  }

  pub async fn run(self) -> Self {
    let shutdown_trigger = ComponentShutdownTrigger::default();
    self
      .run_with_shutdown(shutdown_trigger.make_shutdown())
      .await
  }

  // TODO(mattklein123): This seems to only be used for tests. Figure out how to clean this up
  // so we don't need this just for tests.
  pub async fn run_with_shutdown(mut self, mut shutdown: ComponentShutdown) -> Self {
    // Processes incoming logs and reacts to workflows config updates.
    //
    // The first workflows config update makes the async log buffer disable
    // pre-config log buffer and results in a replay all of the logs stored
    // by the pre-config log buffer. All of that happens in a way where logs
    // stored in pre-config log buffer are guaranteed to be replayed before
    // the async log buffer goes back to processing incoming logs.

    let local_shutdown = shutdown.cancelled();
    tokio::pin!(local_shutdown);
    let mut self_shutdown = self.shutdown_trigger_handle.make_shutdown();
    let self_shutdown = self_shutdown.cancelled();
    tokio::pin!(self_shutdown);
    loop {
      let (workflows_engine, buffers_to_flush_rx) =
        if let LoggingState::Initialized(initialized_logging_context) = &mut self.logging_state {
          (
            initialized_logging_context.workflows_engine.as_mut(),
            initialized_logging_context.buffers_to_flush_rx.as_mut(),
          )
        } else {
          (None, None)
        };

      tokio::select! {
        Some(config) = self.config_update_rx.recv() => {
          let workflows_enabled = self.workflows_enabled();
          let (updated_self, maybe_pre_config_buffer)
            = self.update(config, workflows_enabled).await;

          self = updated_self;
          if let Some(pre_config_log_buffer) = maybe_pre_config_buffer {
            self.maybe_replay_pre_config_buffer_logs(pre_config_log_buffer).await;
          }
        },
        Some(async_log_buffer_message) = self.communication_rx.recv() => {
          match async_log_buffer_message {
            AsyncLogBufferMessage::EmitLog(mut log) => {
              for interceptor in &self.interceptors {
                interceptor.process(
                  log.log_level,
                  log.log_type,
                  &log.message,
                  &mut log.fields,
                );
              }

              if let Err(e) = self.process_log(log).await {
                log::debug!("failed to process log: {e}");
              }
            },
            AsyncLogBufferMessage::AddLogField(field) => {
              if let Err(e) = self.metadata_collector.add_field(field.clone()) {
                log::warn!("failed to add log field ({field:?}): {e}");
              }
            },
            AsyncLogBufferMessage::RemoveLogField(field_name) => {
              self.metadata_collector.remove_field(&field_name);
            },
            AsyncLogBufferMessage::FlushState(completion_tx) => {
              if let Some(trigger) = self.logging_state.flush_stats_trigger() {

                let (sender, receiver) = bd_completion::Sender::new();
                if let Err(e) = trigger.flush(Some(sender)).await {
                  log::debug!("flushing state: failed to flush stats: {e}");
                }

                if let Err(e) = receiver.recv().await {
                  log::debug!("flushing state: failed to wait for stats flush: {e}");
                }
              }

              let (sender, receiver) = bd_completion::Sender::new();
              let buffers_with_ack = BuffersWithAck::new_all_buffers(Some(sender));
              if let Err(e) = self.logging_state.flush_buffers_trigger()
                .send(buffers_with_ack).await
              {
                log::debug!("flushing state: failed to flush buffers: {e}");
              }

              if let Err(e) = receiver.recv().await {
                log::debug!("flushing state: failed to wait for buffers flush: {e}");
              }

              self.session_strategy.flush();

              if let Some(workflows_engine) = self.logging_state.workflows_engine() {
                workflows_engine.maybe_persist(true).await;
              }

              if let Some(completion_tx) = completion_tx {
                completion_tx.send(());
              }
            }
          }
        },
        _ = self.workflows_enabled_flag.changed() => {
          if !self.workflows_enabled() {
            if let LoggingState::Initialized(initialized_logging_context)
              = &mut self.logging_state {
              initialized_logging_context.workflows_engine = None;
            }
          }
        }
        () = async { workflows_engine.unwrap().run().await }, if workflows_engine.is_some() => {},
        Some(buffers_to_flush) = async { buffers_to_flush_rx.unwrap().recv().await },
          if buffers_to_flush_rx.is_some() => {

          log::debug!("received flush buffers action signal, buffer IDs to flush: \"{:?}\"", buffers_to_flush.buffer_ids);
          let LoggingState::Initialized(initialized_context) = &self.logging_state else {
            debug_assert!(false, "received flush buffers action signal, but the logging context is not initialized");
            continue;
          };

          let trigger_upload = TriggerUpload::new(
            buffers_to_flush.buffer_ids
              .into_iter()
              .map(|buffer_id| buffer_id.to_string())
              .collect()
            );

          let result = initialized_context.trigger_upload_tx.try_send(trigger_upload.clone());
          match result {
            Ok(()) => {
              log::debug!("triggered flush buffers action with buffer IDs: \"{:?}\"", trigger_upload.buffer_ids);
            },
            Err(e) => {
              log::debug!("failed to send trigger flush: {e}");
              initialized_context.stats.trigger_upload_stats.record(&e);
            }
          }
        },
        () = self.resource_utilization_reporter.run() => {},
        () = self.events_listener.run() => {},
        () = &mut local_shutdown => {
          return self;
        },
        () = &mut self_shutdown => {
          return self;
        },
      }
    }
  }
}
