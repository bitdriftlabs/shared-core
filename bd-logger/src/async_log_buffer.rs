// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./async_log_buffer_test.rs"]
mod async_log_buffer_test;

use crate::bounded_buffer::{channel, MemorySized, Receiver, Sender, TrySendError};
use crate::device_id::DeviceIdInterceptor;
use crate::log_replay::LogReplay;
use crate::logger::with_thread_local_logger_guard;
use crate::logging_state::{ConfigUpdate, LoggingState, UninitializedLoggingContext};
use crate::metadata::MetadataCollector;
use crate::network::{NetworkQualityInterceptor, SystemTimeProvider};
use crate::pre_config_buffer::PreConfigBuffer;
use crate::{internal_report, network};
use anyhow::anyhow;
use bd_buffer::BuffersWithAck;
use bd_client_common::error::{handle_unexpected, handle_unexpected_error_with_details};
use bd_crash_handler::global_state;
use bd_device::Store;
use bd_log_metadata::MetadataProvider;
use bd_log_primitives::{
  log_level,
  AnnotatedLogField,
  AnnotatedLogFields,
  Log,
  LogFieldValue,
  LogFields,
  LogInterceptor,
  LogLevel,
  LogMessage,
  StringOrBytes,
};
use bd_network_quality::NetworkQualityProvider;
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_runtime::runtime::ConfigLoader;
use bd_session_replay::CaptureScreenshotHandler;
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger, ComponentShutdownTriggerHandle};
use std::collections::VecDeque;
use std::mem::size_of_val;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum AsyncLogBufferMessage {
  EmitLog(LogLine),
  AddLogField(String, StringOrBytes<String, Vec<u8>>),
  RemoveLogField(String),
  FlushState(Option<bd_completion::Sender<()>>),
}

impl MemorySized for AsyncLogBufferMessage {
  fn size(&self) -> usize {
    size_of_val(self)
      + match self {
        Self::EmitLog(log) => log.size(),
        Self::AddLogField(key, value) => key.size() + value.size(),
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
  pub attributes_overrides: Option<LogAttributesOverrides>,
  /// Used to send a message when the log is processed. In this context, 'processed' means that the
  /// log was written to either a pre-config buffer or one of the final ring buffers used by the
  /// SDK. Neither one of those guarantees that the log is written to a disk.
  pub log_processing_completed_tx: Option<oneshot::Sender<()>>,
}

//
// LogAttributesOverrides
//

#[derive(Debug)]
pub enum LogAttributesOverrides {
  /// The hint that tells the SDK what the expected previous session ID was. The SDK uses it to
  /// verify whether the passed information matches its internal session ID tracking and drops
  /// logs whose hints are invalid.
  PreviousRunSessionID(String, OffsetDateTime),

  /// Overrides the time when the log occurred at, useful for cases like spans with a provided
  /// time.
  OccurredAt(OffsetDateTime),
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

impl MemorySized for bd_log_primitives::Log {
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

impl MemorySized for AnnotatedLogFields {
  fn size(&self) -> usize {
    self.iter().map(|(k, v)| k.len() + v.size()).sum::<usize>()
  }
}

//
// AsyncLogBuffer
//

// Orchestrates buffering of incoming logs and offloading their processing to
// a run loop in an async way.
pub struct AsyncLogBuffer<R: LogReplay> {
  communication_rx: Receiver<AsyncLogBufferMessage>,
  config_update_rx: mpsc::Receiver<ConfigUpdate>,
  shutdown_trigger_handle: ComponentShutdownTriggerHandle,

  session_strategy: Arc<bd_session::Strategy>,
  metadata_collector: MetadataCollector,
  resource_utilization_reporter: bd_resource_utilization::Reporter,

  session_replay_recorder: bd_session_replay::Recorder,
  session_replay_capture_screenshot_handler: CaptureScreenshotHandler,

  events_listener: bd_events::Listener,

  replayer: R,

  interceptors: Vec<Arc<dyn LogInterceptor>>,

  logging_state: LoggingState<bd_log_primitives::Log>,

  global_state_tracker: global_state::Tracker,
}

impl<R: LogReplay + Send + 'static> AsyncLogBuffer<R> {
  pub(crate) fn new(
    uninitialized_logging_context: UninitializedLoggingContext<bd_log_primitives::Log>,
    replayer: R,
    session_strategy: Arc<bd_session::Strategy>,
    metadata_provider: Arc<dyn MetadataProvider + Send + Sync>,
    resource_utilization_target: Box<dyn bd_resource_utilization::Target + Send + Sync>,
    session_replay_target: Box<dyn bd_session_replay::Target + Send + Sync>,
    events_listener_target: Box<dyn bd_events::ListenerTarget + Send + Sync>,
    config_update_rx: mpsc::Receiver<ConfigUpdate>,
    shutdown_trigger_handle: ComponentShutdownTriggerHandle,
    runtime_loader: &Arc<ConfigLoader>,
    network_quality_provider: Arc<dyn NetworkQualityProvider>,
    device_id: String,
    store: Arc<Store>,
  ) -> (Self, Sender<AsyncLogBufferMessage>) {
    let (async_log_buffer_communication_tx, async_log_buffer_communication_rx) = channel(
      uninitialized_logging_context
        .pre_config_log_buffer
        .max_count(),
      uninitialized_logging_context
        .pre_config_log_buffer
        .max_size(),
    );

    let (
      session_replay_recorder,
      session_replay_capture_screenshot_handler,
      screenshot_log_interceptor,
    ) = bd_session_replay::Recorder::new(
      session_replay_target,
      runtime_loader,
      &uninitialized_logging_context.stats.scope,
    );

    let internal_periodic_fields_reporter =
      Arc::new(internal_report::Reporter::new(runtime_loader));
    let bandwidth_usage_tracker = Arc::new(network::HTTPTrafficDataUsageTracker::new(
      Arc::new(SystemTimeProvider),
      network_quality_provider.clone(),
    ));
    let network_quality_interceptor =
      Arc::new(NetworkQualityInterceptor::new(network_quality_provider));
    let device_id_interceptor = Arc::new(DeviceIdInterceptor::new(device_id));

    (
      Self {
        communication_rx: async_log_buffer_communication_rx,
        config_update_rx,
        shutdown_trigger_handle,

        replayer,

        session_strategy,
        metadata_collector: MetadataCollector::new(metadata_provider),
        resource_utilization_reporter: bd_resource_utilization::Reporter::new(
          resource_utilization_target,
          runtime_loader,
        ),

        session_replay_recorder,
        session_replay_capture_screenshot_handler,

        events_listener: bd_events::Listener::new(events_listener_target, runtime_loader),

        interceptors: vec![
          internal_periodic_fields_reporter,
          bandwidth_usage_tracker,
          network_quality_interceptor,
          Arc::new(screenshot_log_interceptor),
          device_id_interceptor,
        ],

        // The size of the pre-config buffer matches the size of the enclosing
        // async log buffer.
        logging_state: LoggingState::Uninitialized(uninitialized_logging_context),
        global_state_tracker: global_state::Tracker::new(store),
      },
      async_log_buffer_communication_tx,
    )
  }

  #[allow(clippy::result_large_err)]
  pub fn enqueue_log(
    tx: &Sender<AsyncLogBufferMessage>,
    log_level: LogLevel,
    log_type: LogType,
    message: LogMessage,
    fields: AnnotatedLogFields,
    matching_fields: AnnotatedLogFields,
    attributes_overrides: Option<LogAttributesOverrides>,
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

  #[allow(clippy::result_large_err)]
  pub fn add_log_field(
    tx: &Sender<AsyncLogBufferMessage>,
    key: String,
    value: StringOrBytes<String, Vec<u8>>,
  ) -> Result<(), TrySendError<AsyncLogBufferMessage>> {
    tx.try_send(AsyncLogBufferMessage::AddLogField(key, value))
  }

  #[allow(clippy::result_large_err)]
  pub fn remove_log_field(
    tx: &Sender<AsyncLogBufferMessage>,
    field_name: &str,
  ) -> Result<(), TrySendError<AsyncLogBufferMessage>> {
    tx.try_send(AsyncLogBufferMessage::RemoveLogField(
      field_name.to_string(),
    ))
  }

  #[allow(clippy::result_large_err)]
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

  async fn process_all_logs(&mut self, log: LogLine) -> anyhow::Result<()> {
    let mut logs = VecDeque::new();
    logs.push_back(log);
    while let Some(log) = logs.pop_front() {
      let new_logs = self.process_log(log).await?;
      logs.extend(new_logs.into_iter().map(|log| {
        LogLine {
          log_level: log.log_level,
          log_type: log.log_type,
          message: log.message,
          // TODO(mattklein123): Right now we set all fields as OOTB so they can have reserved
          // naming if desired. This may have to change in the future.
          fields: log
            .fields
            .into_iter()
            .map(|(key, value)| (key, AnnotatedLogField::new_ootb(value)))
            .collect(),
          matching_fields: log
            .matching_fields
            .into_iter()
            .map(|(key, value)| {
              (
                key,
                // TODO(mattklein123): Right now the only matching field set on injected logs is
                // the _generate_log_id field used for subsequent matching. If
                // this ever changes we will need to correctly propagate this
                // through.
                AnnotatedLogField::new_ootb(value),
              )
            })
            .collect(),
          // TODO(mattklein123): Technically we should probably propagate overrides to injected
          // logs as well as cover completion under any generated logs, but this gets complicated
          // and is an extreme edge case so we ignore for now until proven it's an issue.
          attributes_overrides: None,
          log_processing_completed_tx: None,
        }
      }));
    }
    Ok(())
  }

  async fn process_log(&mut self, log: LogLine) -> anyhow::Result<Vec<Log>> {
    // Prevent re-entrancy when we are evaluating the log metadata.
    let result = with_thread_local_logger_guard(|| {
      self
        .metadata_collector
        .normalized_metadata_with_extra_fields(
          log.fields,
          log.matching_fields,
          log.log_type,
          &mut self.global_state_tracker,
        )
    });

    match result {
      Ok(metadata) => {
        let (session_id, timestamp, extra_fields) = match log.attributes_overrides {
          Some(LogAttributesOverrides::PreviousRunSessionID(
            expected_previous_process_session_id,
            occurred_at,
          )) => {
            if Some(&expected_previous_process_session_id)
              == self.session_strategy.previous_process_session_id().as_ref()
            {
              // Session ID override hint provided and matches our expectations. Emit log with
              // overrides applied.
              (
                expected_previous_process_session_id,
                occurred_at,
                Some(LogFields::from([(
                  "_logged_at".into(),
                  LogFieldValue::String(metadata.timestamp.to_string()),
                )])),
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
                  expected_previous_process_session_id, metadata.timestamp, occurred_at
                ),
                || None,
              );

              // We log an internal log and continue processing the log.
              let _ignored = self
                .write_log_internal(
                  "failed to override log attributes, provided override attributes do not match \
                   expectations",
                  metadata
                    .fields
                    .clone()
                    .into_iter()
                    .chain([
                      (
                        "_original_session_id".into(),
                        LogFieldValue::String(session_id.clone()),
                      ),
                      (
                        "_override_session_id".into(),
                        LogFieldValue::String(expected_previous_process_session_id.clone()),
                      ),
                    ])
                    .collect(),
                  metadata.matching_fields.clone(),
                  session_id.clone(),
                  metadata.timestamp,
                )
                .await;

              // We drop the log as the provided override attributes do not match our expectations.
              return Ok(vec![]);
            }
          },
          Some(LogAttributesOverrides::OccurredAt(overridden_timestamp)) => {
            // Occurred at override provided. Emit log with overrides applied.
            (
              self.session_strategy.session_id(),
              overridden_timestamp,
              Some(LogFields::from([(
                "_logged_at".into(),
                LogFieldValue::String(metadata.timestamp.to_string()),
              )])),
            )
          },
          None => {
            // No overrides provided. Emit log without any overrides.
            (self.session_strategy.session_id(), metadata.timestamp, None)
          },
        };

        let processed_log = bd_log_primitives::Log {
          log_level: log.log_level,
          log_type: log.log_type,
          message: log.message,
          fields: if let Some(extra_fields) = extra_fields {
            metadata
              .fields
              .iter()
              .chain(extra_fields.iter())
              .map(|(k, v)| (k.clone(), v.clone()))
              .collect()
          } else {
            metadata.fields
          },
          matching_fields: metadata.matching_fields,
          occurred_at: timestamp,
          session_id,
        };

        self
          .write_log(processed_log, log.log_processing_completed_tx)
          .await
      },
      Err(e) => {
        // TODO(Augustyniak): Consider logging as error so that SDK customers can see these
        // errors which are mostly emitted as the result of calls into platform-provided metadata
        // provider.
        anyhow::bail!("failed to process a log inside of process_log section: {e}")
      },
    }
  }

  async fn write_log(
    &mut self,
    log: Log,
    log_processing_completed_tx: Option<oneshot::Sender<()>>,
  ) -> anyhow::Result<Vec<Log>> {
    let logs_to_inject = match &mut self.logging_state {
      LoggingState::Uninitialized(uninitialized_logging_context) => {
        let result = uninitialized_logging_context
          .pre_config_log_buffer
          .push(log);

        uninitialized_logging_context
          .stats
          .pre_config_log_buffer
          .record(&result);
        if let Err(e) = result {
          anyhow::bail!("failed to push log to a pre-config buffer: {e}");
        }

        if let Some(tx) = log_processing_completed_tx {
          if let Err(e) = tx.send(()) {
            anyhow::bail!("failed to send log processing completion message: {e:?}");
          }
        }

        vec![]
      },
      LoggingState::Initialized(initialized_logging_context) => self
        .replayer
        .replay_log(
          log,
          log_processing_completed_tx,
          &mut initialized_logging_context.processing_pipeline,
        )
        .await
        .map_err(|e| anyhow!("failed to replay async log buffer log: {e}"))?,
    };

    Ok(logs_to_inject)
  }

  async fn update(mut self, config: ConfigUpdate) -> (Self, Option<PreConfigBuffer<Log>>) {
    let (initialized_logging_context, maybe_pre_config_log_buffer) = match self.logging_state {
      LoggingState::Uninitialized(uninitialized_logging_context) => {
        let (initialized_logging_context, pre_config_log_buffer) = uninitialized_logging_context
          .updated(
            config,
            self.session_replay_capture_screenshot_handler.clone(),
          )
          .await;
        (initialized_logging_context, Some(pre_config_log_buffer))
      },
      LoggingState::Initialized(mut initialized_logging_context) => {
        initialized_logging_context.update(config);
        (initialized_logging_context, None)
      },
    };

    self.logging_state = LoggingState::Initialized(initialized_logging_context);

    (self, maybe_pre_config_log_buffer)
  }

  async fn maybe_replay_pre_config_buffer_logs(
    &mut self,
    pre_config_log_buffer: PreConfigBuffer<bd_log_primitives::Log>,
    initial_logs: &mut Vec<Log>,
  ) {
    let LoggingState::Initialized(initialized_logging_context) = &mut self.logging_state else {
      return;
    };

    for log_line in initial_logs.drain(..) {
      if let Err(e) = self
        .replayer
        .replay_log(
          log_line,
          None,
          &mut initialized_logging_context.processing_pipeline,
        )
        .await
      {
        log::debug!("failed to reply initial logs: {e}");
      }
    }

    for log_line in pre_config_log_buffer.pop_all() {
      if let Err(e) = self
        .replayer
        .replay_log(
          log_line,
          None,
          &mut initialized_logging_context.processing_pipeline,
        )
        .await
      {
        log::debug!("failed to reply pre-config log buffer logs: {e}");
      }
    }
  }

  pub async fn run(self, initial_logs: Vec<Log>) -> Self {
    let shutdown_trigger = ComponentShutdownTrigger::default();
    self
      .run_with_shutdown(shutdown_trigger.make_shutdown(), initial_logs)
      .await
  }

  // TODO(mattklein123): This seems to only be used for tests. Figure out how to clean this up
  // so we don't need this just for tests.
  pub async fn run_with_shutdown(
    mut self,
    mut shutdown: ComponentShutdown,
    mut initial_logs: Vec<Log>,
  ) -> Self {
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
      let initialized_logging_context =
        if let LoggingState::Initialized(initialized_logging_context) = &mut self.logging_state {
          Some(initialized_logging_context)
        } else {
          None
        };

      tokio::select! {
        Some(config) = self.config_update_rx.recv() => {
          let (updated_self, maybe_pre_config_buffer)
            = self.update(config).await;

          self = updated_self;
          if let Some(pre_config_log_buffer) = maybe_pre_config_buffer {
            self.maybe_replay_pre_config_buffer_logs(
                pre_config_log_buffer,
                &mut initial_logs
            ).await;
          }
        },
        Some(async_log_buffer_message) = self.communication_rx.recv() => {
          match async_log_buffer_message {
            AsyncLogBufferMessage::EmitLog(mut log) => {
              for interceptor in &mut self.interceptors {
                interceptor.process(
                  log.log_level,
                  log.log_type,
                  &log.message,
                  &mut log.fields,
                  &mut log.matching_fields,
                );
              }

              if let Err(e) = self.process_all_logs(log).await {
                log::debug!("failed to process all logs: {e}");
              }
            },
            AsyncLogBufferMessage::AddLogField(key, value) => {
              if let Err(e) = self.metadata_collector.add_field(key.clone().into(), value.clone()) {
                log::warn!("failed to add log field ({key:?}): {e}");
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
        () = async { initialized_logging_context.unwrap().processing_pipeline.run().await },
          if initialized_logging_context.is_some() => {},
        () = self.resource_utilization_reporter.run() => {},
        () = self.session_replay_recorder.run() => {},
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

  async fn write_log_internal(
    &mut self,
    msg: &str,
    fields: LogFields,
    matching_fields: LogFields,
    session_id: String,
    occurred_at: time::OffsetDateTime,
  ) -> anyhow::Result<()> {
    // TODO(mattklein123): Should we support injected logs for internal logs?
    self
      .write_log(
        Log {
          log_level: log_level::WARNING,
          log_type: LogType::InternalSDK,
          message: msg.into(),
          fields,
          matching_fields,
          session_id,
          occurred_at,
        },
        None,
      )
      .await?;

    Ok(())
  }
}
