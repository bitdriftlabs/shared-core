// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Drives the per-log path for benchmarks: admission on the caller's thread, then the event loop's
//! dequeue, normalization, processing pipeline and buffer writes. The event loop's task and its
//! other branches do not run, so each stage can be measured on its own.

use super::{AdmissionError, AsyncLogBuffer, LogAdmission, LogLine, LoggerControl, Sender};
use crate::StartupReplayEligibility;
use crate::buffer_selector::BufferSelector;
use crate::client_config::{Configuration, TailConfigurations};
use crate::device_command::{CommandExecutionPolicy, RegisteredCommandDispatcher};
use crate::log_replay::LoggerReplay;
use crate::logging_state::{BufferProducers, ConfigUpdate, UninitializedLoggingContext};
use bd_api::SimpleNetworkQualityProvider;
use bd_client_common::init_lifecycle::InitLifecycleState;
use bd_client_common::sdk_status::SdkStatusTracker;
use bd_client_stats::{FlushTrigger, Stats};
use bd_client_stats_store::Collector;
use bd_event_buffer::{EventBufferEntry, LoggerIngressPayload};
use bd_log_filter::{FilterChain, ScrubRules};
use bd_log_metadata::MetadataProvider;
use bd_log_primitives::LogFieldValue;
use bd_proto::protos::client::api::configuration_update::StateOfTheWorld;
use bd_runtime::runtime::ConfigLoader;
use bd_shutdown::ComponentShutdownTrigger;
use bd_time::TimeProvider;
use bd_workflows::config::WorkflowsConfiguration;
use bd_workflows::engine::ProcessLocalPendingFlushState;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// The components the platform supplies, which benchmarks build from test helpers.
pub struct PlatformParts {
  pub metadata_provider: Arc<dyn MetadataProvider + Send + Sync>,
  pub session_strategy: Arc<bd_session::Strategy>,
  pub store: Arc<bd_key_value::Store>,
  pub time_provider: Arc<dyn TimeProvider>,
  pub resource_utilization_target: Box<dyn bd_resource_utilization::Target + Send + Sync>,
  pub session_replay_target: Box<dyn bd_session_replay::Target + Send + Sync>,
  pub events_listener_target: Box<dyn bd_events::ListenerTarget + Send + Sync>,
}

/// An initialized logger pipeline whose stages are driven one log at a time.
pub struct PipelineBenchmark {
  // Declared first so it is dropped before the runtime and the directory it uses.
  buffer: Option<AsyncLogBuffer<LoggerReplay>>,
  sender: Sender,
  state_store: bd_state::Store,
  runtime: tokio::runtime::Runtime,
  _buffer_manager: Arc<bd_buffer::Manager>,
  _shutdown: ComponentShutdownTrigger,
  _channels: Vec<Box<dyn std::any::Any>>,
}

impl PipelineBenchmark {
  /// Builds the pipeline in `directory` and applies `state_of_the_world` as the first
  /// configuration. Runtime flags, such as field interning, are read from `runtime_loader`.
  pub fn new(
    directory: &Path,
    runtime_loader: &Arc<ConfigLoader>,
    state_of_the_world: StateOfTheWorld,
    parts: PlatformParts,
  ) -> anyhow::Result<Self> {
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()?;
    let collector = Collector::default();
    let stats = Stats::new(collector.clone());
    let shutdown = ComponentShutdownTrigger::default();

    std::fs::create_dir_all(directory.join("buffer"))?;
    let (buffer_manager, mut buffer_event_rx) = bd_buffer::Manager::new(
      directory.join("buffer"),
      &collector.scope(""),
      runtime_loader,
      Arc::new(bd_versioned_kv::RetentionRegistry::new(
        bd_runtime::runtime::IntWatch::new_for_testing(0),
      )),
    );

    let (trigger_upload_tx, trigger_upload_rx) = tokio::sync::mpsc::channel(1);
    let (remote_flush_streaming_tx, remote_flush_streaming_rx) = tokio::sync::mpsc::channel(1);
    let (data_upload_tx, data_upload_rx) = tokio::sync::mpsc::channel(1);
    let (flush_buffers_tx, flush_buffers_rx) = tokio::sync::mpsc::channel(1);
    let (flush_stats_trigger, flush_stats_rx) = FlushTrigger::new();
    let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
    let (report_tx, report_rx) = tokio::sync::mpsc::channel(1);

    let logging_context = UninitializedLoggingContext::new(
      directory,
      runtime_loader,
      collector.scope(""),
      stats,
      trigger_upload_tx,
      remote_flush_streaming_rx,
      data_upload_tx.clone(),
      flush_buffers_tx,
      flush_stats_trigger,
      1_000_000,
      Arc::new(AtomicBool::new(false)),
      Arc::new(ProcessLocalPendingFlushState::default()),
      None,
    );

    let time_provider = parts.time_provider;
    let network_quality_provider = Arc::new(SimpleNetworkQualityProvider::default());
    let (buffer, sender) = runtime.block_on(async {
      AsyncLogBuffer::new(
        logging_context,
        LoggerReplay,
        parts.session_strategy,
        parts.metadata_provider,
        [].into(),
        [].into(),
        parts.resource_utilization_target,
        parts.session_replay_target,
        parts.events_listener_target,
        RegisteredCommandDispatcher::default(),
        CommandExecutionPolicy::new(runtime_loader),
        config_update_rx,
        report_rx,
        shutdown.make_handle(),
        runtime_loader,
        network_quality_provider.clone(),
        network_quality_provider,
        String::new(),
        &parts.store,
        time_provider.clone(),
        InitLifecycleState::new(),
        SdkStatusTracker::new(),
        data_upload_tx,
        StartupReplayEligibility::Unknown,
      )
    });

    let state_store = bd_state::Store::in_memory(
      time_provider,
      None,
      runtime_loader,
      &collector.scope("state"),
    );

    let buffer = runtime.block_on(async {
      // Buffer configuration waits for the upload side to take each buffer event; nothing
      // uploads here, so the events are only drained.
      let drain = tokio::spawn(async move { while buffer_event_rx.recv().await.is_some() {} });
      let configuration = Configuration::new(state_of_the_world);
      buffer_manager
        .update_from_config(&configuration.buffer, false)
        .await?;
      let (filter_chain, _) = FilterChain::new(configuration.filters);
      let (scrub_rules, _) = ScrubRules::new(configuration.scrub);
      let config = ConfigUpdate {
        buffer_producers: BufferProducers::new(&buffer_manager)?,
        buffer_selector: BufferSelector::new(&configuration.buffer)?,
        workflows_configuration: WorkflowsConfiguration::new(
          configuration.workflows.workflows,
          configuration.debug_workflows.workflows,
        ),
        tail_configs: TailConfigurations::default(),
        scrub_rules: Arc::new(scrub_rules),
        filter_chain,
        from_cache: false,
      };
      let buffer = buffer.update(config, &state_store).await;
      buffer.event_buffer.start_startup_gate(None);
      buffer.event_buffer.mark_startup_gate_ready();
      // The first batch reports the gate opening, without entries.
      let batch = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        buffer.event_buffer.next_batch(1),
      )
      .await?;
      anyhow::ensure!(
        batch.entries.is_empty(),
        "unexpected entry before the first log"
      );
      drop(drain);
      anyhow::Ok(buffer)
    })?;

    Ok(Self {
      buffer: Some(buffer),
      sender,
      state_store,
      runtime,
      _buffer_manager: buffer_manager,
      _shutdown: shutdown,
      _channels: vec![
        Box::new(trigger_upload_rx),
        Box::new(remote_flush_streaming_tx),
        Box::new(data_upload_rx),
        Box::new(flush_buffers_rx),
        Box::new(flush_stats_rx),
        Box::new(config_update_tx),
        Box::new(report_tx),
      ],
    })
  }

  /// Admits a log as `LoggerHandle::log` does on the caller's thread: captures the provider fields
  /// and session, then queues the log.
  pub fn admit(&self, log: LogLine) -> Result<(), AdmissionError> {
    self.sender.try_send_log(log, LogAdmission::Current)
  }

  /// Takes the next queued log and processes it as the event loop does: interceptors,
  /// normalization, then the processing pipeline and buffer writes. Fails unless exactly one log
  /// was queued, so each call measures one log.
  pub fn process_next(&mut self) -> anyhow::Result<()> {
    let buffer = self
      .buffer
      .as_mut()
      .ok_or_else(|| anyhow::anyhow!("closed"))?;
    let state_store = &self.state_store;
    self.runtime.block_on(async {
      let batch = buffer.event_buffer.next_batch(1).await;
      anyhow::ensure!(batch.entries.len() == 1, "expected one queued log");
      for entry in batch.entries {
        let EventBufferEntry::Ingress(event) = entry else {
          anyhow::bail!("unexpected control entry");
        };
        let (context, payload, _completion) = event.into_parts();
        let LoggerIngressPayload::Log(mut log) = payload else {
          anyhow::bail!("unexpected ingress payload");
        };
        for interceptor in &mut buffer.interceptors {
          interceptor.process(
            log.log_level,
            log.log_type,
            &log.message,
            &mut log.fields,
            &mut log.matching_fields,
          );
        }
        buffer
          .process_all_logs(log, state_store, Some(context))
          .await?;
      }
      Ok(())
    })
  }

  /// Sets a persistent custom field, as `LoggerHandle::add_log_field` does.
  pub fn add_field(&mut self, key: &str, value: LogFieldValue) -> anyhow::Result<()> {
    let buffer = self
      .buffer
      .as_mut()
      .ok_or_else(|| anyhow::anyhow!("closed"))?;
    let state_store = &self.state_store;
    self.runtime.block_on(buffer.process_control(
      LoggerControl::AddLogField(key.to_string(), value),
      state_store,
    ));
    Ok(())
  }

  /// Sets a persistent OOTB field, as `LoggerHandle::update_ootb_log_field` does.
  pub fn update_ootb_field(&mut self, key: &str, value: LogFieldValue) -> anyhow::Result<()> {
    let buffer = self
      .buffer
      .as_mut()
      .ok_or_else(|| anyhow::anyhow!("closed"))?;
    let state_store = &self.state_store;
    self.runtime.block_on(buffer.process_control(
      LoggerControl::UpdateOotbLogField(key.to_string(), value),
      state_store,
    ));
    Ok(())
  }
}
