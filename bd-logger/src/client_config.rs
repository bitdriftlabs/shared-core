// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./client_config_test.rs"]
mod client_config_test;

#[path = "./client_config/decode.rs"]
mod decode;

use crate::async_log_buffer::Sender as LogSender;
use crate::buffer_selector::BufferSelector;
use crate::device_command::{DeviceCommandDispatcher, RegisteredCommandDispatcher};
use crate::logging_state::{BufferProducers, ConfigUpdate};
use crate::write_log_to_buffer;
use bd_api::{DataUpload, TriggerUpload};
use bd_buffer::{BufferSettings, RingBuffer as _};
use bd_client_common::error::InvariantError;
use bd_client_common::file::{read_compressed, write_compressed};
use bd_client_common::payload_conversion::{ClientConfigurationUpdateAck, IntoRequest};
use bd_client_common::safe_file_cache::SafeFileCache;
use bd_client_common::{HANDSHAKE_FLAG_CONFIG_UP_TO_DATE, RawConfigurationUpdate};
use bd_client_stats_store::{Counter, Scope};
use bd_log_filter::FilterChain;
use bd_log_matcher::matcher::MatchContext;
use bd_log_primitives::tiny_set::TinyMap;
use bd_log_primitives::{EncodableLog, FieldsRef};
use bd_proto::protos::client::api::configuration_update_ack::Nack;
use bd_proto::protos::client::api::{ApiRequest, ConfigurationUpdateAck, HandshakeRequest};
use bd_stats_common::Counter as _;
use bd_time::TimeProvider;
use bd_workflows::config::WorkflowsConfiguration;
use decode::TailUpdate;
use itertools::Itertools;
use parking_lot::Mutex;
use protobuf::Chars;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc::Sender;

// Helper trait to make it easier to test the internals without having to broadcast to an actual
// logger.
#[async_trait::async_trait]
pub trait ApplyConfig {
  async fn apply_configuration(
    &self,
    configuration: Configuration,
    from_cache: bool,
  ) -> anyhow::Result<()>;
}

pub struct Configuration {
  buffer: Vec<BufferSettings>,
  buffer_selector: BufferSelector,
  workflows: WorkflowsConfiguration,
  bdtail: TailUpdate,
  filters: FilterChain,
  filter_parse_failures: u64,
}

fn decode_cached_configuration(bytes: &[u8]) -> anyhow::Result<RawConfigurationUpdate> {
  RawConfigurationUpdate::new(&read_compressed(bytes)?)
}

// Manages config validation and persistence.
pub struct Config<A: ApplyConfig> {
  file_cache: SafeFileCache<RawConfigurationUpdate>,

  // The currently applied version id, if a configuration has been applied.
  configuration_version_id: Mutex<Option<String>>,

  // Delegate used to apply the configuration. This allows for easier testing.
  #[allow(clippy::struct_field_names)]
  apply_config: A,

  time_provider: Arc<dyn TimeProvider>,
  sdk_status_tracker: bd_client_common::sdk_status::SdkStatusTracker,
}

impl<A: ApplyConfig> Config<A> {
  #[cfg(test)]
  pub fn new(sdk_directory: &Path, apply_config: A) -> Self {
    Self::new_with_time_provider(
      sdk_directory,
      apply_config,
      Arc::new(bd_time::SystemTimeProvider),
      bd_client_common::sdk_status::SdkStatusTracker::new(),
    )
  }

  pub fn new_with_time_provider(
    sdk_directory: &Path,
    apply_config: A,
    time_provider: Arc<dyn TimeProvider>,
    sdk_status_tracker: bd_client_common::sdk_status::SdkStatusTracker,
  ) -> Self {
    Self {
      file_cache: SafeFileCache::new_with_decoder(
        "config",
        sdk_directory,
        time_provider.clone(),
        decode_cached_configuration,
      ),
      configuration_version_id: Mutex::default(),
      apply_config,
      time_provider,
      sdk_status_tracker,
    }
  }

  // Process a new configuration update. If the configuration failed to apply, returns a Nack
  // containing the error details.
  async fn process_configuration_update_inner(
    &self,
    update: RawConfigurationUpdate,
    from_cache: bool,
  ) -> anyhow::Result<()> {
    self
      .apply_config
      .apply_configuration(Configuration::from_bytes(&update.bytes)?, from_cache)
      .await?;

    // Since we've validated that the configuration works and has been applied, we keep track
    // of the id here in order to surface it both via handshake requests sent on stream creation
    // as well as configuration acks/nacks.
    *self.configuration_version_id.lock() = Some(update.version_nonce.clone());

    Ok(())
  }

  pub async fn process_configuration_update(
    &self,
    update: RawConfigurationUpdate,
  ) -> anyhow::Result<()> {
    let compressed_protobuf = write_compressed(&update.bytes)?;
    let version_nonce = update.version_nonce.clone();

    // Upon applying the configuration successfully, write the configuration proto to disk.
    // This ensures that when we come up we can immediately start processing logs without
    // having to wait for the API server to respond.
    // TODO(snowp): Consider storing an intermediate format to avoid all the error checking
    // above on re-read.
    // If we fail writing to disk, move on. We'll continue to operate without disk caching.
    let result = self
      .file_cache
      .cache_update(compressed_protobuf, &version_nonce, async move {
        self.process_configuration_update_inner(update, false).await
      })
      .await;

    if result.is_ok() {
      self
        .sdk_status_tracker
        .record_config_delivery(self.time_provider.now());
    }

    result
  }

  // Attempts to load persisted config and apply as if it was a newly received configuration.
  pub async fn try_load_persisted_config_helper(&self) {
    if let Some(configuration_update) = self.file_cache.handle_cached_config().await {
      // If this function succeeds, it should write back the file to disk.
      let maybe_nack = self
        .process_configuration_update_inner(configuration_update, true)
        .await;

      // We should never persist config that results in a Nack, but if we do we effectively drop
      // the config on startup as the above function won't write it back.
      debug_assert!(maybe_nack.is_ok());
    }
  }
}

#[async_trait::async_trait]
impl<A: ApplyConfig + Send + Sync> bd_client_common::ClientConfigurationUpdate for Config<A> {
  async fn clear_cached_config(&self) {
    self.file_cache.reset().await;
  }

  async fn try_apply_config(&self, configuration_update: RawConfigurationUpdate) -> ApiRequest {
    let version_nonce = configuration_update.version_nonce.clone();

    let nack = if let Err(e) = self
      .process_configuration_update(configuration_update)
      .await
    {
      Some(Nack {
        version_nonce: version_nonce.clone(),
        error_details: e.to_string(),
        ..Default::default()
      })
    } else {
      None
    };

    ClientConfigurationUpdateAck(ConfigurationUpdateAck {
      nack: nack.into(),
      last_applied_version_nonce: version_nonce,
      ..Default::default()
    })
    .into_request()
  }

  async fn try_load_persisted_config(&self) {
    self.try_load_persisted_config_helper().await;
  }

  fn fill_handshake(&self, handshake: &mut HandshakeRequest) {
    handshake.configuration_version_nonce = self
      .configuration_version_id
      .lock()
      .clone()
      .unwrap_or_default();
  }

  async fn on_handshake_complete(&self, configuration_update_status: u32) {
    if configuration_update_status & HANDSHAKE_FLAG_CONFIG_UP_TO_DATE != 0 {
      self.file_cache.mark_safe().await;
      self
        .sdk_status_tracker
        .record_config_delivery(self.time_provider.now());
    }
  }

  async fn mark_safe(&self) {
    self.file_cache.mark_safe().await;
  }
}

// Update handle that updates the buffer configuration and thread local state for a given logger.
pub struct LoggerUpdate {
  buffer_manager: Arc<bd_buffer::Manager>,
  workflow_attachment_cleanup_ready: Arc<AtomicBool>,
  config_update_tx: Sender<ConfigUpdate>,
  device_command_dispatcher: DeviceCommandDispatcher,
  stream_config_parse_failure: Counter,
  filter_config_parse_failure: Counter,
}

impl LoggerUpdate {
  pub(crate) fn new(
    buffer_manager: Arc<bd_buffer::Manager>,
    workflow_attachment_cleanup_ready: Arc<AtomicBool>,
    config_update_tx: Sender<ConfigUpdate>,
    data_upload_tx: Sender<DataUpload>,
    log_sender: LogSender,
    trigger_upload_tx: Sender<TriggerUpload>,
    session_strategy: Arc<bd_session::Strategy>,
    artifact_client: Arc<dyn bd_artifact_upload::Client>,
    command_dispatcher: RegisteredCommandDispatcher,
    remote_screenshot_capture_handler: bd_session_replay::RemoteScreenshotCaptureHandler,
    scope: &Scope,
  ) -> Self {
    Self {
      buffer_manager,
      workflow_attachment_cleanup_ready,
      config_update_tx,
      device_command_dispatcher: DeviceCommandDispatcher::new(
        data_upload_tx,
        log_sender,
        trigger_upload_tx,
        session_strategy,
        artifact_client,
        command_dispatcher,
        remote_screenshot_capture_handler,
      ),
      stream_config_parse_failure: scope.counter("stream_config_parse_failure"),
      filter_config_parse_failure: scope.counter("filter_config_parse_failure"),
    }
  }
}

#[async_trait::async_trait]
impl ApplyConfig for LoggerUpdate {
  async fn apply_configuration(
    &self,
    configuration: Configuration,
    from_cache: bool,
  ) -> anyhow::Result<()> {
    let Configuration {
      buffer,
      buffer_selector,
      workflows,
      mut bdtail,
      filters,
      filter_parse_failures,
    } = configuration;
    let has_active_tail_streams = bdtail.has_live_streams;

    // During startup, this first trigger-buffer config update is the ordering barrier for
    // trigger-upload recovery. The buffer manager does not return until the full trigger-buffer
    // config update has been processed by BufferUploadManager, and that path reconciles and
    // replays persisted trigger uploads against the final configured trigger-buffer set. Because
    // BufferProducers are only rebuilt after this await, recovered trigger uploads can re-lock
    // trigger buffers before new thread-local producers are exposed to normal log writing.
    let maybe_stream_buffer = self
      .buffer_manager
      .update_buffers(&buffer, has_active_tail_streams)
      .await?;
    self
      .workflow_attachment_cleanup_ready
      .store(true, Ordering::Release);

    debug_assert_eq!(maybe_stream_buffer.is_some(), has_active_tail_streams);

    let workflows_configuration = workflows;
    let filter_chain = filters;
    self
      .filter_config_parse_failure
      .inc_by(filter_parse_failures);

    let device_commands = std::mem::take(&mut bdtail.device_commands);

    if let Err(e) = self
      .config_update_tx
      .send(ConfigUpdate {
        buffer_producers: BufferProducers::new(&self.buffer_manager)?,
        buffer_selector,
        // TODO(Augustyniak): Propagate the information about invalid workflows to server.
        workflows_configuration,
        tail_configs: TailConfigurations::new(
          bdtail,
          || {
            // This is only called if we have active streams, which means we should have an active
            // stream buffer.
            Ok(
              maybe_stream_buffer
                .ok_or(InvariantError::Invariant)?
                .register_producer()?,
            )
          },
          || self.stream_config_parse_failure.inc(),
        )?,
        filter_chain,
        from_cache,
      })
      .await
    {
      log::debug!("failed to push config update to a channel: {e:?}");
      return Ok(());
    }

    if !from_cache {
      self
        .device_command_dispatcher
        .dispatch_configuration(device_commands);
    }

    Ok(())
  }
}

// Helper struct that allows us to bundle all the relevant data into one Option within
// TailConfigurations.
struct Inner {
  // List of active tail configurations with their optional log matcher.
  active_streams: Vec<(Chars, Option<bd_log_matcher::matcher::Tree>)>,

  // The buffer producer to write streamd logs to. When there are no active streams the streaming
  // buffer is deallocated.
  stream_producer: bd_buffer::Producer,
}

#[derive(Default)]
pub struct TailConfigurations {
  // The inner structure is only initialized when there are any active streams.
  inner: Option<Inner>,
}

impl TailConfigurations {
  fn new(
    config: TailUpdate,
    producer: impl FnOnce() -> anyhow::Result<bd_buffer::Producer>,
    on_parse_failure: impl Fn(),
  ) -> anyhow::Result<Self> {
    for _ in 0 .. config.parse_failures {
      on_parse_failure();
    }
    if config.active_streams.is_empty() {
      log::debug!("zero active bdtail streams");
      return Ok(Self::default());
    }

    let active_streams = config.active_streams;

    if active_streams.is_empty() {
      log::debug!("zero active live bdtail streams");
      return Ok(Self::default());
    }

    log::debug!("{} active bdtail streams", active_streams.len());

    Ok(Self {
      inner: Some(Inner {
        active_streams,
        stream_producer: producer()?,
      }),
    })
  }

  pub(crate) fn maybe_stream_log(
    &mut self,
    log: &mut EncodableLog,
    state: &dyn bd_state::StateReader,
  ) -> anyhow::Result<bool> {
    let Some(inner) = &mut self.inner else {
      return Ok(false);
    };

    let active_streams = inner
      .active_streams
      .iter()
      .filter_map(|(id, matcher)| {
        matcher
          .as_ref()
          .is_none_or(|matcher| {
            matcher.do_match(
              log.log.log_level,
              log.log.log_type,
              &log.log.message,
              FieldsRef::new(&log.log.fields, &log.log.matching_fields),
              state,
              &TinyMap::default(),
              0,
              MatchContext::default(),
            )
          })
          .then_some(id.as_str())
      })
      .collect_vec();

    if active_streams.is_empty() {
      return Ok(false);
    }

    write_log_to_buffer(&mut inner.stream_producer, log, &[], &active_streams)?;

    Ok(true)
  }
}
