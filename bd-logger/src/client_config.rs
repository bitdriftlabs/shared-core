// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./client_config_test.rs"]
mod client_config_test;

use crate::logging_state::{BufferProducers, ConfigUpdate};
use anyhow::anyhow;
use bd_api::{ClientConfigurationUpdate, FromResponse, IntoRequest};
use bd_buffer::{AbslCode, RingBuffer as _};
use bd_client_common::fb::make_log;
use bd_client_stats_store::{Counter, Scope};
use bd_filters::FiltersChain;
use bd_log_primitives::LogRef;
use bd_proto::protos::bdtail::bdtail_config::BdTailConfigurations;
use bd_proto::protos::client::api::configuration_update::{StateOfTheWorld, Update_type};
use bd_proto::protos::client::api::configuration_update_ack::Nack;
use bd_proto::protos::client::api::{
  ApiRequest,
  ApiResponse,
  ConfigurationUpdate,
  ConfigurationUpdateAck,
  HandshakeRequest,
};
use bd_proto::protos::config::v1::config::BufferConfigList;
use bd_proto::protos::filter::filter::FiltersConfiguration;
use bd_proto::protos::insight::insight::InsightsConfiguration;
use bd_proto::protos::workflow::workflow::WorkflowsConfiguration as WorkflowsConfigurationProto;
use bd_runtime::runtime::workflows::WorkflowsEnabledFlag;
use bd_runtime::runtime::{ConfigLoader, Watch};
use bd_workflows::config::WorkflowsConfiguration;
use itertools::Itertools;
use protobuf::{Chars, Message};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

// Helper trait to make it easier to test the internals without having to broadcast to an actual
// logger.
#[async_trait::async_trait]
pub trait ApplyConfig {
  async fn apply_configuration(&mut self, configuration: Configuration) -> anyhow::Result<()>;
}

#[cfg_attr(test, derive(Debug, Default, PartialEq))]
pub struct Configuration {
  buffer: BufferConfigList,
  workflows: WorkflowsConfigurationProto,
  insights: InsightsConfiguration,
  bdtail: BdTailConfigurations,
  filters: FiltersConfiguration,
}

impl Configuration {
  fn new(sow: &StateOfTheWorld) -> Self {
    Self {
      buffer: sow.buffer_config_list.clone().unwrap_or_default(),
      workflows: sow.workflows_configuration.clone().unwrap_or_default(),
      insights: sow.insights_configuration.clone().unwrap_or_default(),
      bdtail: sow.bdtail_configuration.clone().unwrap_or_default(),
      filters: sow.filters_configuration.clone().unwrap_or_default(),
    }
  }
}

// Manages config validation and persistence.
pub struct Config<A: ApplyConfig> {
  // The file to write the configuration data to.
  configuration_file: PathBuf,

  // The currently applied version id, if a configuration has been applied.
  configuration_version_id: Option<String>,

  // Tracks how often we fail to serialize the configuration to disk.
  #[allow(clippy::struct_field_names)]
  config_cache_failure: Counter,

  // Delegate used to apply the configuration. This allows for easier testing.
  #[allow(clippy::struct_field_names)]
  apply_config: A,
}

impl<A: ApplyConfig> Config<A> {
  pub fn new(sdk_directory: &Path, apply_config: A, stats: &Scope) -> anyhow::Result<Self> {
    if !sdk_directory.exists() {
      anyhow::bail!("an invalid sdk directory was provided: {sdk_directory:?}");
    }

    Ok(Self {
      configuration_file: sdk_directory.join("config.pb"),
      configuration_version_id: None,
      config_cache_failure: stats.scope("config").counter("cache_failure"),
      apply_config,
    })
  }

  // Process a new configuration update. If the configuration failed to apply, returns a Nack
  // containing the error details.
  pub async fn process_configuration_update(
    &mut self,
    update: &ConfigurationUpdate,
  ) -> anyhow::Result<()> {
    let config = update
      .update_type
      .as_ref()
      .ok_or_else(|| anyhow!("An invalid match configuration was received: missing oneof"))?;

    let Update_type::StateOfTheWorld(sotw) = config;

    self
      .apply_config
      .apply_configuration(Configuration::new(sotw))
      .await?;

    // Upon applying the configuration sucesfully, write the configuration proto to disk.
    // This ensures that when we come up we can immediately start processing logs without
    // having to wait for the API server to respond.
    // TODO(snowp): Consider storing an intermediate format to avoid all the error checking
    // above on re-read.
    let buffer = update.write_to_bytes()?;

    // If we fail writing to disk, record a counter and move on. We'll continue to operate
    // without disk caching.
    // TODO(snowp): Consider ways to expose what is going on here, Rust doesn't expose a way to
    // check if this is an out of disk issue without parsing the error message (not platform
    // independent) so it's tricky to avoid this being noisy. We may consider doing such
    // parsing on the server side.
    if std::fs::write(&self.configuration_file, buffer).is_err() {
      self.config_cache_failure.inc();
    }

    // Since we've validated that the configuration works and has been applied, we keep track
    // of the id here in order to surface it both via handshake requests sent on stream creation
    // as well as configuration acks/nacks.
    self.configuration_version_id = Some(update.version_nonce.clone());

    Ok(())
  }

  // Attempts to load persisted config and apply as if it was a newly received configuration.
  pub async fn try_load_persisted_config_helper(&mut self) -> anyhow::Result<()> {
    let result: anyhow::Result<()> = {
      let data = std::fs::read(&self.configuration_file)?;

      // As soon as we've read the file contents, delete it. If we fail to parse the contents,
      // the file will be gone and it won't be read again. If we succeed to parse and apply the
      // contents, we end up writing the file back to disk.
      std::fs::remove_file(&self.configuration_file)?;

      // We don't use handle_unexpected_error here for custom logging and to let us unit test this
      // code path.
      // TODO(snowp): Track failures here via analytic events.
      let configuration_update: ConfigurationUpdate = ConfigurationUpdate::parse_from_bytes(&data)?;

      // If this function succeeds, it should write back the file to disk.
      let maybe_nack = self
        .process_configuration_update(&configuration_update)
        .await;

      // We should never persist config that results in a Nack, but if we do we effectively drop the
      // config on startup as the above function won't write it back.
      debug_assert!(maybe_nack.is_ok());

      Ok(())
    };

    result.map_err(|e| e.context("failed to load persisted config"))
  }
}

#[async_trait::async_trait]
impl<A: ApplyConfig + Send + Sync> bd_api::ConfigurationUpdate for Config<A> {
  async fn try_apply_config(&mut self, response: &ApiResponse) -> Option<ApiRequest> {
    let configuration_update = ConfigurationUpdate::from_response(response)?;
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

    Some(
      ClientConfigurationUpdate(ConfigurationUpdateAck {
        nack: nack.into(),
        last_applied_version_nonce: version_nonce,
        ..Default::default()
      })
      .into_request(),
    )
  }

  async fn try_load_persisted_config(&mut self) {
    let _ignored = self.try_load_persisted_config_helper().await;
  }

  fn partial_handshake(&self) -> HandshakeRequest {
    HandshakeRequest {
      configuration_version_nonce: self.configuration_version_id.clone().unwrap_or_default(),
      ..Default::default()
    }
  }

  fn on_handshake_complete(&self) {}
}

// Update handle that updates the buffer configuration and thread local state for a given logger.
pub struct LoggerUpdate {
  buffer_manager: Arc<bd_buffer::Manager>,
  config_update_tx: Sender<ConfigUpdate>,
  workflows_enabled_flag: Watch<bool, WorkflowsEnabledFlag>,
  stream_config_parse_failure: Counter,
}

impl LoggerUpdate {
  pub(crate) fn new(
    buffer_manager: Arc<bd_buffer::Manager>,
    config_update_tx: Sender<ConfigUpdate>,
    runtime: &Arc<ConfigLoader>,
    scope: &Scope,
  ) -> Self {
    Self {
      buffer_manager,
      config_update_tx,
      workflows_enabled_flag: runtime.register_watch().unwrap(),
      stream_config_parse_failure: scope.counter("stream_config_parse_failure"),
    }
  }
}

#[async_trait::async_trait]
impl ApplyConfig for LoggerUpdate {
  async fn apply_configuration(&mut self, configuration: Configuration) -> anyhow::Result<()> {
    let Configuration {
      buffer,
      workflows,
      insights,
      bdtail,
      ..
    } = configuration;

    let maybe_stream_buffer = self
      .buffer_manager
      .update_from_config(&buffer, !bdtail.active_streams.is_empty())
      .await?;

    debug_assert_eq!(
      maybe_stream_buffer.is_some(),
      !bdtail.active_streams.is_empty()
    );

    // It's in here so that we do not even attempt to parse workflow protos if workflows
    // are disabled.
    // TODO(Augustyniak): Consider removing this feature flag once workflows APIs are stable.
    let workflows_configuration = if self.workflows_enabled_flag.read() {
      WorkflowsConfiguration::new(&workflows, &insights)
    } else {
      WorkflowsConfiguration::default()
    };

    if let Err(e) = self
      .config_update_tx
      .send(ConfigUpdate {
        buffer_producers: BufferProducers::new(&self.buffer_manager)?,
        buffer_selector: bd_matcher::buffer_selector::BufferSelector::new(&buffer)?,
        // TODO(Augustyniak): Propagate the information about invalid workflows to server.
        workflows_configuration,
        tail_configs: TailConfigurations::new(
          bdtail,
          || {
            // This is only called if we have active streams, which means we should have an active
            // stream buffer.
            // register_producer never fails for the volatile buffer.
            maybe_stream_buffer.unwrap().register_producer().unwrap()
          },
          || self.stream_config_parse_failure.inc(),
        )?,
        filters_chain: FiltersChain::new(configuration.filters),
      })
      .await
    {
      log::debug!("failed to inform push workflows config update to a channel: {e:?}");
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

impl Inner {
  fn write_to_stream_buffer<'a>(
    stream_producer: &mut bd_buffer::Producer,
    buffers: &mut BufferProducers,
    stream_ids: impl Iterator<Item = &'a str>,
    log: &LogRef<'_>,
  ) -> anyhow::Result<()> {
    make_log(
      &mut buffers.builder,
      log.log_level,
      log.log_type,
      log.message,
      log.fields.captured_fields,
      log.session_id,
      &log.occurred_at,
      std::iter::empty(),
      stream_ids,
      |data| {
        // TODO(snowp): For both logger and buffer lookup we end up doing a map lookup, which
        // seems less than ideal in the logging path. Look into ways to optimize this,
        // possibly via vector indices instead of string keys.
        match stream_producer.write(data) {
          // If the buffer is locked, drop the error. This helps ensure that we are able to
          // log to all buffers even if one of them is locked.
          // TODO(snowp): Track how often logs are dropped due to locks.
          // If the buffer is out of space, drop the error.
          // TODO(mattklein123): Track this via stats.
          Err(bd_buffer::Error::AbslStatus(
            AbslCode::FailedPrecondition | AbslCode::ResourceExhausted,
            _,
          )) => Ok(()),
          e => e,
        }?;
        Ok(())
      },
    )?;

    Ok(())
  }
}

#[derive(Default)]
pub(crate) struct TailConfigurations {
  // The inner structure is only initialized when there are any active streams.
  inner: Option<Inner>,
}

impl TailConfigurations {
  fn new(
    config: BdTailConfigurations,
    producer: impl FnOnce() -> bd_buffer::Producer,
    on_parse_failure: impl Fn(),
  ) -> anyhow::Result<Self> {
    if config.active_streams.is_empty() {
      log::debug!("zero active bdtail streams");
      return Ok(Self::default());
    }

    let mut active_streams = Vec::new();
    for stream in config.active_streams {
      let matcher = if let Some(matcher_config) = stream.matcher.into_option() {
        match bd_log_matcher::matcher::Tree::new(&matcher_config) {
          Ok(matcher) => Some(matcher),
          Err(e) => {
            // If the are unable to parse the config, ignore the stream config but do not fail
            // the overall config update.
            log::debug!("failed to parse stream match config: {e}");
            on_parse_failure();
            continue;
          },
        }
      } else {
        None
      };

      active_streams.push((stream.stream_id.clone(), matcher));
    }

    log::debug!("{} active bdtail streams", active_streams.len());

    Ok(Self {
      inner: Some(Inner {
        active_streams,
        stream_producer: producer(),
      }),
    })
  }

  pub(crate) fn maybe_stream_log(
    &mut self,
    buffers: &mut BufferProducers,
    log: &LogRef<'_>,
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
          .map_or(true, |matcher| {
            matcher.do_match(log.log_level, log.log_type, log.message, log.fields)
          })
          .then_some(id.as_str())
      })
      .collect_vec();

    if active_streams.is_empty() {
      return Ok(false);
    }

    Inner::write_to_stream_buffer(
      &mut inner.stream_producer,
      buffers,
      active_streams.into_iter(),
      log,
    )?;

    Ok(true)
  }
}
