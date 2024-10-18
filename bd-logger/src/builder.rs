// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::async_log_buffer::AsyncLogBuffer;
use crate::client_config::{self, LoggerUpdate};
use crate::consumer::BufferUploadManager;
use crate::internal::InternalLogger;
use crate::log_replay::LoggerReplay;
use crate::logger::{ChannelPair, Logger};
use crate::logging_state::UninitializedLoggingContext;
use crate::InitParams;
use bd_api::api::SimpleNetworkQualityProvider;
use bd_api::DataUpload;
use bd_client_common::error::handle_unexpected;
use bd_client_stats_store::{Collector, Scope};
use bd_internal_logging::NoopLogger;
use bd_runtime::runtime;
use bd_shutdown::{ComponentShutdownTrigger, ComponentShutdownTriggerHandle};
use bd_time::SystemTimeProvider;
use futures_util::{try_join, Future};
use std::pin::Pin;
use std::sync::Arc;

/// A builder for the logger.
pub struct LoggerBuilder {
  // The parameters required to initialize the logger.
  params: InitParams,

  component_shutdown_handle: Option<ComponentShutdownTriggerHandle>,
  mobile_features: bool,
  internal_logger: bool,
  stats_scope: Option<Scope>,
}

impl LoggerBuilder {
  /// Creates a new logger builder with the provided parameters.
  #[must_use]
  pub const fn new(params: InitParams) -> Self {
    Self {
      params,
      component_shutdown_handle: None,
      mobile_features: false,
      internal_logger: false,
      stats_scope: None,
    }
  }

  /// Sets the component shutdown handle to be used by the logger. By default the logger will
  /// create its own shutdown handle and manage it internally, invoked through the `shutdown`
  /// function. If a handle is provided, the logger will shut down in response to the provided
  /// handle.
  #[must_use]
  pub fn with_shutdown_handle(mut self, handle: ComponentShutdownTriggerHandle) -> Self {
    self.component_shutdown_handle = Some(handle);
    self
  }

  /// Enables mobile features, such as periodic stats flushing and stats uploading.
  #[must_use]
  pub const fn with_mobile_features(mut self, mobile_features: bool) -> Self {
    self.mobile_features = mobile_features;
    self
  }

  /// Provides an explicit stat scope to be used by the logger. If this is not set, the logger will
  /// manage its own collector.
  #[must_use]
  pub fn with_stats_scope(mut self, scope: Scope) -> Self {
    self.stats_scope = Some(scope);
    self
  }

  #[must_use]
  /// Enables the internal logger, which logs internal events to the logger. This is sometimes
  /// useful to aid debugging the logger in the wild.
  pub const fn with_internal_logger(mut self, internal_logger: bool) -> Self {
    self.internal_logger = internal_logger;
    self
  }

  /// Builds the logger, returning the logger, a channel to send data uploads to the logger, and a
  /// future that must be awaited to run the logger.
  pub fn build(
    self,
  ) -> anyhow::Result<(
    Logger,
    tokio::sync::mpsc::Sender<DataUpload>,
    Pin<Box<impl Future<Output = anyhow::Result<()>> + 'static>>,
  )> {
    if self.mobile_features && self.stats_scope.is_some() {
      anyhow::bail!(
        "Cannot use mobile features and a custom stats scope at the same time, as the stats scope \
         must be managed by the logger in order to also flush stats"
      );
    }

    log::info!(
      "bitdrift Capture SDK: {:?}",
      self.params.static_metadata.sdk_version()
    );

    let (shutdown_handle, maybe_shutdown_trigger) = self.component_shutdown_handle.map_or_else(
      || {
        let shutdown_trigger = ComponentShutdownTrigger::default();
        (shutdown_trigger.make_handle(), Some(shutdown_trigger))
      },
      |handle| (handle, None),
    );

    let (trigger_upload_tx, trigger_upload_rx) = tokio::sync::mpsc::channel(1);
    let (flush_buffers_tx, flush_buffers_rx) = tokio::sync::mpsc::channel(1);
    let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

    let data_upload_ch: ChannelPair<DataUpload> = tokio::sync::mpsc::channel(1).into();
    let runtime_loader = runtime::ConfigLoader::new(&self.params.sdk_directory);

    let (scope, maybe_managed_collector) = self.stats_scope.map_or_else(
      || {
        let collector = Collector::default();
        (collector.scope(""), Some(collector))
      },
      |scope| (scope, None),
    );

    let dynamic_stats = Arc::new(bd_client_stats::DynamicStats::new(&scope, &runtime_loader));

    let (maybe_stats_flusher, maybe_stats_uploader, maybe_flusher_trigger) = if self.mobile_features
    {
      let stats =
        bd_client_stats::Stats::new(maybe_managed_collector.unwrap(), dynamic_stats.clone());
      let flush_handles = stats.flush_handle(
        &runtime_loader,
        shutdown_handle.make_shutdown(),
        &self.params.sdk_directory,
        data_upload_ch.tx.clone(),
      )?;

      (
        Some(flush_handles.flusher),
        Some(flush_handles.uploader),
        Some(flush_handles.flush_trigger),
      )
    } else {
      (None, None, None)
    };

    let network_quality_provider = Arc::new(SimpleNetworkQualityProvider::default());
    let (async_log_buffer, async_log_buffer_communication_tx) = AsyncLogBuffer::<LoggerReplay>::new(
      UninitializedLoggingContext::new(
        &self.params.sdk_directory,
        &runtime_loader,
        scope.clone(),
        dynamic_stats,
        trigger_upload_tx.clone(),
        data_upload_ch.tx.clone(),
        flush_buffers_tx,
        maybe_flusher_trigger,
        512,
        1024 * 1024,
      ),
      LoggerReplay {},
      self.params.session_strategy.clone(),
      self.params.metadata_provider.clone(),
      self.params.resource_utilization_target,
      self.params.events_listener_target,
      config_update_rx,
      shutdown_handle.clone(),
      &runtime_loader,
      network_quality_provider.clone(),
    );

    let logger = Logger::new(
      maybe_shutdown_trigger,
      runtime_loader.clone(),
      scope.clone(),
      async_log_buffer_communication_tx,
      self.params.session_strategy,
      self.params.device,
      self.params.static_metadata.sdk_version(),
      self.params.store,
    );

    let log = if self.internal_logger {
      Arc::new(InternalLogger::new(
        logger.new_logger_handle(),
        &runtime_loader,
      )?) as Arc<dyn bd_internal_logging::Logger>
    } else {
      Arc::new(NoopLogger) as Arc<dyn bd_internal_logging::Logger>
    };

    // TODO(Augustyniak): Move the initialization of the SDK directory off the calling thread to
    // improve the perceived performance of the logger initialization.
    let buffer_directory = Logger::initialize_buffer_directory(&self.params.sdk_directory)?;
    let (buffer_manager, buffer_event_rx) =
      bd_buffer::Manager::new(buffer_directory, &scope, &runtime_loader);
    let buffer_uploader = BufferUploadManager::new(
      data_upload_ch.tx.clone(),
      &runtime_loader,
      shutdown_handle.make_shutdown(),
      buffer_event_rx,
      trigger_upload_rx,
      &scope,
      log.clone(),
    )?;

    let updater = Box::new(client_config::Config::new(
      &self.params.sdk_directory,
      LoggerUpdate::new(
        buffer_manager.clone(),
        config_update_tx,
        &runtime_loader,
        &scope.scope("config"),
      ),
      &scope,
    )?);

    let api = bd_api::api::Api::new(
      self.params.sdk_directory,
      self.params.api_key,
      self.params.network,
      shutdown_handle.make_shutdown(),
      data_upload_ch.rx,
      trigger_upload_tx,
      self.params.static_metadata,
      runtime_loader.clone(),
      vec![
        Box::new(bd_runtime::runtime::RuntimeManager::new(runtime_loader)),
        updater,
      ],
      Arc::new(SystemTimeProvider {}),
      network_quality_provider,
      log.clone(),
      &scope.scope("api"),
    )?;

    bd_client_common::error::UnexpectedErrorHandler::register_stats(&scope);

    let logger_future = async {
      try_join!(
        async move { api.start().await },
        async move { buffer_uploader.run().await },
        async move {
          async_log_buffer.run().await;
          Ok(())
        },
        async move { buffer_manager.process_flushes(flush_buffers_rx).await },
        async move {
          if let Some(stats_flusher) = maybe_stats_flusher {
            stats_flusher.periodic_flush().await;
          };

          Ok(())
        },
        async move {
          if let Some(stats_uploader) = maybe_stats_uploader {
            stats_uploader.upload_stats().await;
          }

          Ok(())
        },
      )
      .map(|_| ())
    };

    Ok((logger, data_upload_ch.tx, Box::pin(logger_future)))
  }

  /// Builds the builder, running the logger on a dedicated thread. This is useful for running the
  /// logger outside of a tokio runtime.
  pub fn build_dedicated_thread(
    self,
  ) -> anyhow::Result<(Logger, tokio::sync::mpsc::Sender<DataUpload>)> {
    if self.component_shutdown_handle.is_some() {
      anyhow::bail!("Cannot use a dedicated thread with a custom shutdown handle");
    }

    let (logger, ch, future) = self.build()?;

    std::thread::Builder::new()
      .name("io.bitdrift.capture.logger".to_string())
      .spawn(move || {
        tokio::runtime::Builder::new_current_thread()
          .thread_name("io.bitdrift.capture.logger")
          .thread_name_fn(|| "io.bitdrift.capture.logger.worker".to_string())
          .enable_all()
          .build()
          .unwrap()
          .block_on(async {
            handle_unexpected(future.await, "logger top level run loop");
          });
      })?;

    Ok((logger, ch))
  }
}
