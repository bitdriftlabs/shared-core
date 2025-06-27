// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::InitParams;
use crate::async_log_buffer::AsyncLogBuffer;
use crate::client_config::{self, LoggerUpdate};
use crate::consumer::BufferUploadManager;
use crate::internal::InternalLogger;
use crate::log_replay::LoggerReplay;
use crate::logger::Logger;
use crate::logging_state::UninitializedLoggingContext;
use bd_api::DataUpload;
use bd_api::api::SimpleNetworkQualityProvider;
use bd_client_common::ConfigurationUpdate;
use bd_client_common::error::handle_unexpected;
use bd_client_common::file_system::RealFileSystem;
use bd_client_stats::FlushTrigger;
use bd_client_stats::stats::{
  JitteredIntervalCreator,
  RuntimeWatchTicker,
  SleepModeAwareRuntimeWatchTicker,
  Ticker,
};
use bd_client_stats_store::Collector;
use bd_internal_logging::NoopLogger;
use bd_log_primitives::{Log, LogType, log_level};
use bd_runtime::runtime::stats::{DirectStatFlushIntervalFlag, UploadStatFlushIntervalFlag};
use bd_runtime::runtime::{self, ConfigLoader, Watch, sleep_mode};
use bd_shutdown::{ComponentShutdownTrigger, ComponentShutdownTriggerHandle};
use bd_time::SystemTimeProvider;
use futures_util::{Future, try_join};
use std::pin::Pin;
use std::sync::Arc;
use time::Duration;
use tokio::sync::watch;

pub fn default_stats_flush_triggers(
  sleep_mode_active: watch::Receiver<bool>,
  runtime_loader: &ConfigLoader,
) -> anyhow::Result<(Box<dyn Ticker>, Box<dyn Ticker>)> {
  let flush_interval_flag: Watch<Duration, DirectStatFlushIntervalFlag> =
    runtime_loader.register_watch()?;
  let flush_ticker = RuntimeWatchTicker::new(flush_interval_flag.into_inner());

  let live_mode_upload_interval_flag: Watch<Duration, UploadStatFlushIntervalFlag> =
    runtime_loader.register_watch()?;
  let sleep_mode_upload_interval_flag: Watch<Duration, sleep_mode::UploadStatFlushIntervalFlag> =
    runtime_loader.register_watch()?;
  let upload_ticker = SleepModeAwareRuntimeWatchTicker::<JitteredIntervalCreator>::new(
    live_mode_upload_interval_flag.into_inner(),
    sleep_mode_upload_interval_flag.into_inner(),
    sleep_mode_active,
  );

  Ok((
    Box::new(flush_ticker) as Box<dyn Ticker>,
    Box::new(upload_ticker) as Box<dyn Ticker>,
  ))
}

/// A builder for the logger.
pub struct LoggerBuilder {
  // The parameters required to initialize the logger.
  params: InitParams,

  component_shutdown_handle: Option<ComponentShutdownTriggerHandle>,
  client_stats_tickers: Option<(Box<dyn Ticker>, Box<dyn Ticker>)>,
  internal_logger: bool,
}

impl LoggerBuilder {
  /// Creates a new logger builder with the provided parameters.
  #[must_use]
  pub const fn new(params: InitParams) -> Self {
    Self {
      params,
      component_shutdown_handle: None,
      client_stats_tickers: None,
      internal_logger: false,
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

  /// Sets the tickers to be used for flushing client stats. If not set, the default tickers will
  /// be used.
  #[must_use]
  pub fn with_client_stats_tickers(
    mut self,
    flush_ticker: Box<dyn Ticker>,
    upload_ticker: Box<dyn Ticker>,
  ) -> Self {
    self.client_stats_tickers = Some((flush_ticker, upload_ticker));
    self
  }

  /// Enables the internal logger, which logs internal events to the logger. This is sometimes
  /// useful to aid debugging the logger in the wild.
  #[must_use]
  pub const fn with_internal_logger(mut self, internal_logger: bool) -> Self {
    self.internal_logger = internal_logger;
    self
  }

  /// Builds the logger.
  ///
  /// The returned feature must be awaited on in order for the logger to run. This future will
  /// resolve when the logger has shut down.
  #[allow(clippy::type_complexity)]
  pub fn build(
    self,
  ) -> anyhow::Result<(
    Logger,
    tokio::sync::mpsc::Sender<DataUpload>,
    Pin<Box<impl Future<Output = anyhow::Result<()>> + 'static>>,
    FlushTrigger,
  )> {
    bd_panic::default(bd_panic::PanicType::ForceAbort);
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

    let (data_upload_tx, data_upload_rx) = tokio::sync::mpsc::channel(1);
    let runtime_loader = runtime::ConfigLoader::new(&self.params.sdk_directory);

    let max_dynamic_stats =
      bd_runtime::runtime::stats::MaxDynamicCountersFlag::register(&runtime_loader)
        .unwrap()
        .into_inner();
    let collector = Collector::new(Some(max_dynamic_stats));

    let scope = collector.scope("");
    let stats = bd_client_stats::Stats::new(collector.clone());
    let (sleep_mode_active_tx, sleep_mode_active_rx) =
      watch::channel(self.params.start_in_sleep_mode);

    let (stats_flusher, flusher_trigger) = {
      let (flush_ticker, upload_ticker) =
        if let Some((flush_ticker, upload_ticker)) = self.client_stats_tickers {
          (flush_ticker, upload_ticker)
        } else {
          default_stats_flush_triggers(sleep_mode_active_rx.clone(), &runtime_loader)?
        };
      let flush_handles = stats.flush_handle(
        &runtime_loader,
        shutdown_handle.make_shutdown(),
        &self.params.sdk_directory,
        data_upload_tx.clone(),
        flush_ticker,
        upload_ticker,
      )?;

      (flush_handles.flusher, flush_handles.flush_trigger)
    };
    let (trigger_upload_tx, trigger_upload_rx) = tokio::sync::mpsc::channel(1);
    let (flush_buffers_tx, flush_buffers_rx) = tokio::sync::mpsc::channel(1);
    let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);

    let network_quality_provider = Arc::new(SimpleNetworkQualityProvider::default());
    let (async_log_buffer, async_log_buffer_communication_tx) = AsyncLogBuffer::<LoggerReplay>::new(
      UninitializedLoggingContext::new(
        &self.params.sdk_directory,
        &runtime_loader,
        scope.clone(),
        stats,
        trigger_upload_tx.clone(),
        data_upload_tx.clone(),
        flush_buffers_tx,
        flusher_trigger.clone(),
        512,
        1024 * 1024,
      ),
      LoggerReplay,
      self.params.session_strategy.clone(),
      self.params.metadata_provider.clone(),
      self.params.resource_utilization_target,
      self.params.session_replay_target,
      self.params.events_listener_target,
      config_update_rx,
      shutdown_handle.clone(),
      &runtime_loader,
      network_quality_provider.clone(),
      self.params.device.id(),
      self.params.store.clone(),
    );

    let logger = Logger::new(
      maybe_shutdown_trigger,
      runtime_loader.clone(),
      scope.clone(),
      async_log_buffer_communication_tx,
      self.params.session_strategy.clone(),
      self.params.device,
      self.params.static_metadata.sdk_version(),
      self.params.store.clone(),
      sleep_mode_active_tx,
    );
    let log = if self.internal_logger {
      Arc::new(InternalLogger::new(
        logger.new_logger_handle(),
        &runtime_loader,
      )?) as Arc<dyn bd_internal_logging::Logger>
    } else {
      Arc::new(NoopLogger) as Arc<dyn bd_internal_logging::Logger>
    };

    bd_client_common::error::UnexpectedErrorHandler::register_stats(&scope);

    let data_upload_tx_clone = data_upload_tx.clone();
    let collector_clone = collector;
    let logger_future = async move {
      runtime_loader.try_load_persisted_config().await;

      let (artifact_uploader, artifact_client) = bd_artifact_upload::Uploader::new(
        Arc::new(RealFileSystem::new(self.params.sdk_directory.clone())),
        data_upload_tx_clone.clone(),
        Arc::new(SystemTimeProvider),
        &runtime_loader,
        &collector_clone,
        shutdown_handle.make_shutdown(),
      );

      let mut crash_monitor = bd_crash_handler::Monitor::new(
        &runtime_loader,
        &self.params.sdk_directory,
        self.params.store.clone(),
        Arc::new(artifact_client),
        self
          .params
          .session_strategy
          .previous_process_session_id()
          .unwrap_or_default(),
        shutdown_handle.make_shutdown(),
      );

      // TODO(Augustyniak): Move the initialization of the SDK directory off the calling thread to
      // improve the perceived performance of the logger initialization.
      let buffer_directory = Logger::initialize_buffer_directory(&self.params.sdk_directory)?;
      let (buffer_manager, buffer_event_rx) =
        bd_buffer::Manager::new(buffer_directory, &scope, &runtime_loader);
      let buffer_uploader = BufferUploadManager::new(
        data_upload_tx_clone.clone(),
        &runtime_loader,
        shutdown_handle.make_shutdown(),
        buffer_event_rx,
        trigger_upload_rx,
        &scope,
        log.clone(),
      )?;

      let updater = Arc::new(client_config::Config::new(
        &self.params.sdk_directory,
        LoggerUpdate::new(
          buffer_manager.clone(),
          config_update_tx,
          &scope.scope("config"),
        ),
      ));

      let api = bd_api::api::Api::new(
        self.params.sdk_directory,
        self.params.api_key,
        self.params.network,
        shutdown_handle.make_shutdown(),
        data_upload_rx,
        trigger_upload_tx,
        self.params.static_metadata,
        runtime_loader.clone(),
        updater,
        Arc::new(SystemTimeProvider),
        network_quality_provider,
        log.clone(),
        &scope.scope("api"),
        sleep_mode_active_rx,
      )?;

      bd_client_common::error::UnexpectedErrorHandler::register_stats(&scope);

      let session_strategy = self.params.session_strategy;

      // By running it before we start all the other components, we ensure that the crash is
      // processed before cached configuration is loaded, allowing us to pass a fixed set of logs
      // to the async buffer that it can emit once it transitions into the configured mode,
      // emitting these logs before any logs emitted by the application while we were starting
      // up.
      let crash_logs = crash_monitor
        .process_new_reports()
        .await
        .into_iter()
        .map(|crash_log| Log {
          log_level: log_level::ERROR,
          log_type: LogType::Lifecycle,
          message: crash_log.message,
          fields: crash_log.fields,
          matching_fields: [].into(),
          session_id: session_strategy
            .previous_process_session_id()
            .unwrap_or_else(|| session_strategy.session_id()),
          occurred_at: crash_log.timestamp,
          // Always capture the session when we process a crash log.
          // TODO(snowp): Ideally we should include information like the report and client side
          // grouping here to help make smarter decisions during intent negotiation.
          capture_session: Some("crash_handler".to_string()),
        })
        .collect();

      try_join!(
        async move {
          api.start().await?;

          log::info!("API ENDED");

          Ok(())
        },
        async move { buffer_uploader.run().await },
        async move {
          async_log_buffer.run(crash_logs).await;

          log::info!("ASYNC LOG ENDED");
          Ok(())
        },
        async move {
          buffer_manager.process_flushes(flush_buffers_rx).await?;

          log::info!("BUFFER MANAGER ENDED");
          Ok(())
        },
        async move {
          stats_flusher.periodic_flush().await;

          log::info!("STATS FLUSHER ENDED");
          Ok(())
        },
        async move {
          crash_monitor.run().await?;

          log::info!("CRASH MONITOR ENDED");
          Ok(())
        },
        async move {
          artifact_uploader.run().await;
          log::info!("ARTIFACT UPLOADER ENDED");
          Ok(())
        }
      )
      .inspect_err(|e| {
        log::error!("Error running logger: {:?}", e);
      })
      .map(|_| ())
    };

    Ok((
      logger,
      data_upload_tx,
      Box::pin(logger_future),
      flusher_trigger,
    ))
  }

  /// Builds the logger and runs the associated future on a dedicated thread. This is useful for
  /// running the logger outside of a tokio runtime.
  pub fn build_dedicated_thread(
    self,
  ) -> anyhow::Result<(Logger, tokio::sync::mpsc::Sender<DataUpload>, FlushTrigger)> {
    if self.component_shutdown_handle.is_some() {
      anyhow::bail!("Cannot use a dedicated thread with a custom shutdown handle");
    }

    let (logger, ch, future, flush_trigger) = self.build()?;

    Self::run_logger_runtime(future)?;

    Ok((logger, ch, flush_trigger))
  }

  /// Creates a new tokio runtime on a dedicated thread suitable for running the logger. The
  /// provided future will be awaited on the runtime, and any errors will be reported to the
  /// `handle_unexpected` system.
  ///
  /// This is exposed in order to make it possible to run more than just the logger future on the
  /// newly spawned runtime.
  pub fn run_logger_runtime(
    f: impl Future<Output = anyhow::Result<()>> + Send + 'static,
  ) -> anyhow::Result<()> {
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
            let result = f.await;
            log::info!(
              "bitdrift runtime has finished running with result: {:?}",
              result
            );
            handle_unexpected(result, "logger top level run loop");
          });
      })?;

    Ok(())
  }
}
