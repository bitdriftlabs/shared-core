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
use crate::logger::Logger;
use crate::logging_state::UninitializedLoggingContext;
use crate::{InitParams, LogAttributesOverrides};
use bd_api::{
  AggregatedNetworkQualityProvider,
  DataUpload,
  SimpleNetworkQualityProvider,
  TimedNetworkQualityProvider,
};
use bd_client_common::file_system::RealFileSystem;
use bd_client_common::init_lifecycle::InitLifecycleState;
use bd_client_stats::FlushTrigger;
use bd_client_stats::stats::{
  JitteredIntervalCreator,
  RuntimeWatchTicker,
  SleepModeAwareRuntimeWatchTicker,
};
use bd_client_stats_store::Collector;
use bd_crash_handler::Monitor;
use bd_error_reporter::reporter::{UnexpectedErrorHandler, handle_unexpected};
use bd_internal_logging::NoopLogger;
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::network_quality::NetworkCallOnlineIndicatorTimeout;
use bd_runtime::runtime::stats::{DirectStatFlushIntervalFlag, UploadStatFlushIntervalFlag};
use bd_runtime::runtime::{self, ConfigLoader, Watch, sleep_mode};
use bd_shutdown::{ComponentShutdownTrigger, ComponentShutdownTriggerHandle};
use bd_time::{SystemTimeProvider, Ticker, TimeProvider};
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
    runtime_loader.register_duration_watch();
  let flush_ticker = RuntimeWatchTicker::new(flush_interval_flag.into_inner());

  let live_mode_upload_interval_flag: Watch<Duration, UploadStatFlushIntervalFlag> =
    runtime_loader.register_duration_watch();
  let sleep_mode_upload_interval_flag: Watch<Duration, sleep_mode::UploadStatFlushIntervalFlag> =
    runtime_loader.register_duration_watch();
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
  time_provider: Option<Arc<dyn TimeProvider>>,
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
      time_provider: None,
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

  /// Sets the time provider to be used by the logger. If not set, the system time provider will
  /// be used.
  #[must_use]
  pub fn with_time_provider(mut self, time_provider: Option<Arc<dyn TimeProvider>>) -> Self {
    self.time_provider = time_provider;
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
    log::info!(
      "bitdrift Capture SDK: {:?}",
      self.params.static_metadata.sdk_version()
    );

    let init_lifecycle = InitLifecycleState::new();

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
      bd_runtime::runtime::stats::MaxDynamicCountersFlag::register(&runtime_loader).into_inner();
    let collector = Collector::new(Some(max_dynamic_stats));

    let scope = collector.scope("");
    let stats = bd_client_stats::Stats::new(collector.clone());
    let (sleep_mode_active_tx, sleep_mode_active_rx) =
      watch::channel(self.params.start_in_sleep_mode);
    let time_provider = self
      .time_provider
      .unwrap_or_else(|| Arc::new(SystemTimeProvider));

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
      );

      (flush_handles.flusher, flush_handles.flush_trigger)
    };
    let (trigger_upload_tx, trigger_upload_rx) = tokio::sync::mpsc::channel(1);
    let (flush_buffers_tx, flush_buffers_rx) = tokio::sync::mpsc::channel(1);
    let (config_update_tx, config_update_rx) = tokio::sync::mpsc::channel(1);
    let (report_proc_tx, report_proc_rx) = tokio::sync::mpsc::channel(1);

    let api_network_quality_provider = Arc::new(SimpleNetworkQualityProvider::default());
    let log_network_quality_provider = Arc::new(TimedNetworkQualityProvider::new(
      time_provider.clone(),
      runtime_loader.register_duration_watch::<NetworkCallOnlineIndicatorTimeout>(),
    ));
    let aggregated_network_quality_provider =
      Arc::new(AggregatedNetworkQualityProvider::new(vec![
        api_network_quality_provider.clone(),
        log_network_quality_provider.clone(),
      ]));

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
      report_proc_rx,
      shutdown_handle.clone(),
      &runtime_loader,
      log_network_quality_provider,
      aggregated_network_quality_provider,
      self.params.device.id(),
      &self.params.store,
      time_provider.clone(),
      init_lifecycle.clone(),
      data_upload_tx.clone(),
    );

    let data_upload_tx_clone = data_upload_tx.clone();
    let collector_clone = collector;

    let logger = Logger::new(
      maybe_shutdown_trigger,
      runtime_loader.clone(),
      scope.clone(),
      async_log_buffer_communication_tx.clone(),
      report_proc_tx,
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
      )) as Arc<dyn bd_internal_logging::Logger>
    } else {
      Arc::new(NoopLogger) as Arc<dyn bd_internal_logging::Logger>
    };

    UnexpectedErrorHandler::register_stats(&scope);

    let logger_future = async move {
      runtime_loader.try_load_persisted_config().await;
      init_lifecycle.set(bd_client_common::init_lifecycle::InitLifecycle::RuntimeLoaded);

      // Load the previous state snapshot into memory-mapped storage for crash reporting.
      // This loads the state from disk without maintaining an open file handle.
      let state_directory = self.params.sdk_directory.join("state");
      std::fs::create_dir_all(&state_directory)?;

      // Initialize state store based on runtime configuration
      let use_persistent_storage_flag =
        bd_runtime::runtime::global_state::UsePersistentStorage::register(&runtime_loader)
          .into_inner();

      let result = bd_state::Store::from_runtime_config(
        &state_directory,
        bd_state::PersistentStoreConfig::default(),
        time_provider.clone(),
        use_persistent_storage_flag,
      )
      .await;

      let (state_store, previous_run_state) = (result.store, result.previous_state);

      if result.fallback_occurred {
        handle_unexpected(
          Err::<(), anyhow::Error>(anyhow::anyhow!(
            "Failed to initialize persistent state store, using in-memory fallback"
          )),
          "state initialization",
        );
      }

      let (artifact_uploader, artifact_client) = bd_artifact_upload::Uploader::new(
        Arc::new(RealFileSystem::new(self.params.sdk_directory.clone())),
        data_upload_tx_clone.clone(),
        time_provider.clone(),
        &runtime_loader,
        &collector_clone,
        shutdown_handle.make_shutdown(),
      );

      let crash_monitor = Monitor::new(
        &self.params.sdk_directory,
        self.params.store.clone(),
        Arc::new(artifact_client),
        self.params.session_strategy.clone(),
        &init_lifecycle,
        state_store.clone(),
        previous_run_state,
        move |log: bd_crash_handler::CrashLog| {
          AsyncLogBuffer::<LoggerReplay>::enqueue_log(
            &async_log_buffer_communication_tx,
            log.log_level,
            LogType::LIFECYCLE,
            log.message,
            log.fields,
            [].into(),
            LogAttributesOverrides::OccurredAt(log.timestamp).into(),
            crate::Block::No,
            None,
          )
          .map_err(Into::into)
        },
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
      );

      let updater = Arc::new(client_config::Config::new(
        &self.params.sdk_directory,
        LoggerUpdate::new(
          buffer_manager.clone(),
          config_update_tx,
          &scope.scope("config"),
        ),
      ));

      let api = bd_api::api::Api::new(
        self.params.sdk_directory.clone(),
        self.params.api_key,
        self.params.network,
        data_upload_rx,
        trigger_upload_tx,
        self.params.static_metadata,
        runtime_loader.clone(),
        updater,
        time_provider,
        api_network_quality_provider,
        log.clone(),
        &scope.scope("api"),
        sleep_mode_active_rx,
        self.params.store.clone(),
      );

      let mut config_writer = bd_crash_handler::ConfigWriter::new(
        &runtime_loader,
        &self.params.sdk_directory,
        shutdown_handle.make_shutdown(),
      );

      UnexpectedErrorHandler::register_stats(&scope);

      let mut api_shutdown = shutdown_handle.make_shutdown();
      try_join!(
        async move {
          tokio::select! {
            () = api.start() => { Ok(()) },
            () = api_shutdown.cancelled() => { Ok(()) }
          }
        },
        async move { buffer_uploader.run().await },
        async move { config_writer.run().await },
        async move {
          async_log_buffer.run(state_store, crash_monitor).await;
          Ok(())
        },
        async move { buffer_manager.process_flushes(flush_buffers_rx).await },
        async move {
          stats_flusher.periodic_flush().await;
          Ok(())
        },
        async move {
          artifact_uploader.run().await;
          Ok(())
        }
      )
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
      .name("bd-tokio".to_string())
      .spawn(move || {
        tokio::runtime::Builder::new_current_thread()
          .thread_name("bd-tokio-worker")
          .enable_all()
          .build()?
          .block_on(async {
            handle_unexpected(f.await, "logger top level run loop");
          });
        Ok::<_, anyhow::Error>(())
      })?;

    Ok(())
  }
}
