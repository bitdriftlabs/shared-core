// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./api_test.rs"]
mod api_test;

use crate::upload::{self, StateTracker};
use crate::{
  DataUpload,
  PlatformNetworkManager,
  PlatformNetworkStream,
  StreamEvent,
  TriggerUpload,
};
use anyhow::anyhow;
use backoff::SystemClock;
use backoff::backoff::Backoff;
use backoff::exponential::ExponentialBackoff;
use bd_client_common::file::{read_compressed_protobuf, write_compressed_protobuf};
use bd_client_common::payload_conversion::IntoRequest;
use bd_client_common::zlib::DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL;
use bd_client_common::{ClientConfigurationUpdate, maybe_await};
use bd_client_stats_store::{Counter, CounterWrapper, Scope};
use bd_error_reporter::reporter::UnexpectedErrorHandler;
use bd_grpc_codec::code::Code;
use bd_grpc_codec::{
  Compression,
  Encoder,
  GRPC_ACCEPT_ENCODING_HEADER,
  GRPC_ENCODING_DEFLATE,
  GRPC_ENCODING_HEADER,
  OptimizeFor,
};
use bd_metadata::Metadata;
use bd_network_quality::{NetworkQuality, NetworkQualityProvider};
use bd_proto::protos::client::api::api_response::Response_type;
pub use bd_proto::protos::client::api::log_upload_intent_response::{
  Decision as LogsUploadDecision,
  Drop as LogsUploadDecisionDrop,
  UploadImmediately as LogsUploadDecisionUploadImmediately,
};
pub use bd_proto::protos::client::api::sankey_intent_response::{
  Decision as SankeyPathUploadDecision,
  Drop as SankeyPathUploadDecisionDrop,
  UploadImmediately as SankeyPathUploadDecisionImmediately,
};
pub use bd_proto::protos::client::api::upload_artifact_intent_response::{
  Decision as ArtifactIntentDecision,
  Drop as ArtifactIntentDecisionDrop,
};
use bd_proto::protos::client::api::{
  ApiRequest,
  ApiResponse,
  ClientKillFile,
  HandshakeRequest,
  PingRequest,
  handshake_response,
};
use bd_proto::protos::logging::payload::Data as ProtoData;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_runtime::runtime::DurationWatch;
use bd_time::{OffsetDateTimeExt, TimeProvider, TimestampExt};
use parking_lot::RwLock;
use std::cmp::max;
use std::collections::HashMap;
use std::future::pending;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use time::Duration;
use time::ext::NumericalStdDuration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;
use tokio::time::{Instant, Sleep, sleep};

// The amount of time the API has to be in the disconnected state before network quality will be
// switched to "offline". This offline grace period also governs when cached configuration will
// be marked as safe to use if we can't contact the server. This prevents cached configuration
// from being deleted during perpetually offline states if the process has been up for long
// enough without crashing.
const DISCONNECTED_OFFLINE_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_secs(15);

//
// SimpleNetworkQualityProvider
//

pub struct SimpleNetworkQualityProvider {
  network_quality: RwLock<NetworkQuality>,
}

impl Default for SimpleNetworkQualityProvider {
  fn default() -> Self {
    Self {
      network_quality: RwLock::new(NetworkQuality::Unknown),
    }
  }
}

impl NetworkQualityProvider for SimpleNetworkQualityProvider {
  fn get_network_quality(&self) -> NetworkQuality {
    *self.network_quality.read()
  }

  fn set_network_quality(&self, quality: NetworkQuality) {
    *self.network_quality.write() = quality;
  }
}

//
// StreamClosureInfo
//

struct StreamClosureInfo {
  reason: String,
  retry_after: Option<Duration>,
}

//
// HandshakeResult
//

/// The result of waiting for a handshake response.
enum HandshakeResult {
  /// We received a handshake response with with a set of stream settings, and a list of extra
  /// responses that were decoded in addition to the handshake response.
  Received {
    stream_settings: Option<handshake_response::StreamSettings>,
    configuration_update_status: u32,
    remaining_responses: Vec<ApiResponse>,
  },

  /// The stream closed while waiting for the handshake.
  StreamClosure(StreamClosureInfo),

  /// Server responded specifically with an unauthenticated error.
  Unauthenticated,
}

//
// StreamState
//

// State scoped to a specific stream.
struct StreamState {
  request_encoder: Encoder<ApiRequest>,
  response_decoder: bd_grpc_codec::Decoder<ApiResponse>,
  ping_interval: Option<tokio::time::Duration>,
  ping_sleep: Option<Pin<Box<Sleep>>>,

  upload_state_tracker: upload::StateTracker,

  stream_handle: Box<dyn PlatformNetworkStream>,

  stream_event_rx: tokio::sync::mpsc::Receiver<StreamEvent>,

  time_provider: Arc<dyn TimeProvider>,

  sleep_mode_active: watch::Receiver<bool>,
}

impl StreamState {
  fn new(
    compression: Option<Compression>,
    stream_handle: Box<dyn PlatformNetworkStream>,
    stream_event_rx: tokio::sync::mpsc::Receiver<StreamEvent>,
    time_provider: Arc<dyn TimeProvider>,
    stats: &Stats,
    sleep_mode_active: watch::Receiver<bool>,
  ) -> Self {
    // TODO(mattklein123): We should be pivoting based on the grpc-encoding response header, but
    // currently we are not passing the response headers back to Rust. Given that we currently
    // have prior knowledge of what the server will do this is OK but we should fix this in the
    // future.
    let mut decoder = bd_grpc_codec::Decoder::new(
      Some(bd_grpc_codec::Decompression::StatelessZlib),
      OptimizeFor::Cpu,
    );
    decoder.initialize_stats(
      CounterWrapper::make_dyn(stats.rx_bytes.clone()),
      CounterWrapper::make_dyn(stats.rx_bytes_decompressed.clone()),
    );

    let mut encoder = Encoder::new(compression);
    encoder.initialize_stats(
      CounterWrapper::make_dyn(stats.tx_bytes.clone()),
      CounterWrapper::make_dyn(stats.tx_bytes_uncompressed.clone()),
    );

    Self {
      request_encoder: encoder,
      response_decoder: decoder,
      ping_interval: None,
      ping_sleep: None,
      upload_state_tracker: StateTracker::new(),
      stream_handle,
      stream_event_rx,
      time_provider,
      sleep_mode_active,
    }
  }

  fn initialize_stream_settings(
    &mut self,
    stream_settings: Option<handshake_response::StreamSettings>,
  ) {
    self.ping_interval = stream_settings.and_then(|mut settings| {
      settings
        .ping_interval
        .take()
        .and_then(|d| TryInto::try_into(d).ok())
    });

    self.maybe_schedule_ping();
  }

  fn maybe_schedule_ping(&mut self) {
    self.ping_sleep = self.ping_interval.map(|ping_in| {
      log::debug!("scheduling ping in {ping_in:?}");

      Box::pin(sleep(ping_in))
    });
  }

  async fn send_request<R: IntoRequest>(&mut self, request: R) -> anyhow::Result<()> {
    let framed_message = self.request_encoder.encode(&request.into_request())?;
    self.stream_handle.send_data(&framed_message).await
  }

  async fn send_ping(&mut self) -> anyhow::Result<()> {
    log::debug!("sending ping");
    let sleep_mode = *self.sleep_mode_active.borrow();

    self
      .send_request(PingRequest {
        sleep_mode,
        ..Default::default()
      })
      .await
  }

  // Processes a single upstream event (data/close) being provided by the platform network.
  async fn handle_upstream_event(
    &mut self,
    event: Option<StreamEvent>,
  ) -> anyhow::Result<UpstreamEvent> {
    match event {
      // We received some data on the stream. Attempt to decode this and
      // return up any decoded responses.
      Some(StreamEvent::Data(data)) => {
        log::trace!("received {} bytes", data.len());

        let frames = self.response_decoder.decode_data(&data)?;
        log::trace!("decoded {} frames", frames.len());
        Ok(UpstreamEvent::UpstreamMessages(frames))
      },
      // We observed a close event, propagate this back up.
      Some(StreamEvent::StreamClosed(reason)) => {
        log::debug!("stream closed due to '{reason}'");

        Ok(UpstreamEvent::StreamClosed(reason))
      },
      // The upstream event sender has dropped before we terminated the stream. This can happen
      // if the logger gets destroyed before the API task is dropped.
      None => pending().await,
    }
  }

  async fn handle_data_upload(&mut self, data_upload: DataUpload) -> anyhow::Result<()> {
    match data_upload {
      DataUpload::LogsUploadIntent(intent) => {
        let req = self.upload_state_tracker.track_intent(intent);
        self.send_request(req).await
      },
      DataUpload::LogsUpload(request) => {
        let req = self.upload_state_tracker.track_upload(request);
        self.send_request(req).await
      },
      DataUpload::AcklessLogsUpload(request) => self.send_request(request).await,
      DataUpload::StatsUpload(mut request) => {
        request.payload.sent_at = self.time_provider.now().into_proto();
        let req = self.upload_state_tracker.track_upload(request);
        self.send_request(req).await
      },
      DataUpload::SankeyPathUploadIntent(request) => {
        let req = self.upload_state_tracker.track_intent(request);
        self.send_request(req).await
      },
      DataUpload::SankeyPathUpload(request) => {
        let req = self.upload_state_tracker.track_upload(request);
        self.send_request(req).await
      },
      DataUpload::ArtifactUploadIntent(tracked) => {
        let req = self.upload_state_tracker.track_intent(tracked);
        self.send_request(req).await
      },
      DataUpload::ArtifactUpload(tracked) => {
        let req = self.upload_state_tracker.track_upload(tracked);
        self.send_request(req).await
      },
      DataUpload::DebugData(request) => self.send_request(request).await,
    }
  }
}

//
// UpstreamEvent
//

/// Describes a single event received through the network API.
enum UpstreamEvent {
  // One or more API response messages have been received via the upstream API stream.
  UpstreamMessages(Vec<ApiResponse>),

  // The API stream closed (either locally or remotely).
  StreamClosed(String),
}

//
// Stats
//

#[derive(Clone)]
struct Stats {
  tx_bytes_uncompressed: Counter,
  tx_bytes: Counter,
  rx_bytes: Counter,
  rx_bytes_decompressed: Counter,
  stream_total: Counter,
  remote_connect_failure: Counter,
  data_idle_timeout: Counter,
  error_shutdown_total: Counter,
}

impl Stats {
  fn new(scope: &Scope) -> Self {
    Self {
      tx_bytes_uncompressed: scope.counter("bandwidth_tx_uncompressed"),
      tx_bytes: scope.counter("bandwidth_tx"),
      rx_bytes: scope.counter("bandwidth_rx"),
      rx_bytes_decompressed: scope.counter("bandwidth_rx_decompressed"),
      stream_total: scope.counter("stream_total"),
      remote_connect_failure: scope.counter("remote_connect_failure"),
        data_idle_timeout: scope.counter("data_idle_timeout"),
      error_shutdown_total: scope.counter("error_shutdown_total"),
    }
  }
}

//
// Api
//

/// The main handle for handling the Mux API. The API consumer communicates with this handler via a
/// number of channels, both for sending data (e.g. log/stats upload) or receiving updates
/// (configuration updates, upload acks).
pub struct Api {
  sdk_directory: PathBuf,
  api_key: String,
  manager: Box<dyn PlatformNetworkManager<bd_runtime::runtime::ConfigLoader>>,
  data_upload_rx: Receiver<DataUpload>,
  trigger_upload_tx: Sender<TriggerUpload>,
  sleep_mode_active: watch::Receiver<bool>,

  static_metadata: Arc<dyn Metadata + Send + Sync>,

  max_backoff_interval: DurationWatch<bd_runtime::runtime::api::MaxBackoffInterval>,
  initial_backoff_interval: DurationWatch<bd_runtime::runtime::api::InitialBackoffInterval>,
  backoff: ExponentialBackoff<SystemClock>,
  config_updater: Arc<dyn ClientConfigurationUpdate>,
  runtime_loader: Arc<bd_runtime::runtime::ConfigLoader>,

  internal_logger: Arc<dyn bd_internal_logging::Logger>,
  time_provider: Arc<dyn TimeProvider>,
  network_quality_provider: Arc<SimpleNetworkQualityProvider>,

  config_marked_safe_due_to_offline: bool,

  stats: Stats,

  // This indicates whether this client has been placed into a "killed" state in which it will not
  // initiate any communication with the server. In order to simplify how this works, when in this
  // state, all API futures related to networking will just stay in pending. This allows the rest
  // of the system to keep functioning per normal. Obviously the various queues will fill up and
  // things will start to be dropped but that is the expected behavior.
  client_killed: bool,
  generic_kill_duration: DurationWatch<bd_runtime::runtime::client_kill::GenericKillDuration>,
  unauthenticated_kill_duration:
    DurationWatch<bd_runtime::runtime::client_kill::UnauthenticatedKillDuration>,
  data_idle_timeout_interval: DurationWatch<bd_runtime::runtime::api::DataIdleTimeoutInterval>,
  data_idle_reconnect_interval: DurationWatch<bd_runtime::runtime::api::DataIdleReconnectInterval>,
}

impl Api {
  pub fn new(
    sdk_directory: PathBuf,
    api_key: String,
    manager: Box<dyn PlatformNetworkManager<bd_runtime::runtime::ConfigLoader>>,
    data_upload_rx: Receiver<DataUpload>,
    trigger_upload_tx: Sender<TriggerUpload>,
    static_metadata: Arc<dyn Metadata + Send + Sync>,
    runtime_loader: Arc<bd_runtime::runtime::ConfigLoader>,
    config_updater: Arc<dyn ClientConfigurationUpdate>,
    time_provider: Arc<dyn TimeProvider>,
    network_quality_provider: Arc<SimpleNetworkQualityProvider>,
    self_logger: Arc<dyn bd_internal_logging::Logger>,
    stats: &Scope,
    sleep_mode_active: watch::Receiver<bool>,
  ) -> Self {
    let mut max_backoff_interval = runtime_loader.register_duration_watch();
    let mut initial_backoff_interval = runtime_loader.register_duration_watch();
    let generic_kill_duration = runtime_loader.register_duration_watch();
    let unauthenticated_kill_duration = runtime_loader.register_duration_watch();
    let data_idle_timeout_interval = runtime_loader.register_duration_watch();
    let data_idle_reconnect_interval = runtime_loader.register_duration_watch();

    let backoff = crate::backoff_policy(&mut initial_backoff_interval, &mut max_backoff_interval);

    Self {
      sdk_directory,
      api_key,
      manager,
      static_metadata,
      data_upload_rx,
      trigger_upload_tx,
      time_provider,
      network_quality_provider,
      runtime_loader,
      max_backoff_interval,
      initial_backoff_interval,
      internal_logger: self_logger,
      config_updater,
      stats: Stats::new(stats),
      client_killed: false,
      generic_kill_duration,
      unauthenticated_kill_duration,
      config_marked_safe_due_to_offline: false,
      sleep_mode_active,
      backoff,
      data_idle_timeout_interval,
      data_idle_reconnect_interval,
    }
  }

  pub async fn start(mut self) -> anyhow::Result<()> {
    self.config_updater.try_load_persisted_config().await;

    // To make the client kill process as simple as possible we won't watch for runtime updates
    // during operation. We will assume that runtime gets cached during the update process, and
    // the next time the client starts it will have the new configuration. We can consider
    // adding additional logic to handle this in the future. We can also send a synthetic
    // unauthenticated response to the client to trigger a kill if we need to.
    self.runtime_loader.expect_initialized();
    self.maybe_kill_client().await;

    self.maintain_active_stream().await
  }

  fn kill_file_path(&self) -> PathBuf {
    self.sdk_directory.join("client_kill_until")
  }

  fn opaque_client_state_path(&self) -> PathBuf {
    self.sdk_directory.join("opaque_client_state")
  }

  fn hash_api_key(&self) -> Vec<u8> {
    // This hash is not guaranteed to be stable across different Rust releases, but it should be
    // stable for the lifetime of this SDK version, which is good enough for our use case. We
    // are primarily trying to not write the actual API key into the kill file if somehow it is
    // delivered some other way.
    let mut hasher = DefaultHasher::new();
    self.api_key.hash(&mut hasher);
    hasher.finish().to_be_bytes().to_vec()
  }

  async fn set_client_killed(&mut self) {
    self.client_killed = true;
    UnexpectedErrorHandler::disable();
    // Make sure that we clear out cached config so that when we eventually come back online we
    // can accept new config if the server sends it.
    self.runtime_loader.clear_cached_config().await;
    self.config_updater.clear_cached_config().await;
  }

  async fn maybe_kill_client(&mut self) {
    // First we will try to read the kill file to see if it exists and we are within the kill
    // duration.
    let kill_until = match self.read_kill_file().await {
      Ok(kill_until) => kill_until,
      Err(e) => {
        log::warn!("failed to read kill file: {e}");
        None
      },
    };

    if let Some(kill_until) = kill_until {
      let kill_until_time = kill_until.kill_until.to_offset_date_time();
      if kill_until_time > self.time_provider.now()
        && self.hash_api_key() == kill_until.api_key_hash
      {
        log::debug!(
          "kill file exists and is still active ({} remaining), killing client",
          kill_until_time - self.time_provider.now()
        );
        log::warn!(
          "Attention: The Capture SDK has been force disabled due to either a previous \
           authentication failure or a remote server configuration. Double check your API key or \
           contact support."
        );
        self.set_client_killed().await;
      } else {
        // Delete the kill file if the kill duration has passed or the API key has changed. This
        // will allow the client to come up and contact the server again to see if anything
        // has changed. It will likely get killed again on the next startup.
        log::debug!("kill file has expired or the API key has changed, removing");
        if let Err(e) = tokio::fs::remove_file(self.kill_file_path()).await {
          log::warn!("failed to remove kill file: {e}");
        }
      }
    } else if !self.generic_kill_duration.read().is_zero() {
      log::debug!("client has been killed, writing kill file");
      let generic_kill_duration = *self.generic_kill_duration.read();
      if let Err(e) = self.write_kill_file(generic_kill_duration).await {
        log::warn!("failed to write kill file: {e}");
      }
    }
  }

  async fn read_kill_file(&self) -> anyhow::Result<Option<ClientKillFile>> {
    if !tokio::fs::try_exists(self.kill_file_path()).await? {
      return Ok(None);
    }

    // In order to have basic protection around IO errors we will zlib "decompress" the kill file
    // primarily as a free way to get the CRC.
    let compressed = tokio::fs::read(self.kill_file_path()).await?;
    Ok(Some(read_compressed_protobuf(&compressed)?))
  }

  async fn write_kill_file(&mut self, duration: Duration) -> anyhow::Result<()> {
    self.set_client_killed().await;

    // In order to have basic protection around IO errors we will zlib "compress" the kill file
    // primarily as a free way to get the CRC.
    let kill_until = self.time_provider.now() + duration;
    let kill_file = ClientKillFile {
      api_key_hash: self.hash_api_key(),
      kill_until: kill_until.into_proto(),
      ..Default::default()
    };
    let compressed = write_compressed_protobuf(&kill_file)?;
    tokio::fs::write(self.kill_file_path(), compressed).await?;

    Ok(())
  }

  async fn do_reconnect_backoff(&mut self, min_retry_after: Option<Duration>) {
    // We have no max timeout, hence this should always return a backoff value.
    // Before moving to the next iteration, sleep according to the backoff strategy.
    let reconnect_delay = self
      .backoff
      .next_backoff()
      .unwrap_or_else(|| 1.std_minutes());
    let reconnect_delay =
      min_retry_after.map_or(reconnect_delay, |f| max(f.unsigned_abs(), reconnect_delay));
    log::debug!("reconnecting in {} ms", reconnect_delay.as_millis());

    // Pause execution until we are ready to reconnect.
    tokio::time::sleep(reconnect_delay).await;
  }

  // Maintains an active stream by re-establishing a stream whenever we disconnect (with backoff),
  // until shutdown has been signaled.
  #[tracing::instrument(skip(self), level = "debug", name = "api")]
  async fn maintain_active_stream(mut self) -> anyhow::Result<()> {
    // Collect metadata as part of maintaining active stream and not earlier to ensure that the
    // collection happens on SDK run loop.
    let metadata = self.static_metadata.collect();
    let app_id = metadata.get("app_id").cloned().unwrap_or_default();

    let handshake_metadata: HashMap<_, _> = metadata
      .iter()
      .map(|(k, v)| {
        (
          k.to_string(),
          ProtoData {
            data_type: Some(Data_type::StringData(v.clone())),
            ..Default::default()
          },
        )
      })
      .collect();

    let mut disconnected_at = None;
    let mut last_disconnect_reason = None;
    let mut idle_timeout_reconnect_in: Option<std::time::Duration> = None;
    let mut last_data_received_at = tokio::time::Instant::now();

    loop {
      // If we're blocked from connectecing due to a recent idle timeout, we'll want to wait until
      // there are either new uploads or the idle timeout reconnect timer is hit. We need to make
      // sure that any uploads we receive while waiting for the timeout are processed, so we have
      // to carry with us the upload we receive while waiting.
      let upload_during_idle_timeout =
        if let Some(idle_timeout_reconnect_in) = idle_timeout_reconnect_in {
          log::trace!(
            "reconnecting from data idle timeout in {idle_timeout_reconnect_in:?}",
          );

          tokio::select! {
              () = tokio::time::sleep(idle_timeout_reconnect_in) => {None},
              upload = self.data_upload_rx.recv() => {upload},
          }
        } else {
          None
        };

      // TODO(snowp): This feature would probably be completely broken when data idle timeouts are
      // being hit as it would be indistinguishable from a normal disconnect.
      // If we have been disconnected for more than 15s switch our network quality to offline. We
      // don't want to do this immediately as we might be in a transient state during a normal
      // reconnect.
      if *disconnected_at.get_or_insert_with(Instant::now) + DISCONNECTED_OFFLINE_GRACE_PERIOD
        < Instant::now()
      {
        *self.network_quality_provider.network_quality.write() = NetworkQuality::Offline;
        if !self.config_marked_safe_due_to_offline {
          self.config_marked_safe_due_to_offline = true;
          self.runtime_loader.mark_safe().await;
          self.config_updater.mark_safe().await;
        }
      } else {
        *self.network_quality_provider.network_quality.write() = NetworkQuality::Unknown;
      }

      // If we have been killed, just put ourselves into a permanent pending state until the
      // process restarts.
      if self.client_killed {
        log::debug!("client has been killed, entering pending state");
        return pending().await;
      }

      // Construct a new backoff policy if the runtime values have changed since last time.
      if self.max_backoff_interval.has_changed() || self.initial_backoff_interval.has_changed() {
        log::debug!("backoff policy has changed, recreating");
        self.backoff = crate::backoff_policy(
          &mut self.initial_backoff_interval,
          &mut self.max_backoff_interval,
        );
      }

      self.stats.stream_total.inc();

      log::debug!("starting new stream");

      self
        .internal_logger
        .log_internal("Starting new bitdrift Capture API stream");

      let headers = HashMap::from([
        ("x-bitdrift-api-key", self.api_key.as_ref()),
        ("x-bitdrift-app-id", app_id.as_ref()),
        ("content-type", "application/grpc"),
        (GRPC_ENCODING_HEADER, GRPC_ENCODING_DEFLATE),
        (GRPC_ACCEPT_ENCODING_HEADER, GRPC_ENCODING_DEFLATE),
      ]);

      // Set the size to 16 to avoid blocking if we get back to back upstream events.
      let (stream_event_tx, stream_event_rx) = tokio::sync::mpsc::channel(16);
      let mut stream_state = StreamState::new(
        Some(Compression::StatelessZlib {
          level: DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL,
        }),
        self
          .manager
          .start_stream(stream_event_tx.clone(), &self.runtime_loader, &headers)
          .await?,
        stream_event_rx,
        self.time_provider.clone(),
        &self.stats,
        self.sleep_mode_active.clone(),
      );

      log::debug!("sending handshake");

      stream_state
        .send_request(
          self
            .handshake_request(&handshake_metadata, last_disconnect_reason.take())
            .await,
        )
        .await?;

      log::debug!("waiting for handshake");

      match self.wait_for_handshake(&mut stream_state).await? {
        // We received a handshake, so initialize the stream state with the stream settings
        // and process any additional frames we read together with the handshake.
        HandshakeResult::Received {
          stream_settings,
          configuration_update_status,
          remaining_responses,
        } => {
          stream_state.initialize_stream_settings(stream_settings);

          if let Some(stream_closure_info) = self
            .handle_responses(remaining_responses, &mut stream_state)
            .await?
          {
            last_disconnect_reason = Some(stream_closure_info.reason);
            self.stats.remote_connect_failure.inc();
            self
              .do_reconnect_backoff(stream_closure_info.retry_after)
              .await;
            continue;
          }

          // Let all the configuration pipelines know that we are able to connect to the
          // backend. We supply the configuration update status to the pipelines so they can
          // determine whether cached configuration can be marked safe immediately. It's possible
          // that we already processed a configuration update frame in which case the config was
          // already marked safe.
          self
            .runtime_loader
            .on_handshake_complete(configuration_update_status)
            .await;

          self
            .config_updater
            .on_handshake_complete(configuration_update_status)
            .await;
        },
        // The stream closed while waiting for the handshake. Move to the next loop iteration
        // to attempt to re-create a stream.
        HandshakeResult::StreamClosure(info) => {
          // The network manager API doesn't expose the underlying issue in a type-safe manner,
          // so just treat all failures to handshake as a connect failure.
          last_disconnect_reason = Some(info.reason);
          self.stats.remote_connect_failure.inc();
          self.do_reconnect_backoff(info.retry_after).await;
          continue;
        },
        // Unauthenticated, we need to perform a kill action to stop the API.
        HandshakeResult::Unauthenticated => {
          log::debug!("unauthenticated, killing client");
          let unauthorized_kill_duration = *self.unauthenticated_kill_duration.read();
          if let Err(e) = self.write_kill_file(unauthorized_kill_duration).await {
            log::warn!("failed to write kill file: {e}");
          }
          continue;
        },
      }

      log::debug!("handshake received, entering main loop");
      let handshake_established = Instant::now();
      *self.network_quality_provider.network_quality.write() = NetworkQuality::Online;
      disconnected_at = None;

      if let Some(spurious_upload) = upload_during_idle_timeout {
        // If we received a spurious upload while waiting to reconnect, process it now.
        last_data_received_at = Instant::now();
        stream_state.handle_data_upload(spurious_upload).await?;
      }

      // At this point we have established the stream, so we should start the general
      // request/response handling.
      loop {
        let data_idle_timeout_at = last_data_received_at + self.data_idle_timeout_interval.read().unsigned_abs();
        log::info!("data idle timeout at {:?}", self.data_idle_timeout_interval.read().unsigned_abs());
        let data_idle_timeout_at = tokio::time::sleep_until(data_idle_timeout_at);

        let upstream_event = tokio::select! {
          // Fire a ping timer if the ping timer elapses.
          () = maybe_await(&mut stream_state.ping_sleep) => {
            stream_state.send_ping().await?;
            continue;
          }
          Some(data_upload) = self.data_upload_rx.recv() => {
            last_data_received_at = Instant::now();
            stream_state.handle_data_upload(data_upload).await?;
            continue;
          },
          () = data_idle_timeout_at => {
            // We haven't received any data to upload for a while, so we'll shut down the stream and
            // then reconnect after a short delay.
            log::debug!("no data received for {}, reconnecting", *self.data_idle_timeout_interval.read());
            self.stats.data_idle_timeout.inc();
            idle_timeout_reconnect_in = Some(self.data_idle_reconnect_interval.read().unsigned_abs());
            break;
          },
          // Process network events coming from the platform layer, i.e. response data or stream
          // closures.
          event = stream_state.stream_event_rx.recv() => event,
        };
        
        log::info!("upstream event: {:?}", upstream_event);

        let stream_closure_info = match stream_state.handle_upstream_event(upstream_event).await? {
          UpstreamEvent::UpstreamMessages(responses) => {
            self.handle_responses(responses, &mut stream_state).await?
          },
          UpstreamEvent::StreamClosed(reason) => Some(StreamClosureInfo {
            reason,
            retry_after: None,
          }),
        };

        if let Some(stream_closure_info) = stream_closure_info {
          // We want to avoid a case in which we spin on an error shutdown. We do this by just
          // checking to see if the stream lived for longer than 1 minute, and if so reset the
          // backoff. If the process restarts everything starts over again anyway.
          last_disconnect_reason = Some(stream_closure_info.reason);
          if stream_closure_info.retry_after.is_none()
            && Instant::now() - handshake_established > Duration::minutes(1)
          {
            log::debug!("stream lived for more than 1 minute, resetting backoff");
            self.backoff.reset();
          } else {
            self
              .do_reconnect_backoff(stream_closure_info.retry_after)
              .await;
          }

          break;
        }
      }
    }
  }

  /// Handles any number of non-handshake responses. Receiving a handshake response at this point
  /// is considered a protocol error.
  async fn handle_responses(
    &self,
    responses: Vec<ApiResponse>,
    stream_state: &mut StreamState,
  ) -> anyhow::Result<Option<StreamClosureInfo>> {
    for response in responses {
      match response.response_type {
        Some(Response_type::Handshake(_)) => {
          anyhow::bail!("unexpected api response: spurious handshake")
        },
        Some(Response_type::Pong(_)) => stream_state.maybe_schedule_ping(),
        Some(Response_type::ErrorShutdown(error)) => {
          log::debug!(
            "close with status {:?}, message {:?}",
            error.grpc_status,
            error.grpc_message
          );
          self.stats.error_shutdown_total.inc();
          return Ok(Some(StreamClosureInfo {
            reason: error.grpc_message,
            retry_after: error
              .rate_limited
              .retry_after
              .as_ref()
              .map(bd_time::ProtoDurationExt::to_time_duration),
          }));
        },
        Some(Response_type::LogUpload(log_upload)) => {
          log::debug!(
            "received ack for log upload {:?} ({} dropped), error: {:?}",
            log_upload.upload_uuid,
            log_upload.logs_dropped,
            log_upload.error
          );

          stream_state
            .upload_state_tracker
            .resolve_pending_upload(&log_upload.upload_uuid, &log_upload.error)?;
        },
        Some(Response_type::LogUploadIntent(intent)) => {
          stream_state
            .upload_state_tracker
            .resolve_intent(&intent.intent_uuid, intent.decision)?;
        },
        Some(Response_type::StatsUpload(stats_upload)) => {
          log::debug!(
            "received ack for stats upload {:?}, error: {:?}",
            stats_upload.upload_uuid,
            stats_upload.error
          );

          stream_state
            .upload_state_tracker
            .resolve_pending_upload(&stats_upload.upload_uuid, &stats_upload.error)?;
        },
        Some(Response_type::FlushBuffers(flush_buffers)) => {
          let (tx, _rx) = tokio::sync::oneshot::channel();

          self
            .trigger_upload_tx
            .send(TriggerUpload::new(flush_buffers.buffer_id_list, tx))
            .await
            .map_err(|_| anyhow!("remote trigger upload tx"))?;
        },
        Some(Response_type::SankeyDiagramUpload(sankey_path_upload)) => {
          log::debug!(
            "received ack for sankey path upload {:?}, error: {:?}",
            sankey_path_upload.upload_uuid,
            sankey_path_upload.error
          );

          stream_state
            .upload_state_tracker
            .resolve_pending_upload(&sankey_path_upload.upload_uuid, &sankey_path_upload.error)?;
        },
        Some(Response_type::SankeyIntentResponse(sankey_path_upload_intent)) => {
          log::debug!(
            "received ack for sankey path upload intent {:?}, decision: {:?}",
            sankey_path_upload_intent.intent_uuid,
            sankey_path_upload_intent.decision
          );

          stream_state.upload_state_tracker.resolve_intent(
            &sankey_path_upload_intent.intent_uuid,
            sankey_path_upload_intent.decision,
          )?;
        },
        Some(Response_type::ArtifactUpload(artifact_upload)) => {
          log::debug!(
            "received ack for artifact upload {:?}, error: {:?}",
            artifact_upload.upload_uuid,
            artifact_upload.error
          );

          stream_state
            .upload_state_tracker
            .resolve_pending_upload(&artifact_upload.upload_uuid, &artifact_upload.error)?;
        },
        Some(Response_type::ArtifactIntent(artifact_upload_intent)) => {
          log::debug!(
            "received ack for artifact upload intent {:?}, decision: {:?}",
            artifact_upload_intent.intent_uuid,
            artifact_upload_intent.decision
          );

          stream_state.upload_state_tracker.resolve_intent(
            &artifact_upload_intent.intent_uuid,
            artifact_upload_intent.decision,
          )?;
        },
        Some(Response_type::ConfigurationUpdate(update)) => {
          if let Some(request) = self.config_updater.try_apply_config(update).await {
            stream_state.send_request(request).await?;
          }
        },
        Some(Response_type::RuntimeUpdate(update)) => {
          if let Some(request) = self.runtime_loader.try_apply_config(update).await {
            stream_state.send_request(request).await?;
          }
        },
        None => {
          debug_assert!(false, "not handled");
        },
      }
    }

    Ok(None)
  }

  // Creates a handshake request containing the current version nonces and static metadata.
  async fn handshake_request(
    &self,
    metadata: &HashMap<String, ProtoData>,
    previous_disconnect_reason: Option<String>,
  ) -> HandshakeRequest {
    let opaque_client_state = tokio::fs::read(&self.opaque_client_state_path()).await.ok();
    let mut handshake = HandshakeRequest {
      static_device_metadata: metadata.clone(),
      previous_disconnect_reason: previous_disconnect_reason.unwrap_or_default(),
      sleep_mode: *self.sleep_mode_active.borrow(),
      opaque_client_state,
      ..Default::default()
    };

    self.config_updater.fill_handshake(&mut handshake);
    self.runtime_loader.fill_handshake(&mut handshake);

    handshake
  }

  async fn process_opaque_client_state(&self, state: Option<Vec<u8>>) {
    if let Some(state) = state {
      log::debug!("storing opaque client state ({} bytes)", state.len());
      if let Err(e) = tokio::fs::write(self.opaque_client_state_path(), state).await {
        log::warn!("failed to store opaque client state: {e}");
      }
    } else {
      log::debug!("no opaque client state to store");
      if let Err(e) = tokio::fs::remove_file(self.opaque_client_state_path()).await
        && e.kind() != std::io::ErrorKind::NotFound
      {
        log::warn!("failed to remove opaque client state: {e}");
      }
    }
  }

  /// Waits for enough data to be collected to parse the initial handshake, handling various error
  /// conditions we might run into at the start of the stream.
  async fn wait_for_handshake(
    &self,
    stream_state: &mut StreamState,
  ) -> anyhow::Result<HandshakeResult> {
    loop {
      let event = stream_state.stream_event_rx.recv().await;
      match stream_state.handle_upstream_event(event).await? {
        UpstreamEvent::UpstreamMessages(responses) => {
          // This happens if we received data, but not enough to form a full gRPC message.
          let mut responses = responses.into_iter();
          let Some(first) = responses.next() else {
            continue;
          };

          // Since we process raw bytes, we might have received more than one frame.
          // First we look at the first response message, and make sure that this is a handshake
          // response. If not, we error out.
          //
          // Once we know that it is a handshake response, we extract the relevant pieces from
          // it and pass it back up as well as any other frames we decoded for immediate
          // processing. The response decoder might still contain data, so we continue to use
          // it for further decoding.
          match first.response_type {
            Some(Response_type::Handshake(mut h)) => {
              log::debug!("received handshake");
              self
                .process_opaque_client_state(h.opaque_client_state_to_echo)
                .await;
              return Ok(HandshakeResult::Received {
                stream_settings: h.stream_settings.take(),
                configuration_update_status: h.configuration_update_status,
                remaining_responses: responses.collect(),
              });
            },
            Some(Response_type::ErrorShutdown(error)) => {
              // If we're provided with an invalid API key or otherwise fail to authenticate with
              // the backend, log this as a warning to surface this to developers trying to set up
              // the SDK.
              if error.grpc_status == Code::Unauthenticated.to_int() {
                log::warn!(
                  "failed to authenticate with the backend: {}",
                  error.grpc_message
                );

                return Ok(HandshakeResult::Unauthenticated);
              }

              // Only count non-auth errors here as an auth error is effectively a misconfiguration
              // issue. We may consider tracking auth failures separately.
              self.stats.error_shutdown_total.inc();

              // This might happen in production should the server shut down before it has
              // been able to respond with a handshake.
              log::debug!(
                "received close before handshake with status '{}', message '{}'",
                error.grpc_status,
                error.grpc_message
              );
              return Ok(HandshakeResult::StreamClosure(StreamClosureInfo {
                reason: format!("error shutdown: {}", error.grpc_message),
                retry_after: error
                  .rate_limited
                  .retry_after
                  .as_ref()
                  .map(bd_time::ProtoDurationExt::to_time_duration),
              }));
            },
            _ => anyhow::bail!("unexpected api response: spurious response before handshake"),
          }
        },
        UpstreamEvent::StreamClosed(reason) => {
          return Ok(HandshakeResult::StreamClosure(StreamClosureInfo {
            reason,
            retry_after: None,
          }));
        },
      }
    }
  }
}
