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
use backoff::backoff::Backoff;
use backoff::exponential::{ExponentialBackoff, ExponentialBackoffBuilder};
use backoff::SystemClock;
use bd_client_common::error::UnexpectedErrorHandler;
use bd_client_common::file::{read_compressed_protobuf, write_compressed_protobuf};
use bd_client_common::payload_conversion::{IntoRequest, MuxResponse, ResponseKind};
use bd_client_common::zlib::DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL;
use bd_client_common::ConfigurationUpdate;
use bd_client_stats_store::{Counter, CounterWrapper, Scope};
use bd_grpc_codec::{
  Compression,
  Encoder,
  OptimizeFor,
  GRPC_ACCEPT_ENCODING_HEADER,
  GRPC_ENCODING_DEFLATE,
  GRPC_ENCODING_HEADER,
};
use bd_metadata::Metadata;
use bd_network_quality::{NetworkQuality, NetworkQualityProvider};
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
  handshake_response,
  ApiRequest,
  ApiResponse,
  ClientKillFile,
  HandshakeRequest,
  PingRequest,
};
use bd_proto::protos::logging::payload::data::Data_type;
use bd_proto::protos::logging::payload::Data as ProtoData;
use bd_runtime::runtime::DurationWatch;
use bd_shutdown::ComponentShutdown;
use bd_time::{OffsetDateTimeExt, TimeProvider, TimestampExt};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Instant;

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
  StreamClosure(String),

  /// We gave up waiting for the handshake due to shutdown.
  ShuttingDown,

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
  ping_deadline: Option<tokio::time::Instant>,

  upload_state_tracker: upload::StateTracker,

  stream_handle: Box<dyn PlatformNetworkStream>,

  stream_event_rx: tokio::sync::mpsc::Receiver<StreamEvent>,

  time_provider: Arc<dyn TimeProvider>,
}

impl StreamState {
  fn new(
    compression: Option<Compression>,
    stream_handle: Box<dyn PlatformNetworkStream>,
    stream_event_rx: tokio::sync::mpsc::Receiver<StreamEvent>,
    time_provider: Arc<dyn TimeProvider>,
    stats: &Stats,
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
      ping_deadline: None,
      upload_state_tracker: StateTracker::new(),
      stream_handle,
      stream_event_rx,
      time_provider,
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
    self.ping_deadline = self.ping_interval.map(|ping_in| {
      log::debug!("scheduling ping in {ping_in:?}");

      Instant::now() + ping_in
    });
  }

  async fn send_request<R: IntoRequest>(&mut self, request: R) -> anyhow::Result<()> {
    let framed_message = self.request_encoder.encode(&request.into_request());
    self.stream_handle.send_data(&framed_message).await
  }

  async fn send_ping(&mut self) -> anyhow::Result<()> {
    log::debug!("sending ping");

    self.send_request(PingRequest::default()).await
  }

  // Processes a single upstream event (data/close) being provided by the platform network.
  fn handle_upstream_event(&mut self, event: Option<StreamEvent>) -> anyhow::Result<UpstreamEvent> {
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
      // if the logger gets destroyed before the API stream receives the shutdown signal.
      None => Ok(UpstreamEvent::Shutdown),
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

  // The event channel has been shut down, indicating that we are shutting down.
  Shutdown,
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
  shutdown: ComponentShutdown,
  data_upload_rx: Receiver<DataUpload>,
  trigger_upload_tx: Sender<TriggerUpload>,

  static_metadata: Arc<dyn Metadata + Send + Sync>,

  max_backoff_interval: DurationWatch<bd_runtime::runtime::api::MaxBackoffInterval>,
  initial_backoff_interval: DurationWatch<bd_runtime::runtime::api::InitialBackoffInterval>,
  configuration_pipelines: Vec<Arc<dyn ConfigurationUpdate>>,
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
}

impl Api {
  pub fn new(
    sdk_directory: PathBuf,
    api_key: String,
    manager: Box<dyn PlatformNetworkManager<bd_runtime::runtime::ConfigLoader>>,
    shutdown: ComponentShutdown,
    data_upload_rx: Receiver<DataUpload>,
    trigger_upload_tx: Sender<TriggerUpload>,
    static_metadata: Arc<dyn Metadata + Send + Sync>,
    runtime_loader: Arc<bd_runtime::runtime::ConfigLoader>,
    configuration_pipelines: Vec<Arc<dyn ConfigurationUpdate>>,
    time_provider: Arc<dyn TimeProvider>,
    network_quality_provider: Arc<SimpleNetworkQualityProvider>,
    self_logger: Arc<dyn bd_internal_logging::Logger>,
    stats: &Scope,
  ) -> anyhow::Result<Self> {
    let max_backoff_interval = runtime_loader.register_watch()?;
    let initial_backoff_interval = runtime_loader.register_watch()?;
    let generic_kill_duration = runtime_loader.register_watch()?;
    let unauthenticated_kill_duration = runtime_loader.register_watch()?;

    Ok(Self {
      sdk_directory,
      api_key,
      manager,
      shutdown,
      static_metadata,
      data_upload_rx,
      trigger_upload_tx,
      time_provider,
      network_quality_provider,
      runtime_loader,
      max_backoff_interval,
      initial_backoff_interval,
      internal_logger: self_logger,
      configuration_pipelines,
      stats: Stats::new(stats),
      client_killed: false,
      generic_kill_duration,
      unauthenticated_kill_duration,
      config_marked_safe_due_to_offline: false,
    })
  }

  pub async fn start(mut self) -> anyhow::Result<()> {
    // TODO(snowp): It would be nicer if we did this before we started the API but seems like a
    // risky change.

    // Loading the configuration can fail for a number of reasons like the file not existing, so
    // ignore.
    for pipeline in &mut self.configuration_pipelines {
      pipeline.try_load_persisted_config().await;
    }

    // To make the client kill process as simple as possible we won't watch for runtime updates
    // during operation. We will assume that runtime gets cached during the update process, and
    // the next time the client starts it will have the new configuration. We can consider
    // adding additional logic to handle this in the future. We can also send a synthetic
    // unauthenticated response to the client to trigger a kill if we need to.
    self.maybe_kill_client().await;

    self.maintain_active_stream().await
  }

  fn kill_file_path(&self) -> PathBuf {
    self.sdk_directory.join("client_kill_until")
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

  fn set_client_killed(&mut self) {
    self.client_killed = true;
    UnexpectedErrorHandler::disable();
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
        self.set_client_killed();
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
    self.set_client_killed();

    // In order to have basic protection around IO errors we will zlib "compress" the kill file
    // primarily as a free way to get the CRC.
    let kill_until = self.time_provider.now() + duration;
    let kill_file = ClientKillFile {
      api_key_hash: self.hash_api_key(),
      kill_until: kill_until.into_proto(),
      ..Default::default()
    };
    let compressed = write_compressed_protobuf(&kill_file);
    tokio::fs::write(self.kill_file_path(), compressed).await?;

    Ok(())
  }

  #[must_use]
  async fn do_reconnect_backoff(&mut self, backoff: &mut ExponentialBackoff<SystemClock>) -> bool {
    // We have no max timeout, hence this should always return a backoff value.
    // Before moving to the next iteration, sleep according to the backoff strategy.
    let reconnect_delay = backoff.next_backoff().unwrap();
    log::debug!("reconnecting in {} ms", reconnect_delay.as_millis());

    // Pause execution until we are ready to reconnect, or the task is canceled.
    tokio::select! {
      () = tokio::time::sleep(reconnect_delay) => true,
      () = self.shutdown.cancelled() => {
        log::debug!("received shutdown while waiting for reconnect, shutting down");
        false
      },
    }
  }

  // Maintains an active stream by re-establishing a stream whenever we disconnect (with backoff),
  // until shutdown has been signaled.
  #[tracing::instrument(skip(self), level = "debug", name = "api")]
  async fn maintain_active_stream(mut self) -> anyhow::Result<()> {
    let mut backoff = self.backoff_policy();

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

    loop {
      // If we have been disconnected for more than 15s switch our network quality to offline. We
      // don't want to do this immediately as we might be in a transient state during a normal
      // reconnect.
      if *disconnected_at.get_or_insert_with(Instant::now) + DISCONNECTED_OFFLINE_GRACE_PERIOD
        < Instant::now()
      {
        *self.network_quality_provider.network_quality.write() = NetworkQuality::Offline;
        if !self.config_marked_safe_due_to_offline {
          self.config_marked_safe_due_to_offline = true;
          for pipeline in &self.configuration_pipelines {
            pipeline.mark_safe().await;
          }
        }
      } else {
        *self.network_quality_provider.network_quality.write() = NetworkQuality::Unknown;
      }

      // If we have been killed, just put ourselves into a permanent pending state until the
      // process restarts. We will wait for the shutdown notification.
      if self.client_killed {
        log::debug!("client has been killed, entering pending state");
        self.shutdown.cancelled().await;
        return Ok(());
      }

      // Construct a new backoff policy if the runtime values have changed since last time.
      if self.max_backoff_interval.has_changed() || self.initial_backoff_interval.has_changed() {
        log::debug!("backoff policy has changed, recreating");
        backoff = self.backoff_policy();
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
      );

      log::debug!("sending handshake");

      stream_state
        .send_request(self.handshake_request(&handshake_metadata, last_disconnect_reason.take()))
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

          self
            .handle_responses(remaining_responses, &mut stream_state)
            .await?;

          // Let the all the configuration pipelines know that we are able to connect to the
          // backend. We do this after we process responses that immediately followed the handshake
          // in order to make it possible for the server to push down configuration that
          // would get applied as part of the handshake response.
          for pipeline in &self.configuration_pipelines {
            pipeline
              .on_handshake_complete(configuration_update_status)
              .await;
          }
        },
        // The stream closed while waiting for the handshake. Move to the next loop iteration
        // to attempt to re-create a stream.
        HandshakeResult::StreamClosure(reason) => {
          // The network manager API doesn't expose the underlying issue in a type-safe manner,
          // so just treat all failures to handshake as a connect failure.
          last_disconnect_reason = Some(reason);
          self.stats.remote_connect_failure.inc();
          if self.do_reconnect_backoff(&mut backoff).await {
            continue;
          }
          return Ok(());
        },
        // We received the shutdown notification, so exit out.
        HandshakeResult::ShuttingDown => return Ok(()),
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

      // At this point we have established the stream, so we should start the general
      // request/response handling.
      loop {
        let ping_timer = stream_state.ping_deadline.map(tokio::time::sleep_until);
        let upstream_event = tokio::select! {
          // If we receive the shutdown signal, shut down the loop.
          () = self.shutdown.cancelled() => {
            log::debug!("received shutdown signal, shutting down");
            return Ok(());
          },
          // Fire a ping timer if the ping timer elapses.
          () = async { ping_timer.unwrap().await }, if ping_timer.is_some() => {
            stream_state.ping_deadline = None;
            stream_state.send_ping().await?;
            continue;
          }
          Some(data_upload) = self.data_upload_rx.recv() => {
            stream_state.handle_data_upload(data_upload).await?;
            continue;
          },
          // Process network events coming from the platform layer, i.e. response data or stream
          // closures.
          event = stream_state.stream_event_rx.recv() => event,
        };

        match stream_state.handle_upstream_event(upstream_event)? {
          UpstreamEvent::UpstreamMessages(responses) => {
            self.handle_responses(responses, &mut stream_state).await?;
          },
          UpstreamEvent::StreamClosed(reason) => {
            // We want to avoid a case in which we spin on an error shutdown. We do this by just
            // checking to see if the stream lived for longer than 1 minute, and if so reset the
            // backoff. If the process restarts everything starts over again anyway.
            last_disconnect_reason = Some(reason);
            if Instant::now() - handshake_established > Duration::minutes(1) {
              log::debug!("stream lived for more than 1 minute, resetting backoff");
              backoff.reset();
            } else if !self.do_reconnect_backoff(&mut backoff).await {
              return Ok(());
            }

            break;
          },
          UpstreamEvent::Shutdown => return Ok(()),
        }
      }
    }
  }

  /// Handles any number of non-handshake responses. Receiving a handshake response at this point
  /// is considered a protocol error.
  async fn handle_responses(
    &mut self,
    responses: Vec<ApiResponse>,
    stream_state: &mut StreamState,
  ) -> anyhow::Result<()> {
    for response in responses {
      match response.demux() {
        Some(ResponseKind::Handshake(_)) => {
          anyhow::bail!("unexpected api response: spurious handshake")
        },
        Some(ResponseKind::Pong(_)) => stream_state.maybe_schedule_ping(),
        Some(ResponseKind::ErrorShutdown(error)) => {
          log::debug!(
            "close with status {:?}, message {:?}",
            error.grpc_status,
            error.grpc_message
          );
          self.stats.error_shutdown_total.inc();
        },
        Some(ResponseKind::LogUpload(log_upload)) => {
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
        Some(ResponseKind::LogUploadIntent(intent)) => {
          stream_state
            .upload_state_tracker
            .resolve_intent(&intent.intent_uuid, intent.decision.clone())?;
        },
        Some(ResponseKind::StatsUpload(stats_upload)) => {
          log::debug!(
            "received ack for stats upload {:?}, error: {:?}",
            stats_upload.upload_uuid,
            stats_upload.error
          );

          stream_state
            .upload_state_tracker
            .resolve_pending_upload(&stats_upload.upload_uuid, &stats_upload.error)?;
        },
        Some(ResponseKind::FlushBuffers(flush_buffers)) => {
          let (tx, _rx) = tokio::sync::oneshot::channel();

          self
            .trigger_upload_tx
            .send(TriggerUpload::new(flush_buffers.buffer_id_list.clone(), tx))
            .await
            .map_err(|_| anyhow!("remote trigger upload tx"))?;
        },
        Some(ResponseKind::SankeyPathUpload(sankey_path_upload)) => {
          log::debug!(
            "received ack for sankey path upload {:?}, error: {:?}",
            sankey_path_upload.upload_uuid,
            sankey_path_upload.error
          );

          stream_state
            .upload_state_tracker
            .resolve_pending_upload(&sankey_path_upload.upload_uuid, &sankey_path_upload.error)?;
        },
        Some(ResponseKind::SankeyPathUploadIntent(sankey_path_upload_intent)) => {
          log::debug!(
            "received ack for sankey path upload intent {:?}, decision: {:?}",
            sankey_path_upload_intent.intent_uuid,
            sankey_path_upload_intent.decision
          );

          stream_state.upload_state_tracker.resolve_intent(
            &sankey_path_upload_intent.intent_uuid,
            sankey_path_upload_intent.decision.clone(),
          )?;
        },
        Some(ResponseKind::ArtifactUpload(artifact_upload)) => {
          log::debug!(
            "received ack for artifact upload {:?}, error: {:?}",
            artifact_upload.upload_uuid,
            artifact_upload.error
          );

          stream_state
            .upload_state_tracker
            .resolve_pending_upload(&artifact_upload.upload_uuid, &artifact_upload.error)?;
        },
        Some(ResponseKind::ArtifactUploadIntent(artifact_upload_intent)) => {
          log::debug!(
            "received ack for artifact upload intent {:?}, decision: {:?}",
            artifact_upload_intent.intent_uuid,
            artifact_upload_intent.decision
          );

          stream_state.upload_state_tracker.resolve_intent(
            &artifact_upload_intent.intent_uuid,
            artifact_upload_intent.decision.clone(),
          )?;
        },
        Some(ResponseKind::Untyped) => {
          log::debug!("received untyped response: {response}");
          for pipeline in &mut self.configuration_pipelines {
            if let Some(request) = pipeline.try_apply_config(&response).await {
              stream_state.send_request(request).await?;
            }
          }
        },
        None => {
          debug_assert!(false, "not handled");
        },
      }
    }

    Ok(())
  }

  // Creates a handshake request containing the current version nonces and static metadata.
  fn handshake_request(
    &self,
    metadata: &HashMap<String, ProtoData>,
    previous_disconnect_reason: Option<String>,
  ) -> HandshakeRequest {
    let mut handshake = HandshakeRequest {
      static_device_metadata: metadata.clone(),
      previous_disconnect_reason: previous_disconnect_reason.unwrap_or_default(),
      ..Default::default()
    };

    for pipeline in &self.configuration_pipelines {
      pipeline.fill_handshake(&mut handshake);
    }

    handshake
  }

  /// Waits for enough data to be collected to parse the initial handshake, handling various error
  /// conditions we might run into at the start of the stream.
  async fn wait_for_handshake(
    &mut self,
    stream_state: &mut StreamState,
  ) -> anyhow::Result<HandshakeResult> {
    loop {
      let event = tokio::select! {
        () = self.shutdown.cancelled() => {
          log::debug!("received shutdown broadcast");
          return Ok(HandshakeResult::ShuttingDown)
        },
        event = stream_state.stream_event_rx.recv() => event,
      };

      match stream_state.handle_upstream_event(event)? {
        UpstreamEvent::UpstreamMessages(responses) => {
          // This happens if we received data, but not enough to form a full gRPC message.
          if responses.is_empty() {
            continue;
          }

          // Since we process raw bytes, we might have received more than one frame.
          // First we look at the first response message, and make sure that this is a handshake
          // response. If not, we error out.
          //
          // Once we know that it is a handshake response, we extract the relevant pieces from
          // it and pass it back up as well as any other frames we decoded for immediate
          // processing. The response decoder might still contain data, so we continue to use
          // it for further decoding.
          let (first, rest) = responses.split_first().unwrap();
          match first.demux() {
            Some(ResponseKind::Handshake(h)) => {
              log::debug!("received handshake");
              return Ok(HandshakeResult::Received {
                stream_settings: h.stream_settings.clone().take(),
                configuration_update_status: h.configuration_update_status,
                remaining_responses: Vec::from(rest),
              });
            },
            Some(ResponseKind::ErrorShutdown(error)) => {
              static UNAUTHENTICATED: i32 = 16;

              // If we're provided with an invalid API key or otherwise fail to authenticate with
              // the backend, log this as a warning to surface this to developers trying to set up
              // the SDK.
              if error.grpc_status == UNAUTHENTICATED {
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
              return Ok(HandshakeResult::StreamClosure(format!(
                "error shutdown: {}",
                error.grpc_message
              )));
            },
            _ => anyhow::bail!("unexpected api response: spurious response before handshake"),
          }
        },
        UpstreamEvent::Shutdown => return Ok(HandshakeResult::ShuttingDown),
        UpstreamEvent::StreamClosed(reason) => return Ok(HandshakeResult::StreamClosure(reason)),
      }
    }
  }

  /// Constructs a new `ExponentialBackoff` based on the current runtime values.
  fn backoff_policy(&mut self) -> ExponentialBackoff<SystemClock> {
    ExponentialBackoffBuilder::<SystemClock>::new()
      .with_initial_interval(
        self
          .initial_backoff_interval
          .read_mark_update()
          .unsigned_abs(),
      )
      .with_max_interval(self.max_backoff_interval.read_mark_update().unsigned_abs())
      .with_max_elapsed_time(None)
      .build()
  }
}
