// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./api_test.rs"]
mod api_test;

use crate::payload_conversion::RuntimeConfigurationUpdate;
use crate::upload::{self, StateTracker};
use crate::{
  ConfigurationUpdate,
  DataUpload,
  FromResponse,
  IntoRequest,
  MuxResponse,
  PlatformNetworkManager,
  PlatformNetworkStream,
  ResponseKind,
  StreamEvent,
  TriggerUpload,
};
use anyhow::anyhow;
use backoff::backoff::Backoff;
use backoff::exponential::{ExponentialBackoff, ExponentialBackoffBuilder};
use backoff::SystemClock;
use bd_client_stats_store::{Counter, CounterWrapper, Scope};
use bd_grpc_codec::{Compression, Encoder};
use bd_metadata::Metadata;
pub use bd_proto::protos::client::api::log_upload_intent_response::{
  Decision,
  Drop as DropDecision,
  UploadImmediately,
};
use bd_proto::protos::client::api::{
  handshake_response,
  ApiRequest,
  ApiResponse,
  ConfigurationUpdateAck,
  HandshakeRequest,
  PingRequest,
  RuntimeUpdate,
};
use bd_proto::protos::logging::payload::data::Data_type;
use bd_proto::protos::logging::payload::Data as ProtoData;
use bd_runtime::runtime::{RuntimeManager, Watch};
use bd_shutdown::ComponentShutdown;
use protobuf::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Instant;

// TODO(snowp): This shouldn't be in this file, but runtime depends on api so we run into a
// circular dependency. Maybe we should split the runtime read path from the updater?
#[async_trait::async_trait]
impl ConfigurationUpdate for RuntimeManager {
  async fn try_apply_config(&mut self, response: &ApiResponse) -> Option<ApiRequest> {
    let update = RuntimeUpdate::from_response(response)?;
    self.process_update(update);

    Some(
      RuntimeConfigurationUpdate(ConfigurationUpdateAck {
        last_applied_version_nonce: self.version_nonce().unwrap_or_default(),
        nack: None.into(),
        ..Default::default()
      })
      .into_request(),
    )
  }

  async fn try_load_persisted_config(&mut self) {
    let _ignored = self.handle_cached_config();
  }

  fn partial_handshake(&self) -> HandshakeRequest {
    HandshakeRequest {
      runtime_version_nonce: self.version_nonce().unwrap_or_default(),
      ..Default::default()
    }
  }

  fn on_handshake_complete(&self) {
    self.server_is_available();
  }
}

//
// HandshakeResult
//

/// The result of waiting for a handshake response.
enum HandshakeResult {
  /// We received a handshake response with with a set of stream settings, and a list of extra
  /// responses that were decoded in addition to the handshake response.
  Received(Option<handshake_response::StreamSettings>, Vec<ApiResponse>),

  /// The stream closed while waiting for the handshake.
  StreamClosure,

  /// We gave up waiting for the handshake due to shutdown.
  ShuttingDown,
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

  stats: Stats,
}

impl StreamState {
  fn new(
    compression: Option<Compression>,
    stream_handle: Box<dyn PlatformNetworkStream>,
    stream_event_rx: tokio::sync::mpsc::Receiver<StreamEvent>,
    stats: Stats,
  ) -> Self {
    let mut decoder = bd_grpc_codec::Decoder::default();
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
      stats,
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
      log::debug!("scheduling ping in {:?}", ping_in);

      Instant::now() + ping_in
    });
  }

  async fn send_request<R: IntoRequest>(&mut self, request: R) -> anyhow::Result<()> {
    let framed_message = self.request_encoder.encode(&request.into_request());

    self.stats.tx_bytes.inc_by(framed_message.len() as u64);

    self.stream_handle.send_data(&framed_message).await
  }

  async fn send_ping(&mut self) -> anyhow::Result<()> {
    log::debug!("sending ping");

    self.send_request(PingRequest::default()).await
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
        Ok(UpstreamEvent::UpstreamMessages(frames))
      },
      // We observed a close event, propagate this back up.
      Some(StreamEvent::StreamClosed(reason)) => {
        log::debug!("stream closed due to '{}'", reason);

        Ok(UpstreamEvent::StreamClosed)
      },
      // The upstream event sender has dropped before we terminated the stream. This can happen
      // if the logger gets destroyed before the API stream receives the shutdown signal.
      None => Ok(UpstreamEvent::Shutdown),
    }
  }

  async fn handle_data_upload(&mut self, data_upload: DataUpload) -> anyhow::Result<()> {
    match data_upload {
      DataUpload::LogsUploadIntentRequest(intent) => {
        let req = self.upload_state_tracker.track_intent(intent);
        self.send_request(req).await
      },
      DataUpload::LogsUploadRequest(request) => {
        let req = self.upload_state_tracker.track_upload(request);
        self.send_request(req).await
      },
      DataUpload::StatsUploadRequest(request) => {
        let req = self.upload_state_tracker.track_upload(request);
        self.send_request(req).await
      },
      DataUpload::OpaqueRequest(request) => {
        let req = self.upload_state_tracker.track_upload(request);
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
  StreamClosed,

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
  api_key: String,
  manager: Box<dyn PlatformNetworkManager<bd_runtime::runtime::ConfigLoader>>,
  shutdown: ComponentShutdown,
  data_upload_rx: Receiver<DataUpload>,
  trigger_upload_tx: Sender<TriggerUpload>,

  static_metadata: Arc<dyn Metadata + Send + Sync>,

  max_backoff_interval: Watch<u32, bd_runtime::runtime::api::MaxBackoffInterval>,
  initial_backoff_interval: Watch<u32, bd_runtime::runtime::api::InitialBackoffInterval>,
  compression_enabled: Watch<bool, bd_runtime::runtime::api::CompressionEnabled>,
  configuration_pipelines: Vec<Box<dyn ConfigurationUpdate>>,
  runtime_loader: Arc<bd_runtime::runtime::ConfigLoader>,

  internal_logger: Arc<dyn bd_internal_logging::Logger>,

  stats: Stats,
}

impl Api {
  pub fn new(
    api_key: String,
    manager: Box<dyn PlatformNetworkManager<bd_runtime::runtime::ConfigLoader>>,
    shutdown: ComponentShutdown,
    data_upload_rx: Receiver<DataUpload>,
    trigger_upload_tx: Sender<TriggerUpload>,
    static_metadata: Arc<dyn Metadata + Send + Sync>,
    runtime_loader: Arc<bd_runtime::runtime::ConfigLoader>,
    configuration_pipelines: Vec<Box<dyn ConfigurationUpdate>>,
    self_logger: Arc<dyn bd_internal_logging::Logger>,
    stats: &Scope,
  ) -> anyhow::Result<Self> {
    let max_backoff_interval = runtime_loader.register_watch()?;
    let initial_backoff_interval = runtime_loader.register_watch()?;
    let compression_enabled = runtime_loader.register_watch()?;

    Ok(Self {
      api_key,
      manager,
      shutdown,
      static_metadata,
      data_upload_rx,
      trigger_upload_tx,
      runtime_loader,
      max_backoff_interval,
      initial_backoff_interval,
      internal_logger: self_logger,
      compression_enabled,
      configuration_pipelines,
      stats: Stats::new(stats),
    })
  }

  pub async fn start(mut self) -> anyhow::Result<()> {
    // Loading the configuration can fail for a number of reasons like the file not existing, so
    // ignore.
    for pipeline in &mut self.configuration_pipelines {
      pipeline.try_load_persisted_config().await;
    }

    self.maintain_active_stream().await
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

    loop {
      // Construct a new backoff policy if the runtime values have changed since last time.
      if self.max_backoff_interval.has_changed() || self.initial_backoff_interval.has_changed() {
        backoff = self.backoff_policy();
      }

      self.stats.stream_total.inc();

      log::debug!("starting new stream");

      self
        .internal_logger
        .log_internal("Starting new bitdrift Capture API stream");

      let headers = &mut HashMap::from([
        ("x-bitdrift-api-key", self.api_key.as_ref()),
        ("x-bitdrift-app-id", app_id.as_ref()),
        ("content-type", "application/grpc"),
      ]);

      let compression_enabled = self.compression_enabled.read();
      let compression = compression_enabled.then_some(Compression::Zlib);
      if compression.is_some() {
        headers.insert("x-grpc-encoding", "deflate");
      }

      // Set the size to 16 to avoid blocking if we get back to back upstream events.
      let (stream_event_tx, stream_event_rx) = tokio::sync::mpsc::channel(16);
      let mut stream_state = StreamState::new(
        compression,
        self
          .manager
          .start_stream(stream_event_tx.clone(), &self.runtime_loader, headers)
          .await?,
        stream_event_rx,
        self.stats.clone(),
      );

      log::debug!("sending handshake");

      stream_state
        .send_request(self.handshake_request(&handshake_metadata))
        .await?;

      log::debug!("waiting for handshake");

      match self.wait_for_handshake(&mut stream_state).await? {
        // We received a handshake, so initialize the stream state with the stream settings
        // and process any additional frames we read together with the handshake.
        HandshakeResult::Received(stream_settings, remaining_responses) => {
          stream_state.initialize_stream_settings(stream_settings);

          self
            .handle_responses(remaining_responses, &mut stream_state)
            .await?;
        },
        // The stream closed while waiting for the handshake. Move to the next loop iteration
        // to attempt to re-create a stream.
        HandshakeResult::StreamClosure => {
          // The network manager API doesn't expose the underlying issue in a type-safe manner,
          // so just treat all failures to handshake as a connect failure.
          self.stats.remote_connect_failure.inc();

          // We have no max timeout, hence this should always return a backoff value.
          // Before moving to the next iteration, sleep according to the backoff strategy.
          let reconnect_delay = backoff.next_backoff().unwrap();
          log::debug!("reconnecting in {} ms", reconnect_delay.as_millis());

          // Pause execution until we are ready to reconnect, or the task is canceled.
          tokio::select! {
            () = tokio::time::sleep(backoff.next_backoff().unwrap()) => continue,
            () = self.shutdown.cancelled() => {
              log::debug!("received shutdown while waiting for reconnect, shutting down");

              return Ok(())
            },
          }
        },
        // We received the shutdown notification, so exit out.
        HandshakeResult::ShuttingDown => return Ok(()),
      }

      // Let the all the configuration pipelines know that we are able to connect to the
      // backend. We do this after we process responses that immediately followed the handshake in
      // order to make it possible for the server to push down configuration that would get applied
      // as part of the handshake response.
      for pipeline in &self.configuration_pipelines {
        pipeline.on_handshake_complete();
      }

      log::debug!("handshake received, resetting connection backoff");

      // Reset the backoff on a sucessful handshake.
      backoff.reset();

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

        match stream_state.handle_upstream_event(upstream_event).await? {
          UpstreamEvent::UpstreamMessages(responses) => {
            self.handle_responses(responses, &mut stream_state).await?;
          },
          UpstreamEvent::StreamClosed => {
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
            "close with status '{}', message '{}'",
            error.grpc_status,
            error.grpc_message
          );
          self.stats.error_shutdown_total.inc();
        },
        // TODO(snowp): Make these upload acks generic similar to DataUpload.
        Some(ResponseKind::Opaque(opaque)) => {
          log::debug!(
            "received ack for opaque upload {}, error: {:?}",
            opaque.uuid,
            opaque.error
          );

          stream_state
            .upload_state_tracker
            .resolve_pending_upload(&opaque.uuid, &opaque.error.clone().unwrap_or_default())?;
        },
        Some(ResponseKind::LogUpload(log_upload)) => {
          log::debug!(
            "received ack for log upload {} ({} dropped), error: {}",
            log_upload.upload_uuid,
            log_upload.logs_dropped,
            log_upload.error
          );

          stream_state
            .upload_state_tracker
            .resolve_pending_upload(&log_upload.upload_uuid, &log_upload.error)?;
        },
        Some(ResponseKind::LogUploadIntent(intent)) => {
          stream_state.upload_state_tracker.resolve_intent(
            &intent.intent_uuid,
            intent
              .decision
              .clone()
              .unwrap_or_else(|| Decision::Drop(DropDecision::default())),
          )?;
        },
        Some(ResponseKind::StatsUpload(stats_upload)) => {
          log::debug!(
            "received ack for stats upload {}, error: {}",
            stats_upload.upload_uuid,
            stats_upload.error
          );

          stream_state
            .upload_state_tracker
            .resolve_pending_upload(&stats_upload.upload_uuid, &stats_upload.error)?;
        },
        Some(ResponseKind::FlushBuffers(flush_buffers)) => self
          .trigger_upload_tx
          .send(TriggerUpload::new(flush_buffers.buffer_id_list.clone()))
          .await
          .map_err(|_| anyhow!("remote trigger upload tx"))?,
        Some(ResponseKind::Untyped) => {
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
  fn handshake_request(&self, metadata: &HashMap<String, ProtoData>) -> HandshakeRequest {
    self.configuration_pipelines.iter().fold(
      HandshakeRequest {
        static_device_metadata: metadata.clone(),
        ..Default::default()
      },
      |mut handshake, pipeline| {
        handshake
          .merge_from_bytes(&pipeline.partial_handshake().write_to_bytes().unwrap())
          .unwrap();
        handshake
      },
    )
  }

  /// Waits for enough data to be collected to parse the initial handshake, handeling various error
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

      match stream_state.handle_upstream_event(event).await? {
        UpstreamEvent::UpstreamMessages(responses) => {
          // This happens if we received data, but enough enough to form a full gRPC message.
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
              return Ok(HandshakeResult::Received(
                h.stream_settings.clone().take(),
                Vec::from(rest),
              ));
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

                // TODO(snowp): Consider handling a failure to auth in some way - right now we'll
                // continue to retry even though we are unlikely to be able to recover from this.
                return Ok(HandshakeResult::StreamClosure);
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
              return Ok(HandshakeResult::StreamClosure);
            },
            _ => anyhow::bail!("unexpected api response: spurious response before handshake"),
          }
        },
        UpstreamEvent::Shutdown => return Ok(HandshakeResult::ShuttingDown),
        UpstreamEvent::StreamClosed => return Ok(HandshakeResult::StreamClosure),
      }
    }
  }

  /// Constructs a new `ExponentialBackoff` based on the current runtime values.
  fn backoff_policy(&mut self) -> ExponentialBackoff<SystemClock> {
    ExponentialBackoffBuilder::<SystemClock>::new()
      .with_initial_interval(Duration::from_millis(
        self.initial_backoff_interval.read_mark_update().into(),
      ))
      .with_max_interval(Duration::from_millis(
        self.max_backoff_interval.read_mark_update().into(),
      ))
      .with_max_elapsed_time(None)
      .build()
  }
}
