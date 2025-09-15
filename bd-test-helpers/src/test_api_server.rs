// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod log_upload;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::routing::post;
use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use bd_grpc::finalize_response_compression;
use bd_grpc_codec::{Compression, OptimizeFor};
use bd_proto::protos::client::api::api_request::Request_type;
use bd_proto::protos::client::api::api_response::Response_type;
use bd_proto::protos::client::api::configuration_update::{StateOfTheWorld, Update_type};
use bd_proto::protos::client::api::handshake_response::StreamSettings;
use bd_proto::protos::client::api::log_upload_intent_response::{Decision, UploadImmediately};
use bd_proto::protos::client::api::{
  ApiRequest,
  ApiResponse,
  ConfigurationUpdate,
  ConfigurationUpdateAck,
  FlushBuffers,
  HandshakeResponse,
  LogUploadIntentRequest,
  LogUploadIntentResponse,
  LogUploadRequest,
  LogUploadResponse,
  PongResponse,
  RuntimeUpdate,
  StatsUploadRequest,
  StatsUploadResponse,
  UploadArtifactIntentRequest,
  UploadArtifactIntentResponse,
  UploadArtifactRequest,
  UploadArtifactResponse,
  upload_artifact_intent_response,
};
use bd_proto::protos::logging::payload::data::Data_type;
use bd_time::{TimeDurationExt, ToProtoDuration};
use http_body_util::StreamBody;
use log_upload::LogUpload;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use time::Duration;
use time::ext::{NumericalDuration, NumericalStdDuration};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::{broadcast, mpsc};
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;

//
// StreamAccounting
//

#[derive(Debug)]
struct HandshakeAccounting {
  metadata: HashMap<String, String>,
  sleep_mode: bool,
}
#[derive(Debug)]
struct StreamAccounting {
  api_key: Option<String>,
  handshake_received: Option<HandshakeAccounting>,
  stream_closed: bool,
}

impl StreamAccounting {
  const fn new(api_key: Option<String>) -> Self {
    Self {
      handshake_received: None,
      stream_closed: false,
      api_key,
    }
  }
}

//
// ObservedEvent
//

#[derive(Debug)]
enum ObservedEvent {
  StreamCreation(i32),
  StreamEvent(i32, StreamEvent),
}

//
// ServiceState
//

struct ServiceState {
  ping_interval: Option<Duration>,
  stream_id_generator: Arc<AtomicI32>,

  event_tx: Sender<ObservedEvent>,
  per_stream_action_txs: Arc<Mutex<HashMap<i32, Arc<Sender<StreamAction>>>>>,
  log_upload_tx: Sender<LogUploadRequest>,
  log_intent_tx: Sender<LogUploadIntentRequest>,
  artifact_upload_tx: Sender<UploadArtifactRequest>,
  artifact_intent_tx: Sender<UploadArtifactIntentRequest>,
  stats_upload_tx: Sender<StatsUploadRequest>,
  configuration_ack_tx: Sender<(i32, ConfigurationUpdateAck)>,
  runtime_ack_tx: Sender<(i32, ConfigurationUpdateAck)>,

  shutdown_tx: broadcast::Sender<()>,
}

//
// RequestProcessor
//

// Helper scoped to a single stream for processing requests.
struct RequestProcessor {
  stream_id: i32,
  stream_state: Arc<ServiceState>,
}

impl RequestProcessor {
  // Process a single request, possibly responding with a response.
  async fn process_request(&self, request: &ApiRequest) -> Option<ApiResponse> {
    match &request.request_type {
      Some(Request_type::Handshake(h)) => {
        log::debug!("[S{}] received handshake", self.stream_id);

        let attributes_as_strings = h
          .static_device_metadata
          .iter()
          .map(|(key, value)| {
            let string_value = match &value.data_type {
              Some(Data_type::StringData(s)) => s.to_string(),
              _ => String::new(),
            };
            (key.to_string(), string_value)
          })
          .collect();

        self
          .stream_state
          .event_tx
          .send(ObservedEvent::StreamEvent(
            self.stream_id,
            StreamEvent::Handshake {
              metadata: attributes_as_strings,
              sleep_mode: h.sleep_mode,
            },
          ))
          .await
          .expect("event channel should not fail");

        log::debug!(
          "[S{}] sending handshake response with {:?} ping interval",
          self.stream_id,
          self.stream_state.ping_interval
        );

        let stream_settings = self
          .stream_state
          .ping_interval
          .map(|d| StreamSettings {
            ping_interval: d.into_proto(),
            ..Default::default()
          })
          .into();

        Some(ApiResponse {
          response_type: Some(
            bd_proto::protos::client::api::api_response::Response_type::Handshake(
              HandshakeResponse {
                stream_settings,
                ..Default::default()
              },
            ),
          ),
          ..Default::default()
        })
      },
      Some(Request_type::Ping(_)) => {
        log::debug!("[S{}] received ping", self.stream_id);
        Some(ApiResponse {
          response_type: Some(Response_type::Pong(PongResponse::default())),
          ..Default::default()
        })
      },
      Some(Request_type::ConfigurationUpdateAck(ack)) => {
        log::debug!("[S{}] received configuration ack {ack:?}", self.stream_id);

        self
          .stream_state
          .configuration_ack_tx
          .send((self.stream_id, ack.clone()))
          .await
          .unwrap();

        None
      },
      Some(Request_type::ArtifactUpload(upload)) => {
        log::debug!("[S{}] received artifact upload", self.stream_id);

        self
          .stream_state
          .artifact_upload_tx
          .send(upload.clone())
          .await
          .unwrap();

        Some(ApiResponse {
          response_type: Some(Response_type::ArtifactUpload(UploadArtifactResponse {
            upload_uuid: upload.upload_uuid.clone(),
            error: String::new(),
            ..Default::default()
          })),
          ..Default::default()
        })
      },
      Some(Request_type::ArtifactIntent(intent)) => {
        log::debug!("[S{}] received artifact intent {intent:?}", self.stream_id);

        self
          .stream_state
          .artifact_intent_tx
          .send(intent.clone())
          .await
          .unwrap();

        Some(ApiResponse {
          response_type: Some(Response_type::ArtifactIntent(
            UploadArtifactIntentResponse {
              intent_uuid: intent.intent_uuid.clone(),
              decision: Some(
                upload_artifact_intent_response::Decision::UploadImmediately(
                  upload_artifact_intent_response::UploadImmediately::default(),
                ),
              ),
              ..Default::default()
            },
          )),
          ..Default::default()
        })
      },
      Some(Request_type::RuntimeUpdateAck(ack)) => {
        log::debug!("[S{}] received runtime ack {ack:?}", self.stream_id);
        self
          .stream_state
          .runtime_ack_tx
          .send((self.stream_id, ack.clone()))
          .await
          .unwrap();

        None
      },
      Some(Request_type::LogUploadIntent(intent)) => {
        log::debug!(
          "[S{}] received log upload intent {intent:?}",
          self.stream_id
        );

        self
          .stream_state
          .log_intent_tx
          .send(intent.clone())
          .await
          .unwrap();

        Some(ApiResponse {
          response_type: Some(Response_type::LogUploadIntent(LogUploadIntentResponse {
            intent_uuid: intent.intent_uuid.clone(),
            decision: Some(Decision::UploadImmediately(UploadImmediately::default())),
            ..Default::default()
          })),
          ..Default::default()
        })
      },
      Some(Request_type::LogUpload(log_upload)) => {
        log::debug!(
          "[S{}] received log upload {}",
          self.stream_id,
          log_upload.upload_uuid
        );
        let upload_uuid = log_upload.upload_uuid.clone();

        let _ignored = self
          .stream_state
          .log_upload_tx
          .send(log_upload.clone())
          .await;
        if log_upload.ackless {
          return None;
        }
        Some(ApiResponse {
          response_type: Some(Response_type::LogUpload(LogUploadResponse {
            upload_uuid,
            rate_limited: None.into(),
            error: String::new(),
            logs_dropped: 0,
            ..Default::default()
          })),
          ..Default::default()
        })
      },
      Some(Request_type::StatsUpload(stat_upload)) => {
        let upload_uuid = stat_upload.upload_uuid.clone();
        let _ignored = self
          .stream_state
          .stats_upload_tx
          .send(stat_upload.clone())
          .await;
        Some(ApiResponse {
          response_type: Some(Response_type::StatsUpload(StatsUploadResponse {
            upload_uuid,
            error: String::new(),
            metrics_dropped: 0,
            ..Default::default()
          })),
          ..Default::default()
        })
      },
      r => panic!("received unknown reqest type: {r:?}"),
    }
  }
}

async fn mux(
  State(stream_state): State<Arc<ServiceState>>,
  request: Request,
) -> axum::response::Response {
  let mut shutdown_rx = stream_state.shutdown_tx.subscribe();

  let (tx, rx) = mpsc::channel(1);
  let (request_parts, request_body) = request.into_parts();

  let api_key = request_parts
    .headers
    .get("x-bitdrift-api-key")
    .cloned()
    .map(|m| m.to_str().unwrap().to_string());

  let compression = finalize_response_compression(
    Some(Compression::StatelessZlib { level: 3 }),
    &request_parts.headers,
  );
  let mut api = bd_grpc::StreamingApi::new(
    tx,
    request_parts.headers,
    request_body,
    true,
    compression,
    OptimizeFor::Memory,
    None,
  );

  // Allocate a new id for the new stream + notify consumers about the creation.
  let stream_id = stream_state
    .stream_id_generator
    .fetch_add(1, Ordering::Relaxed);

  log::debug!("[S{stream_id}], initialized new stream");

  stream_state
    .event_tx
    .send(ObservedEvent::StreamCreation(stream_id))
    .await
    .unwrap();

  stream_state
    .event_tx
    .send(ObservedEvent::StreamEvent(
      stream_id,
      StreamEvent::Created(api_key),
    ))
    .await
    .unwrap();

  let (per_stream_action_tx, mut per_stream_action_rx) = channel(1);
  {
    let mut l = stream_state.per_stream_action_txs.lock().unwrap();
    l.insert(stream_id, Arc::new(per_stream_action_tx));
  }

  tokio::spawn(async move {
    let request_processor = RequestProcessor {
      stream_id,
      stream_state: stream_state.clone(),
    };
    loop {
      tokio::select! {
        Some(stream_action) = per_stream_action_rx.recv() => {
          log::debug!("[S{stream_id}] received request to perform stream action {stream_action:?}");

          let response = match stream_action {
            StreamAction::FlushBuffers(buffers) =>
              ApiResponse {
                  response_type: Some(Response_type::FlushBuffers(FlushBuffers {
                      buffer_id_list: buffers,
                      ..Default::default()
                  })),
                  ..Default::default()
              },
            StreamAction::SendRuntime(runtime) =>
              ApiResponse {
                response_type: Some(Response_type::RuntimeUpdate(runtime)),
                ..Default::default()
              },
            StreamAction::SendConfiguration(configuration) =>
              ApiResponse {
                response_type: Some(Response_type::ConfigurationUpdate(configuration)),
                ..Default::default()
              },
              StreamAction::CloseStream => {
                return;
              }
          };
          api.send(response).await.unwrap();
        },
        requests = api.next() => {
          match requests {
            Ok(None) => {
              log::debug!("[S{stream_id}] stream closed gracefully");
              break;
            }
            Ok(rs) => {
              for request in rs.iter().flatten() {
                if let Some(response) = request_processor.process_request(request).await {
                  api.send(response).await.unwrap();
                }
              }
            },
            Err(e) => { log::debug!("[S{stream_id}] closed with error {e}"); break; }
          }
        },
        _ = shutdown_rx.recv() => {
          log::debug!("[S{stream_id}] closing stream due to server shutdown");
          break;
        },
      }
    }

    stream_state
      .event_tx
      .send(ObservedEvent::StreamEvent(stream_id, StreamEvent::Closed))
      .await
      .expect("event channel should not fail");
  });

  bd_grpc::new_grpc_response(
    Body::new(StreamBody::new(ReceiverStream::new(rx))),
    compression,
    None,
  )
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum StreamEvent {
  // Stream creation, with an optional required API key check.
  Created(Option<String>),
  // Handshake event, with an optional required set of client attributes that must be present on
  // the handshake request, as well as the indicated sleep mode.
  Handshake {
    metadata: HashMap<String, String>,
    sleep_mode: bool,
  },
  Closed,
}

//
// ExpectedStreamEvent
//

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ExpectedStreamEvent {
  Created(Option<String>),
  Handshake {
    matcher: Option<HandshakeMatcher>,
    sleep_mode: bool,
  },
  Closed,
}

//
// HandshakeMatcher
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandshakeMatcher {
  pub attributes: Option<HashMap<String, String>>,
  pub attribute_keys_to_ignore: Option<Vec<String>>,
}

impl HandshakeMatcher {
  fn matches(&self, attributes: &HashMap<String, String>) -> bool {
    let mut attributes = attributes.clone();

    if let Some(attribute_keys_to_ignore) = &self.attribute_keys_to_ignore {
      for attribute_key_to_ignore in attribute_keys_to_ignore {
        if attributes.remove(attribute_key_to_ignore).is_none() {
          return false;
        }
      }
    }

    self
      .attributes
      .as_ref()
      .is_none_or(|attributes_match| attributes_match == &attributes)
  }
}

pub trait EventCallback<T: Send + Sync>: Send + Sync + std::fmt::Debug {
  fn triggered(&mut self, value: T);
  fn timeout(&mut self);
}

#[derive(Debug)]
enum Response<T> {
  Value(T),
  Timeout,
}

#[derive(Debug)]
struct OneshotCallback<T> {
  tx: Option<tokio::sync::oneshot::Sender<Response<T>>>,
}

impl<T: Send + Sync + std::fmt::Debug> EventCallback<T> for OneshotCallback<T> {
  fn triggered(&mut self, value: T) {
    self
      .tx
      .take()
      .unwrap()
      .send(Response::Value(value))
      .unwrap();
  }

  fn timeout(&mut self) {
    self.tx.take().unwrap().send(Response::Timeout).unwrap();
  }
}

#[derive(Debug)]
pub enum Event {
  StreamCreation(Box<dyn EventCallback<i32>>),
  StreamEvent(i32, ExpectedStreamEvent, Box<dyn EventCallback<()>>),
}

#[derive(Debug)]
struct TimedEventQuery {
  deadline: Instant,
  event: Event,
}

// Sort by deadline.
fn cmp_timed_event_query(lhs: &TimedEventQuery, rhs: &TimedEventQuery) -> std::cmp::Ordering {
  lhs.deadline.cmp(&rhs.deadline)
}

#[must_use]
pub fn default_configuration_update() -> ConfigurationUpdate {
  ConfigurationUpdate {
    version_nonce: String::new(),
    update_type: Some(Update_type::StateOfTheWorld(StateOfTheWorld::default())),
    ..Default::default()
  }
}

#[must_use]
pub fn start_server(tls: bool, ping_interval: Option<Duration>) -> Box<ServerHandle> {
  // Make sure there is some buffer here so we don't block stream creation on this channel.
  let (timed_event_wait_tx, timed_event_wait_rx) = channel(1);
  let (shutdown_tx, _) = broadcast::channel(1);
  let (event_tx, event_rx) = channel(1);
  let (log_upload_tx, log_upload_rx) = channel(256);
  let (log_intent_tx, log_intent_rx) = channel(256);
  let (artifact_upload_tx, artifact_upload_rx) = channel(256);
  let (artifact_intent_tx, artifact_intent_rx) = channel(256);
  let (stats_upload_tx, stats_upload_rx) = channel(256);
  let (configuration_ack_tx, configuration_ack_rx) = channel(1);
  let (runtime_ack_tx, runtime_ack_rx) = channel(256);

  let (stream_action_tx, stream_action_rx) = channel(1);

  let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
  let local_addr = listener.local_addr().unwrap();

  log::debug!("binding test server to {local_addr}");

  std::thread::spawn(move || {
    tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .unwrap()
      .block_on(async {
        let per_stream_action_txs = Arc::new(Mutex::new(HashMap::new()));

        let service_state = Arc::new(ServiceState {
          ping_interval,
          stream_id_generator: Arc::new(AtomicI32::new(0)),
          event_tx,
          shutdown_tx,
          per_stream_action_txs: per_stream_action_txs.clone(),
          log_upload_tx,
          log_intent_tx,
          artifact_upload_tx,
          artifact_intent_tx,
          stats_upload_tx,
          configuration_ack_tx,
          runtime_ack_tx,
        });

        // Forward actions sent over the per stream action channel to the channel associated with
        // the specified stream id.
        tokio::spawn(dispatch_per_stream_actions(
          stream_action_rx,
          per_stream_action_txs,
        ));

        // Spawn one routing which handles responding to queries against events happening on API
        // streams.
        tokio::spawn(
          TestEventProcessor::new(timed_event_wait_rx, event_rx).process_event_queries(),
        );

        serve(listener, tls, service_state).await;
      });

    log::debug!("stopping test server");
  });

  Box::new(ServerHandle {
    timed_event_wait_tx,
    stream_action_tx,
    log_upload_rx,
    log_intent_rx,
    artifact_upload_rx,
    artifact_intent_rx,
    stats_upload_rx,
    configuration_ack_rx,
    runtime_ack_rx,
    port: local_addr.port(),
  })
}

fn router(service_state: Arc<ServiceState>) -> axum::Router {
  axum::Router::new()
    .route(
      "/bitdrift_public.protobuf.client.v1.ApiService/Mux",
      post(mux),
    )
    .with_state(service_state)
}

async fn dispatch_per_stream_actions(
  mut stream_action_rx: Receiver<(i32, StreamAction)>,
  per_stream_action_txs: Arc<Mutex<HashMap<i32, Arc<Sender<StreamAction>>>>>,
) {
  while let Some((stream_id, config)) = stream_action_rx.recv().await {
    let ch = {
      let l = per_stream_action_txs.lock().unwrap();

      l.get(&stream_id).unwrap().clone()
    };
    let _ignored = ch.send(config).await;
  }
}

async fn serve(listener: std::net::TcpListener, tls: bool, service_state: Arc<ServiceState>) {
  if tls {
    let server_handle = Handle::new();
    let handle_clone = server_handle.clone();
    let state_clone = service_state.clone();
    tokio::spawn(async move {
      let mut rx = state_clone.shutdown_tx.subscribe();

      let _ignored = rx.recv().await;

      handle_clone.shutdown();
    });

    // Embed the strings into the source file instead of reading from file - this works better
    // on iOS where the files aren't easily available at runtime.
    let cert = include_str!("certs/Bitdrift.crt");
    let key = include_str!("certs/Bitdrift.key");
    axum_server::from_tcp_rustls(
      listener,
      RustlsConfig::from_pem(cert.bytes().collect(), key.bytes().collect())
        .await
        .unwrap(),
    )
    .handle(server_handle)
    .serve(router(service_state.clone()).into_make_service())
    .await
    .unwrap();
  } else {
    // TODO(mattklein123): axum_server takes a std TcpListener while axum now takes a tokio
    // TcpListener. This is not trivial to fix as the listener is currently created outside of
    // async context above. This took me a long time to debug and I would like to clean this up
    // but leaving that for a future change.
    listener.set_nonblocking(true).unwrap();
    let _ignored = axum::serve(
      TcpListener::from_std(listener).unwrap(),
      router(service_state.clone()).into_make_service(),
    )
    .with_graceful_shutdown(async move {
      let mut rx = service_state.shutdown_tx.subscribe();
      let _ignored = rx.recv().await;
    })
    .await;
  }
}

#[derive(Debug)]
pub enum StreamAction {
  /// Sends a runtime configuration update over the stream.
  SendRuntime(RuntimeUpdate),

  /// Sends a configuration update over the stream.
  SendConfiguration(ConfigurationUpdate),

  /// Sends a `FlushBuffers` response over the stream with the specified buffers.
  FlushBuffers(Vec<String>),

  /// Closes the stream.
  CloseStream,
}

pub struct ServerHandle {
  // Used to coordinate queries against streams.
  timed_event_wait_tx: Sender<TimedEventQuery>,

  // Used to instruct a specific stream to perform an action.
  stream_action_tx: Sender<(i32, StreamAction)>,

  log_upload_rx: Receiver<LogUploadRequest>,
  artifact_upload_rx: Receiver<UploadArtifactRequest>,
  artifact_intent_rx: Receiver<UploadArtifactIntentRequest>,
  log_intent_rx: Receiver<LogUploadIntentRequest>,
  stats_upload_rx: Receiver<StatsUploadRequest>,

  configuration_ack_rx: Receiver<(i32, ConfigurationUpdateAck)>,
  runtime_ack_rx: Receiver<(i32, ConfigurationUpdateAck)>,

  // The port this server is bound to.
  pub port: u16,
}

impl ServerHandle {
  /// Waits for a new stream to be established, returning the stream id.
  #[must_use]
  pub async fn next_stream(&self) -> Option<i32> {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();

    self
      .timed_event_wait_tx
      .send(TimedEventQuery {
        deadline: Instant::now() + 5.std_seconds(),
        event: Event::StreamCreation(Box::new(OneshotCallback {
          tx: Some(response_tx),
        })),
      })
      .await
      .unwrap();

    match response_rx.await.unwrap() {
      Response::Value(stream_id) => Some(stream_id),
      Response::Timeout => None,
    }
  }

  // Blocks for a new stream to be established, returning the stream id.
  #[must_use]
  pub fn blocking_next_stream(&self) -> Option<StreamHandle> {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();

    self
      .timed_event_wait_tx
      .blocking_send(TimedEventQuery {
        deadline: Instant::now() + 5.std_seconds(),
        event: Event::StreamCreation(Box::new(OneshotCallback {
          tx: Some(response_tx),
        })),
      })
      .unwrap();

    let stream_id = match response_rx.blocking_recv().unwrap() {
      Response::Value(stream_id) => Some(stream_id),
      Response::Timeout => None,
    }?;

    Some(StreamHandle {
      stream_id,
      timed_event_wait_tx: self.timed_event_wait_tx.clone(),
      stream_action_tx: self.stream_action_tx.clone(),
    })
  }

  /// Enqueues an event that is expected to be observed by the test server. The provided
  /// callback will be invoked when the event is observed, or will be invoked with a timeout if the
  /// provided timeout elapses.
  pub fn enqueue_expected_event(&self, event: Event, timeout: Duration) {
    self
      .timed_event_wait_tx
      .blocking_send(TimedEventQuery {
        deadline: timeout.add_tokio_now(),
        event,
      })
      .unwrap();
  }

  /// Waits for a new stream to be established and initialized, returning the stream id. Prefer
  /// using this function over `next_stream` when the stream is expected to be initialized
  /// immediately.
  #[must_use]
  pub async fn next_initialized_stream(&self, expect_sleep_mode: bool) -> Option<StreamHandle> {
    let stream = StreamHandle {
      stream_id: self.next_stream().await?,
      timed_event_wait_tx: self.timed_event_wait_tx.clone(),
      stream_action_tx: self.stream_action_tx.clone(),
    };

    assert!(
      stream
        .expect_event(
          ExpectedStreamEvent::Handshake {
            matcher: None,
            sleep_mode: expect_sleep_mode
          },
          1.seconds()
        )
        .await
    );

    Some(stream)
  }
  /// Blocks waiting for request to be received over the provided receiver. Times out ofter the
  /// provided duration.
  fn blocking_next_request_with_timeout<T>(receiver: &mut Receiver<T>) -> Option<T> {
    let deadline = Instant::now() + 5.std_seconds();
    while Instant::now() < deadline {
      if let Ok(upload) = receiver.try_recv() {
        return Some(upload);
      }

      std::thread::sleep(10.std_milliseconds());
    }

    log::debug!("blocking_next_request_with_timeout timed out");
    None
  }

  /// Waits for a request to be received over the provided receiver. Times out ofter the
  /// provided duration.
  async fn next_request_with_timeout<T>(receiver: &mut Receiver<T>) -> Option<T> {
    receiver.recv().await
  }

  /// Awaits a request to be received over the provided receiver.
  async fn next_request<T>(receiver: &mut Receiver<T>) -> Option<T> {
    receiver.recv().await
  }

  pub async fn next_artifact_upload(&mut self) -> Option<UploadArtifactRequest> {
    Self::next_request(&mut self.artifact_upload_rx).await
  }

  pub fn blocking_next_artifact_upload(&mut self) -> Option<UploadArtifactRequest> {
    Self::blocking_next_request_with_timeout(&mut self.artifact_upload_rx)
  }

  pub fn next_log_intent(&mut self) -> Option<LogUploadIntentRequest> {
    Self::blocking_next_request_with_timeout(&mut self.log_intent_rx)
  }

  pub async fn next_artifact_intent(&mut self) -> Option<UploadArtifactIntentRequest> {
    Self::next_request(&mut self.artifact_intent_rx).await
  }

  pub fn blocking_next_artifact_intent(&mut self) -> Option<UploadArtifactIntentRequest> {
    Self::blocking_next_request_with_timeout(&mut self.artifact_intent_rx)
  }

  pub async fn next_log_upload(&mut self) -> Option<LogUploadRequest> {
    Self::next_request_with_timeout(&mut self.log_upload_rx).await
  }

  pub fn blocking_next_log_upload(&mut self) -> Option<LogUpload> {
    Self::blocking_next_request_with_timeout(&mut self.log_upload_rx).map(LogUpload)
  }

  pub fn next_stat_upload(&mut self) -> Option<StatsUploadRequest> {
    Self::blocking_next_request_with_timeout(&mut self.stats_upload_rx)
  }

  /// Blocks for the next configuration update ack to be received by the server.
  pub fn blocking_next_configuration_ack(&mut self) -> (i32, ConfigurationUpdateAck) {
    self.configuration_ack_rx.blocking_recv().unwrap()
  }

  /// Waits for the next configuration update ack to be received by the server.
  pub async fn next_configuration_ack(&mut self, stream: &StreamHandle) {
    let (id, ack) = self.configuration_ack_rx.recv().await.unwrap();

    assert_eq!(id, stream.stream_id);
    assert!(ack.nack.is_none());
  }

  /// Blocks for the next runtime ack to be received by the server.
  pub fn blocking_next_runtime_ack(&mut self) -> (i32, ConfigurationUpdateAck) {
    self.runtime_ack_rx.blocking_recv().unwrap()
  }

  /// Waits for the next runtime ack to be received by the server.
  pub async fn next_runtime_ack(&mut self, stream: &StreamHandle) {
    let (id, ack) = self.runtime_ack_rx.recv().await.unwrap();

    assert_eq!(id, stream.stream_id);
    assert!(ack.nack.is_none());
  }
}

pub struct StreamHandle {
  stream_id: i32,
  timed_event_wait_tx: Sender<TimedEventQuery>,
  stream_action_tx: Sender<(i32, StreamAction)>,
}

impl StreamHandle {
  #[must_use]
  pub fn from_stream_id(stream_id: i32, server: &ServerHandle) -> Self {
    Self {
      stream_id,
      timed_event_wait_tx: Sender::clone(&server.timed_event_wait_tx),
      stream_action_tx: Sender::clone(&server.stream_action_tx),
    }
  }

  #[must_use]
  pub const fn id(&self) -> i32 {
    self.stream_id
  }

  #[must_use]
  pub fn await_event_with_timeout(&self, event: ExpectedStreamEvent, timeout: Duration) -> bool {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    self
      .timed_event_wait_tx
      .blocking_send(TimedEventQuery {
        deadline: timeout.add_tokio_now(),
        event: Event::StreamEvent(
          self.stream_id,
          event,
          Box::new(OneshotCallback {
            tx: Some(response_tx),
          }),
        ),
      })
      .unwrap();

    std::matches!(response_rx.blocking_recv().unwrap(), Response::Value(()))
  }

  #[must_use]
  pub async fn expect_event(&self, event: ExpectedStreamEvent, timeout: Duration) -> bool {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    self
      .timed_event_wait_tx
      .send(TimedEventQuery {
        deadline: timeout.add_tokio_now(),
        event: Event::StreamEvent(
          self.stream_id,
          event,
          Box::new(OneshotCallback {
            tx: Some(response_tx),
          }),
        ),
      })
      .await
      .unwrap();

    std::matches!(response_rx.await.unwrap(), Response::Value(()))
  }

  pub fn blocking_stream_action(&self, action: StreamAction) {
    log::debug!(
      "sending stream action {:?} to stream {}",
      action,
      self.stream_id,
    );

    self
      .stream_action_tx
      .blocking_send((self.stream_id, action))
      .unwrap();
  }

  pub async fn stream_action(&self, action: StreamAction) {
    log::debug!(
      "sending stream action {:?} to stream {}",
      action,
      self.stream_id
    );

    self
      .stream_action_tx
      .send((self.stream_id, action))
      .await
      .unwrap();
  }
}

//
// TestEventProcessor
//

// Manages "wait until" semantics for a number of stream events.
struct TestEventProcessor {
  timed_event_wait_rx: Receiver<TimedEventQuery>,
  event_rx: Receiver<ObservedEvent>,
  pending_new_streams: Vec<i32>,
  streams: HashMap<i32, StreamAccounting>,
  events: Vec<TimedEventQuery>,
}

impl TestEventProcessor {
  fn new(
    timed_event_wait_rx: Receiver<TimedEventQuery>,
    event_rx: Receiver<ObservedEvent>,
  ) -> Self {
    Self {
      timed_event_wait_rx,
      event_rx,
      streams: HashMap::new(),
      pending_new_streams: Vec::new(),
      events: Vec::new(),
    }
  }

  async fn process_event_queries(mut self) {
    // We want to support "wait until" semantics, but we also want to be able to immediately respond
    // if the desired state change has already happened. To accomplish this we maintain some data
    // on each stream that we consult first, then if it's clear that we haven't met the desired
    // condition yet we keep track of the event and check it every time we receive a new event.

    loop {
      let mut maybe_timeout = self
        .events
        .iter()
        .map(|t| t.deadline)
        .min()
        .map(tokio::time::sleep_until);

      tokio::select! {
        () = async { maybe_timeout.take().unwrap().await }, if maybe_timeout.is_some()
          => self.handle_event_timeout(),
      Some(event_query) = self.timed_event_wait_rx.recv() => self.process_event_query(event_query),
      Some(observed_event) = self.event_rx.recv() => self.process_event(observed_event),
      else => break,
      }
    }
  }

  // Evicts all pending events whose deadline has exceeded, signaling a failure.
  fn handle_event_timeout(&mut self) {
    // Sort the events so that the earlieest deadline is always first. We could do this on insertion
    // but it doesn't really matter in test.
    self.events.sort_by(cmp_timed_event_query);
    while let Some(TimedEventQuery { deadline, .. }) = self.events.first() {
      if Instant::now() >= *deadline {
        let event_query = self.events.remove(0);

        log::debug!("timed event query {event_query:?} timed out");

        match event_query.event {
          Event::StreamCreation(mut callback) => callback.timeout(),
          Event::StreamEvent(_, _, mut callback) => callback.timeout(),
        }
      } else {
        break;
      }
    }
  }

  // Processes an incoming event query, either satisfying it immediately or keeping track of the
  // event pending new stream events.
  fn process_event_query(&mut self, event_query: TimedEventQuery) {
    log::debug!(
      "processing inbound event query {:?}",
      (&event_query.event, event_query.deadline)
    );

    if let Some(unsatisfied_event_query) = self.maybe_satisfy_event(event_query) {
      self.events.push(unsatisfied_event_query);
    }
  }

  // Processes a single stream event, updating the stream accounting and triggering any pending
  // queries waiting for this event.
  fn process_event(&mut self, event: ObservedEvent) {
    match event {
      ObservedEvent::StreamCreation(stream_id) => {
        log::debug!("[S{stream_id}] observing new stream");
        self.pending_new_streams.push(stream_id);
      },
      ObservedEvent::StreamEvent(stream_id, event) => {
        log::debug!("[S{stream_id}] observing event {event:?}");

        match event {
          StreamEvent::Created(maybe_key) => {
            self
              .streams
              .insert(stream_id, StreamAccounting::new(maybe_key));
          },
          StreamEvent::Handshake {
            metadata,
            sleep_mode,
          } => {
            self.streams.get_mut(&stream_id).unwrap().handshake_received =
              Some(HandshakeAccounting {
                metadata,
                sleep_mode,
              });
          },
          StreamEvent::Closed => {
            self.streams.get_mut(&stream_id).unwrap().stream_closed = true;
          },
        }
      },
    }

    // Check all pending events against the new event.
    self.events = {
      let mut new_events = Vec::new();

      while let Some(event_query) = self.events.pop() {
        if let Some(unsatisfied_event_query) = self.maybe_satisfy_event(event_query) {
          new_events.push(unsatisfied_event_query);
        }
      }

      new_events
    };
  }

  // Checks to see if the provided event has been satisfied, invoking the response oneshot if so. If
  // the event is not satisfied, returns ownership of the oneshot sender.
  fn maybe_satisfy_event(&mut self, mut event_query: TimedEventQuery) -> Option<TimedEventQuery> {
    match event_query.event {
      // If the event is asking for the next stream, return if we have a pending stream tracked.
      Event::StreamCreation(ref mut callback) => {
        if !self.pending_new_streams.is_empty() {
          let stream_id = self.pending_new_streams.remove(0);
          log::debug!("[S{stream_id}] satisfying query for stream creation");

          callback.triggered(stream_id);
          return None;
        }

        Some(event_query)
      },
      // If the event is asking for a specific event on a given stream, inspect our per-stream
      // state to determine if we've reached this state already.
      Event::StreamEvent(stream_id, ref event, ref mut callback) => {
        // First check if we're already at the desired state.
        let event_satisfied = self
          .streams
          .get(&stream_id)
          .is_some_and(|state| match event {
            ExpectedStreamEvent::Created(key) => state.api_key == *key,
            ExpectedStreamEvent::Closed => state.stream_closed,
            ExpectedStreamEvent::Handshake {
              matcher,
              sleep_mode,
            } => matcher.as_ref().map_or_else(
              || {
                state
                  .handshake_received
                  .as_ref()
                  .is_some_and(|h| h.sleep_mode == *sleep_mode)
              },
              |attribute_match| {
                state.handshake_received.as_ref().is_some_and(|h| {
                  attribute_match.matches(&h.metadata) && h.sleep_mode == *sleep_mode
                })
              },
            ),
          });

        if event_satisfied {
          callback.triggered(());
          log::debug!("[S{stream_id}] satisfying stream event query {event:?}");
          None
        } else {
          Some(event_query)
        }
      },
    }
  }
}
