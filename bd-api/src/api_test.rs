// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{Api, PlatformNetworkManager, PlatformNetworkStream};
use crate::api::{DISCONNECTED_OFFLINE_GRACE_PERIOD, StreamEvent};
use crate::upload::Tracked;
use crate::{DataUpload, SimpleNetworkQualityProvider};
use anyhow::anyhow;
use assert_matches::assert_matches;
use bd_client_common::{
  HANDSHAKE_FLAG_CONFIG_UP_TO_DATE,
  HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
  MockClientConfigurationUpdate,
};
use bd_client_stats_store::Collector;
use bd_client_stats_store::test::StatsHelper;
use bd_grpc_codec::code::Code;
use bd_grpc_codec::{Decompression, Encoder, OptimizeFor};
use bd_internal_logging::{LogFields, LogLevel, LogType};
use bd_key_value::Store;
use bd_metadata::{Metadata, Platform};
use bd_network_quality::{NetworkQuality, NetworkQualityResolver as _};
use bd_proto::protos::client::api::api_request::Request_type;
use bd_proto::protos::client::api::api_response::Response_type;
use bd_proto::protos::client::api::handshake_response::StreamSettings;
use bd_proto::protos::client::api::{
  ApiRequest,
  ApiResponse,
  ErrorShutdown,
  HandshakeRequest,
  HandshakeResponse,
  RateLimited,
  RuntimeUpdate,
  StatsUploadRequest,
};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_stats_common::labels;
use bd_test_helpers::make_mut;
use bd_test_helpers::session::in_memory_store;
use bd_time::{OffsetDateTimeExt, TimeDurationExt, TimeProvider, ToProtoDuration};
use mockall::predicate::eq;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use time::ext::{NumericalDuration, NumericalStdDuration};
use time::{Duration, OffsetDateTime};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};

//
// EmptyMetadata
//

struct EmptyMetadata;

impl Metadata for EmptyMetadata {
  fn sdk_version(&self) -> &'static str {
    "test"
  }

  fn platform(&self) -> &Platform {
    &Platform::Apple
  }

  fn os(&self) -> String {
    "ios".to_string()
  }

  fn device_id(&self) -> String {
    String::new()
  }

  fn collect_inner(&self) -> HashMap<String, String> {
    HashMap::new()
  }
}

//
// PlatformNetwork
//

#[derive(Clone)]
#[allow(clippy::struct_field_names)]
struct PlatformNetwork {
  start_stream_tx: Sender<()>,
  send_data_tx: Sender<Vec<u8>>,

  current_stream_tx: Arc<Mutex<Option<Sender<StreamEvent>>>>,
}

impl PlatformNetwork {
  const fn new(
    start_stream_tx: Sender<()>,
    send_data_tx: Sender<Vec<u8>>,
    current_stream_tx: Arc<Mutex<Option<Sender<StreamEvent>>>>,
  ) -> Self {
    Self {
      start_stream_tx,
      send_data_tx,
      current_stream_tx,
    }
  }
}

#[async_trait::async_trait]
impl<T> PlatformNetworkManager<T> for PlatformNetwork {
  async fn start_stream(
    &self,
    event_tx: Sender<StreamEvent>,
    _runtime: &T,
    _headers: &HashMap<&str, &str>,
  ) -> anyhow::Result<Box<dyn PlatformNetworkStream>> {
    self
      .start_stream_tx
      .send(())
      .await
      .map_err(|_| anyhow!("start stream"))?;

    *self.current_stream_tx.lock().unwrap() = Some(event_tx.clone());

    Ok(Box::new(Stream {
      _event_tx: event_tx,
      send_data_tx: self.send_data_tx.clone(),
    }))
  }
}

//
// Stream
//

struct Stream {
  _event_tx: Sender<StreamEvent>,

  send_data_tx: Sender<Vec<u8>>,
}

#[async_trait::async_trait]
impl PlatformNetworkStream for Stream {
  async fn send_data(&mut self, data: &[u8]) -> anyhow::Result<()> {
    self
      .send_data_tx
      .send(data.to_vec())
      .await
      .map_err(|_| anyhow!("start stream"))
  }
}

//
// TestLog
//

struct TestLog;

impl bd_internal_logging::Logger for TestLog {
  fn log(&self, _level: LogLevel, _log_type: LogType, _msg: &str, _fields: LogFields) {}
}

//
// Setup
//

struct Setup {
  sdk_directory: tempfile::TempDir,
  data_tx: Sender<DataUpload>,
  send_data_rx: Receiver<Vec<u8>>,
  start_stream_rx: Receiver<()>,
  collector: Collector,
  requests_decoder: bd_grpc_codec::Decoder<ApiRequest>,
  time_provider: Arc<bd_time::TestTimeProvider>,
  current_stream_tx: Arc<Mutex<Option<Sender<StreamEvent>>>>,
  api_task: Option<JoinHandle<anyhow::Result<()>>>,
  api_key: String,
  network_quality_provider: Arc<SimpleNetworkQualityProvider>,
  sleep_mode_active: watch::Sender<bool>,
  store: Arc<Store>,
  config_updater: Arc<MockClientConfigurationUpdate>,
}

impl Setup {
  async fn new() -> Self {
    Self::new_ex(Self::make_nice_mock_updater(), None, None, None).await
  }

  async fn new_ex(
    config_updater: Arc<MockClientConfigurationUpdate>,
    initial_runtime: Option<RuntimeUpdate>,
    idle_timeout_tx: Option<Sender<()>>,
    network_quality_provider: Option<SimpleNetworkQualityProvider>,
  ) -> Self {
    let sdk_directory = tempfile::TempDir::with_prefix("sdk").unwrap();

    let (start_stream_tx, start_stream_rx) = channel(1);
    let (send_data_tx, send_data_rx) = channel(1);
    let current_stream_tx = Arc::new(Mutex::new(None));
    let manager = Box::new(PlatformNetwork::new(
      start_stream_tx,
      send_data_tx,
      current_stream_tx.clone(),
    ));
    let (data_tx, data_rx) = channel(1);
    let (trigger_upload_tx, _trigger_upload_rx) = channel(1);
    let (sleep_mode_active_tx, sleep_mode_active_rx) = watch::channel(false);

    let time_provider = Arc::new(bd_time::TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH));

    let collector = Collector::default();

    let runtime_loader = ConfigLoader::new(sdk_directory.path());
    if let Some(initial_runtime) = initial_runtime {
      runtime_loader
        .try_apply_config(initial_runtime)
        .await
        .unwrap();
    }

    let api_key = "api-key-test".to_string();
    let network_quality_provider = Arc::new(network_quality_provider.unwrap_or_default());

    let store = in_memory_store();
    let mut api = Api::new(
      sdk_directory.path().to_path_buf(),
      api_key.clone(),
      manager,
      data_rx,
      trigger_upload_tx,
      Arc::new(EmptyMetadata),
      runtime_loader.clone(),
      config_updater.clone(),
      time_provider.clone(),
      network_quality_provider.clone(),
      Arc::new(TestLog {}),
      &collector.scope("api"),
      sleep_mode_active_rx,
      store.clone(),
    );
    api.data_idle_timeout_test_hook = idle_timeout_tx;

    let api_task = tokio::task::spawn(async move {
      runtime_loader.try_load_persisted_config().await;
      api.start().await
    });

    Self {
      current_stream_tx,
      sdk_directory,
      start_stream_rx,
      data_tx,
      send_data_rx,
      collector,
      time_provider,
      requests_decoder: bd_grpc_codec::Decoder::new(
        Some(Decompression::StatelessZlib),
        OptimizeFor::Memory,
      ),
      api_task: Some(api_task),
      api_key,
      network_quality_provider,
      sleep_mode_active: sleep_mode_active_tx,
      store,
      config_updater,
    }
  }

  async fn restart(&mut self) {
    log::info!("restarting api");
    let (start_stream_tx, start_stream_rx) = channel(1);
    let (send_data_tx, send_data_rx) = channel(1);
    let current_stream_tx = Arc::new(Mutex::new(None));
    let manager = Box::new(PlatformNetwork::new(
      start_stream_tx,
      send_data_tx,
      current_stream_tx.clone(),
    ));
    let (data_tx, data_rx) = channel(1);
    let (trigger_upload_tx, _trigger_upload_rx) = channel(1);

    let runtime_loader = ConfigLoader::new(self.sdk_directory.path());
    runtime_loader.try_load_persisted_config().await;
    let api = Api::new(
      self.sdk_directory.path().to_path_buf(),
      self.api_key.clone(),
      manager,
      data_rx,
      trigger_upload_tx,
      Arc::new(EmptyMetadata),
      runtime_loader.clone(),
      self.config_updater.clone(),
      self.time_provider.clone(),
      self.network_quality_provider.clone(),
      Arc::new(TestLog {}),
      &self.collector.scope("api"),
      self.sleep_mode_active.subscribe(),
      self.store.clone(),
    );

    self.api_task = Some(tokio::task::spawn(api.start()));
    self.start_stream_rx = start_stream_rx;
    self.data_tx = data_tx;
    self.send_data_rx = send_data_rx;
    self.current_stream_tx = current_stream_tx;
  }

  fn make_handshake(
    configuration_update_status: u32,
    stream_settings: Option<StreamSettings>,
    opaque_client_state_to_echo: Option<Vec<u8>>,
  ) -> ApiResponse {
    ApiResponse {
      response_type: Some(Response_type::Handshake(HandshakeResponse {
        stream_settings: stream_settings.into(),
        configuration_update_status,
        opaque_client_state_to_echo,
        ..Default::default()
      })),
      ..Default::default()
    }
  }

  async fn handshake_response(
    &self,
    configuration_update_status: u32,
    stream_settings: Option<StreamSettings>,
    opaque_client_state_to_echo: Option<Vec<u8>>,
  ) {
    self
      .send_response(Self::make_handshake(
        configuration_update_status,
        stream_settings,
        opaque_client_state_to_echo,
      ))
      .await;
  }

  fn make_error_shutdown(code: Code, message: &str, retry_after: Option<Duration>) -> ApiResponse {
    ApiResponse {
      response_type: Some(Response_type::ErrorShutdown(ErrorShutdown {
        grpc_status: code.to_int(),
        grpc_message: message.to_string(),
        rate_limited: retry_after
          .map(|d| RateLimited {
            retry_after: d.into_proto(),
            ..Default::default()
          })
          .into(),
        ..Default::default()
      })),
      ..Default::default()
    }
  }

  async fn error_shutdown(&self, code: Code, message: &str, retry_after: Option<Duration>) {
    self
      .send_response(Self::make_error_shutdown(code, message, retry_after))
      .await;
  }

  async fn send_request(&self, data: DataUpload) {
    self.data_tx.send(data).await.unwrap();
  }

  async fn send_response(&self, response: ApiResponse) {
    let tx = self
      .current_stream_tx
      .lock()
      .unwrap()
      .as_ref()
      .unwrap()
      .clone();

    let mut encoder = Encoder::new(None);
    let encoded = encoder.encode(&response).unwrap();
    tx.send(StreamEvent::Data(encoded.to_vec())).await.unwrap();
  }

  async fn send_multiple_messages(&self, responses: Vec<ApiResponse>) {
    let tx = self
      .current_stream_tx
      .lock()
      .unwrap()
      .as_ref()
      .unwrap()
      .clone();

    let mut encoder = Encoder::new(None);
    let mut encoded = Vec::new();
    for response in responses {
      let data = encoder.encode(&response).unwrap();
      encoded.extend(data);
    }

    tx.send(StreamEvent::Data(encoded)).await.unwrap();
  }

  async fn close_stream(&self) {
    let tx = self
      .current_stream_tx
      .lock()
      .unwrap()
      .take()
      .unwrap()
      .clone();

    tx.send(StreamEvent::StreamClosed("test".to_string()))
      .await
      .unwrap();
  }

  #[must_use]
  async fn next_stream(&mut self, wait: Duration) -> Option<HandshakeRequest> {
    tokio::select! {
      _ = self.start_stream_rx.recv() => {},
      () = wait.sleep() => {
        return None;
      }
    };
    let data = self.send_data_rx.recv().await.unwrap();
    let request = self.decode(&data).unwrap();
    let Some(Request_type::Handshake(request)) = request.request_type else {
      panic!("expected handshake request, got {request:?}");
    };

    Some(request)
  }

  fn decode(&mut self, data: &[u8]) -> Option<ApiRequest> {
    if let Ok(requests) = self.requests_decoder.decode_data(data) {
      assert!(
        requests.len() == 1,
        "expected 1 request, got {}",
        requests.len()
      );
      return requests.first().cloned();
    }

    None
  }

  async fn next_request(&mut self, wait: Duration) -> Option<ApiRequest> {
    let data = tokio::select! {
      data = self.send_data_rx.recv() => { data },
      () = wait.sleep() => {
        return None;
      }
    }?;

    self.decode(&data)
  }

  fn make_nice_mock_updater() -> Arc<MockClientConfigurationUpdate> {
    let mut mock_updater = Arc::new(MockClientConfigurationUpdate::new());
    make_mut(&mut mock_updater)
      .expect_fill_handshake()
      .times(..)
      .returning(|_| ());
    make_mut(&mut mock_updater)
      .expect_mark_safe()
      .times(..)
      .returning(|| ());
    make_mut(&mut mock_updater)
      .expect_try_load_persisted_config()
      .times(..)
      .returning(|| ());
    make_mut(&mut mock_updater)
      .expect_on_handshake_complete()
      .times(..)
      .returning(|_| ());

    mock_updater
  }
}

#[tokio::test(start_paused = true)]
async fn api_retry_stream() {
  let mut mock_updater = Arc::new(MockClientConfigurationUpdate::new());
  make_mut(&mut mock_updater)
    .expect_fill_handshake()
    .times(..)
    .returning(|_| ());
  make_mut(&mut mock_updater)
    .expect_try_load_persisted_config()
    .times(..)
    .returning(|| ());
  let mut setup = Setup::new_ex(
    mock_updater.clone(),
    RuntimeUpdate {
      version_nonce: "test".to_string(),
      runtime: Some(bd_test_helpers::runtime::make_proto(vec![(
        bd_runtime::runtime::api::DataIdleTimeoutInterval::path(),
        bd_test_helpers::runtime::ValueKind::Int(0),
      )]))
      .into(),
      ..Default::default()
    }
    .into(),
    None,
    None,
  )
  .await;

  // Since the backoff uses random values we have no control over, we loop multiple attempts
  // until it takes over a minute before we get a new stream. This demonstrates that the delay
  // grows past our initial delay of 500ms. We should have cached config marked safe during this
  // process.
  make_mut(&mut mock_updater)
    .expect_mark_safe()
    .once()
    .returning(|| ());
  assert_eq!(
    NetworkQuality::Unknown,
    setup.network_quality_provider.get_network_quality()
  );
  let now = Instant::now();
  while setup.next_stream(1.minutes()).await.is_some() {
    if now + DISCONNECTED_OFFLINE_GRACE_PERIOD > Instant::now() {
      assert_eq!(
        NetworkQuality::Unknown,
        setup.network_quality_provider.get_network_quality()
      );
    } else {
      assert_eq!(
        NetworkQuality::Offline,
        setup.network_quality_provider.get_network_quality()
      );
    }

    setup.close_stream().await;
  }

  // At this point we've verified that we've taken over 60s to get a new stream. Now let a stream
  // finalize the handshake and then verify that it resets the interval after being alive for 1m.

  5.minutes().advance().await;

  assert!(setup.next_stream(1.seconds()).await.is_some());

  make_mut(&mut mock_updater)
    .expect_on_handshake_complete()
    .with(eq(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
    ))
    .once()
    .returning(|_| ());
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      None,
    )
    .await;

  61.seconds().sleep().await;
  assert_eq!(
    NetworkQuality::Online,
    setup.network_quality_provider.get_network_quality()
  );
  setup.close_stream().await;

  assert!(setup.next_stream(1.seconds()).await.is_some());
  assert_eq!(
    NetworkQuality::Unknown,
    setup.network_quality_provider.get_network_quality()
  );
  setup.close_stream().await;

  // Now ramp up the backoff again to 1m, do the handshake and immediate shutdown, and verify the
  // backoff is not reset.
  let now = Instant::now();
  while setup.next_stream(1.minutes()).await.is_some() {
    if now + DISCONNECTED_OFFLINE_GRACE_PERIOD > Instant::now() {
      assert_eq!(
        NetworkQuality::Unknown,
        setup.network_quality_provider.get_network_quality()
      );
    } else {
      assert_eq!(
        NetworkQuality::Offline,
        setup.network_quality_provider.get_network_quality()
      );
    }

    setup.close_stream().await;
  }
  5.minutes().advance().await;
  assert!(setup.next_stream(1.seconds()).await.is_some());
  make_mut(&mut mock_updater)
    .expect_on_handshake_complete()
    .with(eq(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
    ))
    .once()
    .returning(|_| ());
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      None,
    )
    .await;
  setup.close_stream().await;
  assert!(setup.next_stream(10.seconds()).await.is_none());
  assert_eq!(
    NetworkQuality::Online,
    setup.network_quality_provider.get_network_quality()
  );
}

#[tokio::test(start_paused = true)]
async fn client_kill() {
  let mut setup = Setup::new().await;

  assert!(setup.next_stream(1.seconds()).await.is_some());
  setup
    .handshake_response(HANDSHAKE_FLAG_CONFIG_UP_TO_DATE, None, None)
    .await;

  let runtime_response = ApiResponse {
    response_type: Some(Response_type::RuntimeUpdate(RuntimeUpdate {
      version_nonce: "test".to_string(),
      runtime: Some(bd_test_helpers::runtime::make_proto(vec![(
        bd_runtime::runtime::client_kill::GenericKillDuration::path(),
        bd_test_helpers::runtime::ValueKind::Int(1.days().whole_milliseconds().try_into().unwrap()),
      )]))
      .into(),
      ..Default::default()
    })),
    ..Default::default()
  };
  setup.send_response(runtime_response.clone()).await;

  // Wait for the ACK to make sure the runtime update is processed.
  setup.next_request(1.seconds()).await.unwrap();

  // Restart to make sure we are killed.
  make_mut(&mut setup.config_updater)
    .expect_clear_cached_config()
    .times(1)
    .return_once(|| ());
  setup.restart().await;
  assert!(setup.next_stream(1.seconds()).await.is_none());

  // Advance 12 hours, we should still be killed.
  setup.time_provider.advance(12.hours());
  make_mut(&mut setup.config_updater)
    .expect_clear_cached_config()
    .times(1)
    .return_once(|| ());
  setup.restart().await;
  assert!(setup.next_stream(1.seconds()).await.is_none());

  // Advance another 13 hours, we should come back up.
  setup.time_provider.advance(13.hours());
  setup.restart().await;
  assert!(setup.next_stream(1.seconds()).await.is_some());

  // The client should be killed again after sending down the same config.
  setup
    .handshake_response(HANDSHAKE_FLAG_CONFIG_UP_TO_DATE, None, None)
    .await;
  setup.send_response(runtime_response).await;
  setup.next_request(1.seconds()).await.unwrap();
  make_mut(&mut setup.config_updater)
    .expect_clear_cached_config()
    .times(1)
    .return_once(|| ());
  setup.restart().await;
  assert!(setup.next_stream(1.seconds()).await.is_none());

  // Change the API key which without advancing time should allow the client to come up.
  setup.api_key = "other".to_string();
  setup.restart().await;
  assert!(setup.next_stream(1.seconds()).await.is_some());
}

#[tokio::test(start_paused = true)]
async fn bad_client_kill_file() {
  let mut setup = Setup::new().await;

  assert!(setup.next_stream(1.seconds()).await.is_some());

  tokio::fs::write(setup.sdk_directory.path().join("client_kill_until"), b"bad")
    .await
    .unwrap();
  setup.restart().await;

  assert!(setup.next_stream(1.seconds()).await.is_some());
}

#[tokio::test(start_paused = true)]
async fn data_idle_timeout() {
  let mut setup = Setup::new_ex(
    Setup::make_nice_mock_updater(),
    Some(RuntimeUpdate {
      version_nonce: "test".to_string(),
      runtime: Some(bd_test_helpers::runtime::make_proto(vec![
        (
          bd_runtime::runtime::api::DataIdleTimeoutInterval::path(),
          bd_test_helpers::runtime::ValueKind::Int(
            10.seconds().whole_milliseconds().try_into().unwrap(),
          ),
        ),
        (
          bd_runtime::runtime::api::MinReconnectInterval::path(),
          bd_test_helpers::runtime::ValueKind::Int(
            15.minutes().whole_milliseconds().try_into().unwrap(),
          ),
        ),
      ]))
      .into(),
      ..Default::default()
    }),
    None,
    None,
  )
  .await;

  assert!(setup.next_stream(1.seconds()).await.is_some());
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      None,
    )
    .await;

  1.seconds().sleep().await;

  assert_eq!(
    setup.network_quality_provider.get_network_quality(),
    NetworkQuality::Online
  );

  // Now sleep for 11 seconds to trigger the idle timeout.
  tokio::time::advance(11.std_seconds()).await;

  setup
    .collector
    .wait_for_counter_eq(1, "api:data_idle_timeout", labels! {})
    .await;

  assert_eq!(
    setup.network_quality_provider.get_network_quality(),
    NetworkQuality::Unknown
  );

  assert!(setup.next_stream(1.seconds()).await.is_none());

  tokio::time::advance(20.std_minutes()).await;

  assert!(setup.next_stream(1.seconds()).await.is_some());

  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      None,
    )
    .await;

  1.seconds().sleep().await;

  assert_eq!(
    setup.network_quality_provider.get_network_quality(),
    NetworkQuality::Online
  );
}

#[tokio::test(start_paused = true)]
async fn data_idle_timeout_fails_to_connect() {
  let mut network_quality = SimpleNetworkQualityProvider::default();
  let mut quality_updates_rx = network_quality.with_update_channel();

  let mut setup = Setup::new_ex(
    Setup::make_nice_mock_updater(),
    Some(RuntimeUpdate {
      version_nonce: "test".to_string(),
      runtime: Some(bd_test_helpers::runtime::make_proto(vec![
        (
          bd_runtime::runtime::api::DataIdleTimeoutInterval::path(),
          bd_test_helpers::runtime::ValueKind::Int(
            10.seconds().whole_milliseconds().try_into().unwrap(),
          ),
        ),
        (
          bd_runtime::runtime::api::MinReconnectInterval::path(),
          bd_test_helpers::runtime::ValueKind::Int(
            15.minutes().whole_milliseconds().try_into().unwrap(),
          ),
        ),
      ]))
      .into(),
      ..Default::default()
    }),
    None,
    Some(network_quality),
  )
  .await;

  quality_updates_rx.recv().await.unwrap();
  assert_eq!(
    setup.network_quality_provider.get_network_quality(),
    NetworkQuality::Unknown
  );

  assert!(setup.next_stream(1.seconds()).await.is_some());
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      None,
    )
    .await;

  quality_updates_rx.recv().await.unwrap();
  assert_eq!(
    setup.network_quality_provider.get_network_quality(),
    NetworkQuality::Online
  );

  // Now sleep for 11 seconds to trigger the idle timeout.
  tokio::time::advance(11.std_seconds()).await;

  setup
    .collector
    .wait_for_counter_eq(1, "api:data_idle_timeout", labels! {})
    .await;
  quality_updates_rx.recv().await.unwrap();

  // For the first attempt that is held back by the reconnect time we're marking the connectivity
  // status as unknown.
  assert_eq!(
    setup.network_quality_provider.get_network_quality(),
    NetworkQuality::Unknown
  );

  // Now advance 20 minutes so that we can reconnect.
  20.minutes().advance().await;
  setup.time_provider.advance(20.minutes());

  assert!(setup.next_stream(1.seconds()).await.is_some());
  1.minutes().advance().await;
  setup.time_provider.advance(1.minutes());
  setup.close_stream().await;

  quality_updates_rx.recv().await.unwrap();
  assert_eq!(
    setup.network_quality_provider.get_network_quality(),
    NetworkQuality::Offline
  );
}

#[tokio::test(start_paused = true)]
async fn data_idle_timeout_data_resets_timeout() {
  let (idle_timeout_tx, mut idle_timeout_rx) = channel::<()>(1);
  let mut setup = Setup::new_ex(
    Setup::make_nice_mock_updater(),
    Some(RuntimeUpdate {
      version_nonce: "test".to_string(),
      runtime: Some(bd_test_helpers::runtime::make_proto(vec![(
        bd_runtime::runtime::api::DataIdleTimeoutInterval::path(),
        bd_test_helpers::runtime::ValueKind::Int(
          10.seconds().whole_milliseconds().try_into().unwrap(),
        ),
      )]))
      .into(),
      ..Default::default()
    }),
    Some(idle_timeout_tx),
    None,
  )
  .await;

  assert!(setup.next_stream(1.seconds()).await.is_some());
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      None,
    )
    .await;

  1.seconds().sleep().await;

  assert_eq!(
    setup.network_quality_provider.get_network_quality(),
    NetworkQuality::Online
  );

  // Now sleep for 8 seconds, which should not be enough to trigger the timeout.
  tokio::time::advance(8.std_seconds()).await;

  setup
    .data_tx
    .send(DataUpload::StatsUpload(
      Tracked::new("test".to_string(), StatsUploadRequest::default()).0,
    ))
    .await
    .unwrap();

  // Make sure we grab the request to ensure the data was processed.
  setup.next_request(1.seconds()).await.unwrap();

  // Advancing time by 4 seconds would have triggered the timeout if not for the data sent above.
  tokio::time::advance(4.std_seconds()).await;

  assert!(
    timeout(1.std_seconds(), idle_timeout_rx.recv())
      .await
      .is_err()
  );

  assert_eq!(
    setup.network_quality_provider.get_network_quality(),
    NetworkQuality::Online
  );

  // Advancing another 7 is enough to trigger the timeout.
  tokio::time::advance(7.std_seconds()).await;

  setup
    .collector
    .wait_for_counter_eq(1, "api:data_idle_timeout", labels! {})
    .await;

  tokio::time::advance(20.std_minutes()).await;

  assert!(setup.next_stream(1.seconds()).await.is_some());

  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      None,
    )
    .await;

  1.seconds().sleep().await;

  assert_eq!(
    setup.network_quality_provider.get_network_quality(),
    NetworkQuality::Online
  );
}

#[tokio::test(start_paused = true)]
async fn data_idle_timeout_data_sent_during_reconnect_timeout() {
  let (idle_timeout_tx, mut idle_timeout_rx) = channel::<()>(1);
  let mut setup = Setup::new_ex(
    Setup::make_nice_mock_updater(),
    Some(RuntimeUpdate {
      version_nonce: "test".to_string(),
      runtime: Some(bd_test_helpers::runtime::make_proto(vec![(
        bd_runtime::runtime::api::DataIdleTimeoutInterval::path(),
        bd_test_helpers::runtime::ValueKind::Int(
          10.seconds().whole_milliseconds().try_into().unwrap(),
        ),
      )]))
      .into(),
      ..Default::default()
    }),
    Some(idle_timeout_tx),
    None,
  )
  .await;

  assert!(setup.next_stream(1.seconds()).await.is_some());
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      None,
    )
    .await;

  1.seconds().sleep().await;

  assert_eq!(
    setup.network_quality_provider.get_network_quality(),
    NetworkQuality::Online
  );

  // Now sleep for 8 seconds, which should trigger the reconnect timeout.
  tokio::time::advance(11.std_seconds()).await;

  idle_timeout_rx.recv().await.unwrap();
  setup
    .collector
    .assert_counter_eq(1, "api:data_idle_timeout", BTreeMap::new());

  // Send a data upload during the reconnect timeout period, this should trigger an immediate
  // reconnect.
  setup
    .data_tx
    .send(DataUpload::StatsUpload(
      Tracked::new("test".to_string(), StatsUploadRequest::default()).0,
    ))
    .await
    .unwrap();

  assert!(setup.next_stream(1.seconds()).await.is_some());
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      None,
    )
    .await;
  // Make sure the stats upload is processed.
  assert_matches!(
    setup
      .next_request(1.seconds())
      .await
      .unwrap()
      .request_type
      .as_ref()
      .unwrap(),
    Request_type::StatsUpload(_)
  );
}

#[tokio::test(start_paused = true)]
async fn api_retry_stream_runtime_override() {
  let mut setup = Setup::new().await;

  assert!(setup.next_stream(1.seconds()).await.is_some());
  setup
    .handshake_response(HANDSHAKE_FLAG_CONFIG_UP_TO_DATE, None, None)
    .await;

  setup
    .send_response(ApiResponse {
      response_type: Some(Response_type::RuntimeUpdate(RuntimeUpdate {
        version_nonce: "test".to_string(),
        runtime: Some(bd_test_helpers::runtime::make_proto(vec![(
          bd_runtime::runtime::api::MaxBackoffInterval::path(),
          bd_test_helpers::runtime::ValueKind::Int(1000),
        )]))
        .into(),
        ..Default::default()
      })),
      ..Default::default()
    })
    .await;
  setup.next_request(1.seconds()).await.unwrap();

  // Reconnect 10 times, asserting that it never takes more than 1s to connect. This proves that the
  // backoff never exceeds 1.5s, per the runtime override and default 50% randomization.
  for _ in 0 .. 10 {
    setup.close_stream().await;
    assert!(setup.next_stream(1500.milliseconds()).await.is_some());
  }
}

#[tokio::test(start_paused = true)]
async fn multiple_handshake_messages_with_error() {
  let mut setup = Setup::new().await;

  assert!(setup.next_stream(1.seconds()).await.is_some());
  let now = Instant::now();
  setup
    .send_multiple_messages(vec![
      Setup::make_handshake(
        HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
        None,
        None,
      ),
      Setup::make_error_shutdown(Code::Internal, "some message", Some(5.minutes())),
    ])
    .await;

  assert!(setup.next_stream(6.minutes()).await.is_some());
  assert!(now.elapsed() >= 5.minutes());
}

#[tokio::test(start_paused = true)]
async fn error_response_with_retry_after() {
  let mut setup = Setup::new().await;

  assert!(setup.next_stream(1.seconds()).await.is_some());
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      None,
    )
    .await;

  let now = Instant::now();
  setup
    .error_shutdown(Code::Internal, "some message", Some(1.minutes()))
    .await;

  assert!(setup.next_stream(2.minutes()).await.is_some());
  assert!(now.elapsed() >= 1.minutes());

  setup
    .collector
    .assert_counter_eq(1, "api:error_shutdown_total", labels! {});
}

#[tokio::test(start_paused = true)]
async fn error_response_before_handshake() {
  let mut setup = Setup::new().await;

  assert!(setup.next_stream(1.seconds()).await.is_some());

  setup
    .error_shutdown(Code::Internal, "some message", None)
    .await;

  assert!(setup.next_stream(1.seconds()).await.is_some());

  setup
    .collector
    .assert_counter_eq(1, "api:error_shutdown_total", labels! {});
  setup
    .collector
    .assert_counter_eq(1, "api:remote_connect_failure", labels! {});
}

#[tokio::test(start_paused = true)]
async fn rate_limited_response_before_handshake() {
  let mut setup = Setup::new().await;

  assert!(setup.next_stream(1.seconds()).await.is_some());

  let now = Instant::now();
  setup
    .error_shutdown(Code::ResourceExhausted, "rate limited", Some(1.minutes()))
    .await;

  assert!(setup.next_stream(2.minutes()).await.is_some());
  assert!(now.elapsed() >= 1.minutes());
}

#[tokio::test(start_paused = true)]
async fn unauthenticated_response_before_handshake() {
  let mut setup = Setup::new().await;

  assert!(setup.next_stream(1.seconds()).await.is_some());

  make_mut(&mut setup.config_updater)
    .expect_clear_cached_config()
    .times(1)
    .return_once(|| ());
  setup
    .error_shutdown(Code::Unauthenticated, "some message", None)
    .await;

  // The unauthenticated response will kill the client for the default 1 day period. Make sure that
  // we don't reconnect.
  setup.close_stream().await;
  assert!(setup.next_stream(1.seconds()).await.is_none());

  setup
    .collector
    .assert_counter_eq(0, "api:error_shutdown_total", labels! {});

  // Restart to make sure the client is still killed. We should not see any stream requests.
  make_mut(&mut setup.config_updater)
    .expect_clear_cached_config()
    .times(1)
    .return_once(|| ());
  setup.restart().await;
  assert!(setup.next_stream(1.seconds()).await.is_none());

  // Now let's wait a day and make sure the client comes back online.
  setup.time_provider.advance(1.days());
  setup.restart().await;
  assert!(setup.next_stream(1.seconds()).await.is_some());
}

#[tokio::test]
async fn set_stats_upload_request_sent_at_field() {
  let mut setup = Setup::new().await;

  assert!(setup.next_stream(1.seconds()).await.is_some());
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      None,
    )
    .await;

  let (tracked, _) = Tracked::new("123".to_string(), StatsUploadRequest::default());

  let data_upload = DataUpload::StatsUpload(tracked);
  setup.send_request(data_upload).await;

  let next_received_request = setup.next_request(1.seconds()).await.unwrap();
  assert_matches!(next_received_request.request_type, Some(Request_type::StatsUpload(payload))
    if { payload.sent_at == setup.time_provider.now().into_proto() });
}

#[tokio::test(start_paused = true)]
async fn sleep_mode() {
  let mut setup = Setup::new().await;
  setup.sleep_mode_active.send(true).unwrap();
  assert!(setup.next_stream(1.seconds()).await.unwrap().sleep_mode);
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      Some(StreamSettings {
        ping_interval: 60.seconds().into_proto(),
        ..Default::default()
      }),
      None,
    )
    .await;

  let Some(Request_type::Ping(ping_request)) =
    setup.next_request(61.seconds()).await.unwrap().request_type
  else {
    panic!("expected ping request");
  };
  assert!(ping_request.sleep_mode);
}

#[tokio::test(start_paused = true)]
async fn opaque_client_state() {
  let mut setup = Setup::new().await;
  assert!(
    setup
      .next_stream(1.seconds())
      .await
      .unwrap()
      .opaque_client_state
      .is_none()
  );
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      Some(b"state".to_vec()),
    )
    .await;
  setup.close_stream().await;
  assert_eq!(
    setup
      .next_stream(2.seconds())
      .await
      .unwrap()
      .opaque_client_state,
    Some(b"state".to_vec())
  );
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      Some(b"state2".to_vec()),
    )
    .await;
  setup.close_stream().await;
  assert_eq!(
    setup
      .next_stream(3.seconds())
      .await
      .unwrap()
      .opaque_client_state,
    Some(b"state2".to_vec())
  );
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      None,
    )
    .await;
  setup.close_stream().await;
  assert!(
    setup
      .next_stream(4.seconds())
      .await
      .unwrap()
      .opaque_client_state
      .is_none()
  );
}
