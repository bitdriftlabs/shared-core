// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{EmptyMetadata, Setup, pending_command_update};
use crate::DataUpload;
use crate::upload::{IntentResponse, Tracked, UploadResponse};
use bd_client_common::{HANDSHAKE_FLAG_CONFIG_UP_TO_DATE, HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE};
use bd_client_stats_store::test::StatsHelper as _;
use bd_proto::protos::client::api::api_request::Request_type;
use bd_proto::protos::client::api::api_response::Response_type;
use bd_proto::protos::client::api::device_command_update_ack::Disposition;
use bd_proto::protos::client::api::handshake_response::StreamSettings;
use bd_proto::protos::client::api::{
  ApiResponse,
  DebugDataRequest,
  DeviceCommandUpdateAck,
  ErrorShutdown,
  LogUploadIntentRequest,
  LogUploadIntentResponse,
  LogUploadRequest,
  LogUploadResponse,
  PongResponse,
  SankeyIntentRequest,
  SankeyIntentResponse,
  SankeyPathUploadRequest,
  SankeyPathUploadResponse,
  StatsUploadRequest,
  StatsUploadResponse,
  UploadArtifactIntentRequest,
  UploadArtifactIntentResponse,
  UploadArtifactRequest,
  UploadArtifactResponse,
};
use bd_stats_common::labels;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_time::ToProtoDuration;
use std::sync::Arc;
use time::ext::NumericalDuration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

//
// PendingResponse
//

enum PendingResponse {
  Upload(oneshot::Receiver<UploadResponse>),
  Intent(oneshot::Receiver<IntentResponse>),
}

impl PendingResponse {
  async fn wait(self) {
    match self {
      Self::Upload(response) => {
        response.await.unwrap();
      },
      Self::Intent(response) => {
        response.await.unwrap();
      },
    }
  }
}

//
// RequestKind
//

#[derive(Clone, Copy)]
enum RequestKind {
  Stats,
  Logs,
  Sankey,
  Artifact,
  LogIntent,
  SankeyIntent,
  ArtifactIntent,
}

fn pending_request(kind: RequestKind, uuid: &str) -> (DataUpload, ApiResponse, PendingResponse) {
  let (upload, response, receiver) = match kind {
    RequestKind::Stats => {
      let (tracked, receiver) = Tracked::new(uuid.to_string(), StatsUploadRequest::default());
      (
        DataUpload::StatsUpload(tracked),
        Response_type::StatsUpload(StatsUploadResponse {
          upload_uuid: uuid.to_string(),
          ..Default::default()
        }),
        PendingResponse::Upload(receiver),
      )
    },
    RequestKind::Logs => {
      let (tracked, receiver) = Tracked::new(uuid.to_string(), LogUploadRequest::default());
      (
        DataUpload::LogsUpload(tracked),
        Response_type::LogUpload(LogUploadResponse {
          upload_uuid: uuid.to_string(),
          ..Default::default()
        }),
        PendingResponse::Upload(receiver),
      )
    },
    RequestKind::Sankey => {
      let (tracked, receiver) = Tracked::new(uuid.to_string(), SankeyPathUploadRequest::default());
      (
        DataUpload::SankeyPathUpload(tracked),
        Response_type::SankeyDiagramUpload(SankeyPathUploadResponse {
          upload_uuid: uuid.to_string(),
          ..Default::default()
        }),
        PendingResponse::Upload(receiver),
      )
    },
    RequestKind::Artifact => {
      let (tracked, receiver) = Tracked::new(uuid.to_string(), UploadArtifactRequest::default());
      (
        DataUpload::ArtifactUpload(tracked),
        Response_type::ArtifactUpload(UploadArtifactResponse {
          upload_uuid: uuid.to_string(),
          ..Default::default()
        }),
        PendingResponse::Upload(receiver),
      )
    },
    RequestKind::LogIntent => {
      let (tracked, receiver) = Tracked::new(uuid.to_string(), LogUploadIntentRequest::default());
      (
        DataUpload::LogsUploadIntent(tracked),
        Response_type::LogUploadIntent(LogUploadIntentResponse {
          intent_uuid: uuid.to_string(),
          ..Default::default()
        }),
        PendingResponse::Intent(receiver),
      )
    },
    RequestKind::SankeyIntent => {
      let (tracked, receiver) = Tracked::new(uuid.to_string(), SankeyIntentRequest::default());
      (
        DataUpload::SankeyPathUploadIntent(tracked),
        Response_type::SankeyIntentResponse(SankeyIntentResponse {
          intent_uuid: uuid.to_string(),
          ..Default::default()
        }),
        PendingResponse::Intent(receiver),
      )
    },
    RequestKind::ArtifactIntent => {
      let (tracked, receiver) =
        Tracked::new(uuid.to_string(), UploadArtifactIntentRequest::default());
      (
        DataUpload::ArtifactUploadIntent(tracked),
        Response_type::ArtifactIntent(UploadArtifactIntentResponse {
          intent_uuid: uuid.to_string(),
          ..Default::default()
        }),
        PendingResponse::Intent(receiver),
      )
    },
  };
  (
    upload,
    ApiResponse {
      response_type: Some(response),
      ..Default::default()
    },
    receiver,
  )
}

async fn setup(idle_ms: u32) -> (Setup, mpsc::Receiver<()>) {
  let (idle_tx, idle_rx) = mpsc::channel(1);
  let mut setup = Setup::new_ex(
    Setup::make_nice_mock_updater(),
    Some(make_simple_update(vec![
      ("api.data_idle_timeout_interval_ms", ValueKind::Int(idle_ms)),
      ("api.min_reconnect_interval_ms", ValueKind::Int(900_000)),
      (
        "sleep_mode.api_min_reconnect_interval_ms",
        ValueKind::Int(900_000),
      ),
    ])),
    Some(idle_tx),
    None,
    Arc::new(EmptyMetadata),
  )
  .await;
  setup.next_stream(1.seconds()).await.unwrap();
  setup
    .handshake_response(
      HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      None,
      None,
    )
    .await;
  setup.wait_for_pending_requests(false).await;
  (setup, idle_rx)
}

async fn send_pending(setup: &mut Setup, request: DataUpload) {
  setup.send_request(request).await;
  setup.next_request(1.seconds()).await.unwrap();
  setup.wait_for_pending_requests(true).await;
}

#[tokio::test(start_paused = true)]
async fn tracked_uploads_and_intents_keep_stream_alive_until_response() {
  for kind in [
    RequestKind::Stats,
    RequestKind::Logs,
    RequestKind::Sankey,
    RequestKind::Artifact,
    RequestKind::LogIntent,
    RequestKind::SankeyIntent,
    RequestKind::ArtifactIntent,
  ] {
    let (mut setup, mut idle_rx) = setup(1500).await;
    let (request, response, receiver) = pending_request(kind, "pending");
    send_pending(&mut setup, request).await;
    tokio::time::advance(10.seconds().unsigned_abs()).await;
    let acknowledged_at = Instant::now();
    setup.send_response(response).await;
    receiver.wait().await;
    setup.wait_for_pending_requests(false).await;
    idle_rx.recv().await.unwrap();
    assert_eq!(Instant::now(), acknowledged_at);
    setup
      .collector
      .assert_counter_eq(1, "api:data_idle_timeout", labels! {});
  }
}

#[tokio::test(start_paused = true)]
async fn pending_requests_keep_stream_alive_after_waiter_is_dropped() {
  let (mut setup, mut idle_rx) = setup(1500).await;
  let (request, response, receiver) = pending_request(RequestKind::Stats, "pending");
  drop(receiver);
  send_pending(&mut setup, request).await;
  tokio::time::advance(10.seconds().unsigned_abs()).await;
  setup.send_response(response).await;
  setup.wait_for_pending_requests(false).await;
  idle_rx.recv().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn only_the_last_response_releases_idle_keepalive() {
  let (mut setup, mut idle_rx) = setup(1500).await;
  let (first, first_ack, first_rx) = pending_request(RequestKind::Stats, "first");
  let (second, second_ack, second_rx) = pending_request(RequestKind::LogIntent, "second");
  send_pending(&mut setup, first).await;
  send_pending(&mut setup, second).await;
  tokio::time::advance(10.seconds().unsigned_abs()).await;
  setup.send_response(second_ack).await;
  second_rx.wait().await;
  setup.wait_for_pending_requests(true).await;
  tokio::time::advance(10.seconds().unsigned_abs()).await;
  setup.send_response(first_ack).await;
  first_rx.wait().await;
  idle_rx.recv().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn error_response_releases_idle_keepalive() {
  let (mut setup, mut idle_rx) = setup(1500).await;
  let (tracked, receiver) = Tracked::new("pending".to_string(), StatsUploadRequest::default());
  send_pending(&mut setup, DataUpload::StatsUpload(tracked)).await;
  tokio::time::advance(10.seconds().unsigned_abs()).await;
  setup
    .send_response(ApiResponse {
      response_type: Some(Response_type::StatsUpload(StatsUploadResponse {
        upload_uuid: "pending".to_string(),
        error: "rejected".to_string(),
        ..Default::default()
      })),
      ..Default::default()
    })
    .await;
  assert!(!receiver.await.unwrap().success);
  idle_rx.recv().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn state_update_ack_keeps_stream_alive() {
  for new_session in [false, true] {
    let (mut setup, mut idle_rx) = setup(1500).await;
    if new_session {
      setup.session_strategy.start_new_session(None).unwrap();
    } else {
      setup.set_opaque_entity_id(Some("entity"));
    }
    assert!(matches!(
      setup.next_request(1.seconds()).await.unwrap().request_type,
      Some(Request_type::StateUpdate(_))
    ));
    setup.wait_for_pending_requests(true).await;
    tokio::time::advance(10.seconds().unsigned_abs()).await;
    setup.state_update_response().await;
    setup.wait_for_pending_requests(false).await;
    idle_rx.recv().await.unwrap();
  }
}

#[tokio::test(start_paused = true)]
async fn sleep_and_shorter_runtime_timeout_do_not_override_pending_ack() {
  let (mut setup, mut idle_rx) = setup(1500).await;
  let (request, response, receiver) = pending_request(RequestKind::Stats, "pending");
  send_pending(&mut setup, request).await;
  setup
    .send_response(ApiResponse {
      response_type: Some(Response_type::RuntimeUpdate(make_simple_update(vec![
        ("api.data_idle_timeout_interval_ms", ValueKind::Int(10)),
        (
          "sleep_mode.api_data_idle_timeout_interval_ms",
          ValueKind::Int(1),
        ),
        ("api.min_reconnect_interval_ms", ValueKind::Int(900_000)),
        (
          "sleep_mode.api_min_reconnect_interval_ms",
          ValueKind::Int(900_000),
        ),
      ]))),
      ..Default::default()
    })
    .await;
  assert!(matches!(
    setup.next_request(1.seconds()).await.unwrap().request_type,
    Some(Request_type::RuntimeUpdateAck(_))
  ));
  setup.wait_for_pending_requests(true).await;
  setup.sleep_mode_active.send(true).unwrap();
  setup.idle_timeout_state_rx.changed().await.unwrap();
  tokio::time::advance(30.seconds().unsigned_abs()).await;
  setup.send_response(response).await;
  receiver.wait().await;
  idle_rx.recv().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn command_ack_keeps_stream_alive_below_retry_interval() {
  let (mut setup, mut idle_rx) = setup(100).await;
  let (tracked, receiver) = pending_command_update();
  let update = tracked.payload.clone();
  send_pending(&mut setup, DataUpload::DeviceCommandUpdate(tracked)).await;
  tokio::time::advance(500.milliseconds().unsigned_abs()).await;
  setup
    .send_response(ApiResponse {
      response_type: Some(Response_type::DeviceCommandUpdateAck(
        DeviceCommandUpdateAck {
          command_id: update.command_id,
          update_sequence_number: update.update_sequence_number,
          disposition: Disposition::DISPOSITION_APPLIED.into(),
          ..Default::default()
        },
      )),
      ..Default::default()
    })
    .await;
  receiver.await.unwrap();
  idle_rx.recv().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn untracked_requests_do_not_hold_idle_keepalive() {
  for request in [
    DataUpload::AcklessLogsUpload(LogUploadRequest::default()),
    DataUpload::DebugData(DebugDataRequest::default()),
  ] {
    let (mut setup, mut idle_rx) = setup(1500).await;
    setup.send_request(request).await;
    setup.next_request(1.seconds()).await.unwrap();
    setup.wait_for_pending_requests(false).await;
    tokio::time::advance(2.seconds().unsigned_abs()).await;
    idle_rx.recv().await.unwrap();
  }
}

#[tokio::test(start_paused = true)]
async fn ping_and_pong_do_not_hold_or_reset_idle_timeout() {
  for acknowledge_ping in [false, true] {
    let (idle_tx, mut idle_rx) = mpsc::channel(1);
    let mut setup = Setup::new_ex(
      Setup::make_nice_mock_updater(),
      Some(make_simple_update(vec![
        ("api.data_idle_timeout_interval_ms", ValueKind::Int(1500)),
        ("api.min_reconnect_interval_ms", ValueKind::Int(900_000)),
      ])),
      Some(idle_tx),
      None,
      Arc::new(EmptyMetadata),
    )
    .await;
    setup.next_stream(1.seconds()).await.unwrap();
    let established_at = Instant::now();
    setup
      .handshake_response(
        HANDSHAKE_FLAG_CONFIG_UP_TO_DATE | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
        Some(StreamSettings {
          ping_interval: 1.seconds().into_proto(),
          ..Default::default()
        }),
        None,
      )
      .await;
    assert!(matches!(
      setup.next_request(2.seconds()).await.unwrap().request_type,
      Some(Request_type::Ping(_))
    ));
    setup.wait_for_pending_requests(false).await;
    if acknowledge_ping {
      setup
        .send_response(ApiResponse {
          response_type: Some(Response_type::Pong(PongResponse::default())),
          ..Default::default()
        })
        .await;
    }
    idle_rx.recv().await.unwrap();
    assert_eq!(
      Instant::now() - established_at,
      1500.milliseconds().unsigned_abs()
    );
  }
}

#[tokio::test(start_paused = true)]
async fn zero_idle_timeout_remains_disabled_after_ack() {
  let (mut setup, mut idle_rx) = setup(0).await;
  let (request, response, receiver) = pending_request(RequestKind::Stats, "pending");
  send_pending(&mut setup, request).await;
  tokio::time::advance(10.seconds().unsigned_abs()).await;
  setup.send_response(response).await;
  receiver.wait().await;
  setup.wait_for_pending_requests(false).await;
  tokio::time::advance(60.seconds().unsigned_abs()).await;
  setup.state_update_response().await;
  setup.idle_timeout_state_rx.changed().await.unwrap();
  assert_eq!(idle_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
}

#[tokio::test(start_paused = true)]
async fn transport_close_still_abandons_pending_upload() {
  for error_shutdown in [false, true] {
    let (mut setup, _) = setup(1500).await;
    let (tracked, receiver) = Tracked::new("pending".to_string(), StatsUploadRequest::default());
    send_pending(&mut setup, DataUpload::StatsUpload(tracked)).await;
    tokio::time::advance(10.seconds().unsigned_abs()).await;
    if error_shutdown {
      setup
        .send_response(ApiResponse {
          response_type: Some(Response_type::ErrorShutdown(ErrorShutdown::default())),
          ..Default::default()
        })
        .await;
    } else {
      setup.close_stream().await;
    }
    assert!(receiver.await.is_err());
    setup.wait_for_reconnect_backoff_started().await;
    setup
      .collector
      .assert_counter_eq(0, "api:data_idle_timeout", labels! {});
  }
}
