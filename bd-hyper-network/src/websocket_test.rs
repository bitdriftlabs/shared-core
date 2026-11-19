// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::WebSocketNetwork;
use bd_api::{PlatformNetworkManager, StreamEvent};
use futures_util::{SinkExt, StreamExt};
use http::HeaderValue;
use http::header::SEC_WEBSOCKET_PROTOCOL;
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::Message;

#[tokio::test]
async fn websocket_bridges_binary_records_and_can_open_a_replacement_after_close() {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let network =
    WebSocketNetwork::new(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
  let exchange = async {
    let server = async {
      for _ in 0 .. 2 {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = accept_hdr_async(tcp, |request: &Request, mut response: Response| {
          assert_eq!(request.uri().path(), "/v1/mux/ws");
          assert_eq!(request.headers()["x-bitdrift-api-key"], "test-key");
          assert_eq!(request.headers()["x-bitdrift-app-id"], "test-app");
          assert_eq!(request.headers()[SEC_WEBSOCKET_PROTOCOL], "bitdrift.mux.v1");
          response.headers_mut().insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("bitdrift.mux.v1"),
          );
          Ok(response)
        })
        .await
        .unwrap();
        assert_eq!(
          ws.next().await.unwrap().unwrap(),
          Message::Binary(vec![0, 0, 0, 0, 1, 7].into())
        );
        ws.send(Message::Binary(vec![0, 0, 0, 0, 1, 9].into()))
          .await
          .unwrap();
        ws.close(None).await.unwrap();
      }
    };
    let client = async {
      let headers = HashMap::from([
        ("x-bitdrift-api-key", "test-key"),
        ("x-bitdrift-app-id", "test-app"),
      ]);
      for _ in 0 .. 2 {
        let (event_tx, mut event_rx) = mpsc::channel(1);
        let mut stream = network.start_stream(event_tx, &(), &headers).await.unwrap();
        stream.send_data(&[0, 0, 0, 0, 1, 7]).await.unwrap();
        assert!(matches!(
          event_rx.recv().await,
          Some(StreamEvent::Data(data)) if data == [0, 0, 0, 0, 1, 9]
        ));
        assert!(matches!(
          event_rx.recv().await,
          Some(StreamEvent::StreamClosed(_))
        ));
      }
    };
    tokio::join!(server, client);
  };
  timeout(Duration::from_secs(5), exchange).await.unwrap();
}
