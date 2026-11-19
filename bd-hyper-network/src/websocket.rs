// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./websocket_test.rs"]
mod tests;

use super::StreamHandle;
use bd_api::{PlatformNetworkManager, PlatformNetworkStream, StreamEvent, StreamEventSender};
use futures_util::{SinkExt, StreamExt};
use http::header::SEC_WEBSOCKET_PROTOCOL;
use http::{HeaderValue, Uri};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::Error;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};

const PROTOCOL: &str = "bitdrift.mux.v1";
const MAX_RECORD_SIZE: usize = 10 * 1024 * 1024 + 5;
const IO_TIMEOUT: Duration = Duration::from_secs(30);

//
// WebSocketNetwork
//

pub struct WebSocketNetwork {
  uri: Uri,
}

impl WebSocketNetwork {
  pub fn new(api_base: &str) -> anyhow::Result<Self> {
    let base: Uri = api_base.parse()?;
    let scheme = match base.scheme_str() {
      Some("https") => "wss",
      Some("http") => "ws",
      _ => anyhow::bail!("mux WebSocket requires an HTTP or HTTPS API base"),
    };
    let authority = base
      .authority()
      .ok_or_else(|| anyhow::anyhow!("mux WebSocket API base has no authority"))?;
    Ok(Self {
      uri: Uri::builder()
        .scheme(scheme)
        .authority(authority.clone())
        .path_and_query("/v1/mux/ws")
        .build()?,
    })
  }
}

#[async_trait::async_trait]
impl<T> PlatformNetworkManager<T> for WebSocketNetwork {
  async fn start_stream(
    &self,
    event_tx: StreamEventSender,
    _runtime: &T,
    headers: &HashMap<&str, &str>,
  ) -> anyhow::Result<Box<dyn PlatformNetworkStream>> {
    let mut request = self.uri.to_string().into_client_request()?;
    request
      .headers_mut()
      .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(PROTOCOL));
    for name in ["x-bitdrift-api-key", "x-bitdrift-app-id"] {
      if let Some(value) = headers.get(name) {
        let mut value = HeaderValue::from_str(value)?;
        value.set_sensitive(name == "x-bitdrift-api-key");
        request.headers_mut().insert(name, value);
      }
    }

    let config = WebSocketConfig::default()
      .max_message_size(Some(MAX_RECORD_SIZE))
      .max_frame_size(Some(MAX_RECORD_SIZE))
      .write_buffer_size(0)
      .max_write_buffer_size(MAX_RECORD_SIZE + 1024);
    let (socket, response) = timeout(
      IO_TIMEOUT,
      connect_async_with_config(request, Some(config), true),
    )
    .await?
    .map_err(|error| match error {
      Error::Http(response) => {
        anyhow::anyhow!("mux WebSocket upgrade returned HTTP {}", response.status())
      },
      _ => anyhow::anyhow!("mux WebSocket upgrade failed"),
    })?;
    if response.headers().get(SEC_WEBSOCKET_PROTOCOL) != Some(&HeaderValue::from_static(PROTOCOL)) {
      anyhow::bail!("mux WebSocket did not negotiate the requested subprotocol");
    }

    let (data_tx, mut data_rx) = mpsc::channel::<Vec<u8>>(1);
    tokio::spawn(async move {
      let (mut writer, mut reader) = socket.split();
      let send = async {
        while let Some(bytes) = data_rx.recv().await {
          if bytes.len() > MAX_RECORD_SIZE {
            return Some("mux WebSocket request exceeds the record size limit");
          }
          if !matches!(
            timeout(IO_TIMEOUT, writer.send(Message::Binary(bytes.into()))).await,
            Ok(Ok(()))
          ) {
            return Some("mux WebSocket send failed");
          }
        }
        let _ = timeout(IO_TIMEOUT, writer.send(Message::Close(None))).await;
        None
      };
      let receive = async {
        while let Some(incoming) = reader.next().await {
          match incoming {
            Ok(Message::Binary(bytes)) => {
              if event_tx
                .send(StreamEvent::Data(bytes.to_vec()))
                .await
                .is_err()
              {
                return None;
              }
            },
            Ok(Message::Ping(_) | Message::Pong(_)) => {},
            Ok(Message::Close(_)) => break,
            _ => return Some("mux WebSocket receive failed"),
          }
        }
        Some("mux WebSocket closed")
      };
      let reason = tokio::select! {
        () = event_tx.closed() => None,
        reason = send => reason,
        reason = receive => reason,
      };
      if let Some(reason) = reason {
        let _ = event_tx
          .send(StreamEvent::StreamClosed(reason.to_string()))
          .await;
      }
    });
    Ok(Box::new(StreamHandle { data_tx }))
  }
}
