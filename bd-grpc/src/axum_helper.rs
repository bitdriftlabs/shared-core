// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use axum::Router;
use bd_server_stats::stats::AutoGauge;
use bd_shutdown::ComponentShutdownTrigger;
use futures::future::FutureExt;
use http::Request;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use prometheus::{IntCounter, IntGauge};
use std::future::Future;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tower::util::ServiceExt;
use tower::Service;
use unwrap_infallible::UnwrapInfallible;

pub async fn serve_with_connect_info(
  router: Router,
  listener: TcpListener,
  cx_total: IntCounter,
  cx_active: IntGauge,
  shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
  serve_with_connect_info_ex(router, listener, cx_total, cx_active, shutdown, || {
    Builder::new(TokioExecutor::new())
  })
  .await
}

// This is a manual implementation of the axum accept loop with graceful shutdown. It adds:
// 1) Connection count tracking.
// In the future there are likely to be further customizations indicated by the TODOs below.
pub async fn serve_with_connect_info_ex(
  router: Router,
  listener: TcpListener,
  cx_total: IntCounter,
  cx_active: IntGauge,
  shutdown: impl Future<Output = ()>,
  builder_fn: impl Fn() -> Builder<TokioExecutor>,
) -> anyhow::Result<()> {
  let mut make_service = router.into_make_service_with_connect_info::<SocketAddr>();
  tokio::pin!(shutdown);

  let cx_shutdown_trigger = ComponentShutdownTrigger::default();

  loop {
    // TODO(mattklein123): Add optional bounds on max active connections.
    let (socket, remote_addr) = tokio::select! {
      result = listener.accept() => result.map_err(|e| {
        log::warn!("listener accept failure, shutting down: {e}");
        e
      })?,
      () = &mut shutdown => {
        break;
      },
    };

    // We don't need to call `poll_ready` because `IntoMakeServiceWithConnectInfo` is always
    // ready.
    let tower_service = make_service.call(remote_addr).await.unwrap_infallible();

    let mut cx_shutdown = cx_shutdown_trigger.make_shutdown();
    cx_total.inc();
    let cx_active = AutoGauge::new(cx_active.clone());
    let builder = builder_fn();

    tokio::spawn(async move {
      let socket = TokioIo::new(socket);

      let hyper_service = hyper::service::service_fn(move |request: Request<Incoming>| {
        tower_service.clone().oneshot(request)
      });

      let cx = builder.serve_connection_with_upgrades(socket, hyper_service);
      tokio::pin!(cx);
      let start_shutdown = cx_shutdown.cancelled().fuse();
      tokio::pin!(start_shutdown);

      // TODO(mattklein123): Add optional max connection duration.
      loop {
        tokio::select! {
          result = &mut cx => {
            if let Err(e) = result {
              log::debug!("connection serve failure: {e}");
            }
            break;
          },
          () = &mut start_shutdown => {
            cx.as_mut().graceful_shutdown();
          }
        }
      }

      drop(cx_active);
    });
  }

  drop(listener);
  cx_shutdown_trigger.shutdown().await;

  Ok(())
}
