// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./service_test.rs"]
mod service_test;

use crate::service::ratelimit::RequestSized;
use backoff::backoff::Backoff;
use bd_api::upload::{LogBatch, TrackedLogBatch};
use bd_api::DataUpload;
use bd_client_stats_store::{Counter, Scope};
use bd_proto::protos::client::api::LogUploadRequest;
use bd_runtime::runtime::{ConfigLoader, DurationWatch, IntWatch};
use bd_shutdown::ComponentShutdown;
use bd_stats_common::labels;
use futures_util::future::BoxFuture;
use std::convert::Infallible;
use std::sync::Arc;
use std::task::Poll;
use tokio::sync::mpsc::Sender;
use tower::util::BoxCloneService;
use tower::ServiceBuilder;

#[path = "./ratelimit.rs"]
mod ratelimit;

pub type Upload = BoxCloneService<UploadRequest, UploadResult, Infallible>;

// Creates a new UploadService with retries enabled.
pub fn new(
  data_upload_tx: Sender<DataUpload>,
  // TODO(snowp): Consider whether we need this at all or if it should be folded in with the
  // shutdown checks done in consumer.rs when trying to issue an upload.
  shutdown: ComponentShutdown,
  runtime_loader: &ConfigLoader,
  stats: &Scope,
) -> anyhow::Result<Upload> {
  let scope = stats.scope("uploader");
  let retry_limit_exceeded = scope.counter("retry_limit_exceeded");
  let retry_limit_exceeded_dropped_logs = scope.counter("retry_limit_exceeded_dropped_logs");

  let ratelimit = ratelimit::RateLimitLayer::new(ratelimit::Rate::new(runtime_loader)?);
  // TODO(snowp): Consider adding concurrency limits. With the way we do uploads, this only matters
  // if we have multiple buffers being uploaded at the same time.
  Ok(
    ServiceBuilder::new()
      .boxed_clone()
      .layer(ratelimit)
      .retry(RetryPolicy {
        attempts: 0,
        max_retries: runtime_loader.register_watch()?,
        retry_limit_exceeded,
        retry_limit_exceeded_dropped_logs,
        backoff: None,
        backoff_provider: BackoffProvider::new(runtime_loader)?,
      })
      .service(Uploader {
        data_upload_tx,
        shutdown,
        scope,
      }),
  )
}

// An upload request is a UUID and a set of logs.
#[derive(Clone)]
pub struct UploadRequest {
  uuid: String,
  log_upload: Arc<LogBatch>,
}

impl UploadRequest {
  pub(crate) fn new(log_upload: LogBatch) -> Self {
    Self {
      uuid: TrackedLogBatch::upload_uuid(),
      log_upload: Arc::new(log_upload),
    }
  }
}

// For ratelimiting purposes we count the number of log bytes uploaded (not including extra request
// overhead).
impl RequestSized for UploadRequest {
  fn per_request_size(&self) -> u32 {
    self
      .log_upload
      .logs
      .iter()
      .map(Vec::len)
      .sum::<usize>()
      .try_into()
      .unwrap()
  }
}

// The result of an upload attempt.
#[derive(Debug)]
pub enum UploadResult {
  // The upload was completed sucessfully.
  Success,

  // The upload failed.
  Failure,

  // The upload was canceled.
  Canceled,
}

// A service which sends a log upload request over a channel, then awaits the response to said
// upload. This is abstracted into a tower::Service in order to layer retries and rate limiting.
#[derive(Clone)]
struct Uploader {
  // The channel used for log uploads.
  data_upload_tx: Sender<DataUpload>,

  // Used to cancel the upload attempt.
  shutdown: ComponentShutdown,

  scope: Scope,
}

impl tower::Service<UploadRequest> for Uploader {
  type Response = UploadResult;
  type Error = Infallible;
  type Future = BoxFuture<'static, std::result::Result<Self::Response, Self::Error>>;

  fn poll_ready(
    &mut self,
    _cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
    Poll::Ready(Ok(()))
  }

  fn call(&mut self, request: UploadRequest) -> Self::Future {
    let buffer_id = request.log_upload.buffer_id.clone();

    let (upload, response_rx) = TrackedLogBatch::new(
      request.uuid.clone(),
      LogUploadRequest {
        upload_uuid: request.uuid.clone(),
        logs: request.log_upload.logs.clone(),
        buffer_uuid: request.log_upload.buffer_id.clone(),
        ..Default::default()
      },
    );

    let log_upload_attempt = self
      .scope
      .counter_with_labels("log_upload_attempt", labels!("buffer_id" => &buffer_id));
    let log_upload_failure = self
      .scope
      .counter_with_labels("log_upload_failure", labels!("buffer_id" => &buffer_id));

    let mut shutdown = self.shutdown.clone();
    let data_upload_tx = self.data_upload_tx.clone();
    Box::pin(async move {
      log_upload_attempt.inc();

      // If the send fails or we're canceled treat this as the shutdown case and mark the upload as
      // canceled. Errors are not retried.
      tokio::select! {
        () = shutdown.cancelled() => return Ok(UploadResult::Canceled),
        r = data_upload_tx.send(DataUpload::LogsUpload(upload)) => {
            if r.is_err() {
              return Ok(UploadResult::Canceled);
            }
          }
      }

      // If the response sender is dropped it means that the api received
      // the event but the stream closed before we got a response, so treat
      // this as a failure to upload.
      let result = tokio::select! {
        () = shutdown.cancelled() => Ok(UploadResult::Canceled),
        response = response_rx => Ok(response
          .map(|result|
            if result.success {
              UploadResult::Success
            } else {
              UploadResult::Failure
            })
          .unwrap_or(UploadResult::Failure))
      };

      if matches!(result, Ok(UploadResult::Failure)) {
        log_upload_failure.inc();
      }

      result
    })
  }
}

//
// RetryPolicy
//

// The retry policy used to retry log uploads. For now we use a static number of attempts, but we
// may expand this to be more sophisticated.
#[derive(Clone, Debug)]
struct RetryPolicy {
  attempts: u32,
  max_retries: IntWatch<bd_runtime::runtime::log_upload::RetryCountFlag>,
  backoff: Option<backoff::ExponentialBackoff>,
  backoff_provider: BackoffProvider,

  // Tracks how often we give up on a request due to the retry limit being exceeded.
  retry_limit_exceeded: Counter,
  // Tracks how many logs we lose due to giving up on a request due to the retry limit being
  // exceeded.
  retry_limit_exceeded_dropped_logs: Counter,
}

impl RetryPolicy {
  fn should_retry(&self) -> bool {
    self.attempts < *self.max_retries.read()
  }

  fn remaining(&self) -> u32 {
    *self.max_retries.read() - self.attempts
  }
}

impl tower::retry::Policy<UploadRequest, UploadResult, Infallible> for RetryPolicy {
  type Future = futures_util::future::BoxFuture<'static, ()>;

  fn retry(
    &mut self,
    req: &mut UploadRequest,
    result: &mut std::result::Result<UploadResult, Infallible>,
  ) -> Option<Self::Future> {
    log::debug!(
      "considering request with {} logs, {} attempts left",
      req.log_upload.logs.len(),
      self.remaining()
    );

    if !self.should_retry() {
      self.retry_limit_exceeded.inc();
      self
        .retry_limit_exceeded_dropped_logs
        .inc_by(req.log_upload.logs.len() as u64);
      return None;
    }

    let backoff = self
      .backoff
      .get_or_insert_with(|| self.backoff_provider.backoff());
    let backoff_delay = backoff.next_backoff().unwrap();
    self.attempts += 1;

    match result {
      Ok(UploadResult::Failure) => Some(Box::pin(async move {
        log::trace!(
          "waiting for {}ms for next attempt",
          backoff_delay.as_millis()
        );
        tokio::time::sleep(backoff_delay).await;
      })),
      _ => None,
    }
  }

  fn clone_request(&mut self, req: &UploadRequest) -> Option<UploadRequest> {
    Some(req.clone())
  }
}

//
// BackoffProvider
//

#[derive(Clone, Debug)]
struct BackoffProvider {
  initial_backoff: DurationWatch<bd_runtime::runtime::log_upload::RetryBackoffInitialFlag>,
  max_backoff: DurationWatch<bd_runtime::runtime::log_upload::RetryBackoffMaxFlag>,
}

impl BackoffProvider {
  fn new(runtime: &ConfigLoader) -> anyhow::Result<Self> {
    Ok(Self {
      initial_backoff: runtime.register_watch()?,
      max_backoff: runtime.register_watch()?,
    })
  }

  fn backoff(&self) -> backoff::ExponentialBackoff {
    backoff::ExponentialBackoffBuilder::new()
      .with_initial_interval(self.initial_backoff.read().unsigned_abs())
      .with_max_interval(self.max_backoff.read().unsigned_abs())
      .with_max_elapsed_time(None)
      .build()
  }
}
