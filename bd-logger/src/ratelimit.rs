// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./ratelimit_test.rs"]
mod ratelimit_test;

use bd_runtime::runtime::{ConfigLoader, DurationWatch, IntWatch};
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use std::convert::Infallible;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::time::Instant;
use tower::{Layer, Service};

/// A rate of requests per time period backed by feature flags.
#[derive(Debug, Clone)]
pub struct Rate {
  num: IntWatch<bd_runtime::runtime::log_upload::RatelimitByteCountPerPeriodFlag>,
  per: DurationWatch<bd_runtime::runtime::log_upload::RatelimitPeriodFlag>,
}

impl Rate {
  /// Create a new rate.
  pub fn new(runtime_loader: &ConfigLoader) -> Self {
    Self {
      num: runtime_loader.register_int_watch(),
      per: runtime_loader.register_duration_watch(),
    }
  }

  pub(crate) fn num(&self) -> u32 {
    *self.num.read()
  }

  pub(crate) fn per(&self) -> time::Duration {
    *self.per.read()
  }
}

/// Enforces a rate limit on the number of requests the underlying service can handle over a period
/// of time.
#[derive(Debug, Clone)]
pub struct RateLimit<T> {
  // The inner service.
  inner: T,

  // This ratelimit instance is cloned between all the service clones, so in order to provide
  // global ratelimiting between all of them we need some shared state. We only use a single
  // thread, so in theory it would be nice to avoid the complexities that a Mutex brings, but
  // BoxedCloneService requires services to be Send, which prohibits the use of something like an
  // Rc to share state between same-thread services.
  shared_state: Arc<Mutex<SharedState>>,
}

// The shared state between cloned services. This tracks both the quota left in the rate limit as
// well as when the ratelimit quota should be refilled.
#[derive(Debug)]
struct SharedState {
  state: State,
  sleep_until: Instant,

  // The rate which specifies how many requests can be sent within a specific time interval.
  rate: Rate,
}

/// Tracks the state of the rate limiter, i.e. whether it will accept new requests.
#[derive(Debug)]
enum State {
  // The service has hit its limit
  Limited,
  // The service is ready and can serve more requests
  Ready { rem: u32 },
}

impl State {
  pub const fn new(rem: u32) -> Self {
    Self::Ready { rem }
  }
}

impl<T> RateLimit<T> {
  /// Create a new rate limiter
  const fn new(inner: T, shared_state: Arc<Mutex<SharedState>>) -> Self {
    Self {
      inner,
      shared_state,
    }
  }
}

impl<S, Request: 'static + RequestSized + Send> Service<Request> for RateLimit<S>
where
  S: Service<Request> + 'static + Send,
  <S as Service<Request>>::Error: From<Infallible>,
  <S as Service<Request>>::Future: Send,
{
  type Response = S::Response;
  type Error = S::Error;
  type Future = BoxFuture<'static, std::result::Result<Self::Response, Self::Error>>;

  fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
    // Delegate readiness to the inner service. We rely purely on work done in call() to handle
    // ratelimiting.
    // Note that this implementation means that we are not able to use tower's backpressure
    // mechanism, as it relies on reacting to the wrapper service declaring itself pending. This
    // is not a problem right now but if we ever want to use load_shed() or something similar
    // we'll have to rework this.
    self.inner.poll_ready(cx)
  }

  fn call(&mut self, request: Request) -> Self::Future {
    let request_size = request.per_request_size();
    let call_future = self.inner.call(request);

    let state = self.shared_state.clone();

    // We defer all the work to a future which async loops until it is able to acquire a ratelimit
    // permit, checking every time the quota has been filled.
    Box::pin(async move {
      loop {
        // If this doesn't succeed, we are limited and we should wait for sleep_until before we
        // check again.
        if try_acquire_ratelimit_permit(request_size, &state) {
          log::debug!("ratelimit permit acquired, executing inner service");
          return call_future.await;
        }

        log::debug!("ratelimit limits hit, sleeping");

        let sleep_until = state.lock().sleep_until;
        tokio::time::sleep_until(sleep_until).await;
      }
    })
  }
}

/// Attempts to acquire a ratelimit permit from the shared state.
fn try_acquire_ratelimit_permit(request_size: u32, state: &Mutex<SharedState>) -> bool {
  let mut l = state.lock();

  // First check to see if time has exceeded the next fill instant. If so, we reset the remainder
  // and specify when the next fill interval is.
  // This implementation isn't perfect, as we might process a fill later than the fill time. This
  // makes the math a bit sloppy but doesn't fundamentally change the behavior of the rate limiting.
  let now = Instant::now();
  if l.sleep_until < now {
    l.state = State::Ready { rem: l.rate.num() };
    l.sleep_until = now + l.rate.per().unsigned_abs();
  }

  match &mut l.state {
    // We have exceeded our rate for this period, immediate rejection.
    State::Limited => false,
    State::Ready { rem } => {
      // Adjust the remainder based on the size of the request.
      if *rem > request_size {
        *rem -= request_size;
      } else {
        // We've hit our limit, so mark the service as Limited, which should reject all requests
        // until the next fill interval.

        // Note that in this case we still permit the request through, even though the size is
        // bigger than the remainder. This makes the math a little bit inaccurate, but ensures that
        // we won't get into a state where a request which exceeds the max quota never gets through.
        l.state = State::Limited;
      }
      true
    },
  }
}

/// Describes how "big" each request is, allowing for rate limiting on more complex values than just
/// the number of requests.
/// For logs being uploaded, this will be equal to the size of the logs in the request in bytes, but
/// we keep this generic to simplify using a different request type in test.
pub trait RequestSized {
  /// How "big" a request is, for some definition of big.
  fn per_request_size(&self) -> u32;
}

/// Enforces a rate limit on the number of requests the underlying service can handle over a period
/// of time.
#[derive(Debug, Clone)]
pub struct RateLimitLayer {
  state_with_sleep: Arc<Mutex<SharedState>>,
}

impl RateLimitLayer {
  /// Create new rate limit layer.
  pub fn new(rate: Rate) -> Self {
    let until = Instant::now();

    let rate_num = rate.num();

    Self {
      state_with_sleep: Arc::new(Mutex::new(SharedState {
        state: State::new(rate_num),
        // The sleep won't actually be used with this duration, but
        // we create it eagerly so that we can reset it in place rather than
        // `Box::pin`ning a new `Sleep` every time we need one.
        sleep_until: until,
        rate,
      })),
    }
  }
}

impl<S> Layer<S> for RateLimitLayer {
  type Service = RateLimit<S>;

  fn layer(&self, service: S) -> Self::Service {
    RateLimit::new(service, self.state_with_sleep.clone())
  }
}
