#[cfg(test)]
#[path = "./wasm_test.rs"]
pub mod wasm_test;

use crate::{MissedTickBehavior, TimeDurationExt};
use futures::task::AtomicWaker;
use js_sys::Object;
use rand::{rng, Rng};
use std::future::{poll_fn, Future, IntoFuture};
use std::ops::{Add, Sub};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;
use wasm_bindgen::prelude::{wasm_bindgen, Closure};
use wasm_bindgen::JsCast;

#[wasm_bindgen]
extern "C" {
  pub type GlobalScope;

  pub type Performance;

  #[wasm_bindgen(structural, method, getter, js_name = "performance")]
  pub fn performance(this: &GlobalScope) -> Performance;

  #[wasm_bindgen(method, js_name = "now")]
  pub fn now(this: &Performance) -> f64;

  #[wasm_bindgen(catch , method, js_name = setTimeout)]
  pub fn set_timeout_with_callback_and_timeout_and_arguments_0(
    this: &GlobalScope,
    handler: &::js_sys::Function,
    timeout: i32,
  ) -> Result<wasm_bindgen::JsValue, wasm_bindgen::JsValue>;
}

pub fn performance_now() -> f64 {
  let global_this: Object = js_sys::global();
  let global_scope = global_this.unchecked_ref::<GlobalScope>();
  global_scope.performance().now()
}

pub fn set_timeout(
  handler: &::js_sys::Function,
  timeout: i32,
) -> Result<wasm_bindgen::JsValue, wasm_bindgen::JsValue> {
  let global_this: Object = js_sys::global();
  let global_scope = global_this.unchecked_ref::<GlobalScope>();
  global_scope.set_timeout_with_callback_and_timeout_and_arguments_0(handler, timeout)
}

//
// Instant
//

#[derive(Clone, Copy)]
pub struct Instant {
  inner: f64,
}

impl Instant {
  pub fn now() -> Instant {
    Instant {
      inner: performance_now() / 1000.0,
    }
  }

  pub fn elapsed(&self) -> Duration {
    Duration::from_secs_f64(performance_now() / 1000.0 - self.inner)
  }
}

impl Add<Duration> for Instant {
  type Output = Instant;

  fn add(self, rhs: Duration) -> Instant {
    Instant {
      inner: self.inner + rhs.as_secs_f64(),
    }
  }
}

impl Sub<Instant> for Instant {
  type Output = Duration;

  fn sub(self, rhs: Instant) -> Duration {
    Duration::from_secs_f64(self.inner - rhs.inner)
  }
}

//
// Sleep
//

struct SleepInner {
  waker: AtomicWaker,
  called: AtomicBool,
}
pub struct Sleep {
  inner: Arc<SleepInner>,
}

impl Sleep {
  fn new(duration: Duration) -> Self {
    let inner = Arc::new(SleepInner {
      waker: AtomicWaker::new(),
      called: AtomicBool::new(false),
    });

    let cloned_inner = inner.clone();
    set_timeout(
      Closure::once_into_js(move || {
        cloned_inner.called.store(true, Ordering::Relaxed);
        cloned_inner.waker.wake();
      })
      .unchecked_ref(),
      i32::try_from(duration.as_millis()).unwrap_or(0),
    )
    .unwrap();

    Self { inner }
  }
}

impl Future for Sleep {
  type Output = ();

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    if self.inner.called.load(Ordering::Relaxed) {
      return Poll::Ready(());
    }

    self.inner.waker.register(cx.waker());

    if self.inner.called.load(Ordering::Relaxed) {
      Poll::Ready(())
    } else {
      Poll::Pending
    }
  }
}

//
// Interval
//

pub struct Interval {
  period: Duration,
  sleep: Pin<Box<Sleep>>,
}

impl Interval {
  fn new(period: Duration, first_tick_offset: Duration) -> Self {
    Self {
      period,
      sleep: Box::pin(Sleep::new(first_tick_offset)),
    }
  }

  pub async fn tick(&mut self) {
    poll_fn(|cx| self.poll_tick(cx)).await;
  }

  fn poll_tick(&mut self, cx: &mut Context<'_>) -> Poll<()> {
    ready!(self.sleep.as_mut().poll(cx));

    // fixfix account for drift.
    self.sleep = Box::pin(Sleep::new(self.period));

    Poll::Ready(())
  }

  pub fn period(&self) -> Duration {
    self.period
  }
}

//
// Timeout
//

pub struct Timeout<F: Future> {
  _f: F,
}

impl TimeDurationExt for time::Duration {
  fn advance(self) -> impl Future<Output = ()> {
    unimplemented!("fake time not supported in wasm");
    #[allow(unreachable_code)] // Spurious
    async {}
  }

  fn sleep(self) -> Sleep {
    Sleep::new(self.unsigned_abs())
  }

  fn interval(self, _behavior: MissedTickBehavior) -> Interval {
    Interval::new(self.unsigned_abs(), Duration::ZERO)
  }

  fn interval_at(self, _behavior: MissedTickBehavior) -> Interval {
    Interval::new(self.unsigned_abs(), self.unsigned_abs())
  }

  fn jittered_interval_at(self, _behavior: MissedTickBehavior) -> Interval {
    let millis: u64 = self.whole_milliseconds().try_into().unwrap();
    let jittered = Duration::from_millis(rng().random_range(0 ..= millis));
    Interval::new(self.unsigned_abs(), jittered)
  }

  fn timeout<F: IntoFuture>(self, f: F) -> Timeout<F::IntoFuture> {
    //tokio::time::timeout(self.unsigned_abs(), f) fixfix
    Timeout {
      _f: f.into_future(),
    }
  }

  fn add_now(self) -> Instant {
    Instant::now() + self.unsigned_abs()
  }

  fn add_instant(self, instant: Instant) -> Instant {
    instant + self.unsigned_abs()
  }
}
