// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use anyhow::bail;
use backoff::backoff::Backoff;
use futures::Future;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep_until};

const DEFAULT_BUDGET: f64 = 0.1;

//
// RetryConfig
//

#[derive(Clone, Copy, Debug, Default)]
pub struct RetryConfig {
  pub budget: Option<f64>,
  pub max_retries: Option<u32>,
}

//
// LockedData
//

#[derive(Default)]
struct LockedData {
  active_requests: u64,
  active_retries: u64,
}

//
// ActiveRequest
//

struct ActiveRequest<'a> {
  locked_data: &'a Mutex<LockedData>,
  retry_active: bool,
}

impl ActiveRequest<'_> {
  fn finish_retry(&mut self) {
    if self.retry_active {
      let mut locked_data = self.locked_data.lock();
      debug_assert!(locked_data.active_retries > 0);
      locked_data.active_retries -= 1;
      self.retry_active = false;
    }
  }
}

impl Drop for ActiveRequest<'_> {
  fn drop(&mut self) {
    self.finish_retry();
    let mut locked_data = self.locked_data.lock();
    debug_assert!(locked_data.active_requests > 0);
    locked_data.active_requests -= 1;
  }
}

// Retry budgets are fractional, so admission compares counters in the configured float domain.
#[allow(clippy::cast_precision_loss)]
fn retry_budget_available(active_retries: u64, active_requests: u64, budget: f64) -> bool {
  (active_retries as f64) < active_requests as f64 * budget
}

//
// Retry
//

pub struct Retry {
  config: RetryConfig,
  locked_data: Mutex<LockedData>,
}

impl Retry {
  pub fn new(config: RetryConfig) -> anyhow::Result<Arc<Self>> {
    let budget = config.budget.unwrap_or(DEFAULT_BUDGET);
    if !budget.is_finite() || budget <= 0.0 || budget > 1.0 {
      bail!("retry budget must be between > 0.0 and <= 1.0");
    }

    Ok(Arc::new(Self {
      config,
      locked_data: Mutex::default(),
    }))
  }

  async fn maybe_retry(
    &self,
    retry_count: &mut u32,
    backoff: &mut impl Backoff,
    notify: &mut impl FnMut(),
    retry_after: Option<Duration>,
    deadline: Option<Instant>,
    request: &mut ActiveRequest<'_>,
  ) -> bool {
    *retry_count += 1;
    if self
      .config
      .max_retries
      .is_some_and(|max_retries| *retry_count > max_retries)
    {
      log::debug!("no further retries available (max retries)");
      return false;
    }

    let Some(backoff) = backoff.next_backoff() else {
      log::debug!("no further retries available (backoff exhausted)");
      return false;
    };

    let delay = retry_after.map_or(backoff, |delay| delay.max(backoff));
    let Some(wakeup) = Instant::now().checked_add(delay) else {
      log::debug!("retry delay exceeds the clock range");
      return false;
    };
    if deadline.is_some_and(|deadline| wakeup >= deadline) {
      log::debug!("retry delay exceeds the remaining delivery budget");
      return false;
    }

    if !{
      let mut locked_data = self.locked_data.lock();
      if retry_budget_available(
        locked_data.active_retries,
        locked_data.active_requests,
        self.budget(),
      ) {
        log::debug!("doing retry");
        locked_data.active_retries += 1;
        true
      } else {
        log::debug!("retry budget not available");
        false
      }
    } {
      return false;
    }

    request.retry_active = true;
    notify();
    if !delay.is_zero() {
      sleep_until(wakeup).await;
    }
    log::debug!("retry sleep complete");
    true
  }

  #[must_use]
  pub fn budget(&self) -> f64 {
    self.config.budget.unwrap_or(DEFAULT_BUDGET)
  }

  pub async fn retry_notify<T, E, FutureType: Future<Output = Result<T, backoff::Error<E>>>>(
    &self,
    backoff: impl Backoff,
    operation: impl FnMut() -> FutureType,
    notify: impl FnMut(),
  ) -> Result<T, E> {
    self
      .retry_notify_until(backoff, operation, notify, None)
      .await
  }

  pub async fn retry_notify_until<
    T,
    E,
    FutureType: Future<Output = Result<T, backoff::Error<E>>>,
  >(
    &self,
    mut backoff: impl Backoff,
    mut operation: impl FnMut() -> FutureType,
    mut notify: impl FnMut(),
    deadline: Option<Instant>,
  ) -> Result<T, E> {
    self.locked_data.lock().active_requests += 1;
    let mut request = ActiveRequest {
      locked_data: &self.locked_data,
      retry_active: false,
    };
    let mut retry_count = 0;
    loop {
      let result = operation().await;
      request.finish_retry();

      match result {
        Ok(result) => break Ok(result),
        Err(backoff::Error::Permanent(error)) => break Err(error),
        Err(backoff::Error::Transient { err, retry_after }) => {
          if !self
            .maybe_retry(
              &mut retry_count,
              &mut backoff,
              &mut notify,
              retry_after,
              deadline,
              &mut request,
            )
            .await
          {
            break Err(err);
          }
        },
      }
    }
  }
}
