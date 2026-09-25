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
use tokio::time::sleep;

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

    notify();
    if !backoff.is_zero() {
      sleep(backoff).await;
    }
    log::debug!("retry sleep complete");
    true
  }

  #[must_use]
  pub fn budget(&self) -> f64 {
    self.config.budget.unwrap_or(DEFAULT_BUDGET)
  }

  #[allow(unused_assignments)]
  pub async fn retry_notify<T, E, FutureType: Future<Output = Result<T, backoff::Error<E>>>>(
    &self,
    mut backoff: impl Backoff,
    mut operation: impl FnMut() -> FutureType,
    mut notify: impl FnMut(),
  ) -> Result<T, E> {
    self.locked_data.lock().active_requests += 1;
    let mut doing_retry = false;
    let mut retry_count = 0;
    let result = loop {
      let result = operation().await;
      if doing_retry {
        let mut locked_data = self.locked_data.lock();
        debug_assert!(locked_data.active_retries > 0);
        locked_data.active_retries -= 1;
        doing_retry = false;
      }

      match result {
        Ok(result) => break Ok(result),
        Err(backoff::Error::Permanent(error)) => break Err(error),
        Err(backoff::Error::Transient { err, .. }) => {
          if self
            .maybe_retry(&mut retry_count, &mut backoff, &mut notify)
            .await
          {
            doing_retry = true;
          } else {
            break Err(err);
          }
        },
      }
    };

    let mut locked_data = self.locked_data.lock();
    debug_assert!(locked_data.active_requests > 0);
    locked_data.active_requests -= 1;
    result
  }
}
