// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::retention::EventBufferState;
use crate::{EventBufferEntry, StartupGateReleaseRequest};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

//
// StartupGate
//

#[derive(Default)]
pub(crate) struct StartupGate {
  pub ready: bool,
  started_at: Option<Instant>,
  deadline: Option<Instant>,
  delay: Option<watch::Receiver<time::Duration>>,
}

impl StartupGate {
  pub fn start(&mut self, delay: Option<watch::Receiver<time::Duration>>) {
    assert!(self.started_at.is_none(), "startup gate already started");
    self.started_at = Some(Instant::now());
    self.delay = delay;
    self.refresh_delay();
  }

  fn refresh_delay(&mut self) {
    if let Some(delay) = &mut self.delay {
      let deadline = self.started_at.expect("gate started before reading delay")
        + delay.borrow_and_update().unsigned_abs();
      if self.deadline.is_none_or(|current| deadline > current) {
        self.deadline = Some(deadline);
      }
    }
  }

  // Only the consumer calls this. Producers may request release but cannot change the startup
  // partition while the consumer is preparing a previous-process report batch.
  pub fn poll(
    &mut self,
    retention: &mut EventBufferState<EventBufferEntry>,
  ) -> Option<StartupGateOpening> {
    if retention.is_gate_open() || retention.is_closed() {
      return None;
    }
    // Read the watch even when the timer won the wakeup race. An available extension must be
    // observed before making the permanent open transition.
    self.refresh_delay();
    if !self.ready {
      return None;
    }
    let now = Instant::now();
    let reason = if self.delay.is_none() {
      StartupGateReleaseReason::NoPriorCrash
    } else if self.deadline.is_some_and(|deadline| deadline <= now) {
      StartupGateReleaseReason::Timer
    } else if let Some(request) = retention.take_gate_release_request() {
      match request {
        StartupGateReleaseRequest::BlockingFlush => StartupGateReleaseReason::Barrier,
        StartupGateReleaseRequest::ProtectedHighWatermark => {
          StartupGateReleaseReason::HighWatermark
        },
      }
    } else if retention.reaches_protected_high_watermark() {
      StartupGateReleaseReason::HighWatermark
    } else {
      return None;
    };
    let opened = retention.open_gate();
    debug_assert!(opened);
    Some(StartupGateOpening {
      reason,
      hold_duration: self.started_at.map_or(Duration::ZERO, |start| now - start),
    })
  }

  pub fn wait_state(&self) -> (Option<Instant>, Option<watch::Receiver<time::Duration>>) {
    // An elapsed timer before configuration must not spin or release the gate. Readiness and
    // later runtime updates will wake the consumer again.
    (self.deadline.filter(|_| self.ready), self.delay.clone())
  }
}

//
// StartupGateOpening
//

/// The one-time opening of the startup gate, delivered before processing the accompanying batch.
#[derive(Clone, Copy, Debug)]
pub struct StartupGateOpening {
  pub reason: StartupGateReleaseReason,
  pub hold_duration: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupGateReleaseReason {
  NoPriorCrash,
  Timer,
  HighWatermark,
  Barrier,
}

impl StartupGateReleaseReason {
  #[must_use]
  pub const fn label(self) -> &'static str {
    match self {
      Self::NoPriorCrash => "no_prior_crash",
      Self::Timer => "timer",
      Self::HighWatermark => "high_watermark",
      Self::Barrier => "barrier",
    }
  }
}
