// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_event_buffer::{
  AdmissionContext,
  AdmissionOutcome,
  EventBuffer,
  EventBufferEntry,
  EventBufferLimits,
  EventContext,
  LoggerIngressEvent,
  ProviderSnapshot,
};
use bd_log_primitives::{AnnotatedLogFields, LogFields, LogLine, log_level};
use bd_macros::ApproximateSize;
use bd_proto::protos::logging::payload::LogType;
use time::OffsetDateTime;

//
// AdmissionSetup
//

/// An admission setup shared by the instruction-count and wall-clock benchmarks.
pub struct AdmissionSetup {
  buffer: EventBuffer,
  incoming: Option<EventBufferEntry>,
}

impl AdmissionSetup {
  pub fn admit(&mut self) -> AdmissionOutcome {
    self.buffer.admit(
      self
        .incoming
        .take()
        .expect("each benchmark setup admits exactly one entry"),
    )
  }
}

fn current_process_context() -> EventContext {
  EventContext::CurrentProcess(AdmissionContext {
    session_id: String::new().into(),
    provider: ProviderSnapshot {
      timestamp: OffsetDateTime::UNIX_EPOCH,
      ootb_fields: LogFields::default(),
      custom_fields: LogFields::default(),
    },
    admitted_at: OffsetDateTime::UNIX_EPOCH,
  })
}

fn log(
  level: bd_log_primitives::LogLevel,
  bytes: usize,
  context: EventContext,
) -> EventBufferEntry {
  EventBufferEntry::ingress(LoggerIngressEvent::log(
    LogLine {
      log_level: level,
      log_type: LogType::NORMAL,
      message: "x".repeat(bytes).into(),
      fields: AnnotatedLogFields::default(),
      matching_fields: AnnotatedLogFields::default(),
      attributes_overrides: None,
      capture_session: None,
    },
    context,
    None,
  ))
}

fn low_log(bytes: usize) -> EventBufferEntry {
  log(log_level::DEBUG, bytes, current_process_context())
}

fn high_log(bytes: usize) -> EventBufferEntry {
  log(log_level::INFO, bytes, current_process_context())
}

fn protected_log(bytes: usize) -> EventBufferEntry {
  // Previous-process logs use the protected lane even when their payload is an ordinary log.
  log(
    log_level::INFO,
    bytes,
    EventContext::PreviousProcess {
      logged_at: OffsetDateTime::UNIX_EPOCH,
    },
  )
}

fn limits(log_limit_bytes: usize, total_limit_bytes: usize) -> EventBufferLimits {
  EventBufferLimits {
    log_limit_bytes,
    total_limit_bytes,
  }
}

fn assert_admitted(buffer: &EventBuffer, entry: EventBufferEntry) {
  assert_eq!(AdmissionOutcome::Admitted, buffer.admit(entry));
}

pub fn insertion_setup() -> AdmissionSetup {
  let incoming = high_log(0);
  let size = incoming.approximate_size_bytes();

  AdmissionSetup {
    buffer: EventBuffer::new(limits(size, size)),
    incoming: Some(incoming),
  }
}

pub fn insertion_with_capacity_setup() -> AdmissionSetup {
  let entry_size = high_log(0).approximate_size_bytes();
  let buffer = EventBuffer::new(limits(8 * entry_size, 8 * entry_size));
  assert_admitted(&buffer, high_log(0));

  AdmissionSetup {
    buffer,
    incoming: Some(high_log(0)),
  }
}

pub fn ingress_and_insertion_setup(bytes: usize) -> EventBuffer {
  let size = high_log(bytes).approximate_size_bytes();
  EventBuffer::new(limits(size, size))
}

pub fn single_victim_setup() -> AdmissionSetup {
  let low_size = low_log(0).approximate_size_bytes();
  let high_base_size = high_log(0).approximate_size_bytes();
  let buffer = EventBuffer::new(limits(low_size, low_size));
  assert_admitted(&buffer, low_log(0));

  AdmissionSetup {
    buffer,
    incoming: Some(high_log(low_size.saturating_sub(high_base_size))),
  }
}

pub fn multiple_victims_setup() -> AdmissionSetup {
  const VICTIM_COUNT: usize = 8;

  let low_size = low_log(0).approximate_size_bytes();
  let high_base_size = high_log(0).approximate_size_bytes();
  let limit = VICTIM_COUNT * low_size;
  let buffer = EventBuffer::new(limits(limit, limit));
  for _ in 0 .. VICTIM_COUNT {
    assert_admitted(&buffer, low_log(0));
  }

  AdmissionSetup {
    buffer,
    incoming: Some(high_log(limit.saturating_sub(high_base_size))),
  }
}

pub fn multi_lane_eviction_setup() -> AdmissionSetup {
  const PROTECTED_RETAINED_COUNT: usize = 4;
  const LOW_VICTIM_COUNT: usize = 4;
  const HIGH_VICTIM_COUNT: usize = 8;
  const HIGH_EVICTION_COUNT: usize = 2;

  let low_size = low_log(0).approximate_size_bytes();
  let high_size = high_log(0).approximate_size_bytes();
  let protected_size = protected_log(0).approximate_size_bytes();
  let evictable_bytes = LOW_VICTIM_COUNT * low_size + HIGH_VICTIM_COUNT * high_size;
  let total_retained_bytes = PROTECTED_RETAINED_COUNT * protected_size + evictable_bytes;
  let incoming_bytes = LOW_VICTIM_COUNT * low_size + HIGH_EVICTION_COUNT * high_size;
  let buffer = EventBuffer::new(limits(evictable_bytes, total_retained_bytes));
  // Fill the incoming lane's initial allocation before it evicts lower-priority work. This
  // measures the `VecDeque` growth that boxing would make cheaper.
  for _ in 0 .. PROTECTED_RETAINED_COUNT {
    assert_admitted(&buffer, protected_log(0));
  }
  for _ in 0 .. LOW_VICTIM_COUNT {
    assert_admitted(&buffer, low_log(0));
  }
  for _ in 0 .. HIGH_VICTIM_COUNT {
    assert_admitted(&buffer, high_log(0));
  }

  AdmissionSetup {
    buffer,
    incoming: Some(protected_log(incoming_bytes.saturating_sub(protected_size))),
  }
}

pub fn ingress_and_admit(buffer: &EventBuffer, bytes: usize) -> AdmissionOutcome {
  buffer.admit(high_log(bytes))
}
