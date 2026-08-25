// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{
  AdmissionContext,
  AdmissionOutcome,
  EventBuffer,
  EventBufferEntry,
  EventBufferLimits,
  EventBufferState,
  EventContext,
  LoggerControl,
  LoggerIngressEvent,
  LoggerIngressPayload,
  ProviderSnapshot,
  RetentionLane,
  retention_lane,
};
use bd_log_primitives::{AnnotatedLogFields, DataValue, LogFields, LogLine, log_level};
use bd_macros::ApproximateSize;
use bd_proto::protos::logging::payload::LogType;
use time::OffsetDateTime;
use tokio::sync::oneshot;

fn current_process_context() -> EventContext {
  EventContext::CurrentProcess(AdmissionContext {
    session_id: "session".to_string(),
    provider: ProviderSnapshot {
      timestamp: OffsetDateTime::UNIX_EPOCH,
      ootb_fields: LogFields::default(),
      custom_fields: LogFields::default(),
    },
    admitted_at: OffsetDateTime::UNIX_EPOCH,
  })
}

fn log(level: bd_log_primitives::LogLevel, log_type: LogType, bytes: usize) -> EventBufferEntry {
  log_with_completion(level, log_type, bytes, None)
}

fn log_with_completion(
  level: bd_log_primitives::LogLevel,
  log_type: LogType,
  bytes: usize,
  completion: Option<bd_completion::Sender<()>>,
) -> EventBufferEntry {
  EventBufferEntry::ingress(LoggerIngressEvent::log(
    LogLine {
      log_level: level,
      log_type,
      message: "x".repeat(bytes).into(),
      fields: AnnotatedLogFields::default(),
      matching_fields: AnnotatedLogFields::default(),
      attributes_overrides: None,
      capture_session: None,
    },
    current_process_context(),
    completion,
  ))
}

fn limits(bytes: usize) -> EventBufferLimits {
  EventBufferLimits {
    log_limit_bytes: bytes,
    total_limit_bytes: bytes,
  }
}

fn buffer(bytes: usize) -> EventBuffer {
  EventBuffer::new(limits(bytes))
}

fn add_field(key: &str) -> EventBufferEntry {
  EventBufferEntry::Control(LoggerControl::AddLogField(
    key.to_string(),
    DataValue::String("value".into()),
  ))
}

fn state_limits(log_limit_bytes: usize, total_limit_bytes: usize) -> EventBufferLimits {
  EventBufferLimits {
    log_limit_bytes,
    total_limit_bytes,
  }
}

#[test]
fn priority_mapping_covers_protected_and_evictable_logs() {
  assert_eq!(
    RetentionLane::Low,
    retention_lane(LogType::NORMAL, log_level::DEBUG)
  );
  assert_eq!(
    RetentionLane::High,
    retention_lane(LogType::NORMAL, log_level::INFO)
  );
  assert_eq!(
    RetentionLane::Protected,
    retention_lane(LogType::LIFECYCLE, log_level::TRACE)
  );
}

#[test]
fn feature_flag_exposure_carries_current_process_context_in_the_protected_lane() {
  let entry = EventBufferEntry::ingress(LoggerIngressEvent::feature_flag_exposure(
    "flag".to_string(),
    Some("variant".to_string()),
    match current_process_context() {
      EventContext::CurrentProcess(context) => context,
      EventContext::PreviousProcess { .. } => {
        unreachable!("test helper returns current-process context")
      },
    },
  ));

  assert_eq!(RetentionLane::Protected, entry.lane());
  assert!(matches!(
    entry,
    EventBufferEntry::Ingress(event) if matches!(
      event.as_ref(),
      LoggerIngressEvent {
        context: EventContext::CurrentProcess(AdmissionContext { session_id, .. }),
        payload: LoggerIngressPayload::FeatureFlagExposure { flag, variant },
        ..
      } if session_id == "session" && flag == "flag" && variant.as_deref() == Some("variant")
    )
  ));
}

#[test]
fn previous_process_context_pins_logged_at() {
  let logged_at = OffsetDateTime::UNIX_EPOCH;
  let EventBufferEntry::Ingress(mut event) = log(log_level::INFO, LogType::NORMAL, 1) else {
    unreachable!("log helper creates an ingress event")
  };
  event.context = EventContext::PreviousProcess { logged_at };

  assert!(matches!(
    event.context,
    EventContext::PreviousProcess { logged_at: timestamp } if timestamp == logged_at
  ));
}

#[tokio::test]
async fn preserves_log_and_field_update_admission_order() {
  let buffer = buffer(10_000);
  assert_eq!(
    AdmissionOutcome::Admitted,
    buffer.admit(log(log_level::INFO, LogType::NORMAL, 1))
  );
  assert_eq!(AdmissionOutcome::Admitted, buffer.admit(add_field("field")));
  assert_eq!(
    AdmissionOutcome::Admitted,
    buffer.admit(log(log_level::DEBUG, LogType::NORMAL, 2))
  );

  let entries = buffer
    .next_batch(3)
    .await
    .into_iter()
    .map(|entry| match entry {
      EventBufferEntry::Ingress(event) => match event.payload {
        LoggerIngressPayload::Log(log) => format!("log:{}", log.message),
        LoggerIngressPayload::FeatureFlagExposure { flag, .. } => {
          format!("feature_flag:{flag}")
        },
      },
      EventBufferEntry::Control(LoggerControl::AddLogField(key, _)) => {
        format!("add_field:{key}")
      },
      EventBufferEntry::Control(_) => "other_control".to_string(),
    })
    .collect::<Vec<_>>();
  assert_eq!(vec!["log:x", "add_field:field", "log:xx"], entries);
}

#[tokio::test]
async fn admission_wakes_a_waiting_consumer() {
  let buffer = buffer(10_000);
  let consumer_buffer = buffer.clone();
  let consumer = tokio::spawn(async move { consumer_buffer.next_batch(1).await });

  buffer.wait_for_waiting_consumers(1).await;
  assert_eq!(
    AdmissionOutcome::Admitted,
    buffer.admit(log(log_level::INFO, LogType::NORMAL, 1))
  );

  assert_eq!(
    1,
    consumer.await.expect("consumer task must complete").len()
  );
}

#[tokio::test]
async fn close_wakes_all_waiting_consumers() {
  let buffer = buffer(10_000);
  let first_buffer = buffer.clone();
  let second_buffer = buffer.clone();
  let first = tokio::spawn(async move { first_buffer.next_batch(1).await });
  let second = tokio::spawn(async move { second_buffer.next_batch(1).await });

  buffer.wait_for_waiting_consumers(2).await;
  buffer.close();

  assert!(
    first
      .await
      .expect("first consumer task must complete")
      .is_empty()
  );
  assert!(
    second
      .await
      .expect("second consumer task must complete")
      .is_empty()
  );
}

#[tokio::test]
async fn batch_eviction_removes_tiny_lower_priority_logs() {
  const COUNT: usize = 64;
  let low_size = log(log_level::DEBUG, LogType::NORMAL, 0).approximate_size_bytes();
  let incoming_base = log(log_level::INFO, LogType::NORMAL, 0).approximate_size_bytes();
  let buffer = buffer(COUNT * low_size);
  for _ in 0 .. COUNT {
    assert_eq!(
      AdmissionOutcome::Admitted,
      buffer.admit(log(log_level::DEBUG, LogType::NORMAL, 0))
    );
  }
  assert_eq!(
    AdmissionOutcome::Admitted,
    buffer.admit(log(
      log_level::INFO,
      LogType::NORMAL,
      COUNT * low_size - incoming_base
    ))
  );

  let batch = buffer.next_batch(COUNT + 1).await;
  assert_eq!(1, batch.len());
  let EventBufferEntry::Ingress(event) = &batch[0] else {
    panic!("expected an admitted log");
  };
  assert!(matches!(
    &event.payload,
    LoggerIngressPayload::Log(log) if log.log_level == log_level::INFO
  ));
}

#[tokio::test]
async fn rejected_admission_does_not_partially_evict() {
  let low = log(log_level::DEBUG, LogType::NORMAL, 1);
  let low_size = low.approximate_size_bytes();
  let protected = log(log_level::WARNING, LogType::LIFECYCLE, 1);
  let total = low.approximate_size_bytes() + protected.approximate_size_bytes();
  let incoming_base = log(log_level::INFO, LogType::NORMAL, 0).approximate_size_bytes();
  let buffer = EventBuffer::new(EventBufferLimits {
    log_limit_bytes: usize::MAX,
    total_limit_bytes: total,
  });
  assert_eq!(AdmissionOutcome::Admitted, buffer.admit(low));
  assert_eq!(AdmissionOutcome::Admitted, buffer.admit(protected));
  assert_eq!(
    AdmissionOutcome::RejectedFull,
    buffer.admit(log(
      log_level::INFO,
      LogType::NORMAL,
      low_size + 1 - incoming_base
    ))
  );

  assert_eq!(2, buffer.next_batch(2).await.len());
}

#[tokio::test]
async fn rejected_and_evicted_entries_drop_completion_senders() {
  let (rejected_sender, rejected_receiver) = bd_completion::Sender::new();
  assert_eq!(
    AdmissionOutcome::RejectedOversized,
    buffer(0).admit(log_with_completion(
      log_level::INFO,
      LogType::NORMAL,
      0,
      Some(rejected_sender),
    ))
  );
  assert!(rejected_receiver.recv().await.is_err());

  let (evicted_sender, evicted_receiver) = bd_completion::Sender::new();
  let low = log_with_completion(log_level::DEBUG, LogType::NORMAL, 0, Some(evicted_sender));
  let high = log(log_level::INFO, LogType::NORMAL, 0);
  let buffer = buffer(
    low
      .approximate_size_bytes()
      .max(high.approximate_size_bytes()),
  );
  assert_eq!(AdmissionOutcome::Admitted, buffer.admit(low));
  assert_eq!(AdmissionOutcome::Admitted, buffer.admit(high));
  assert!(evicted_receiver.recv().await.is_err());
}

#[tokio::test]
async fn completion_is_sent_only_after_consumer_processing() {
  let buffer = buffer(10_000);
  let (sender, receiver) = bd_completion::Sender::new();
  let entry = EventBufferEntry::ingress(LoggerIngressEvent::log(
    LogLine {
      log_level: log_level::INFO,
      log_type: LogType::NORMAL,
      message: "x".into(),
      fields: AnnotatedLogFields::default(),
      matching_fields: AnnotatedLogFields::default(),
      attributes_overrides: None,
      capture_session: None,
    },
    current_process_context(),
    Some(sender),
  ));
  assert_eq!(AdmissionOutcome::Admitted, buffer.admit(entry));
  let mut batch = buffer.next_batch(1).await;
  assert_eq!(1, batch.len());
  let entry = batch.remove(0);
  let (receiver_started_tx, receiver_started_rx) = oneshot::channel();
  let completion = tokio::spawn(async move {
    let _ignored = receiver_started_tx.send(());
    receiver.recv().await
  });
  assert!(receiver_started_rx.await.is_ok());
  assert!(!completion.is_finished());

  entry.complete();
  assert!(completion.await.is_ok_and(|result| result.is_ok()));
}

#[tokio::test]
async fn close_drops_unprocessed_completion_senders() {
  let buffer = buffer(10_000);
  let (sender, receiver) = bd_completion::Sender::new();
  let entry = EventBufferEntry::ingress(LoggerIngressEvent::log(
    LogLine {
      log_level: log_level::INFO,
      log_type: LogType::NORMAL,
      message: "x".into(),
      fields: AnnotatedLogFields::default(),
      matching_fields: AnnotatedLogFields::default(),
      attributes_overrides: None,
      capture_session: None,
    },
    current_process_context(),
    Some(sender),
  ));
  assert_eq!(AdmissionOutcome::Admitted, buffer.admit(entry));
  buffer.close();
  assert!(receiver.recv().await.is_err());
  assert!(buffer.next_batch(1).await.is_empty());
}

#[test]
fn pending_log_limit_shrink_evicts_newest_low_then_high_entries() {
  let mut state = EventBufferState::new(state_limits(100, 100));
  for (lane, entry) in [
    (RetentionLane::Low, "low_old"),
    (RetentionLane::Low, "low_new"),
    (RetentionLane::High, "high_old"),
    (RetentionLane::High, "high_new"),
  ] {
    assert_eq!(
      AdmissionOutcome::Admitted,
      state.admit(lane, 10, entry).outcome()
    );
  }

  // Limit updates are deferred until admission so configuration changes do not contend with the
  // producer path until there is real work to do.
  state.set_pending_limits(state_limits(15, 100));
  assert_eq!(
    AdmissionOutcome::Admitted,
    state
      .admit(RetentionLane::Protected, 0, "trigger")
      .outcome()
  );

  assert_eq!(vec!["high_old", "trigger"], state.take_batch(4));
}

#[test]
fn pending_total_limit_shrink_preserves_protected_entries() {
  let mut state = EventBufferState::new(state_limits(200, 200));
  assert_eq!(
    AdmissionOutcome::Admitted,
    state
      .admit(RetentionLane::Protected, 100, "protected")
      .outcome()
  );
  assert_eq!(
    AdmissionOutcome::Admitted,
    state.admit(RetentionLane::Low, 10, "low").outcome()
  );

  state.set_pending_limits(state_limits(200, 50));
  assert_eq!(
    AdmissionOutcome::RejectedFull,
    state
      .admit(RetentionLane::Protected, 0, "trigger")
      .outcome()
  );

  // Protected entries are never evicted, even if they alone exceed a newly reduced total limit.
  assert_eq!(vec!["protected"], state.take_batch(3));
}

#[test]
fn latest_pending_limit_update_wins() {
  let mut state = EventBufferState::new(state_limits(100, 100));
  for entry in ["old", "middle", "new"] {
    assert_eq!(
      AdmissionOutcome::Admitted,
      state.admit(RetentionLane::Low, 10, entry).outcome()
    );
  }

  state.set_pending_limits(state_limits(25, 100));
  state.set_pending_limits(state_limits(15, 100));
  assert_eq!(
    AdmissionOutcome::Admitted,
    state
      .admit(RetentionLane::Protected, 0, "trigger")
      .outcome()
  );

  assert_eq!(vec!["old", "trigger"], state.take_batch(4));
}
