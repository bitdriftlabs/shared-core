use crate::global_state::Tracker;
use bd_device::Store;
use bd_log_primitives::LogField;
use bd_test_helpers::session::InMemoryStorage;
use std::sync::Arc;

#[test]
fn global_state_update() {
  let mut state_tracker = Tracker::new(Arc::new(Store::new(Box::<InMemoryStorage>::default())));

  assert!(state_tracker.global_state_fields().is_empty());

  let fields = vec![
    LogField {
      key: "key2".into(),
      value: "value2".into(),
    },
    LogField {
      key: "key".into(),
      value: "value".into(),
    },
  ];
  assert!(state_tracker.maybe_update_global_state(&fields));
  assert!(!state_tracker.maybe_update_global_state(&fields));

  assert_eq!(state_tracker.global_state_fields(), fields);
  assert_eq!(state_tracker.current_global_state, *fields);

  let updated_fields = vec![LogField {
    key: "key".into(),
    value: "value".into(),
  }];

  assert!(state_tracker.maybe_update_global_state(&updated_fields));
  assert!(!state_tracker.maybe_update_global_state(&updated_fields));

  assert_eq!(state_tracker.global_state_fields(), updated_fields);
  assert_eq!(state_tracker.current_global_state, *updated_fields);

  let updated_fields = vec![LogField {
    key: "key".into(),
    value: b"value".to_vec().into(),
  }];

  assert!(state_tracker.maybe_update_global_state(&updated_fields));
  assert!(!state_tracker.maybe_update_global_state(&updated_fields));

  assert_eq!(state_tracker.global_state_fields(), vec![]);
}
