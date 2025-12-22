// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::activity_based::{self, Callbacks, STATE_KEY, Store};
use bd_key_value::Storage;
use bd_proto::protos::client::key_value::ActivitySessionStrategyState;
use bd_time::{OffsetDateTimeExt as _, TestTimeProvider};
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use std::sync::Arc;
use time::ext::NumericalDuration;
use time::{Duration, OffsetDateTime};

//
// MockCallbacks
//

#[derive(Default)]
struct MockCallbacks {
  session_id_changes: parking_lot::Mutex<Vec<String>>,
}

impl MockCallbacks {
  fn clear(&self) {
    self.session_id_changes.lock().clear();
  }
}

impl Callbacks for MockCallbacks {
  fn session_id_changed(&self, session_id: &str) {
    self.session_id_changes.lock().push(session_id.to_string());
  }
}

//
// MockStorage
//

#[derive(Default)]
pub struct MockStorage {
  state: parking_lot::Mutex<HashMap<String, String>>,
}

impl Storage for MockStorage {
  fn set_string(&self, key: &str, value: &str) -> anyhow::Result<()> {
    let mut guard = self.state.lock();
    let mut state = guard.clone();
    state.insert(key.to_string(), value.to_string());
    *guard = state;

    Ok(())
  }

  fn get_string(&self, key: &str) -> anyhow::Result<Option<String>> {
    Ok(self.state.lock().get(key).cloned())
  }

  fn delete(&self, key: &str) -> anyhow::Result<()> {
    self.state.lock().remove(key);
    Ok(())
  }
}

#[test]
fn generates_new_session_and_stores_it_if_none_exists() {
  let now = OffsetDateTime::now_utc();
  let store = Arc::new(Store::new(Box::<MockStorage>::default()));
  let callbacks = Arc::new(MockCallbacks::default());

  let strategy = activity_based::Strategy::new(
    Duration::seconds(30),
    store.clone(),
    callbacks.clone(),
    Arc::new(TestTimeProvider::new(now)),
  );

  assert!(strategy.previous_process_session_id().is_none());

  let session_id = strategy.session_id();

  assert_eq!(1, callbacks.session_id_changes.lock().len());
  assert_eq!(session_id, callbacks.session_id_changes.lock()[0]);
  assert_eq!(
    ActivitySessionStrategyState {
      session_id,
      last_activity_timestamp: now.into_proto(),
      ..Default::default()
    },
    store.as_ref().get(&STATE_KEY).unwrap(),
  );
  assert!(strategy.previous_process_session_id().is_none());
}

#[test]
fn generates_new_session_and_stores_it_if_old_exceeded_inactivity_threshold() {
  let now = OffsetDateTime::now_utc();
  let store = Arc::new(Store::new(Box::<MockStorage>::default()));
  let callbacks = Arc::new(MockCallbacks::default());

  store.set(
    &STATE_KEY,
    &ActivitySessionStrategyState {
      session_id: "foo".to_string(),
      last_activity_timestamp: (now - std::time::Duration::from_secs(31)).into_proto(),
      ..Default::default()
    },
  );

  let strategy = activity_based::Strategy::new(
    Duration::seconds(30),
    store.clone(),
    callbacks.clone(),
    Arc::new(TestTimeProvider::new(now)),
  );

  assert_eq!(
    Some("foo".to_string()),
    strategy.previous_process_session_id()
  );

  let session_id = strategy.session_id();

  assert_eq!(1, callbacks.session_id_changes.lock().len());
  assert_eq!(session_id, callbacks.session_id_changes.lock()[0]);
  assert_eq!(
    ActivitySessionStrategyState {
      session_id,
      last_activity_timestamp: now.into_proto(),
      ..Default::default()
    },
    store.as_ref().get(&STATE_KEY).unwrap(),
  );
  assert_eq!(
    Some("foo".to_string()),
    strategy.previous_process_session_id()
  );
}

#[test]
fn does_not_update_neither_session_nor_last_activity_if_within_max_write_interval() {
  let now = OffsetDateTime::now_utc();
  let store = Arc::new(Store::new(Box::<MockStorage>::default()));
  let time_provider = Arc::new(TestTimeProvider::new(now));
  let callbacks = Arc::new(MockCallbacks::default());

  let strategy = activity_based::Strategy::new(
    Duration::seconds(30),
    store.clone(),
    callbacks.clone(),
    time_provider.clone(),
  );

  let session_id = strategy.session_id();

  callbacks.clear();

  time_provider.advance(time::Duration::seconds(3));

  let second_session_id = strategy.session_id();

  assert_eq!(session_id, second_session_id);
  assert!(callbacks.session_id_changes.lock().is_empty());
  assert_eq!(
    ActivitySessionStrategyState {
      session_id,
      last_activity_timestamp: now.into_proto(),
      ..Default::default()
    },
    store.as_ref().get(&STATE_KEY).unwrap()
  );
}

#[test]
fn updates_only_last_activity_date_if_after_max_write_interval() {
  let now = OffsetDateTime::now_utc();
  let store = Arc::new(Store::new(Box::<MockStorage>::default()));
  let time_provider = Arc::new(TestTimeProvider::new(now));
  let callbacks = Arc::new(MockCallbacks::default());

  let strategy = activity_based::Strategy::new(
    Duration::seconds(30),
    store.clone(),
    callbacks.clone(),
    time_provider.clone(),
  );

  let session_id = strategy.session_id();

  callbacks.clear();

  let advanced_time = now + std::time::Duration::from_secs(20);
  time_provider.set_time(advanced_time);

  let second_session_id = strategy.session_id();

  assert_eq!(session_id, second_session_id);
  assert!(callbacks.session_id_changes.lock().is_empty());
  assert_eq!(
    ActivitySessionStrategyState {
      session_id,
      last_activity_timestamp: advanced_time.into_proto(),
      ..Default::default()
    },
    store.as_ref().get(&STATE_KEY).unwrap(),
  );
}

#[test]
fn updates_session_and_last_activity_after_inactivity_threshold_is_exceeded() {
  let now = OffsetDateTime::now_utc();
  let store = Arc::new(Store::new(Box::<MockStorage>::default()));
  let time_provider = Arc::new(TestTimeProvider::new(now));
  let callbacks = Arc::new(MockCallbacks::default());

  let strategy = activity_based::Strategy::new(
    Duration::seconds(30),
    store.clone(),
    callbacks.clone(),
    time_provider.clone(),
  );

  let session_id = strategy.session_id();

  callbacks.clear();

  let advanced_time = now + std::time::Duration::from_secs(31);
  time_provider.set_time(advanced_time);

  let second_session_id = strategy.session_id();

  assert_eq!(1, callbacks.session_id_changes.lock().len());
  assert_ne!(session_id, second_session_id);
  assert_eq!(
    ActivitySessionStrategyState {
      session_id: second_session_id,
      last_activity_timestamp: advanced_time.into_proto(),
      ..Default::default()
    },
    store.as_ref().get(&STATE_KEY).unwrap(),
  );
  assert_eq!(None, strategy.previous_process_session_id());
}

#[test]
fn refreshes_session_and_last_activity_after_reboot() {
  let now = OffsetDateTime::now_utc();
  let store = Arc::new(Store::new(Box::<MockStorage>::default()));
  let time_provider = Arc::new(TestTimeProvider::new(now));
  let callbacks = Arc::new(MockCallbacks::default());

  store.set(
    &STATE_KEY,
    &ActivitySessionStrategyState {
      session_id: "foo".to_string(),
      last_activity_timestamp: now.into_proto(),
      ..Default::default()
    },
  );

  let strategy = activity_based::Strategy::new(
    Duration::seconds(30),
    store.clone(),
    callbacks.clone(),
    time_provider.clone(),
  );

  assert_eq!(
    Some("foo".to_string()),
    strategy.previous_process_session_id()
  );

  let past_time = now - 5.seconds();
  time_provider.set_time(past_time);

  let session_id = strategy.session_id();

  assert_eq!(1, callbacks.session_id_changes.lock().len());
  assert_ne!("foo", session_id);
  assert_eq!(
    ActivitySessionStrategyState {
      session_id,
      last_activity_timestamp: past_time.into_proto(),
      ..Default::default()
    },
    store.as_ref().get(&STATE_KEY).unwrap(),
  );
  assert_eq!(
    Some("foo".to_string()),
    strategy.previous_process_session_id()
  );
}

#[test]
fn starts_new_session() {
  let now = OffsetDateTime::now_utc();
  let store = Arc::new(Store::new(Box::<MockStorage>::default()));
  let time_provider = Arc::new(TestTimeProvider::new(now));
  let callbacks = Arc::new(MockCallbacks::default());

  let strategy = activity_based::Strategy::new(
    Duration::seconds(30),
    store.clone(),
    callbacks.clone(),
    time_provider.clone(),
  );

  let session_id = strategy.session_id();
  callbacks.clear();

  let advanced_time = now + 1.seconds();
  time_provider.set_time(advanced_time);

  strategy.start_new_session();

  let next_session_id = strategy.session_id();

  assert_ne!(session_id, next_session_id);
  assert!(callbacks.session_id_changes.lock().is_empty());
  assert_eq!(
    ActivitySessionStrategyState {
      session_id: next_session_id,
      last_activity_timestamp: advanced_time.into_proto(),
      ..Default::default()
    },
    store.as_ref().get(&STATE_KEY).unwrap(),
  );
  assert!(strategy.previous_process_session_id().is_none());
}

#[test]
fn previous_session_id() {
  let now = OffsetDateTime::now_utc();
  let store = Arc::new(Store::new(Box::<MockStorage>::default()));
  let time_provider = Arc::new(TestTimeProvider::new(now));
  let callbacks = Arc::new(MockCallbacks::default());

  let strategy = activity_based::Strategy::new(
    Duration::seconds(30),
    store.clone(),
    callbacks,
    time_provider,
  );

  store.set(
    &STATE_KEY,
    &ActivitySessionStrategyState {
      session_id: "foo".to_string(),
      last_activity_timestamp: now.into_proto(),
      ..Default::default()
    },
  );

  assert_eq!(
    Some("foo".to_string()),
    strategy.previous_process_session_id()
  );

  strategy.start_new_session();

  assert_eq!(
    Some("foo".to_string()),
    strategy.previous_process_session_id()
  );

  strategy.session_id();

  assert_eq!(
    Some("foo".to_string()),
    strategy.previous_process_session_id()
  );
}

#[test]
fn flushes_state() {
  let now = OffsetDateTime::now_utc();
  let store = Arc::new(Store::new(Box::<MockStorage>::default()));
  let time_provider = Arc::new(TestTimeProvider::new(now));
  let callbacks = Arc::new(MockCallbacks::default());

  let strategy = activity_based::Strategy::new(
    Duration::seconds(30),
    store.clone(),
    callbacks,
    time_provider.clone(),
  );

  let session_id = strategy.session_id();

  let advanced_time = now + 1.seconds();
  time_provider.set_time(advanced_time);

  let next_session_id = strategy.session_id();

  strategy.flush();

  assert_eq!(session_id, next_session_id);
  assert_eq!(
    ActivitySessionStrategyState {
      session_id: next_session_id,
      last_activity_timestamp: advanced_time.into_proto(),
      ..Default::default()
    },
    store.as_ref().get(&STATE_KEY).unwrap(),
  );
}
