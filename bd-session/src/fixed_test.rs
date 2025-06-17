// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{Callbacks, State, UUIDCallbacks};
use crate::fixed::{self, STATE_KEY};
use bd_key_value::{Storage, Store};
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

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

#[derive(Default)]
struct MockCallbacks {
  generated_session_ids: parking_lot::Mutex<Vec<String>>,
}

impl Callbacks for MockCallbacks {
  fn generate_session_id(&self) -> anyhow::Result<String> {
    let id = Uuid::new_v4().to_string();
    self.generated_session_ids.lock().push(id.clone());
    Ok(id)
  }
}

#[test]
fn test_session_id() {
  let store = Arc::new(Store::new(Box::<MockStorage>::default()));
  let callbacks = Arc::new(MockCallbacks::default());
  let strategy = fixed::Strategy::new(store.clone(), callbacks.clone());

  store.set(
    &STATE_KEY,
    &State {
      session_id: "foo".to_string(),
    },
  );

  let session_id = strategy.session_id();
  let previous_session_id = strategy.previous_process_session_id();

  assert_eq!(1, callbacks.generated_session_ids.lock().len());
  assert_eq!(callbacks.generated_session_ids.lock()[0], session_id);
  assert_eq!(Some("foo".to_string()), previous_session_id);
}

#[test]
fn test_start_new_session() {
  let store = Arc::new(Store::new(Box::<MockStorage>::default()));
  let callbacks = Arc::new(MockCallbacks::default());
  let strategy = fixed::Strategy::new(store.clone(), callbacks.clone());

  store.set(
    &STATE_KEY,
    &State {
      session_id: "foo".to_string(),
    },
  );

  let session_id = strategy.session_id();

  assert_eq!(1, callbacks.generated_session_ids.lock().len());
  assert_eq!(callbacks.generated_session_ids.lock()[0], session_id);

  let next_session_id = strategy.start_new_session().unwrap();

  assert_eq!(next_session_id, strategy.session_id());
  assert_eq!(2, callbacks.generated_session_ids.lock().len());
  assert_eq!(callbacks.generated_session_ids.lock()[1], next_session_id);
  assert_eq!(
    Some("foo".to_string()),
    strategy.previous_process_session_id()
  );
}

#[test]
fn test_previous_process_session_id() {
  let store = Arc::new(Store::new(Box::<MockStorage>::default()));
  let strategy = fixed::Strategy::new(store.clone(), Arc::new(UUIDCallbacks));

  store.set(
    &STATE_KEY,
    &State {
      session_id: "foo".to_string(),
    },
  );

  assert_eq!(
    Some("foo".to_string()),
    strategy.previous_process_session_id()
  );

  strategy.start_new_session().unwrap();
  let session_id = strategy.session_id();

  assert_eq!(
    Some("foo".to_string()),
    strategy.previous_process_session_id()
  );

  let strategy = fixed::Strategy::new(store, Arc::new(UUIDCallbacks));
  assert_eq!(Some(session_id), strategy.previous_process_session_id());
}

#[derive(Default)]
struct ReEntryCallbacks {
  session_strategy: parking_lot::Mutex<Option<Arc<fixed::Strategy>>>,
}

impl Callbacks for ReEntryCallbacks {
  fn generate_session_id(&self) -> anyhow::Result<String> {
    if let Some(strategy) = &*self.session_strategy.lock() {
      return strategy.start_new_session();
    }

    Ok("should not happen".to_string())
  }
}

#[test]
fn handles_re_entry() {
  let store = Arc::new(Store::new(Box::<MockStorage>::default()));

  let callbacks = Arc::new(ReEntryCallbacks::default());
  let strategy = Arc::new(fixed::Strategy::new(store, callbacks.clone()));

  callbacks.session_strategy.lock().replace(strategy.clone());

  // Confirm that it doesn't deadlock and returns a reasonable ID.
  let session_id = strategy.session_id();
  assert_eq!(36, session_id.len());

  // Confirm that it doesn't deadlock and returns a reasonable ID.
  let new_session_id = strategy.start_new_session().unwrap();
  assert_eq!(36, new_session_id.len());

  assert_ne!(session_id, new_session_id);
}
