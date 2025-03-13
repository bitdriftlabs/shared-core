#[cfg(test)]
#[path = "./global_state_test.rs"]
mod tests;

use bd_device::Store;
use bd_key_value::{Key, Storable};
use bd_log_primitives::{LogField, LogFields};
use std::sync::Arc;

const KEY: Key<State> = Key::new("global_state");

#[derive(Debug, serde::Serialize, serde::Deserialize, Default, PartialEq, Eq)]
struct State(Vec<(String, String)>);

impl Storable for State {}

impl PartialEq<[LogField]> for State {
  fn eq(&self, other: &[LogField]) -> bool {
    // We don't persist binary fields since we don't really need them for ootb support, so filter
    // out binary fields when we make the comparison.

    for (k, v) in other
      .iter()
      .filter_map(|field| Some((field.key.as_str(), field.value.as_str()?)))
    {
      if self.0.iter().all(|(key, value)| k != *key || v != value) {
        return false;
      }
    }

    for (key, value) in &self.0 {
      if !other
        .iter()
        .filter_map(|f| Some((f.key.as_str(), f.value.as_str()?)))
        .any(|(k, v)| *key == *k && value == v)
      {
        return false;
      }
    }

    true
  }
}

//
// GlobalStateTracker
//

pub struct Tracker {
  store: Arc<Store>,
  current_global_state: State,
}

impl Tracker {
  pub fn new(store: Arc<Store>) -> Self {
    let global_state = store.get(&KEY).unwrap_or_default();

    Self {
      store,
      current_global_state: global_state,
    }
  }

  pub fn maybe_update_global_state(&mut self, new_global_state: &[LogField]) -> bool {
    if self.current_global_state == *new_global_state {
      return false;
    }

    let state = State(
      new_global_state
        .iter()
        .filter_map(|field| Some((field.key.clone(), field.value.as_str()?.to_string())))
        .collect(),
    );

    self.current_global_state = state;
    self.store.set(&KEY, &self.current_global_state);

    true
  }

  pub fn global_state_fields(&self) -> LogFields {
    self
      .current_global_state
      .0
      .clone()
      .into_iter()
      .map(|(key, value)| LogField {
        key,
        value: value.into(),
      })
      .collect()
  }
}
