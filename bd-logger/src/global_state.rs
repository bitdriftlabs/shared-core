use bd_device::Store;
use bd_key_value::{Key, Storable};
use bd_log_primitives::{LogField, LogFields};
use std::collections::HashMap;
use std::sync::Arc;

const KEY: Key<State> = Key::new("global_state");

#[derive(Debug, serde::Serialize, serde::Deserialize, Default, PartialEq, Eq)]
struct State(HashMap<String, String>);

impl Storable for State {}

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

    log::error!("global_state: {:?}", global_state);

    Self {
      store,
      current_global_state: global_state,
    }
  }

  pub fn maybe_update_global_state(&mut self, new_global_state: &[LogField]) {
    let state = State(
      new_global_state
        .iter()
        .filter_map(|field| Some((field.key.clone(), field.value.as_str()?.to_string())))
        .collect(),
    );

    if state == self.current_global_state {
      return;
    }

    self.current_global_state = state;
    self.store.set(&KEY, &self.current_global_state);
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
