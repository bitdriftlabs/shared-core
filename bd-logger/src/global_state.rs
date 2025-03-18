// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./global_state_test.rs"]
mod tests;

use bd_device::Store;
use bd_key_value::Key;
use bd_log_primitives::{LogFields, StringOrBytes};
use bd_proto::protos::client::api::GlobalState;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_proto::protos::logging::payload::{BinaryData, Data};
use std::sync::Arc;

const KEY: Key<Vec<u8>> = Key::new("global_state");

#[derive(Debug, serde::Serialize, serde::Deserialize, Default, PartialEq, Eq)]
struct State {
  fields: LogFields,
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
    let global_state: GlobalState = store.get_proto(&KEY).unwrap_or_default();

    Self {
      store,
      current_global_state: State {
        fields: global_state
          .state
          .into_iter()
          .map(|(key, value)| {
            (
              key.into(),
              match value.data_type.unwrap() {
                Data_type::StringData(s) => s.into(),
                Data_type::BinaryData(binary_data) => binary_data.payload.into(),
              },
            )
          })
          .collect(),
      },
    }
  }

  pub fn maybe_update_global_state(&mut self, new_global_state: &LogFields) -> bool {
    if self.current_global_state.fields == *new_global_state {
      return false;
    }

    self.current_global_state.fields = new_global_state.clone();
    self.store.set_proto(
      &KEY,
      &GlobalState {
        state: new_global_state
          .iter()
          .map(|(key, value)| {
            (
              key.clone().into(),
              Data {
                data_type: Some(match value {
                  StringOrBytes::String(s) => Data_type::StringData(s.clone().into()),
                  StringOrBytes::SharedString(s) => Data_type::StringData(s.as_str().to_string()),
                  StringOrBytes::Bytes(b) => Data_type::BinaryData(BinaryData {
                    payload: b.clone(),
                    ..Default::default()
                  }),
                }),
                ..Default::default()
              },
            )
          })
          .collect(),
        ..Default::default()
      },
    );

    true
  }

  pub fn global_state_fields(&self) -> LogFields {
    self.current_global_state.fields.clone()
  }
}
