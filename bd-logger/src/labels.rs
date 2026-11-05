// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

#[cfg(test)]
#[path = "./labels_test.rs"]
mod tests;

use bd_key_value::{Key, Store};
use bd_metadata::labels::{self, Labels};
use bd_proto::protos::client::api::state_update_request::LabelsUpdate;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::watch;

const LABELS_KEY: Key<LabelsUpdate> = Key::new("application_labels.1");

fn persist_labels(store: &Store, labels: &Labels) -> anyhow::Result<()> {
  labels::validate(labels)?;
  store.set_internal(
    &LABELS_KEY,
    &LabelsUpdate {
      labels: labels
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect(),
      ..Default::default()
    },
  )
}

//
// InitialLabels
//

#[derive(Clone, Debug, Default)]
pub enum InitialLabels {
  #[default]
  UseExisting,
  None,
  Set(Labels),
}

//
// LabelsState
//

pub struct LabelsState {
  pub(crate) updates: watch::Sender<Labels>,
  store: Arc<Store>,
  write_lock: Mutex<()>,
}

impl LabelsState {
  pub(crate) fn new(initial: InitialLabels, store: Arc<Store>) -> anyhow::Result<Self> {
    let labels = if matches!(initial, InitialLabels::UseExisting) {
      let labels = store
        .get_internal(&LABELS_KEY)?
        .map_or_default(|value| value.labels.into_iter().collect());
      labels::validate(&labels)?;
      labels
    } else {
      let labels = match initial {
        InitialLabels::Set(labels) => labels,
        _ => Labels::new(),
      };
      persist_labels(&store, &labels)?;
      labels
    };
    Ok(Self {
      updates: watch::Sender::new(labels),
      store,
      write_lock: Mutex::new(()),
    })
  }

  pub(crate) fn update(&self, update: impl FnOnce(&mut Labels)) -> anyhow::Result<()> {
    // Serialize read/modify/persist/publish without holding the watch lock during disk I/O.
    let _guard = self.write_lock.lock();
    let mut next = self.updates.borrow().clone();
    update(&mut next);
    if *self.updates.borrow() == next {
      return Ok(());
    }
    persist_labels(&self.store, &next)?;
    log::debug!("updated application labels: count={}", next.len());
    self.updates.send_replace(next);
    Ok(())
  }
}
