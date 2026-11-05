// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

#[cfg(test)]
#[path = "./labels_test.rs"]
mod tests;

use bd_client_stats_store::Scope;
use bd_key_value::{Key, Store};
use bd_metadata::labels::{self, LABELS_FIELD, Labels};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;

const LABELS_KEY: Key<String> = Key::new("application_labels.1");

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
  pub(crate) initial: Labels,
  pub(crate) desired: Mutex<Labels>,
  pub(crate) updates: watch::Sender<Labels>,
  pub(crate) origin: watch::Sender<HashMap<String, String>>,
  store: Arc<Store>,
  scope: Scope,
}

impl LabelsState {
  pub(crate) fn new(
    initial: InitialLabels,
    store: Arc<Store>,
    scope: Scope,
    mut origin: HashMap<String, String>,
  ) -> anyhow::Result<Self> {
    let labels = match initial {
      InitialLabels::UseExisting => store
        .get_internal_string(&LABELS_KEY)?
        .map(|value| labels::decode(&value))
        .transpose()?
        .unwrap_or_default(),
      InitialLabels::None => Labels::new(),
      InitialLabels::Set(labels) => labels,
    };
    labels::validate(&labels)?;
    let encoded = labels::encode(&labels)?;
    store.set_internal_string(&LABELS_KEY, &encoded)?;
    origin.insert(LABELS_FIELD.to_string(), encoded);
    let (origin, _) = watch::channel(origin);
    let (updates, _) = watch::channel(labels.clone());
    Ok(Self {
      initial: labels.clone(),
      desired: Mutex::new(labels),
      updates,
      origin,
      store,
      scope,
    })
  }

  pub(crate) fn apply(&self, labels: &Labels) -> anyhow::Result<String> {
    let encoded = labels::encode(labels)?;
    self.scope.set_product_origin(LABELS_FIELD, encoded.clone());
    self.origin.send_modify(|origin| {
      origin.insert(LABELS_FIELD.to_string(), encoded.clone());
    });
    log::debug!("applied application labels: count={}", labels.len());
    Ok(encoded)
  }

  pub(crate) fn persist(&self, labels: &Labels) -> anyhow::Result<String> {
    let encoded = labels::encode(labels)?;
    self.store.set_internal_string(&LABELS_KEY, &encoded)?;
    Ok(encoded)
  }
}
