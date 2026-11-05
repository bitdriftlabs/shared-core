// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

#[cfg(test)]
#[path = "./labels_test.rs"]
mod tests;

use anyhow::bail;
use std::collections::BTreeMap;

pub const LABELS_FIELD: &str = "__bd_labels";
pub const ENVIRONMENT_FIELD: &str = "__bd_env";
pub const MAX_LABELS: usize = 5;
pub type Labels = BTreeMap<String, String>;

pub fn validate(labels: &Labels) -> anyhow::Result<()> {
  if labels.len() > MAX_LABELS {
    bail!("at most {MAX_LABELS} labels are permitted");
  }
  let mut total = 0;
  for (key, value) in labels {
    if key.is_empty() || key.len() > 64 || key.starts_with("__bd_") {
      bail!("label keys must be 1-64 UTF-8 bytes and cannot start with __bd_");
    }
    if value.len() > 256 {
      bail!("label values cannot exceed 256 UTF-8 bytes");
    }
    total += key.len() + value.len();
  }
  if total > 8192 {
    bail!("labels cannot exceed 8 KiB");
  }
  Ok(())
}

pub fn encode(labels: &Labels) -> anyhow::Result<String> {
  validate(labels)?;
  Ok(serde_json::to_string(labels)?)
}

pub fn decode(value: &str) -> anyhow::Result<Labels> {
  // JSON escaping can increase the bounded decoded size by up to six times.
  if value.len() > 6 * 8192 {
    bail!("encoded labels exceed the size limit");
  }
  let labels = serde_json::from_str(value)?;
  validate(&labels)?;
  Ok(labels)
}
