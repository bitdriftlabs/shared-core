// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_log_metadata::{LogFields, MetadataProvider};
use parking_lot::Mutex;

pub struct LogMetadata {
  pub timestamp: Mutex<time::OffsetDateTime>,
  pub custom_fields: LogFields,
  pub ootb_fields: LogFields,
}

impl MetadataProvider for LogMetadata {
  fn timestamp(&self) -> anyhow::Result<time::OffsetDateTime> {
    Ok(*self.timestamp.lock())
  }

  fn fields(&self) -> anyhow::Result<(LogFields, LogFields)> {
    Ok((self.custom_fields.clone(), self.ootb_fields.clone()))
  }
}

impl Default for LogMetadata {
  fn default() -> Self {
    Self {
      timestamp: Mutex::new(time::OffsetDateTime::now_utc()),
      custom_fields: LogFields::default(),
      ootb_fields: LogFields::default(),
    }
  }
}
