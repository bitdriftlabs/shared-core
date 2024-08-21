// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_log_metadata::{AnnotatedLogFields, MetadataProvider};

pub struct LogMetadata {
  pub timestamp: time::OffsetDateTime,
  pub fields: AnnotatedLogFields,
}

impl MetadataProvider for LogMetadata {
  fn timestamp(&self) -> anyhow::Result<time::OffsetDateTime> {
    Ok(self.timestamp)
  }

  fn fields(&self) -> anyhow::Result<AnnotatedLogFields> {
    Ok(self.fields.clone())
  }
}
