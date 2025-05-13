// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_log_primitives::{FieldsRef, LogRef, LOG_FIELD_NAME_LEVEL, LOG_FIELD_NAME_TYPE};
use std::borrow::Cow;

pub mod buffer_selector;
pub mod matcher;

#[derive(thiserror::Error, Debug)]
pub enum Error {
  #[error("Invalid regex: {0}")]
  Regex(#[from] regex_lite::Error),

  #[error("Invalid matcher configuration")]
  InvalidConfig,

  #[error("Configuration has unknown fields or enum")]
  UnknownFieldOrEnum,
}

type Result<T> = std::result::Result<T, Error>;

//
// FieldProvider
//

/// Used to look up individual fields.
pub trait FieldProvider {
  /// Looks up the field value corresponding to the provided key. If the field doesn't exist or
  /// contains a binary value, None is returned.
  fn field_value(&self, key: &str) -> Option<Cow<'_, str>>;
}

impl FieldProvider for FieldsRef<'_> {
  fn field_value(&self, field_key: &str) -> Option<Cow<'_, str>> {
    // In cases where there are conflicts between the keys of captured and matching fields, captured
    // fields take precedence, as they are potentially stored and uploaded to the remote server.
    if let Some(value) = self
      .captured_fields
      .get(field_key)
      .and_then(|value| value.as_str())
    {
      return Some(Cow::Borrowed(value));
    }

    self.matching_field_value(field_key).map(Cow::Borrowed)
  }
}

impl FieldProvider for LogRef<'_> {
  fn field_value(&self, key: &str) -> Option<Cow<'_, str>> {
    match key {
      LOG_FIELD_NAME_LEVEL => Some(Cow::Owned(self.log_level.to_string())),
      LOG_FIELD_NAME_TYPE => Some(Cow::Owned(self.log_type.0.to_string())),
      key => self.fields.field_value(key),
    }
  }
}
